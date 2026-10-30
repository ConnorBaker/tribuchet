//! Build sandbox.
//!
//! Linux: user/mount/ipc/uts (and, unless fixed-output, net) namespaces;
//! input paths bind-mounted read-only at their store paths inside a
//! private root, scratch outputs created in a writable store dir, the
//! shipped tmp dir mounted at /build, minimal /dev, fresh /proc, chroot.
//! Reference: `nix/src/libstore/unix/build/derivation-builder.cc`.
//!
//! macOS: builds are not run by the worker at all. Each one is leased
//! to a per-uid launchd agent that applies a seatbelt profile (see
//! worker/agent.rs and worker/agents.rs). The pieces here that stay in
//! use on macOS are the spec type (which the shared output-packing
//! code reads) and the exit/cleanup shims.

use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::{Child, Command, Stdio};

#[cfg(target_os = "linux")]
use rustix::fs::{Mode, OFlags, open};
#[cfg(target_os = "linux")]
use rustix::io::{Errno, write};
#[cfg(target_os = "linux")]
use rustix::pipe::{PipeFlags, pipe_with};

#[cfg(target_os = "linux")]
use super::binfmt;
use crate::build_json;
#[cfg(target_os = "linux")]
use crate::fsutil::io_ctx;
use crate::netpolicy::NetPolicy;
use crate::proto::BuildAssignment;

// On macOS only the fields the shared packing code reads are used;
// the rest exists so a Linux worker's persisted spec deserializes.
#[cfg_attr(target_os = "macos", allow(dead_code))]
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SandboxSpec {
    pub builder: String,
    /// Derivation system, e.g. "i686-linux" (drives Linux personality).
    pub system: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: String,
    /// Fixed-output build: a private netns with the presto-pasta
    /// user-mode NAT, as every build has a netns of its own; host
    /// abstract sockets and loopback services stay unreachable.
    pub network: bool,
    /// Linux: private root directory. macOS: unused.
    pub root: PathBuf,
    /// Host build dir mounted/linked at tmpDirInSandbox ("/build").
    pub build_dir: PathBuf,
    /// (host source, absolute path inside sandbox), mounted read-only.
    pub binds_ro: Vec<(PathBuf, PathBuf)>,
    /// Device nodes the sandbox itself provides; the only binds that
    /// stay writable. Never derived from request-supplied paths.
    pub binds_dev: Vec<(PathBuf, PathBuf)>,
    /// Scratch output store paths.
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    pub outputs: Vec<String>,
    /// Input store paths visible to the builder, temp-rooted again when
    /// a restarted worker adopts the build.
    #[serde(default)]
    pub store_inputs: Vec<String>,
    /// Store objects that are themselves symlinks: (path inside the
    /// sandbox, link target). Bind-mounting such a path would silently
    /// dereference it, so they are recreated as symlinks instead.
    #[serde(default)]
    pub symlink_inputs: Vec<(PathBuf, PathBuf)>,
    /// Per-build cgroup the agent moved the setup stage into; the
    /// sandbox roots its cgroup namespace there.
    pub cgroup: Option<PathBuf>,
    /// Path to the agent's pre-mapped user namespace; the setup stage
    /// joins it instead of writing uid maps itself.
    #[serde(default)]
    pub leased_userns: Option<PathBuf>,
    /// Uids mapped in that namespace at in-ns 0 (1 or 65536).
    #[serde(default)]
    pub leased_uid_count: Option<u32>,
    /// Its network is a namespace of its own; set with `network`
    /// (prepare). A spec with network but without it, as older workers sent
    /// on a host without /dev/net/tun, is unconfined.
    #[serde(default)]
    pub net_isolation: bool,
    /// Flow policy applied to that network.
    #[serde(default)]
    pub net_policy: NetPolicy,
    /// Fd number of the tap-handoff socketpair end inherited from the
    /// agent, which runs the presto-pasta forwarder for the netns.
    #[serde(default)]
    pub net_fwd_fd: Option<i32>,
    /// Whether that network gives the guest IPv6. Set by the agent from
    /// its own routes and addresses, which its datapath's host sockets
    /// use (presto_pasta's `netdev::host_has_ipv6_default_route`). No
    /// IPv6 without a default route the agent can use is Tribuchet's
    /// choice, not pasta's: a guest IPv6 route would only make the guest
    /// try IPv6 first (RFC 6724 rule 2) and wait out UDP timeouts there.
    #[serde(default)]
    pub net_ipv6: bool,
    /// Static emulator binary for foreign-system builds, bound at
    /// binfmt::INTERP_PATH and registered in a per-userns binfmt_misc
    /// instance.
    pub emulator: Option<PathBuf>,
    /// Secret files the build must never read (the worker TLS
    /// key). Defense in depth, the mount namespace already hides them.
    pub deny_read: Vec<PathBuf>,
}

impl SandboxSpec {
    /// Why a build with this spec may not run, if it may not: with
    /// network but in the host's network, or with the host's nix-daemon
    /// socket bound in (recursive Nix, which workers no longer serve). No
    /// spec this worker sends is either, but one an older
    /// worker persisted can still describe a running build: agents
    /// outlive an upgrade.
    pub fn unconfined(&self) -> Option<&'static str> {
        if self.network && !self.net_isolation {
            Some("with network in the host's network")
        } else if (self.binds_ro.iter()).any(|(_, to)| to.starts_with("/nix/var/nix/daemon-socket"))
        {
            Some("with the host's nix-daemon socket")
        } else {
            None
        }
    }
}

/// Host path where the builder's output for `scratch` lands.
pub fn output_host_path(spec: &SandboxSpec, scratch: &str) -> PathBuf {
    if cfg!(target_os = "linux") {
        spec.root.join(scratch.trim_start_matches('/'))
    } else {
        PathBuf::from(scratch)
    }
}

/// Worker-side sandbox configuration for one build.
#[cfg(target_os = "linux")]
#[derive(Default)]
pub struct PrepareOpts<'a> {
    pub bin_sh: Option<&'a Path>,
    pub secrets: &'a [PathBuf],
    /// The agent's pre-mapped user namespace. The worker leaves it
    /// unset; the agent fills it in before spawning the setup stage.
    pub leased_userns: Option<PathBuf>,
    /// Uids the build needs mapped (1 or 65536).
    pub leased_uid_count: Option<u32>,
    pub emulator: Option<&'a Path>,
    /// Flow policy applied to a build's network.
    pub net_policy: NetPolicy,
}

/// Whether the build `a` gets network: its derivation is fixed-output by
/// its own env, structured attrs (in `build_dir`) and output path, as
/// attach tells it (build_json::fixed_output); not by the assignment's
/// `fixed_output`, which any peer of the hub's socket can set.
pub fn network(a: &BuildAssignment, build_dir: &Path) -> bool {
    let env = |name: &str| a.env.get(name).map(String::as_str);
    let attrs = build_json::read_attrs(build_dir, a.env.contains_key("NIX_ATTRS_JSON_FILE"));
    build_json::fixed_output(&a.store_dir, &a.outputs, env, attrs.as_ref())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    #[error("{step}")]
    Step {
        step: String,
        #[source]
        source: io::Error,
    },
    #[cfg(target_os = "linux")]
    #[error("sending sandbox spec")]
    SendSpec(#[source] serde_json::Error),
    #[cfg(target_os = "linux")]
    #[error("no binfmt magic known for system {0}")]
    UnknownBinfmt(String),
}

#[cfg(target_os = "linux")]
fn step<E: Into<io::Error>>(step: impl Into<String>) -> impl FnOnce(E) -> Error {
    |source| Error::Step {
        step: step.into(),
        source: source.into(),
    }
}

#[cfg(target_os = "linux")]
pub fn prepare(
    a: &BuildAssignment,
    dir: &Path,
    inputs: &[String],
    opts: &PrepareOpts,
) -> Result<SandboxSpec, Error> {
    let build_dir = dir.join("top").join("build");
    fs::create_dir_all(&build_dir).map_err(io_ctx("creating", &build_dir))?;
    let network = network(a, &build_dir);
    let mut spec = SandboxSpec {
        builder: a.builder.clone(),
        system: a.system.clone(),
        args: a.args.clone(),
        env: a.env.clone().into_iter().collect(),
        cwd: a.tmp_dir_in_sandbox.clone(),
        network,
        root: dir.join("root"),
        build_dir,
        // inputs live at their real store paths (daemon import)
        binds_ro: inputs
            .iter()
            .filter(|p| !Path::new(p).is_symlink())
            .map(|p| (PathBuf::from(p), PathBuf::from(p)))
            .collect(),
        symlink_inputs: inputs
            .iter()
            .filter(|p| Path::new(p).is_symlink())
            .filter_map(|p| Some((PathBuf::from(p), fs::read_link(p).ok()?)))
            .collect(),
        binds_dev: Vec::new(),
        outputs: a.outputs.values().cloned().collect(),
        store_inputs: inputs.to_vec(),
        cgroup: None,
        leased_userns: opts.leased_userns.clone(),
        leased_uid_count: opts.leased_uid_count,
        net_isolation: network,
        net_policy: opts.net_policy.clone(),
        // filled in by the agent, like the namespace and cgroup
        net_fwd_fd: None,
        net_ipv6: false,
        emulator: opts.emulator.map(Path::to_path_buf),
        deny_read: opts.secrets.to_vec(),
    };
    if let Some(em) = opts.emulator {
        spec.binds_ro
            .push((em.to_owned(), PathBuf::from(binfmt::INTERP_PATH)));
    }
    if let Some(sh) = opts.bin_sh {
        // Like Nix's busybox sandbox path: shebangs and system(3)
        // need a shell at /bin/sh.
        spec.binds_ro
            .push((sh.to_owned(), PathBuf::from("/bin/sh")));
    }
    spec.binds_ro.sort(); // deterministic mount order
    Ok(spec)
}

/// The prepared sandbox as a spawnable command plus the write end of
/// the pipe carrying the serialized spec to the setup stage's stdin.
#[cfg(target_os = "linux")]
fn build_command(spec: &SandboxSpec) -> Result<(Command, Option<OwnedFd>), Error> {
    let mut cmd = platform::command(spec)?;
    // Own process group, so orphaned builder children can be killed
    // after the builder exits.
    CommandExt::process_group(&mut cmd, 0);
    unprotect_from_oom(&mut cmd);
    // The derivation env must not reach the pre-sandbox setup stage
    // (worker binary, worker host credentials): LD_PRELOAD would run
    // client code outside the sandbox. With the spec on stdin the env
    // is applied at the builder exec instead.
    cmd.env_clear();
    if !platform::SPEC_VIA_STDIN {
        cmd.envs(&spec.env);
    }
    // O_CLOEXEC: a write end inherited by a concurrently spawned
    // sibling build would keep the spec read from ever seeing EOF.
    if platform::SPEC_VIA_STDIN {
        let (r, w) = pipe_with(PipeFlags::CLOEXEC).map_err(step("creating spec pipe"))?;
        cmd.stdin(Stdio::from(fs::File::from(r)));
        return Ok((cmd, Some(w)));
    }
    cmd.stdin(Stdio::null());
    Ok((cmd, None))
}

/// Start a build at oom_score_adj 0: the agent runs at -1000, which
/// its children inherit along with that floor. Written while the child
/// still holds the agent's CAP_SYS_RESOURCE, 0 resets the floor as well,
/// so the build cannot lower its score again. EACCES means the floor is
/// already above 0: nothing to undo.
#[cfg(target_os = "linux")]
pub fn unprotect_from_oom(cmd: &mut Command) {
    // SAFETY: open, write and close are async-signal-safe.
    unsafe {
        cmd.pre_exec(|| {
            let fd = open(
                c"/proc/self/oom_score_adj",
                OFlags::WRONLY | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            match write(&fd, b"0") {
                Ok(_) | Err(Errno::ACCESS) => Ok(()),
                Err(e) => Err(e.into()),
            }
        });
    }
}

/// Spawn the sandboxed build with stdout/stderr on `log`. The build
/// is a direct child of the worker, but its state (log, exit status)
/// is persisted on disk so it survives a worker restart. The returned
/// write end must be filled with `send_spec_to` once the spec is
/// complete (the setup stage blocks on stdin until then).
#[cfg(target_os = "linux")]
pub fn spawn(spec: &SandboxSpec, log: &fs::File) -> Result<(Child, Option<OwnedFd>), Error> {
    let (mut cmd, spec_w) = build_command(spec)?;
    cmd.stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log.try_clone()?));
    let child = cmd
        .spawn()
        .map_err(step(format!("spawning builder {}", spec.builder)))?;
    Ok((child, spec_w))
}

/// Write the serialized spec into the setup stage's stdin pipe. Call
/// after the lease filled in the cgroup.
#[cfg(target_os = "linux")]
pub fn send_spec_to(spec: &SandboxSpec, w: OwnedFd) -> Result<(), Error> {
    serde_json::to_writer(fs::File::from(w), spec).map_err(Error::SendSpec)
}

#[cfg(target_os = "linux")]
pub use platform::{SETUP_STAGE_ARG, net_config, prepare_root, setup_error_detail, setup_stage};

#[cfg(target_os = "linux")]
#[path = "sandbox/linux.rs"]
mod platform;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::build_json::tests::{HASH, INPUT_ADDRESSED, SRI};
    use std::{error, result};
    type Result<T> = result::Result<T, Box<dyn error::Error>>;

    /// Common fields for a /bin/sh test build rooted in `dir`;
    /// tests override what they exercise via struct update syntax.
    fn test_spec(dir: &Path) -> SandboxSpec {
        SandboxSpec {
            builder: "/bin/sh".into(),
            system: "x86_64-linux".into(),
            cwd: "/build".into(),
            root: dir.join("root"),
            build_dir: dir.join("top/build"),
            ..SandboxSpec::default()
        }
    }

    /// What a spec may not run with, whoever wrote it.
    #[test]
    fn network_in_the_hosts_network_or_its_daemon_is_unconfined() {
        let dir = tempfile::tempdir().unwrap();
        let spec = |network, net_isolation, bind: &str| SandboxSpec {
            network,
            net_isolation,
            binds_ro: vec![(PathBuf::from("/x"), PathBuf::from(bind))],
            ..test_spec(dir.path())
        };
        let socket = "/nix/var/nix/daemon-socket/socket";
        assert_eq!(spec(false, false, "/nix/store/x").unconfined(), None);
        assert_eq!(spec(true, true, "/nix/store/x").unconfined(), None);
        let host = Some("with network in the host's network");
        assert_eq!(spec(true, false, "/nix/store/x").unconfined(), host);
        let daemon = Some("with the host's nix-daemon socket");
        assert_eq!(spec(false, false, socket).unconfined(), daemon);
        assert_eq!(spec(true, true, socket).unconfined(), daemon);
    }

    /// The setup stage refuses an unconfined spec before it enters any
    /// namespace: the build never runs in the host's network.
    #[test]
    fn the_setup_stage_refuses_an_unconfined_spec() {
        let dir = tempfile::tempdir().unwrap();
        let spec = SandboxSpec {
            network: true,
            ..test_spec(dir.path())
        };
        let status = fs::File::create(dir.path().join("status")).unwrap();
        let err = platform::stage::enter_and_exec(&spec, &status).unwrap_err();
        assert_eq!(
            err.to_string(),
            "refusing a build with network in the host's network"
        );
    }

    /// A fixed-output build, from its env or structured attrs and its
    /// output path, gets network, in a network of its own; the hub's word
    /// that a build is fixed-output gives none, nor does its absence take it.
    #[test]
    fn a_build_has_network_by_its_derivation_not_the_hubs_word() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let build = |hub: bool, out: &str, env: &[(&str, &str)]| BuildAssignment {
            fixed_output: hub,
            store_dir: "/nix/store".into(),
            outputs: [("out".to_owned(), out.to_owned())].into(),
            env: env
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            ..BuildAssignment::default()
        };
        let network = |a: &BuildAssignment| -> Result<(bool, bool)> {
            let spec = prepare(a, dir.path(), &[], &PrepareOpts::default())?;
            assert!(spec.unconfined().is_none());
            Ok((spec.network, spec.net_isolation))
        };
        let fod = build(false, SRI, &[("outputHash", HASH)]);
        assert_eq!(network(&fod)?, (true, true));
        let claimed = build(true, INPUT_ADDRESSED, &[("outputHash", HASH)]);
        assert_eq!(network(&claimed)?, (false, false));
        let attrs = build(false, SRI, &[("NIX_ATTRS_JSON_FILE", "/build/.attrs.json")]);
        assert_eq!(network(&attrs)?, (false, false));
        let file = dir.path().join("top/build/.attrs.json");
        fs::write(file, format!(r#"{{"outputHash": "{HASH}"}}"#))?;
        assert_eq!(network(&attrs)?, (true, true));
        Ok(())
    }

    /// The derivation env is client-controlled (LD_PRELOAD, …) and must
    /// not be applied to the setup stage, which runs the worker binary
    /// with the worker's host credentials before entering the sandbox.
    #[test]
    fn derivation_env_stays_off_the_setup_stage() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let spec = SandboxSpec {
            env: HashMap::from([("LD_PRELOAD".into(), "/nix/store/evil.so".into())]),
            ..test_spec(dir.path())
        };
        fs::create_dir_all(&spec.build_dir)?;
        let (cmd, _w) = build_command(&spec)?;
        assert_eq!(
            cmd.get_envs().count(),
            0,
            "setup stage env: {:?}",
            cmd.get_envs().collect::<Vec<_>>()
        );
        Ok(())
    }
}
