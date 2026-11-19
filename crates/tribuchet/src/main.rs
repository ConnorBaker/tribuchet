mod attach;
mod build_json;
mod ca;
mod chunker;
mod chunkio;
mod chunkstore;
mod config;
mod errors;
mod fsutil;
mod hub;
mod nar;
mod netpolicy;
mod proto;
mod rt;
mod sd;
mod sockpath;
#[cfg(test)]
mod stalltest;
mod store;
mod tailscale;
mod tmpdir;
mod worker;

use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::env;
#[cfg(test)]
use std::error;
use std::fs;
use std::io::{self, IsTerminal};
use std::num::NonZero;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
#[cfg(test)]
use std::slice;
use std::thread;

use clap::{Parser, Subcommand};
use errors::{Error, err_ctx, err_msg};

/// RBE-style remote build execution for Nix, driven by the
/// `external-builders` experimental feature.
#[derive(Parser)]
#[command(name = "tribuchet", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// External-builders shim invoked by Nix; forwards the build to the hub.
    Attach {
        /// Path to the build.json written by Nix.
        build_json: PathBuf,
        /// Hub unix socket.
        #[arg(long, default_value = "/run/tribuchet/hub.sock")]
        socket: PathBuf,
    },
    /// Scheduler and NAR relay; runs next to nix-daemon.
    Hub {
        /// TOML configuration file.
        #[arg(long, default_value = "/etc/tribuchet/hub.toml")]
        config: PathBuf,
    },
    /// Build worker; dials the hub and executes sandboxed builds.
    Worker {
        /// TOML configuration file; re-read on every reload.
        #[arg(long, default_value = "/etc/tribuchet/worker.toml")]
        config: PathBuf,
    },
    /// Certificate authority management (init CA, issue worker certs).
    Ca {
        #[command(subcommand)]
        action: ca::CaAction,
    },
    /// Per-uid build agent, one per pool user.
    Agent {
        /// Unix socket to bind when not socket-activated.
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Inherited listening socket (worker-spawned agents).
        #[arg(long)]
        listen_fd: Option<i32>,
        /// Directory for per-build scratch dirs.
        #[arg(long)]
        state_dir: PathBuf,
        /// Uid allowed to lease builds (the worker user).
        #[arg(long)]
        worker_uid: Option<u32>,
        /// First uid of the agent's 65536-uid block (Linux).
        #[arg(long)]
        uid_base: Option<u32>,
        /// The agent owns its uid even without socket activation.
        #[arg(long)]
        dedicated_uid: bool,
    },
    /// systemd generator: want one agent socket per CPU.
    AgentGenerator {
        /// Unit the sockets are ordered before and wanted by, e.g. tribuchet-worker.service.
        #[arg(long)]
        worker_unit: String,
        /// Socket template, e.g. tribuchet-agent@.socket.
        #[arg(long)]
        template: String,
        /// Exactly this many agents instead of one per CPU.
        #[arg(long)]
        count: Option<usize>,
        /// Agent i's user and group is <user-prefix>i ...
        #[arg(long)]
        user_prefix: String,
        /// ... with uid and gid <uid-base> - i, below the agents' uid blocks.
        #[arg(long)]
        uid_base: u32,
        /// normal, early and late output directories passed by systemd.
        dirs: Vec<PathBuf>,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("tribuchet: {}", errors::chain(&e));
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Error> {
    // Builds re-exec this binary as the sandbox setup stage; divert
    // before clap and tracing touch anything.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::sandbox::SETUP_STAGE_ARG) {
        worker::sandbox::setup_stage();
    }
    // Same for the build agent's userns filesystem helper.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::agent::FS_HELPER_ARG) {
        worker::agent::fs_helper_stage();
    }
    // And for the agent's userns holder child.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::USERNS_HOLD_ARG) {
        worker::userns_hold_stage();
    }
    // Colour only on a terminal: the hub's journal is read back by
    // anyone grepping it.
    tracing_subscriber::fmt()
        .with_ansi(io::stdout().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Attach { build_json, socket } => attach::run(&build_json, &socket),
        Command::Hub { config } => {
            let cfg: config::HubConfig = config::load(&config)?;
            hub::run(cfg)
        }
        Command::Worker { config } => {
            let mut cfg: config::WorkerConfig = config::load(&config)?;
            cfg.apply_env_overrides();
            tracing::info!(?cfg, "worker configuration");
            worker::run(cfg)
        }
        Command::Ca { action } => Ok(ca::run(action)?),
        Command::Agent {
            socket,
            listen_fd,
            state_dir,
            worker_uid,
            uid_base,
            dedicated_uid,
        } => Ok(worker::agent::run(&worker::agent::Options {
            socket,
            listen_fd,
            state_dir,
            worker_uid,
            uid_base,
            dedicated_uid,
        })?),
        Command::AgentGenerator {
            worker_unit,
            template,
            count,
            user_prefix,
            uid_base,
            dirs,
        } => {
            let n =
                count.unwrap_or_else(|| thread::available_parallelism().map_or(1, NonZero::get));
            agent_units_and_users(
                &worker_unit,
                &template,
                n,
                &dirs,
                Path::new("/run/userdb"),
                &user_prefix,
                uid_base,
            )
            .inspect_err(|e| {
                // systemd logs only a generator's exit status; its
                // messages go to the kernel log (systemd.generator(7))
                let line = format!("<3>tribuchet agent-generator: {}\n", errors::chain(e));
                let _ = fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/kmsg")
                    .and_then(|mut kmsg| io::Write::write_all(&mut kmsg, line.as_bytes()));
            })
        }
    }
}

/// The units first, then the users, each whether or not the other
/// failed: a failed userdb write must not also drop every agent socket.
fn agent_units_and_users(
    worker_unit: &str,
    template: &str,
    n: usize,
    dirs: &[PathBuf],
    userdb: &Path,
    user_prefix: &str,
    uid_base: u32,
) -> Result<(), Error> {
    let units = agent_generator(worker_unit, template, n, dirs);
    let users = agent_users(userdb, user_prefix, uid_base, n);
    units.and(users)
}

/// Each agent's own user and group, as systemd userdb drop-ins
/// (resolved through nss-systemd) instead of DynamicUser=, which forces
/// RestrictSUIDSGID= and so narrows the builds' system calls below
/// Nix's. The uid follows from the name, the same at every start.
fn agent_users(dir: &Path, prefix: &str, uid_base: u32, n: usize) -> Result<(), Error> {
    fs::create_dir_all(dir).map_err(err_ctx(format!("mkdir {}", dir.display())))?;
    // record file -> its contents, and the link for lookups by id -> file
    let mut files = HashMap::new();
    let mut links = HashMap::new();
    for i in 1..=n {
        let name = format!("{prefix}{i}");
        let id = u32::try_from(i)
            .ok()
            .and_then(|i| uid_base.checked_sub(i))
            .ok_or_else(|| err_msg("agent uid below 0"))?;
        let user = serde_json::json!({
            "userName": name, "uid": id, "gid": id, "disposition": "system",
        });
        let group = serde_json::json!({ "groupName": name, "gid": id, "disposition": "system" });
        for (kind, record) in [("user", user), ("group", group)] {
            let file = format!("{name}.{kind}");
            links.insert(format!("{id}.{kind}"), PathBuf::from(&file));
            files.insert(file, record.to_string());
        }
    }
    // Only this configuration's records: drop ours that a larger count
    // or another uid base left earlier in this boot.
    for entry in fs::read_dir(dir).map_err(err_ctx(format!("reading {}", dir.display())))? {
        let path = entry
            .map_err(err_ctx(format!("reading {}", dir.display())))?
            .path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let target = fs::read_link(&path).ok();
        let ours = name.starts_with(prefix)
            || target
                .as_ref()
                .is_some_and(|t| t.to_string_lossy().starts_with(prefix));
        let wanted = match &target {
            Some(t) => links.get(&*name) == Some(t),
            None => files.contains_key(&*name),
        };
        if ours && !wanted {
            fs::remove_file(&path).map_err(err_ctx(format!("removing {name}")))?;
        }
    }
    // Replaced by rename, so a lookup meanwhile reads the old record or
    // the new one. A temp file a failed run left is ours, not wanted,
    // and goes in the next run.
    for (file, record) in &files {
        let tmp = dir.join(format!("{file}.tmp"));
        fs::write(&tmp, record).map_err(err_ctx(format!("writing {file}.tmp")))?;
        fs::rename(&tmp, dir.join(file)).map_err(err_ctx(format!("renaming {file}.tmp")))?;
    }
    for (link, file) in &links {
        match symlink(file, dir.join(link)) {
            // the right link: any other of ours is gone
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            r => r.map_err(err_ctx(format!("linking {link}")))?,
        }
    }
    Ok(())
}

fn agent_generator(
    worker_unit: &str,
    template: &str,
    n: usize,
    dirs: &[PathBuf],
) -> Result<(), Error> {
    let out = dirs
        .first()
        .ok_or_else(|| err_msg("systemd passes the output directory"))?;
    let (prefix, suffix) = template
        .split_once("@.")
        .ok_or_else(|| err_msg("template must look like name@.socket"))?;
    for wants in [
        "sockets.target.wants".to_string(),
        format!("{worker_unit}.wants"),
    ] {
        let dir = out.join(wants);
        fs::create_dir_all(&dir).map_err(err_ctx(format!("mkdir {}", dir.display())))?;
        for i in 1..=n {
            let link = dir.join(format!("{prefix}@{i}.{suffix}"));
            symlink(format!("../{template}"), &link)
                .map_err(err_ctx(format!("symlink {}", link.display())))?;
        }
    }
    Ok(())
}

/// The doc comment above Cli is the description --help prints.
#[test]
fn help_describes_the_program() {
    let cli = <Cli as clap::CommandFactory>::command();
    let about = cli.get_about().map(ToString::to_string);
    assert!(about.is_some_and(|a| a.starts_with("RBE-style")));
}

/// The userdb records are those of the last configuration only, beside
/// others' records left alone.
#[test]
fn agent_users_follow_the_configuration() -> Result<(), Box<dyn error::Error>> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    fs::write(dir.join("other.user"), "{}")?;
    symlink("other.user", dir.join("7.user"))?;
    agent_users(dir, "a-", 100, 3)?;
    agent_users(dir, "a-", 101, 2)?;
    let mut names: Vec<_> = fs::read_dir(dir)?
        .map(|e| Ok(e?.file_name().into_string().unwrap_or_default()))
        .collect::<io::Result<_>>()?;
    names.sort();
    assert_eq!(
        names,
        [
            "100.group",
            "100.user",
            "7.user",
            "99.group",
            "99.user",
            "a-1.group",
            "a-1.user",
            "a-2.group",
            "a-2.user",
            "other.user",
        ]
    );
    // 99 was a-1's
    assert_eq!(fs::read_link(dir.join("99.user"))?, Path::new("a-2.user"));
    assert_eq!(
        fs::read_link(dir.join("100.group"))?,
        Path::new("a-1.group")
    );
    let record = |f: &str| -> Result<serde_json::Value, Box<dyn error::Error>> {
        Ok(serde_json::from_str(&fs::read_to_string(dir.join(f))?)?)
    };
    assert_eq!(
        record("a-1.user")?,
        serde_json::json!({ "userName": "a-1", "uid": 100, "gid": 100, "disposition": "system" })
    );
    assert_eq!(
        record("a-1.group")?,
        serde_json::json!({ "groupName": "a-1", "gid": 100, "disposition": "system" })
    );
    Ok(())
}

/// A userdb that cannot be written fails the generator but leaves the
/// agent sockets wanted.
#[test]
fn agent_sockets_survive_a_failed_userdb() -> Result<(), Box<dyn error::Error>> {
    let tmp = tempfile::tempdir()?;
    let out = tmp.path().join("out");
    let not_a_dir = tmp.path().join("userdb");
    fs::write(&not_a_dir, "")?;
    let r = agent_units_and_users(
        "w.service",
        "a@.socket",
        2,
        slice::from_ref(&out),
        &not_a_dir,
        "a-",
        100,
    );
    assert!(r.is_err());
    for wants in ["sockets.target.wants", "w.service.wants"] {
        assert_eq!(
            fs::read_link(out.join(wants).join("a@2.socket"))?,
            Path::new("../a@.socket")
        );
    }
    Ok(())
}
