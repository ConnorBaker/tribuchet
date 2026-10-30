//! Per-build cgroups under the agent's delegated subtree.
//!
//! The agent unit runs with Delegate=yes, so systemd hands the unit
//! cgroup to the agent user. Each build gets a `build-<id>` child next
//! to the agent's own leaf: the sandbox's cgroup namespace is rooted
//! there and the payload (nspawn, NixOS containers) manages its own
//! subgroups, which is why the cgroup is chowned to the build's
//! mapped root uid; that chown is what the agent keeps CAP_CHOWN for.

use crate::errors::{Result, err_ctx, err_msg};
use crate::fsutil::{io_ctx, stat_field};
use crate::worker::arbiter::{cgroup_path, frozen_usec, subgroups};
use std::fs;
use std::io;
use std::io::Write;
use std::os::unix::fs::chown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

/// Vacate the delegated unit cgroup (cgroup v2's no-internal-process
/// rule forbids enabling controllers while it holds processes) and
/// enable the memory controller for the build cgroups created next to
/// the leaf. None when the unit is not delegated (development runs):
/// builds then run without a cgroup of their own.
pub(super) fn init() -> Option<PathBuf> {
    let base = cgroup_path(&fs::read_to_string("/proc/self/cgroup").ok()?)?;
    let controllers = fs::read_to_string(base.join("cgroup.controllers")).ok()?;
    let leaf = base.join("agent");
    if fs::create_dir_all(&leaf).is_err() || fs::write(leaf.join("cgroup.procs"), "0").is_err() {
        tracing::info!("no delegated cgroup; builds run without one");
        return None;
    }
    if controllers.split_whitespace().any(|c| c == "memory")
        && let Err(e) = fs::write(base.join("cgroup.subtree_control"), "+memory")
    {
        tracing::warn!("enabling +memory on {}: {e}", base.display());
    }
    Some(base)
}

/// Create the build's cgroup and hand it to the build's mapped root
/// uid so the in-sandbox payload can manage subgroups. cgroup.procs
/// stays agent-owned until [`enter`] moved the setup stage in;
/// cgroup.kill and memory.max stay agent-owned for good.
pub(super) fn create(base: &Path, build_id: &str, owner_uid: u32) -> Result<PathBuf> {
    let dir = base.join(format!("build-{build_id}"));
    fs::create_dir(&dir).map_err(err_ctx(format!("creating cgroup {}", dir.display())))?;
    hand_over(&dir, owner_uid)?;
    Ok(dir)
}

/// What of its cgroup `dir` the build's mapped root uid gets: the dir and
/// the files it manages subgroups with. Never cgroup.freeze, by which it
/// could thaw itself.
fn hand_over(dir: &Path, owner_uid: u32) -> Result<()> {
    chown(dir, Some(owner_uid), None)?;
    for f in ["cgroup.subtree_control", "cgroup.threads"] {
        chown(dir.join(f), Some(owner_uid), None)?;
    }
    Ok(())
}

/// Cap the build's memory. memory.max stays agent-owned so the
/// payload cannot raise it. oom.group makes the kernel kill the whole
/// build instead of one victim, so it fails instead of hanging.
pub(super) fn set_memory_max(dir: &Path, bytes: u64) -> Result<()> {
    fs::write(dir.join("memory.max"), bytes.to_string())
        .map_err(err_ctx(format!("writing memory.max in {}", dir.display())))?;
    fs::write(dir.join("memory.oom.group"), "1").map_err(err_ctx("writing memory.oom.group"))
}

/// Move a pid into the build cgroup, then hand cgroup.procs to the
/// build's mapped root uid (the payload migrates processes into its
/// own subgroups). Called on the setup stage before the spec is sent,
/// so its CLONE_NEWCGROUP is rooted there.
pub(super) fn enter(dir: &Path, pid: i32, owner_uid: u32) -> Result<()> {
    fs::write(dir.join("cgroup.procs"), pid.to_string())
        .map_err(err_ctx(format!("moving pid {pid} into {}", dir.display())))?;
    chown(dir.join("cgroup.procs"), Some(owner_uid), None)?;
    Ok(())
}

/// The build's cgroup.freeze under `base`, write-only, for the worker to
/// freeze and thaw it with while `lease` lasts; none if the thaw at the
/// lease's end cannot be set up. Opened by the agent, which owns the file:
/// the module's worker, not root, can open none itself.
pub(super) fn freeze_handle(base: &Path, build_id: &str, lease: &UnixStream) -> Option<fs::File> {
    let path = base.join(format!("build-{build_id}/cgroup.freeze"));
    let freeze = fs::OpenOptions::new().write(true).open(path).ok()?;
    let thaw = (lease.try_clone().ok()?, freeze.try_clone().ok()?);
    thaw_when_lease_ends(thaw.0, thaw.1, build_id);
    Some(freeze)
}

/// Whether the build cgroup `dir`'s memory.max OOM-killed the build.
pub(super) fn oom_killed(dir: &Path) -> bool {
    let events = fs::read_to_string(dir.join("memory.events"));
    events.is_ok_and(|e| stat_field(&e, "oom_kill").is_some_and(|n| n > 0))
}

/// The worker freezes and thaws the build through `freeze`, its
/// cgroup.freeze, sent with the lease; no freeze may outlive the lease,
/// however the worker ends it, so the agent thaws the build at its end.
/// The worker writes nothing on the connection after its call: a read
/// returns only at the end.
fn thaw_when_lease_ends(mut lease: UnixStream, mut freeze: fs::File, build_id: &str) {
    let id = build_id.to_owned();
    thread::spawn(move || {
        let ended = io::copy(&mut lease, &mut io::sink());
        match freeze.write_all(b"0") {
            Ok(()) => tracing::info!(id, ?ended, "the lease ended; build thawed"),
            Err(e) => tracing::debug!(id, "thawing the build as the lease ended: {e}"),
        }
    });
}

/// Kill everything in the build cgroup, wait for it to drain and
/// remove it, subgroups first (cgroup dirs can only be rmdir'd).
pub(super) fn destroy(dir: &Path) -> Result<()> {
    match fs::write(dir.join("cgroup.kill"), "1") {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        other => other.map_err(err_ctx("writing cgroup.kill"))?,
    }
    let deadline = Instant::now() + Duration::from_mins(1);
    // A vanished events file (systemd already removed the cgroup)
    // counts as drained.
    while fs::read_to_string(dir.join("cgroup.events")).is_ok_and(|e| !e.contains("populated 0")) {
        if Instant::now() >= deadline {
            return Err(err_msg("cgroup did not drain"));
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mut dirs = vec![dir.to_path_buf()];
    let mut i = 0;
    while i < dirs.len() {
        let below = subgroups(&dirs[i]).map_err(io_ctx("listing", &dirs[i]))?;
        dirs.extend(below);
        i += 1;
    }
    for d in dirs.iter().rev() {
        fs::remove_dir(d).map_err(err_ctx(format!("removing cgroup {}", d.display())))?;
    }
    Ok(())
}

/// What a build used, read from its cgroup after the builder exited and
/// before the cgroup is destroyed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Usage {
    pub(super) cpu_usec: u64,
    pub(super) peak_memory_bytes: u64,
    pub(super) frozen_usec: u64,
}

/// A field is 0 when its file is missing: memory.peak where the memory
/// controller is not enabled for build cgroups (see [`init`]), any of
/// them once the cgroup is gone.
pub(super) fn usage(dir: &Path) -> Usage {
    let read = |f: &str| fs::read_to_string(dir.join(f)).ok();
    Usage {
        cpu_usec: read("cpu.stat")
            .and_then(|s| stat_field(&s, "usage_usec"))
            .unwrap_or(0),
        peak_memory_bytes: read("memory.peak")
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0),
        frozen_usec: frozen_usec(dir).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn usage_reads_cpu_and_peak_memory_from_the_cgroup_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("cpu.stat"),
            "usage_usec 1234567\nuser_usec 1000000\nsystem_usec 234567\n",
        )
        .unwrap();
        fs::write(dir.path().join("memory.peak"), "8110292992\n").unwrap();
        fs::write(
            dir.path().join("cgroup.stat.local"),
            "frozen_usec 75056841\n",
        )
        .unwrap();
        assert_eq!(
            usage(dir.path()),
            Usage {
                cpu_usec: 1_234_567,
                peak_memory_bytes: 8_110_292_992,
                frozen_usec: 75_056_841,
            }
        );
    }

    /// The worker's handle is on its build's cgroup.freeze, not the agent's
    /// own, write-only, and the agent thaws through it once the worker's
    /// end of the lease closes, its own end still open, and not before.
    #[test]
    fn the_freeze_handle_is_on_the_builds_own_cgroup_freeze() {
        let base = tempfile::tempdir().unwrap();
        let (own, build) = (base.path(), base.path().join("build-b"));
        fs::create_dir(&build).unwrap();
        for dir in [own, build.as_path()] {
            fs::write(dir.join("cgroup.freeze"), "").unwrap();
        }
        let (worker, lease) = UnixStream::pair().unwrap();
        let mut handle = freeze_handle(own, "b", &lease).expect("a handle");
        handle.write_all(b"1").unwrap();
        let read = |dir: &Path| fs::read_to_string(dir.join("cgroup.freeze")).unwrap();
        assert_eq!((read(&build), read(own)), ("1".into(), String::new()));
        assert!(handle.read(&mut [0]).is_err(), "readable");
        thread::sleep(Duration::from_millis(200));
        assert_eq!(read(&build), "1", "thawed while the lease lasts");
        drop(worker);
        let end = Instant::now() + Duration::from_secs(10);
        while read(&build) == "1" && Instant::now() < end {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(read(&build), "10", "not thawed as the lease ended");
    }

    /// A build was OOM-killed when its memory.events counts an oom_kill.
    #[test]
    fn oom_killed_reads_the_memory_events() {
        let dir = tempfile::tempdir().unwrap();
        let events = |text: &str| fs::write(dir.path().join("memory.events"), text).unwrap();
        events("oom 1\noom_kill 0\noom_group_kill 1\n");
        assert!(!oom_killed(dir.path()));
        events("oom 1\noom_kill 2\noom_group_kill 1\n");
        assert!(oom_killed(dir.path()));
    }

    /// The build's mapped root uid is handed its cgroup, not cgroup.freeze.
    /// Here that file is absent, so handing it over too fails with ENOENT
    /// whatever uid runs the test.
    #[test]
    fn a_build_is_handed_its_cgroup_but_not_its_cgroup_freeze() {
        let dir = tempfile::tempdir().unwrap();
        for f in ["cgroup.subtree_control", "cgroup.threads"] {
            fs::write(dir.path().join(f), "").unwrap();
        }
        hand_over(dir.path(), rustix::process::getuid().as_raw()).unwrap();
    }

    #[test]
    fn usage_is_zero_where_a_file_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(usage(dir.path()), Usage::default());
        fs::write(dir.path().join("cpu.stat"), "nr_periods 0\n").unwrap();
        assert_eq!(usage(dir.path()), Usage::default());
    }
}
