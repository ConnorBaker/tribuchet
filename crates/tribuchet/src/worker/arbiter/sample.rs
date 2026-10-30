//! What the arbiter reads of a build: where its cgroup is, how many of
//! its threads are runnable, and how long it was frozen.

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::errors::{Result, err_msg};
use crate::fsutil::stat_field;

/// The build cgroup holding `pid`, from `/proc/<pid>/cgroup` cut at
/// `build-<build_id>` (the payload may have moved the pid into a
/// subgroup of its own). None when builds run without a cgroup.
pub(super) fn build_cgroup(pid: i32, build_id: &str) -> Option<PathBuf> {
    cgroup_dir(
        &fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?,
        build_id,
    )
}

/// The first from the root: the payload may name a subgroup alike.
fn cgroup_dir(proc_cgroup: &str, build_id: &str) -> Option<PathBuf> {
    let leaf = format!("build-{build_id}");
    let dirs = cgroup_path(proc_cgroup)?;
    let first = dirs.ancestors().filter(|d| d.ends_with(&leaf)).last();
    first.map(Path::to_path_buf)
}

/// The cgroup v2 dir a `/proc/<pid>/cgroup` text names.
pub(crate) fn cgroup_path(proc_cgroup: &str) -> Option<PathBuf> {
    let path = proc_cgroup.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(PathBuf::from(format!("/sys/fs/cgroup{}", path.trim())))
}

/// The cgroups directly below cgroup `dir`.
pub(crate) fn subgroups(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let dirs = fs::read_dir(dir)?.flatten();
    Ok(dirs
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .map(|e| e.path())
        .collect())
}

/// Threads of the cgroup and its subgroups in state R: running, or
/// waiting for a CPU. A frozen cgroup's threads are not.
pub(super) fn runnable_threads(dir: &Path) -> u32 {
    let mut dirs = vec![dir.to_path_buf()];
    let mut runnable = 0u32;
    while let Some(d) = dirs.pop() {
        if let Ok(tids) = fs::read_to_string(d.join("cgroup.threads")) {
            for tid in tids.lines() {
                if thread_state(tid.trim()) == Some('R') {
                    runnable = runnable.saturating_add(1);
                }
            }
        }
        dirs.extend(subgroups(&d).into_iter().flatten());
    }
    runnable
}

fn thread_state(tid: &str) -> Option<char> {
    stat_state(&fs::read_to_string(format!("/proc/{tid}/stat")).ok()?)
}

/// The state field of `/proc/<tid>/stat`, after the parenthesised
/// command name, which may itself contain ") ".
fn stat_state(stat: &str) -> Option<char> {
    stat.rsplit_once(") ")?.1.chars().next()
}

/// `frozen_usec` of a cgroup's cgroup.stat.local: the time it spent
/// between a freeze and the thaw. None when it cannot be read (the
/// cgroup is gone).
pub(crate) fn frozen_usec(dir: &Path) -> Option<u64> {
    stat_field(
        &fs::read_to_string(dir.join("cgroup.stat.local")).ok()?,
        "frozen_usec",
    )
}

/// The kernel counts each cgroup's frozen time (cgroup.stat.local's
/// frozen_usec, which Linux 6.18 added: cgroup-v2.rst there, "exists in
/// non-root cgroups"), checked on this process's own cgroup, which
/// `proc_cgroup` (/proc/self/cgroup) names, on kernel `release`. The
/// arbiter relies on it to bound freezes (bound.rs) and to keep them out
/// of a build's running time; without it neither works and nothing says
/// so, so the arbiter is off on such a host instead.
pub(super) fn require_frozen_time(proc_cgroup: &str, release: &str) -> Result<()> {
    counts_frozen_time(cgroup_path(proc_cgroup).as_deref(), release)
}

/// Whether `own`, a non-root cgroup, counts frozen time on kernel
/// `release`; an error naming both if not.
fn counts_frozen_time(own: Option<&Path>, release: &str) -> Result<()> {
    if own.and_then(frozen_usec).is_some() {
        return Ok(());
    }
    let place = own.map_or_else(
        || "no cgroup v2 of its own".to_owned(),
        |cg| format!("none in {}", cg.join("cgroup.stat.local").display()),
    );
    Err(err_msg(format!(
        "the CPU arbiter needs Linux 6.18 or later, whose cgroups count their frozen time \
         (cgroup.stat.local frozen_usec); this kernel, {release}, does not ({place}). \
         Run the worker on Linux 6.18 or later for builds to be arbitrated"
    )))
}

/// Whether cgroup.freeze asks for the cgroup to be frozen; None when it
/// cannot be read.
pub(super) fn freeze_requested(dir: &Path) -> Option<bool> {
    match fs::read_to_string(dir.join("cgroup.freeze")).ok()?.trim() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

/// Whether cgroup.events says the cgroup has reached frozen.
pub(super) fn frozen_reached(dir: &Path) -> bool {
    fs::read_to_string(dir.join("cgroup.events")).is_ok_and(|e| e.lines().any(|l| l == "frozen 1"))
}

/// The mean of the runnable-thread samples held; 0 for none.
pub(super) fn mean(samples: &VecDeque<u32>) -> f64 {
    let n = u32::try_from(samples.len()).unwrap_or(u32::MAX).max(1);
    f64::from(samples.iter().copied().fold(0u32, u32::saturating_add)) / f64::from(n)
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use rustix::thread::gettid;

    use super::*;

    #[test]
    fn the_build_cgroup_is_cut_at_its_build_component() {
        let id = "0123456789abcdef0123456789abcdef";
        let line = format!("0::/system.slice/tribuchet-agent@1.service/build-{id}/payload/x\n");
        assert_eq!(
            cgroup_dir(&line, id),
            Some(PathBuf::from(format!(
                "/sys/fs/cgroup/system.slice/tribuchet-agent@1.service/build-{id}"
            )))
        );
        assert_eq!(
            cgroup_dir("0::/system.slice/tribuchet-worker.service\n", id),
            None
        );
        // A payload subgroup named like the build's own is not it.
        let nested = format!("0::/a.service/build-{id}/payload/build-{id}\n");
        let own = format!("/sys/fs/cgroup/a.service/build-{id}");
        assert_eq!(cgroup_dir(&nested, id), Some(PathBuf::from(own)));
    }

    #[test]
    fn the_state_follows_the_last_parenthesis() {
        assert_eq!(stat_state("42 (java) R 1 42 42 0"), Some('R'));
        assert_eq!(stat_state("43 (we) ird) S 1 43"), Some('S'));
        assert_eq!(stat_state("garbage"), None);
    }

    /// The arbiter is off, naming the kernel, where the worker's own
    /// cgroup counts no frozen time (Linux before 6.18, or no cgroup v2
    /// of its own), and on where it does, rather than start unchecked
    /// with its bound and notes failing silently.
    #[test]
    fn the_arbiter_is_off_on_a_kernel_that_counts_no_frozen_time() {
        let dir = tempfile::tempdir().unwrap();
        for own in [Some(dir.path()), None] {
            let err = counts_frozen_time(own, "6.12.58").unwrap_err().to_string();
            assert!(
                err.contains("Linux 6.18")
                    && err.contains("6.12.58")
                    && err.contains("for builds to be arbitrated"),
                "{err}"
            );
        }
        fs::write(dir.path().join("cgroup.stat.local"), "frozen_usec 0\n").unwrap();
        assert!(counts_frozen_time(Some(dir.path()), "7.2.9").is_ok());
        assert!(require_frozen_time("0::/tribuchet-no-such-cgroup\n", "6.12.58").is_err());
        assert_eq!(
            cgroup_path("0::/system.slice/tribuchet-worker.service\n"),
            Some(PathBuf::from(
                "/sys/fs/cgroup/system.slice/tribuchet-worker.service"
            ))
        );
    }

    /// The test's own thread, listed twice: it is running while it reads.
    #[cfg(target_os = "linux")]
    #[test]
    fn runnable_threads_counts_threads_in_state_r() {
        let dir = tempfile::tempdir().unwrap();
        let me = format!("{}\n", gettid().as_raw_nonzero());
        fs::write(dir.path().join("cgroup.threads"), &me).unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/cgroup.threads"), &me).unwrap();
        assert_eq!(runnable_threads(dir.path()), 2);
    }
}
