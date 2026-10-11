//! Exit status and resource usage of a reaped builder, from `wait4`.
//!
//! Linux reads the build cgroup instead. This is what macOS has, where
//! there is no cgroup. `wait4` covers the builder and the descendants it
//! waited for, not orphans, and `ru_maxrss` is the largest single
//! process, not the sum over the tree, so it is a lower bound on the
//! build's peak memory.

use std::io;
use std::mem;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, ExitStatus};

pub(super) struct Reaped {
    pub(super) status: ExitStatus,
    pub(super) cpu_usec: u64,
    pub(super) peak_memory_bytes: u64,
}

fn usec(t: libc::timeval) -> u64 {
    u64::try_from(t.tv_sec).unwrap_or(0) * 1_000_000 + u64::try_from(t.tv_usec).unwrap_or(0)
}

/// Wait for the child like `Child::wait`, and keep its rusage.
pub(super) fn reap(child: &mut Child) -> io::Result<Reaped> {
    let pid = libc::pid_t::try_from(child.id()).map_err(io::Error::other)?;
    let mut raw = 0;
    // SAFETY: an all-zero rusage is a valid value. wait4 fills it in.
    let mut ru: libc::rusage = unsafe { mem::zeroed() };
    loop {
        // SAFETY: both out-pointers are valid for the call.
        let r = unsafe { libc::wait4(pid, &raw mut raw, 0, &raw mut ru) };
        if r == pid {
            break;
        }
        let e = io::Error::last_os_error();
        if r < 0 && e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(e);
    }
    // ru_maxrss is kibibytes on Linux and bytes on macOS.
    let unit: u64 = if cfg!(target_os = "macos") { 1 } else { 1024 };
    Ok(Reaped {
        status: ExitStatus::from_raw(raw),
        cpu_usec: usec(ru.ru_utime) + usec(ru.ru_stime),
        peak_memory_bytes: u64::try_from(ru.ru_maxrss).unwrap_or(0) * unit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn reap_returns_the_exit_code() {
        let mut c = Command::new("sh").args(["-c", "exit 7"]).spawn().unwrap();
        assert_eq!(reap(&mut c).unwrap().status.code(), Some(7));
    }

    #[test]
    fn reap_reports_a_signal_death() {
        let mut c = Command::new("sh")
            .args(["-c", "kill -9 $$"])
            .spawn()
            .unwrap();
        assert_eq!(reap(&mut c).unwrap().status.signal(), Some(9));
    }

    #[test]
    fn reap_measures_cpu_time_and_memory_of_a_busy_child() {
        // ~300 ms of CPU in a child that also touches some memory.
        let mut c = Command::new("sh")
            .args(["-c", "i=0; while [ $i -lt 40000 ]; do i=$((i+1)); done"])
            .spawn()
            .unwrap();
        let r = reap(&mut c).unwrap();
        assert!(r.cpu_usec > 10_000, "cpu {} us", r.cpu_usec);
        assert!(
            r.peak_memory_bytes > 100_000,
            "peak {}",
            r.peak_memory_bytes
        );
    }
}
