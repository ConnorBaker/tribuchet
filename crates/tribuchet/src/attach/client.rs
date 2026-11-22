//! Who asked for a build, as attach names its client.

use std::fs;
use std::os::unix::process::parent_id;

/// The nix-daemon connection that runs this attach, as a client: Nix runs
/// an external builder from the process serving the connection that asked
/// for the build (external-derivation-builder.cc, startProcess), so its
/// parent, by boot, pid and start time (a pid is reused), names that
/// client. None where /proc does not say.
pub(super) fn daemon_connection() -> Option<String> {
    connection_of(parent_id())
}

/// Process `pid` by boot, pid and start time (clock ticks since boot).
fn connection_of(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 22, counted after the parenthesised command, which may hold spaces.
    let started = stat.rsplit_once(')')?.1.split_whitespace().nth(19)?;
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    Some(format!("{}/{pid}/{started}", boot.trim()))
}

#[cfg(test)]
mod tests {
    use std::process;

    use super::*;

    /// The connection is the parent, by this boot and its start time in
    /// /proc (field 22), the same each time it is read: the start of a
    /// child started between two reads of the time since boot lies between
    /// them.
    #[test]
    #[cfg(target_os = "linux")]
    fn the_client_is_the_parent_by_its_start_time() {
        let c = daemon_connection().unwrap();
        assert_eq!(daemon_connection().as_deref(), Some(c.as_str()));
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
        let this = format!("{}/{}/", boot.trim(), parent_id());
        assert!(c.starts_with(&this), "{c} is not of {this}");
        // SAFETY: sysconf reads a constant.
        let hz = f64::from(u32::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).unwrap());
        let since_boot = || -> f64 {
            let up = fs::read_to_string("/proc/uptime").unwrap();
            up.split_whitespace().next().unwrap().parse().unwrap()
        };
        let before = since_boot();
        let mut child = process::Command::new("sleep").arg("5").spawn().unwrap();
        let after = since_boot();
        let named = connection_of(child.id()).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let ticks: f64 = named.rsplit('/').next().unwrap().parse().unwrap();
        let (low, high) = ((before * hz).floor() - 1.0, (after * hz).ceil() + 1.0);
        assert!(
            (low..=high).contains(&ticks),
            "{ticks} not in {low}..={high}"
        );
    }
}
