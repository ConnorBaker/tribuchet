//! What the hub can read of its nix-daemon besides its socket, to tell
//! a failure that waiting cannot clear from one that it can. The first
//! fails the build at once, and the second waits; where the hub cannot
//! tell them apart, the build waits. It reads the daemon's journal, and takes
//! only what the daemon says there itself:
//!
//! - A new connection closed before the handshake is a refusal of the
//!   hub's user only if the journal says so: Nix's daemon closes it
//!   alike when it cannot fork or open its store (nix-daemon.cc
//!   authPeer, startProcess, openStore at Nix 7587788).
//! - A request dropped mid-way is a crash of the daemon's worker only if
//!   the journal says a worker crashed: Nix's daemon logs "daemon worker
//!   N crashed" when it reaps one ended by a signal it counts as a crash
//!   (unix/daemon.cc:335-346), once it reaps it (READS, LOGGED apart).
//!   A worker killed (the kernel's OOM killer, earlyoom, an operator)
//!   leaves no such line, nor does one the daemon does not reap (a kill
//!   of its main process with its workers, a worker a restarted daemon
//!   left running): the build waits. The journal is read by time, not
//!   by connection, from the second the attempt began (daemon.rs), so
//!   another client's crash meanwhile reads as the hub's; three in a
//!   row make that rare.

use std::collections::HashSet;
use std::process::Stdio;
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::process::Command;
use tokio::time::timeout;

/// Its unit, whose journal has the daemon's errors.
const DAEMON_UNIT: &str = "nix-daemon.service";
/// How long reading the journal may take.
const JOURNAL_READ: Duration = Duration::from_secs(10);
/// How often the journal is read for what it should have, LOGGED apart:
/// Nix's daemon closes a connection before it logs why (the AutoCloseFD
/// `remote` unwinds out of the handler before the accept loop's catch
/// logs the error, unix-socket-server.cc:126-143 at Nix 7587788), and
/// logs a worker's crash once it reaps it (daemon.cc:330-346).
const READS: u32 = 3;
/// How long the daemon is given to log once more.
const LOGGED: Duration = Duration::from_secs(1);

/// The daemon's journal, as the hub reads it.
pub(in crate::hub) enum Evidence {
    /// The daemon's journald unit, read with journalctl.
    Unit(String),
    /// A test's journal: its lines, each with when it was written.
    #[cfg(test)]
    Text(Arc<Mutex<Vec<(SystemTime, String)>>>),
    /// None to read: a test's journal that cannot be read.
    #[cfg(test)]
    Unread,
}

impl Default for Evidence {
    /// NixOS's daemon.
    fn default() -> Self {
        Self::Unit(DAEMON_UNIT.to_owned())
    }
}

/// The line of `journal` in which Nix's daemon refused `user` (authPeer:
/// "user '%1%' is not allowed to connect to the Nix daemon").
fn refusal(journal: &str, user: &str) -> Option<String> {
    let said = format!("user '{user}' is not allowed to connect to the Nix daemon");
    journal
        .lines()
        .find(|l| l.contains(&said))
        .map(str::to_owned)
}

/// The first line of `journal` not `counted` in which Nix's daemon says
/// a worker crashed ("daemon worker %1% crashed", daemon.cc:343).
fn crash(journal: &str, counted: &HashSet<String>) -> Option<String> {
    journal
        .lines()
        .find(|l| l.contains("daemon worker ") && l.contains(" crashed") && !counted.contains(*l))
        .map(str::to_owned)
}

/// The daemon's journal since `since` (Unix seconds), as text: what
/// journalctl prints within JOURNAL_READ, if it exits 0.
async fn read_journal(unit: &str, since: u64) -> Option<String> {
    let read = Command::new("journalctl")
        .args(["--quiet", "--no-pager", "--output=cat", "--unit", unit])
        .arg(format!("--since=@{since}"))
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = timeout(JOURNAL_READ, read).await.ok()?.ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

impl Evidence {
    /// The line of the daemon's journal since `since` (Unix seconds) not
    /// `counted` in which it says a worker crashed, or why there is none.
    pub(in crate::hub) async fn crashed(
        &self,
        since: u64,
        counted: &HashSet<String>,
    ) -> Result<String, String> {
        let found = self.find(since, |j| crash(j, counted)).await?;
        found.ok_or_else(|| "the daemon's journal tells of no worker that crashed".to_owned())
    }

    /// The line of the daemon's journal since `since` (Unix seconds) in
    /// which it refused `user`, or why there is none.
    pub(in crate::hub) async fn refused(&self, user: &str, since: u64) -> Result<String, String> {
        let found = self.find(since, |j| refusal(j, user)).await?;
        found.ok_or_else(|| {
            format!("the daemon's journal names no refusal of the hub's user {user}")
        })
    }

    /// The line `find` finds in the daemon's journal since `since` (Unix
    /// seconds), read up to READS times, LOGGED apart, until it does:
    /// None if it finds none, Err if the journal cannot be read. A test's
    /// journal is read the same way, so that its reads take the time the
    /// daemon's do.
    async fn find(
        &self,
        since: u64,
        find: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<String>, String> {
        for read in 1..=READS {
            let journal = self
                .read(since)
                .await
                .ok_or_else(|| "the daemon's journal cannot be read".to_owned())?;
            if let Some(line) = find(&journal) {
                return Ok(Some(line));
            }
            if read < READS {
                tokio::time::sleep(LOGGED).await;
            }
        }
        Ok(None)
    }

    /// The daemon's journal since `since` (Unix seconds), if it can be
    /// read.
    async fn read(&self, since: u64) -> Option<String> {
        match self {
            Self::Unit(unit) => read_journal(unit, since).await,
            #[cfg(test)]
            Self::Text(lines) => {
                let since = UNIX_EPOCH + Duration::from_secs(since);
                let lines = lines.lock().unwrap();
                let read = lines.iter().filter(|(at, _)| *at >= since);
                Some(read.map(|(_, l)| format!("{l}\n")).collect())
            }
            #[cfg(test)]
            Self::Unread => None,
        }
    }

    /// Evidence read from a journal of `journal`'s text, if any, which
    /// every read finds, whenever it begins.
    #[cfg(test)]
    pub(in crate::hub) fn for_test(journal: Option<&str>) -> Self {
        journal.map_or(Self::Unread, |j| {
            let lines = j
                .lines()
                .map(|l| (SystemTime::now() + Duration::from_hours(24), l.to_owned()));
            Self::Text(Arc::new(Mutex::new(lines.collect())))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::relay::daemon::tests::OOM;

    /// Nix's refusal names the user; another user's refusal and the
    /// daemon's other errors do not. Nix logs it uncoloured: its daemon's
    /// logger strips escapes when stderr is not a terminal.
    #[test]
    fn a_refusal_is_read_for_the_user_it_names() {
        let journal = "accepted connection from pid 7, user alice\n\
            error: while processing connection: user 'root' is not allowed to connect to the Nix daemon\n\
            error: while processing connection: unable to start child: Resource temporarily unavailable\n";
        assert!(refusal(journal, "root").is_some_and(|l| l.contains("user 'root'")));
        assert_eq!(refusal(journal, "tribuchet"), None);
        assert_eq!(
            refusal("error: opening store: database is locked\n", "root"),
            None
        );
    }

    /// Nix's own line says a worker crashed, once: a line counted
    /// before is no new crash. The line of its reaping does not, whatever
    /// the signal, nor does another line.
    #[test]
    fn a_crash_is_read_from_the_daemons_own_line() {
        let none = HashSet::new();
        let reaped =
            "reaped child process 41, status = failed due to signal 11 (Segmentation fault)\n";
        assert_eq!(crash(reaped, &none), None);
        let crashed = format!("{reaped}daemon worker 41 crashed\n");
        let line = crash(&crashed, &none).unwrap();
        assert_eq!(line, "daemon worker 41 crashed");
        assert_eq!(crash(&crashed, &HashSet::from([line])), None);
        let killed =
            format!("reaped child process 42, status = failed due to signal 9 (Killed)\n{OOM}");
        assert_eq!(crash(&killed, &none), None);
        let other = "error: opening store: database is locked\n";
        assert_eq!(crash(other, &none), None);
    }
}
