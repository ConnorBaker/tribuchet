//! What a build's wait for the hub's own nix-daemon comes to: which class
//! each failure on the wire is, against fake daemons that speak Nix's
//! protocol 1.37 (texts as Nix 7587788 sends them, colour codes and
//! all); and what a run of failed attempts comes to, against a fake
//! journal. hub/tests/daemon.rs shows a staging build uses both.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use harmonia_store_path::StoreDir;
use harmonia_store_remote::DaemonStore as _;
use tokio::time::Instant;

use super::*;
use crate::hub::state::Replay;
use crate::hub::tests::fake_daemon::{Daemon, hub_with, told};
use crate::hub::tests::job;
use Failure::{Closed, Dropped, Refused, Retried, Unknown, Unspoken};

/// The class of the first failure a daemon behaving as `daemon` gives:
/// in the handshake, or at a query; None if the query is answered.
async fn first_failure(daemon: Daemon) -> Option<(&'static str, Failure)> {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), daemon, 1);
    let mut conn = match state.daemon_pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => return Some(("handshake", classify(&e))),
    };
    let path = StoreDir::default().parse("/nix/store/00000000000000000000000000000000-x");
    let path = path.unwrap();
    let answer = conn.execute(|c| c.query_path_info(&path)).await;
    answer.err().map(|e| ("query", classify(&e)))
}

/// Every answer but a refusal is retried alike, whatever its words; a
/// close is one only in the handshake's first two steps; a daemon down is
/// waited on.
#[tokio::test]
async fn each_failure_on_the_wire_has_its_class() {
    let refusal = |m| (Daemon::Says(m), Some(("query", Refused)));
    let retried = |m| (Daemon::Says(m), Some(("query", Retried)));
    let rows = [
        (Daemon::Lacks, None),
        refusal("path '\x1b[35;1m/nix/store/x\x1b[0m' is not valid"),
        refusal("path '\x1b[35;1m/tmp/x\x1b[0m' is not in the Nix store"),
        (Daemon::RefusesHandshake, Some(("handshake", Refused))),
        (Daemon::Foreign, Some(("handshake", Unspoken))),
        (Daemon::Old, Some(("handshake", Unspoken))),
        (Daemon::Closes, Some(("handshake", Closed))),
        (Daemon::DropsInHandshake, Some(("handshake", Dropped))),
        (Daemon::Drops, Some(("query", Dropped))),
        (Daemon::Unknown, Some(("query", Unknown))),
        (Daemon::Unreadable, Some(("handshake", Unknown))),
        (Daemon::Garbles, Some(("query", Retried))),
        retried(ENOSPC),
        retried(EMFILE),
        retried(SQLITE_FULL),
        retried(SQLITE_IOERR),
        retried(EIO),
        retried("Nix daemon out of memory"),
        retried("interrupted by the user"),
        retried("some failure of its own"),
    ];
    for (n, (daemon, class)) in rows.into_iter().enumerate() {
        assert_eq!(first_failure(daemon).await, class, "row {n}");
    }
    // No daemon listening, as while it restarts: waited on.
    let down = HubState::for_test(Duration::ZERO)
        .daemon_pool
        .acquire()
        .await;
    assert_eq!(down.err().map(|e| classify(&e)), Some(Retried));
}

/// One failed attempt: its class, its request, and the line the
/// daemon's journal gains as it fails, if any, with how long ago it
/// was written.
type Attempt = (Failure, &'static str, Option<(u64, String)>);

const WHAT: &str = "what the attempt saw";

/// The request every attempt but a connect's is made for.
const P: &str = "p";

/// A run that never fails: the build waits.
const WAITS: Option<(usize, &str)> = None;

/// A run that fails at attempt `n`, its error saying `says`.
fn fails(n: usize, says: &str) -> Option<(usize, &str)> {
    Some((n, says))
}

/// `k` attempts failing as `class` at `request`, the journal silent.
fn n_of(class: Failure, request: &'static str, k: usize) -> Vec<Attempt> {
    vec![(class, request, None); k]
}

/// A wait fails at the attempt that brings run(c, r) to θ(c), saying why:
/// θ is 1 for a refusal, an unspeakable handshake, a close the journal
/// says refused the hub's user; 2 for an unknown message; 3 for a drop the
/// journal tells of as a crash. Else it waits, its clients told once. A
/// run is one class on one request in a row (an unexplained drop breaks
/// it); a crash line counts once, and only once its attempt began.
#[tokio::test(start_paused = true)]
async fn a_run_of_failures_comes_to_its_verdict() {
    let now = |line: String| Some((0, line));
    let worker = AtomicUsize::new(0);
    let crash = |ago| {
        Some((
            ago,
            format!("daemon worker {} crashed", worker.fetch_add(1, SeqCst)),
        ))
    };
    let refused = |user: &str| {
        now(format!(
            "error: while processing connection: user '{user}' is not allowed to connect to \
             the Nix daemon"
        ))
    };
    let crashes = |requests: &[&'static str]| -> Vec<Attempt> {
        requests.iter().map(|r| (Dropped, *r, crash(0))).collect()
    };
    let refusing = |user: &str| vec![(Closed, CONNECT, refused(user))];
    let me = hub_user();
    // A crash, a drop the journal does not explain, two crashes.
    let broken = [crashes(&[P]), n_of(Dropped, P, 1), crashes(&[P; 2])].concat();
    let rows: Vec<(Vec<Attempt>, Option<(usize, &str)>)> = vec![
        (n_of(Refused, P, 1), fails(1, WHAT)),
        (n_of(Unspoken, CONNECT, 1), fails(1, "cannot speak")),
        (refusing(&me), fails(1, "allowed-users")),
        (n_of(Unknown, P, 2), fails(2, "does not know")),
        (crashes(&[P; 3]), fails(3, "crashes its worker every time")),
        ([n_of(Unknown, P, 1), n_of(Retried, P, 1)].concat(), WAITS),
        (crashes(&[P; 2]), WAITS),
        (n_of(Retried, P, 13), WAITS),
        (n_of(Closed, CONNECT, 4), WAITS),
        (refusing(&format!("{me}-not")), WAITS),
        (n_of(Dropped, P, 4), WAITS),
        // Drops the journal tells of as the kernel's OOM kills, not crashes.
        (vec![(Dropped, P, now(OOM.into())); 4], WAITS),
        // One crash line, read by every attempt.
        ([crashes(&[P]), n_of(Dropped, P, 3)].concat(), WAITS),
        // Crashes alternately in the handshake and at the query.
        (crashes(&[CONNECT, P, CONNECT, P, CONNECT, P]), WAITS),
        (broken, WAITS),
        // Crash lines an hour old.
        ((0..4).map(|_| (Dropped, P, crash(3600))).collect(), WAITS),
    ];
    // One hub, each row a build of its own: no verdict outlives its build
    // (a hub that kept one fails a second build on the first's crashes).
    let mut state = HubState::for_test(Duration::ZERO);
    for (n, (attempts, verdict)) in rows.into_iter().enumerate() {
        let journal = Arc::new(Mutex::new(Vec::new()));
        state.daemon_evidence = Evidence::Text(journal.clone());
        let j = job(n, Arc::new(Replay::default()));
        let mut client = j.replay.subscribe().await;
        let mut wait = DaemonWait::new(&j, &state);
        let mut failed = None;
        for (a, (failure, request, line)) in attempts.into_iter().enumerate() {
            let began = unix_now();
            if let Some((ago, line)) = line {
                let at = SystemTime::now() - Duration::from_secs(ago);
                journal.lock().unwrap().push((at, line));
            }
            if let Err(e) = wait.failed(failure, began, request, WHAT).await {
                failed = Some((a + 1, e.to_string()));
                break;
            }
        }
        let told = told(&mut client).0.len();
        match (verdict, failed) {
            (Some((at, says)), Some((a, e))) => {
                assert!(a == at && e.contains(says), "row {n}: failed at {a}: {e}");
                assert_eq!(told, usize::from(at > 1), "row {n}");
            }
            (None, None) => assert_eq!(told, 1, "row {n}"),
            (_, failed) => panic!("row {n}: {failed:?}"),
        }
        let untold = j.replay.tell_waiting(Wait::Worker, "w\n".into()).await;
        assert!(untold, "row {n}: the daemon's note silenced the worker's");
    }
}

/// The backoff doubles from 1 s up to 30 s.
#[tokio::test(start_paused = true)]
async fn the_backoff_doubles_to_30_s() {
    let state = HubState::for_test(Duration::ZERO);
    let j = job(0, Arc::new(Replay::default()));
    let mut wait = DaemonWait::new(&j, &state);
    for delay in [1, 2, 4, 8, 16, 30, 30] {
        let t = Instant::now();
        wait.failed(Retried, unix_now(), "p", WHAT).await.unwrap();
        assert_eq!(t.elapsed(), Duration::from_secs(delay));
    }
}

/// What systemd 261.3 writes in the daemon's unit when the kernel's
/// OOM killer kills in it (src/core/cgroup.c:3099-3102, unit.c:6267).
/// The VM test oom_kill_in_the_daemons_unit (tests/e2e/daemon.rs) reads
/// the first from nix-daemon's own journal; these rows are what the hub
/// makes of them.
pub(super) const OOM: &str = "nix-daemon.service: The kernel OOM killer killed some processes in \
    this unit.\nnix-daemon.service: Failed with result 'oom-kill'.\n";
/// SystemError of ENOSPC (error.hh:354).
const ENOSPC: &str = "writing to file: \x1b[35;1mNo space left on device\x1b[0m";
/// SystemError of EMFILE.
const EMFILE: &str =
    "opening file '\x1b[35;1m/nix/store/x\x1b[0m': \x1b[35;1mToo many open files\x1b[0m";
/// SQLiteError of SQLITE_FULL (sqlite.cc:31).
const SQLITE_FULL: &str = "executing SQLite statement '\x1b[35;1minsert into ValidPaths\x1b[0m': \
     \x1b[35;1m\x1b[0m\x1b[35;1mdatabase or disk is full\x1b[0m, \x1b[35;1mdatabase or disk is \
     full\x1b[0m (in '\x1b[35;1m/nix/var/nix/db/db.sqlite\x1b[0m')";
/// SQLiteError of SQLITE_IOERR_WRITE.
const SQLITE_IOERR: &str = "committing transaction: \x1b[35;1m\x1b[0m\x1b[35;1mdisk I/O \
     error\x1b[0m, \x1b[35;1mdisk I/O error\x1b[0m (in '\x1b[35;1m/nix/var/nix/db/db.sqlite\x1b[0m')";
/// SystemError of EIO: the same condition as SQLITE_IOERR, reported by
/// a system call instead of SQLite.
const EIO: &str = "reading file '\x1b[35;1m/nix/var/nix/db/db.sqlite\x1b[0m': \x1b[35;1mInput/output error\x1b[0m";
