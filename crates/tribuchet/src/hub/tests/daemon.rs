//! The hub's own nix-daemon failing a build's staging, end to end, against
//! fake daemons (fake_daemon.rs). Which failure is which, and when a run
//! of them fails the build, is relay/daemon/tests.rs's: these rows show
//! that a staging build, before its assignment goes out and after, and
//! its supervision, keep to it, and heed its client leaving.

use super::fake_daemon::{Daemon, cached_input, error_of, hub_scripted, hub_with, told, worker};
use super::*;
use At::{After, Before};
use Comes::{Dropped, Fails, GivenUp, Kept, Waits};
use relay::{DAEMON_ANSWER, DaemonEvidence};
use state::{FLOOR, Inflight, Wait};

use std::sync::atomic::AtomicU64;

use crate::proto::Need;

/// The worker lacks an input of the build it was assigned and asks
/// for it, so the hub must look the path up in its daemon.
pub(super) async fn staging_build(state: &Arc<HubState>, j: Job) -> Worker {
    let input = j.req.input_paths[0].clone();
    state.queue.lock().await.push_back(j);
    let mut w = worker(state, "w").await;
    let build_id = loop {
        match w.from_hub.recv().await {
            Some(Ok(HubMessage {
                msg: Some(hub_message::Msg::Assignment(a)),
            })) => break a.build_id,
            Some(_) => {}
            None => panic!("session ended before the assignment"),
        }
    };
    let need = Need {
        build_id,
        paths: vec![input],
        hashes: vec![],
    };
    w.send(worker_message::Msg::Need(need)).await;
    w
}

pub(super) fn with_input(n: usize) -> Job {
    let mut j = job(n, Arc::new(Replay::default()));
    j.req.input_paths = vec![format!("/nix/store/{H}-input")];
    j
}

/// What the hub tells the worker and the client within `wait`: the
/// first Cancel or Error, if any.
pub(super) async fn verdict_within(
    wait: Duration,
    from_hub: &mut mpsc::Receiver<Result<HubMessage, Status>>,
    client: &mut mpsc::Receiver<Result<AttachEvent, Status>>,
) -> Option<String> {
    let end = tokio::time::sleep(wait);
    tokio::pin!(end);
    loop {
        tokio::select! {
            m = from_hub.recv() => match m {
                Some(Ok(HubMessage { msg: Some(hub_message::Msg::Cancel(_)) })) => {
                    return Some("worker told to cancel".into());
                }
                Some(_) => {}
                None => return Some("session ended".into()),
            },
            ev = client.recv() => match ev {
                Some(Ok(AttachEvent { event: Some(attach_event::Event::Error(e)) })) => {
                    return Some(e);
                }
                Some(_) => {}
                None => return Some("client stream ended".into()),
            },
            () = &mut end => return None,
        }
    }
}

/// Where the build asks the daemon: for an input whose recipe the hub
/// has cached, before its assignment goes out; or for the input the
/// worker's Need asks for, after.
#[derive(Clone, Copy, PartialEq, Eq)]
enum At {
    Before,
    After,
}

/// What a row's build comes to.
#[derive(Clone, Copy)]
enum Comes {
    /// It fails, its client told this, and its worker told to cancel.
    Fails(&'static str),
    /// It waits through three failed attempts, its client told once.
    Waits,
    /// Its client left once the daemon took this many connections, and
    /// another build wants its slot: given up within FLOOR and a check
    /// (its worker told to cancel), unlisted, neither failed nor requeued.
    GivenUp(usize),
    /// Its client left at the first connection: it fails before dispatch,
    /// is dropped within FLOOR, and is not counted failed.
    Dropped,
    /// Its client left at the first connection and is back within
    /// FLOOR: nothing happens for a minute.
    Kept,
}

/// A journal in which Nix's daemon reaped three workers that crashed.
const CRASHES: &str = "daemon worker 41 crashed\ndaemon worker 42 crashed\n\
    daemon worker 43 crashed\n";

/// A failure of the daemon's fails the build, naming the cause, once its
/// run reaches θ; any other keeps it waiting, before its assignment goes
/// out and after, its client told once and no loss counted; a path the
/// daemon lacks fails it; and a build whose client left is given up
/// while its attempt waits on the daemon or backs off, but kept if the
/// client is back within FLOOR. The first failed attempt with no client
/// attached does not give the build up.
#[test]
fn a_staging_build_keeps_to_its_daemons_verdict() {
    let interrupted = Daemon::Says("interrupted by the user");
    let refusal = Daemon::Says("path '\x1b[35;1m/nix/store/x\x1b[0m' is not valid");
    let rows = [
        (
            Daemon::Drops,
            Some(CRASHES),
            After,
            Fails("crashes its worker every time"),
        ),
        // A refusal, and a handshake the hub cannot speak: the daemon's words.
        (refusal, None, After, Fails("is not valid")),
        (Daemon::Foreign, None, After, Fails("wrong magic")),
        (Daemon::Lacks, None, After, Fails("is not a valid path")),
        (Daemon::Lacks, None, Before, Fails("is not a valid path")),
        (interrupted, None, After, Waits),
        (interrupted, None, Before, Waits),
        (Daemon::Silent, None, After, Waits),
        (Daemon::Silent, None, After, GivenUp(1)),
        (Daemon::Silent, None, Before, GivenUp(1)),
        // Its client leaves during a 30 s backoff.
        (Daemon::RefusesHandshake, None, After, GivenUp(6)),
        (Daemon::LacksSlowly, None, Before, Dropped),
        (Daemon::Silent, None, After, Kept),
        // ... though its attempts failed while it was away.
        (interrupted, None, After, Kept),
    ];
    for (n, (daemon, journal, at, comes)) in rows.into_iter().enumerate() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(comes_to(n, daemon, journal, at, comes));
    }
}

/// Row `n`: what a build staging on a daemon that behaves as `daemon`,
/// its journal `journal`, comes to.
async fn comes_to(n: usize, daemon: Daemon, journal: Option<&str>, at: At, comes: Comes) {
    let dir = tempfile::tempdir().unwrap();
    let (mut state, conns) = hub_scripted(dir.path(), move |_| daemon, 8);
    Arc::get_mut(&mut state).unwrap().daemon_evidence = DaemonEvidence::for_test(journal);
    let (j, _) = match at {
        At::Before => cached_input(&state, 0).await,
        At::After => (with_input(0), String::new()),
    };
    *j.listing.lock().unwrap() = Inflight::list(&state.inflight, &j.key, Vec::new(), &j.replay);
    let replay = j.replay.clone();
    let mut client = replay.subscribe().await;
    let Worker {
        to_hub,
        mut from_hub,
        ..
    } = if at == At::Before {
        state.queue.lock().await.push_back(j);
        worker(&state, "w").await
    } else {
        staging_build(&state, j).await
    };
    keep_alive(&to_hub);
    let conns = &*conns;
    // Every row's attempts come within ten minutes: a query unanswered
    // in ANSWER is asked again.
    let taken = |k| async move {
        let taken = async {
            while conns.load(Ordering::SeqCst) < k {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        let in_time = tokio::time::timeout(Duration::from_mins(10), taken).await;
        assert!(in_time.is_ok(), "row {n}: not {k} attempts in ten minutes");
    };
    let leaves = match comes {
        Comes::GivenUp(k) => Some(k),
        Comes::Dropped | Comes::Kept => Some(1),
        Comes::Fails(_) | Comes::Waits => None,
    };
    let (_quiet, quiet) = mpsc::channel(1);
    let mut _wanting = None;
    if let Some(k) = leaves {
        taken(k).await;
        _wanting = Some(wanting_a_slot(&state, 9).await);
        drop(mem::replace(&mut client, quiet));
    }
    let listed = || state.inflight.lock().unwrap().by_key.contains_key("k0");
    match comes {
        Comes::Fails(says) => {
            let error = error_of(&mut client).await;
            assert!(error.contains(says), "row {n}: {error}");
            let (_quiet, mut none) = mpsc::channel(1);
            let cancel = verdict_within(Duration::from_secs(1), &mut from_hub, &mut none).await;
            assert_eq!(cancel.as_deref(), Some("worker told to cancel"), "row {n}");
        }
        Comes::Waits => {
            taken(3).await;
            let (notes, error) = told(&mut client);
            assert_eq!((notes.len(), error), (1, None), "row {n}");
        }
        Comes::GivenUp(_) => {
            let wait = FLOOR + Duration::from_secs(6);
            let verdict = verdict_within(wait, &mut from_hub, &mut client).await;
            assert_eq!(verdict.as_deref(), Some("worker told to cancel"), "row {n}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            let queue = state.queue.lock().await;
            let queued: Vec<&str> = queue.iter().map(|j| j.key.as_str()).collect();
            assert_eq!((listed(), queued), (false, vec!["k9"]), "row {n}");
        }
        Comes::Dropped => {
            let gone = tokio::time::timeout(FLOOR - Duration::from_secs(1), async {
                while listed() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            assert!(gone.await.is_ok(), "row {n}: not dropped within FLOOR");
        }
        Comes::Kept => {
            tokio::time::sleep(FLOOR / 2).await;
            let mut back = replay.subscribe().await;
            let verdict = verdict_within(Duration::from_mins(1), &mut from_hub, &mut back).await;
            assert_eq!(verdict, None, "row {n}");
        }
    }
    let failed = u64::from(matches!(comes, Comes::Fails(_)));
    let count = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let m = &state.metrics;
    assert_eq!(
        (count(&m.failed), count(&m.requeued)),
        (failed, 0),
        "row {n}"
    );
}

/// A build backing off holds no connection to the daemon, so the other
/// builds' operations need not wait for its next attempt.
#[tokio::test(start_paused = true)]
async fn a_build_backing_off_holds_no_daemon_connection() {
    let log = Captured::default();
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Daemon::Drops, 1);
    let j = with_input(0);
    let _client = j.replay.subscribe().await;
    let _worker = staging_build(&state, j).await;
    let failed = tokio::time::timeout(Duration::from_hours(1), async {
        while !log.has(&["querying path info", "failures=1", "retrying"]) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    assert!(failed.await.is_ok(), "no failed query");
    // The first backoff is 1 s; this looks within 10 ms of its start.
    let (_, active, _) = state.daemon_pool.stats().await;
    assert_eq!(active, 0, "the connection is held through the backoff");
}

/// Waiting for a free connection, all of them busy, has no end of its
/// own: past DAEMON_ANSWER the clients are told, once, and the attempt
/// is neither given up nor retried, which would put the build at the
/// back of the pool's queue.
#[tokio::test]
async fn a_wait_for_a_free_daemon_connection_is_told() {
    let log = Captured::default();
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Daemon::Silent, 1);
    // In real time: paused, the clock would jump to the handshake's
    // timeout, the only timer, at its first message.
    let _busy = state.daemon_pool.acquire().await.unwrap();
    tokio::time::pause();
    let j = with_input(0);
    let mut client = j.replay.subscribe().await;
    let _worker = staging_build(&state, j).await;
    tokio::time::sleep(3 * DAEMON_ANSWER + Duration::from_secs(1)).await;
    let (notes, error) = told(&mut client);
    assert_eq!((notes.len(), error), (1, None), "{notes:?}");
    assert!(!log.has(&["retrying"]), "the wait for a connection ended");
}

/// Each wait is told once per dispatch, and telling one does not
/// silence another: a build told its hub's daemon does not answer,
/// requeued and then held for want of a worker, is told that too.
#[tokio::test]
async fn a_build_is_told_of_each_wait_once_per_dispatch() {
    let replay = Replay::default();
    let mut client = replay.subscribe().await;
    for wait in [Wait::Daemon, Wait::Daemon, Wait::Worker, Wait::Worker] {
        replay.tell_waiting(wait, "note\n".into()).await;
    }
    replay
        .publish(attach_event::Event::Dispatched("w".into()))
        .await;
    replay.tell_waiting(Wait::Daemon, "note\n".into()).await;
    let mut kinds = Vec::new();
    while let Ok(Ok(AttachEvent { event: Some(e) })) = client.try_recv() {
        kinds.push(match e {
            attach_event::Event::Log(_) => "note",
            attach_event::Event::Dispatched(_) => "dispatched",
            _ => "other",
        });
    }
    assert_eq!(kinds, ["note", "note", "dispatched", "note"]);
}
