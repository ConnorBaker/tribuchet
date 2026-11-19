//! The hub's own nix-daemon failing a build's staging, end to end, against
//! fake daemons (fake_daemon.rs). Which failure is which, and when a run
//! of them fails the build, is relay/daemon/tests.rs's: these rows show
//! that a staging build, before its assignment goes out and after, and
//! its supervision, keep to it, and heed its client leaving.

use super::fake_daemon::{Daemon, cached_input, error_of, hub_scripted, hub_with, told, worker};
use super::*;
use At::{After, Before};
use Comes::{Declined, Dropped, Fails, GivenUp, Holds, Kept, Waits};
use relay::{DAEMON_ANSWER, DaemonEvidence};
use state::{FLOOR, Inflight, Wait};

use std::sync::atomic::AtomicU64;

use crate::proto::{DECLINE_EXIT_CODE, Need};

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
    /// It fails, its client, whose Nix cannot build it, told this, and
    /// its worker told to cancel.
    Fails(&'static str),
    /// Where it would fail, its client, whose Nix builds it, is declined
    /// instead (SEMANTICS-7 L12), and its worker told to cancel.
    Declined,
    /// It waits through three failed attempts, its client told once.
    Waits,
    /// It never fails, its client, whose Nix cannot build it, told this;
    /// held out of placement, requeued, if the flag says so (cut-over 10).
    Holds(&'static str, bool),
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

/// What a client waiting on the hub's daemon, or for an input, is told.
const DAEMON: &str = "waiting for the hub's nix-daemon";
const LACKS: &str = "the hub's store lacks";

/// A journal in which Nix's daemon reaped three workers that crashed.
const CRASHES: &str = "daemon worker 41 crashed\ndaemon worker 42 crashed\n\
    daemon worker 43 crashed\n";

/// A daemon failure whose run reaches θ with a fact about the hub fails the
/// build naming the cause, but declines a client whose Nix builds it. A
/// path the daemon lacks, or crashes, are no such fact: they decline that
/// client too, and hold one whose Nix cannot. Any other waits, before the assignment and after, told
/// once, no loss counted. A build whose client left is given up while it
/// waits or backs off, kept if the client is back within FLOOR, and not
/// given up at its first failed attempt with no client.
#[test]
fn a_staging_build_keeps_to_its_daemons_verdict() {
    let interrupted = Daemon::Says("interrupted by the user");
    let refusal = Daemon::Says("path '\x1b[35;1m/nix/store/x\x1b[0m' is not valid");
    let rows = [
        (Daemon::Drops, Some(CRASHES), After, Holds(DAEMON, false)),
        // A refusal, and a handshake the hub cannot speak: the daemon's words.
        (refusal, None, After, Fails("is not valid")),
        (Daemon::Foreign, None, After, Fails("wrong magic")),
        (Daemon::Lacks, None, After, Holds(LACKS, true)),
        (Daemon::Lacks, None, Before, Holds(LACKS, true)),
        (Daemon::Lacks, None, Before, Declined),
        (Daemon::Lacks, None, After, Declined),
        (refusal, None, After, Declined),
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
    let can_local = !matches!(comes, Fails(_) | Holds(..));
    let mut client = (replay.subscribe_as(shadow::Client::untracked(can_local))).await;
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
        Comes::Fails(_) | Comes::Declined | Comes::Waits | Comes::Holds(..) => None,
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
        Comes::Fails(_) | Comes::Declined => {
            if let Comes::Fails(says) = comes {
                let error = error_of(&mut client).await;
                assert!(error.contains(says), "row {n}: {error}");
            } else {
                let code = exit_of(&mut client).await;
                assert_eq!(code, Some(DECLINE_EXIT_CODE), "row {n}");
            }
            let (_quiet, mut none) = mpsc::channel(1);
            let cancel = verdict_within(Duration::from_secs(1), &mut from_hub, &mut none).await;
            assert_eq!(cancel.as_deref(), Some("worker told to cancel"), "row {n}");
        }
        Comes::Waits => {
            taken(3).await;
            let (notes, error) = told(&mut client).await;
            assert_eq!((notes.len(), error), (1, None), "row {n}");
        }
        Comes::Holds(says, held) => holds(n, &state, &mut client, (says, held)).await,
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
    let declined = u64::from(matches!(comes, Comes::Declined));
    let count = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let m = &state.metrics;
    assert_eq!(
        (count(&m.failed), count(&m.declined), count(&m.requeued)),
        (failed, declined, 0),
        "row {n}"
    );
}

/// Row `n`'s client is told `says` and nothing fails it for ten minutes;
/// its build is held out of placement iff `held`.
async fn holds(
    n: usize,
    state: &HubState,
    client: &mut mpsc::Receiver<Result<AttachEvent, Status>>,
    (says, held): (&str, bool),
) {
    let (mut notes, mut error) = (Vec::new(), None);
    let ten = Duration::from_mins(10);
    while let Ok(Some(Ok(AttachEvent { event: Some(e) }))) =
        tokio::time::timeout(ten, client.recv()).await
    {
        match e {
            attach_event::Event::Log(l) => notes.push(String::from_utf8(l).unwrap()),
            attach_event::Event::Error(e) => error = Some(e),
            _ => {}
        }
    }
    assert_eq!(error, None, "row {n}");
    assert!(notes.iter().any(|s| s.contains(says)), "row {n}: {notes:?}");
    let queue = state.queue.lock().await;
    assert_eq!(queue.iter().any(|j| j.replay.held()), held, "row {n}");
}

/// A build held for an input its hub's store lacks waits for a client of
/// it, whose Nix makes its inputs valid: no take has it, with no timer,
/// until one joins; one joining before the hold keeps it from holding; and
/// once its clients left, a take ends it (SEMANTICS-7 cut-over 10).
#[tokio::test(start_paused = true)]
async fn a_build_held_for_an_input_waits_for_a_client_to_join() {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Daemon::Lacks, 8);
    let j = with_input(0);
    let replay = j.replay.clone();
    let first = replay.subscribe_as(shadow::Client::untracked(false)).await;
    let _w = staging_build(&state, j).await;
    let held = || async { state.queue.lock().await.iter().any(|j| j.replay.held()) };
    for _ in (0..600).take_while(|_| !state.queue.try_lock().is_ok_and(|q| !q.is_empty())) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(held().await, "not held in ten minutes");
    let other = caps("w2");
    tokio::time::sleep(Duration::from_hours(24)).await;
    assert!(
        state.take_job(&other, None).await.is_none(),
        "taken while held"
    );
    let since = replay.joins();
    let second = replay.subscribe_as(shadow::Client::untracked(false)).await;
    let job = state
        .take_job(&other, None)
        .await
        .expect("held after a client joined");
    assert!(
        !job.replay.hold(since),
        "held, a client having joined since"
    );
    assert!(job.replay.hold(job.replay.joins()));
    state.queue.lock().await.push_back(job);
    drop((first, second));
    assert!(state.take_job(&other, None).await.is_none());
    assert!(
        state.queue.lock().await.is_empty(),
        "a clientless held build kept"
    );
}

/// The exit code `client` gets, within an hour; none if its stream ends.
async fn exit_of(client: &mut mpsc::Receiver<Result<AttachEvent, Status>>) -> Option<i32> {
    let code = tokio::time::timeout(Duration::from_hours(1), async {
        while let Some(Ok(AttachEvent { event })) = client.recv().await {
            if let Some(attach_event::Event::ExitCode(code)) = event {
                return Some(code);
            }
        }
        None
    });
    code.await.expect("no exit code within an hour")
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
    let (notes, error) = told(&mut client).await;
    assert_eq!((notes.len(), error), (1, None), "{notes:?}");
    assert!(!log.has(&["retrying"]), "the wait for a connection ended");
}

/// Each wait is told once per dispatch, and telling one does not
/// silence another: a build told its hub's daemon does not answer,
/// requeued and then held for want of a worker, is told that too.
#[tokio::test]
async fn a_build_is_told_of_each_wait_once_per_dispatch() {
    let replay = Arc::new(Replay::default());
    let mut client = replay.subscribe().await;
    for wait in [Wait::Daemon, Wait::Daemon, Wait::Worker, Wait::Worker] {
        replay.tell_waiting(wait, "note\n".into()).await;
    }
    replay
        .publish(attach_event::Event::Dispatched("w".into()))
        .await;
    replay.tell_waiting(Wait::Daemon, "note\n".into()).await;
    tokio::task::yield_now().await;
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
