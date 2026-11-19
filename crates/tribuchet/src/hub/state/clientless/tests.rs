//! Which clientless builds the decider gives up.

use std::time::Duration;

use std::time::Instant as Now;

use super::*;
use crate::hub::tests::job;
use crate::hub::{Admission, Loss};
use Queued::{Clientless, Held, Waiting};

/// A build dispatched to worker `on`, `ran` seconds ago; its clients
/// gone `away` seconds ago (None: it has one).
#[derive(Clone, Copy)]
struct Run {
    on: u64,
    ran: u64,
    away: Option<u64>,
}

fn gone(on: u64, ran: u64, away: u64) -> Run {
    Run {
        on,
        ran,
        away: Some(away),
    }
}

fn here(on: u64, ran: u64) -> Run {
    Run {
        away: None,
        ..gone(on, ran, 0)
    }
}

/// Each worker's slots, and whether one of them is free.
type Workers = &'static [(u32, bool)];

const ONE: Workers = &[(1, false)];
const TWO: Workers = &[(2, false)];
const PAIR: Workers = &[(1, false), (1, false)];
const FREE: Workers = &[(1, false), (1, true)];

const HOUR: u64 = 3600;

/// A queued build: with a client, without, or with a client and its key
/// held by a worker.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Queued {
    Waiting,
    Clientless,
    Held,
}

const WAITING: &[Queued] = &[Waiting];

/// Which builds the decider gives up: once their clients have been gone
/// FLOOR, for waiting work (a queued build with a client that no worker
/// with a free slot serves and none holds, or one staged beyond its
/// worker's slots), the one with the least work sunk first, then the one
/// longest gone, no more than that work needs, hub-wide; and any on a
/// worker gone. Each row: the workers' slots (and a free one), the
/// builds dispatched, those queued.
#[tokio::test(start_paused = true)]
async fn the_decider_gives_up_what_waiting_work_needs() {
    let rows: &[(Workers, &[Run], &[Queued], &[usize])] = &[
        (ONE, &[gone(0, 3 * HOUR, 3 * HOUR - 60)], &[], &[]),
        (ONE, &[gone(0, 60, 14)], WAITING, &[0]),
        (ONE, &[gone(0, 60, 13)], WAITING, &[]),
        (ONE, &[here(0, 60)], WAITING, &[]),
        (ONE, &[gone(0, 60, 14)], &[Clientless, Held], &[]),
        (FREE, &[gone(0, 60, 14)], WAITING, &[]),
        // A build with a client staged beyond its worker's two slots.
        (TWO, &[gone(0, 30, 14), here(0, 30), here(0, 30)], &[], &[0]),
        // The one short of FLOOR does not shield the one past it.
        (TWO, &[gone(0, 74, 14), gone(0, 14, 6)], WAITING, &[0]),
        // One waiting build gives up one build, the one that ran least,
        // hub-wide.
        (PAIR, &[gone(0, 601, 600), gone(1, 600, 600)], WAITING, &[1]),
        (TWO, &[gone(0, 20, 20), gone(0, 3020, 20)], WAITING, &[0]),
        // As much run: the one longest gone.
        (TWO, &[gone(0, HOUR, 20), gone(0, HOUR, 30)], WAITING, &[1]),
        // A chain's link 590 s in, gone 55 s.
        (ONE, &[gone(0, 645, 55)], WAITING, &[0]),
        // Two waiting builds use both slots.
        (
            TWO,
            &[gone(0, 3020, 20), gone(0, 20, 20)],
            &[Waiting, Waiting],
            &[0, 1],
        ),
        // Its worker gone.
        (&[], &[gone(7, 60, 14)], &[], &[0]),
    ];
    for (n, &(workers, runs, queued, given_up)) in rows.iter().enumerate() {
        assert_eq!(decided(workers, runs, queued).await, given_up, "row {n}");
    }
}

/// The builds of `runs` that the decider gives up, with `workers` and
/// `queued`.
async fn decided(workers: &[(u32, bool)], runs: &[Run], queued: &[Queued]) -> Vec<usize> {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    for (id, &(slots, free)) in (0..).zip(workers) {
        let systems = [("x86_64-linux".to_owned(), HashSet::new())].into();
        let connected_at = Now::now();
        let (name, max_jobs) = (format!("w{id}"), slots);
        let caps = WorkerCaps {
            name,
            connected_at,
            systems,
            max_jobs,
        };
        state.worker_caps.lock().unwrap().insert(id, caps);
        let free = Admission {
            free: u32::from(free),
            ..Default::default()
        };
        state
            .placement
            .lock()
            .unwrap()
            .open(id, Arc::new(StdMutex::new(free)));
    }
    let mut clients = Vec::new();
    for (n, q) in queued.iter().enumerate() {
        let j = job(100 + n, Arc::new(Replay::default()));
        if *q == Queued::Held {
            state.hold(0, [j.key.clone()].into(), false);
        }
        if *q != Queued::Clientless {
            clients.push(j.replay.subscribe().await);
        }
        state.queue.lock().await.push_back(j);
    }
    let (mut jobs, mut run_clients) = (Vec::new(), Vec::new());
    for b in 0..runs.len() {
        let j = job(b, Arc::new(Replay::default()));
        run_clients.push(Some(j.replay.subscribe().await));
        jobs.push(j);
    }
    // Each build starts `ran` before the decision, and its clients leave
    // `away` before it, in that order.
    let end = runs.iter().map(|r| r.ran).max().unwrap_or(0);
    let mut events: Vec<(u64, bool, usize)> = Vec::new();
    for (b, r) in runs.iter().enumerate() {
        events.push((end - r.ran, false, b));
        events.extend(r.away.map(|a| (end - a, true, b)));
    }
    events.sort_unstable();
    let t0 = Instant::now();
    let mut listed = Vec::new();
    for (at, leaves, b) in events {
        tokio::time::advance(t0 + Duration::from_secs(at) - Instant::now()).await;
        if leaves {
            drop(run_clients[b].take());
            jobs[b].replay.checked(false, Instant::now());
        } else {
            listed.push(state.list_dispatch(&jobs[b], runs[b].on));
        }
    }
    tokio::time::advance(t0 + Duration::from_secs(end) - Instant::now()).await;
    state.give_up_clientless().await;
    let given_up = |b: &usize| state.given_up(&jobs[*b].id).is_cancelled();
    (0..jobs.len()).filter(given_up).collect()
}

/// A build's absence runs from its clients leaving, across its
/// dispatches, and a resume by the worker that lost it keeps its run; a
/// fresh dispatch starts one. The hold does not start again at each
/// dispatch.
#[tokio::test(start_paused = true)]
async fn a_resume_keeps_the_run_and_the_absence() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    let mut j = job(0, Arc::new(Replay::default()));
    let first = state.list_dispatch(&j, 1);
    let started = state.dispatched.lock().unwrap()[&j.id].started;
    j.replay.checked(false, Instant::now());
    let left = j.replay.clientless();
    drop(first);
    tokio::time::advance(Duration::from_secs(8)).await;
    let until = Now::now();
    j.loss = Some(Loss {
        worker: "w".into(),
        session: 1,
        until,
    });
    let _again = state.list_dispatch(&j, 2);
    let resumed = state.dispatched.lock().unwrap()[&j.id].started;
    j.replay.checked(false, Instant::now());
    assert_eq!((resumed, j.replay.clientless()), (started, left));
    j.loss = None;
    let _fresh = state.list_dispatch(&j, 3);
    let fresh = state.dispatched.lock().unwrap()[&j.id].started;
    assert_ne!(fresh, started);
}

/// The decider never holds the hub's queue while it waits on a queued
/// build's replay: a client that stopped reading holds that replay's lock
/// for SUB_STALL in publish (as hold_unservable's note to such a build
/// does), and take_job, requeue and submit all need the queue, which
/// must not wait the 60 s with it.
#[tokio::test(start_paused = true)]
async fn the_decider_never_holds_the_queue_across_a_replay_wait() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let replay = Arc::new(Replay::default());
    // A client that never reads: its channel is full after SUB_CHANNEL_SLACK.
    let _client = replay.subscribe().await;
    for _ in 0..SUB_CHANNEL_SLACK {
        replay
            .publish(attach_event::Event::Log(b"x".to_vec()))
            .await;
    }
    state.queue.lock().await.push_back(job(0, replay.clone()));
    // The next event waits SUB_STALL for it, holding the replay's lock.
    let publisher = replay.clone();
    let note = attach_event::Event::Log(b"note".to_vec());
    let publishing = tokio::spawn(async move { publisher.publish(note).await });
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    let busy = tokio::time::timeout(Duration::ZERO, replay.has_subscribers()).await;
    assert!(busy.is_err(), "publish holds the replay's lock");
    let decider = state.clone();
    let deciding = tokio::spawn(async move { decider.waiting_queued().await.len() });
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    let t0 = Instant::now();
    drop(state.queue.lock().await);
    let waited = t0.elapsed();
    publishing.await.unwrap();
    assert_eq!(deciding.await.unwrap(), 1, "the build waits for a slot");
    assert!(
        waited < Duration::from_secs(1),
        "take_job waited {waited:?} for the queue behind one stalled client"
    );
}
