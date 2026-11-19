//! Placement: a build is left to a less full worker with an idle agent,
//! by what it and the builds there are expected to use (placement::Rooms).
//! The committed cores compared are sums of whole numbers: exact.
#![allow(clippy::float_cmp)]

use super::*;
use std::pin::{Pin, pin};

use tokio::sync::futures::Notified;

use crate::proto::BuildResult;
use placement::{Demand, Expected, GRACE, Level, Room, Rooms, Size};

const GIB: f64 = 1_073_741_824.0;

/// Worker `name` serving x86_64-linux with two agents, `cores` cores and
/// `gib` GiB for its builds.
pub(super) fn sized_worker(state: &Arc<HubState>, name: &str, cores: u32, gib: u64) -> Worker {
    let register = Register {
        worker_name: name.into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec!["big-parallel".into()],
        }],
        resumable_keys: vec![],
        max_jobs: 2,
        cores: Some(cores),
        build_memory: Some(gib << 30),
    };
    let (out_tx, from_hub) = mpsc::channel(256);
    let (to_hub, in_rx) = mpsc::channel(64);
    let hub = tokio::spawn(worker_loop(state.clone(), register, out_tx, in_rx));
    Worker {
        to_hub,
        from_hub,
        hub,
    }
}

/// A declaration of `capacity`, `free_slots` of it free slots, having
/// seen `seen` assignments.
pub(super) fn declare(capacity: u32, free_slots: u32, seen: u64) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::RequestJob(RequestJob {
            capacity,
            assignments: Some(seen),
            free_slots: Some(free_slots),
        })),
    }
}

/// Expected to use `cores` cores and `gib` GiB, as its key learned.
fn expecting(cores: f64, gib: f64) -> Expected {
    Expected {
        demand: Demand {
            cores,
            memory: gib * GIB,
        },
        secs: None,
        level: Level::Key,
        oom: 0,
    }
}

/// Queue build `n`, named `name`, expected to use `e`, with a client.
async fn queue(state: &HubState, n: usize, name: &str, e: Expected) -> Client {
    let mut j = job(n, Arc::new(Replay::default()));
    j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
    j.expected = e;
    let client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    state.notify.notify_waiters();
    client
}

type Client = mpsc::Receiver<Result<AttachEvent, Status>>;

/// The cores the builds handed to every session are expected to use.
fn committed_cores(state: &HubState) -> f64 {
    let placement = state.placement.lock().unwrap();
    (placement.sessions.values()).map(|s| s.load().cores).sum()
}

/// A declaration's idle agents are its free slots less the assignments
/// it had not seen, never the lookahead; a worker predating free_slots
/// has none.
#[test]
fn idle_agents_are_free_slots_not_the_lookahead() {
    let rows = [
        (0, 3, Some(2), 2),
        (0, 1, Some(0), 0),
        (1, 3, Some(2), 1),
        (2, 3, Some(2), 0),
        (0, 3, None, 0),
    ];
    for (sent, capacity, free_slots, idle) in rows {
        let mut a = Admission {
            sent,
            ..Admission::default()
        };
        a.declare(&RequestJob {
            capacity,
            assignments: Some(0),
            free_slots,
        });
        assert_eq!(
            a.idle(),
            idle,
            "{sent} unseen; {capacity} declared, {free_slots:?} free"
        );
    }
}

/// A worker's size is what it registered: its cores, else its agents
/// (a worker predating the field), at least one; and its builds' memory.
/// One of none, idle, would be NaN full, which sorts first on x86: the
/// last row, its rooms sized by size_of, fails only with the size rows.
#[test]
fn a_size_is_the_registered_cores_and_memory() {
    let size = |cores, max_jobs, build_memory| {
        let mut r = Register::default();
        (r.cores, r.max_jobs, r.build_memory) = (cores, max_jobs, build_memory);
        placement::size_of(&r)
    };
    let big = size(Some(32), 3, Some(1 << 30));
    assert_eq!((big.cores, big.memory), (32.0, Some(GIB)));
    assert_eq!(size(None, 3, None).cores, 3.0);
    assert_eq!(size(Some(0), 3, None).cores, 1.0, "no cores reported");
    assert_eq!(size(None, 0, None).cores, 1.0, "no agents at registration");
    assert_eq!(size(None, 3, Some(0)).memory, Some(0.0), "0 bytes: no room");
    let room = |(id, size)| Room {
        id,
        idle: 1,
        size,
        load: Demand::default(),
    };
    let rooms = [(1, size(Some(4), 2, None)), (2, size(None, 0, None))].map(room);
    let mut rooms = Rooms {
        asker: 1,
        rooms: rooms.to_vec(),
    };
    let got = rooms.passes(Demand::default(), &mut None, |_| true, Instant::now());
    assert_eq!(got, None, "an idle build passed to the worker of no cores");
}

/// A build its worker resumes by key counts against that worker too,
/// and stops counting once its supervision ends in a hub-side error:
/// here, the worker no longer lists it, while its session lives.
#[tokio::test(start_paused = true)]
async fn a_resumed_build_counts_until_its_supervision_ends() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut j = job(0, Arc::new(Replay::default()));
    j.expected = expecting(3.0, 1.0);
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w", 1, &["k0"]);
    let build_id = w.assignment().await.expect("not assigned").build_id;
    w.send(resumed(&build_id, false)).await;
    assert_eq!(committed_cores(&state), 3.0, "resumed");
    for keys in [vec!["k0".to_owned()], vec![]] {
        let listing = Heartbeat {
            resumable_keys: keys,
            ..Default::default()
        };
        w.send(worker_message::Msg::Heartbeat(listing)).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert_eq!(state.metrics.requeued.load(Ordering::Relaxed), 1);
    assert_eq!(committed_cores(&state), 0.0, "requeued");
    w.hub.abort();
}

/// A build whose assignment is never sent stops counting against its
/// worker: here the hub cannot prepare it (its daemon does not answer
/// for a cached input) and gives it up, its client gone, for a build that
/// wants the slot.
#[tokio::test(start_paused = true)]
async fn a_build_never_sent_stops_counting() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let input = format!("/nix/store/{H}-cached");
    let mut unsendable = job(0, Arc::new(Replay::default()));
    (unsendable.req.input_paths, unsendable.expected) = (vec![input.clone()], expecting(3.0, 1.0));
    let mut client = unsendable.replay.subscribe().await;
    let wanting = job(1, Arc::new(Replay::default()));
    let _wanting = wanting.replay.subscribe().await;
    state.queue.lock().await.extend([unsendable, wanting]);
    state
        .chunks
        .store_recipe(input, vec![0], Arc::new(Vec::new()));
    let w = sized_worker(&state, "w", 4, 8);
    keep_alive(&w.to_hub);
    w.to_hub.send(declare(1, 1, 0)).await.unwrap();
    while let Some(Ok(AttachEvent { event })) = client.recv().await {
        if matches!(event, Some(attach_event::Event::Dispatched(_))) {
            break;
        }
    }
    assert_eq!(committed_cores(&state), 3.0, "dispatched");
    drop(client);
    tokio::time::sleep(FLOOR + Duration::from_secs(6)).await;
    assert_eq!(committed_cores(&state), 0.0, "given up unsent");
}

/// The dispatch line says what a build declares and is expected to use,
/// how many other workers that serve it have an idle agent, and the
/// worker's size and expected load with it; a load over the worker's
/// memory is said once more, and the build is placed all the same.
#[tokio::test]
async fn the_dispatch_line_names_the_expectation_and_the_room() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w1 = sized_worker(&state, "w1", 4, 8);
    w1.to_hub.send(declare(3, 2, 0)).await.unwrap();
    let _c0 = queue(&state, 0, "big-0", expecting(2.0, 6.0)).await;
    assert!(w1.assignment().await.is_some(), "big-0 not assigned");
    let line = log.line(r#"drv="big-0""#);
    for field in [
        r#"features="""#,
        "idle_elsewhere=0",
        r#"learned="key""#,
        "expect_mem=6442450944",
        "expect_cores=2.0",
        "load_mem=6442450944",
        "load_cores=2.0",
        "memory=8589934592",
        "cores=4.0",
    ] {
        assert!(line.contains(field), "{field} missing from {line}");
    }
    assert!(!log.has(&["placed over predicted memory"]), "big-0 fits");
    let _c1 = queue(&state, 1, "big-1", expecting(1.0, 6.0)).await;
    assert!(w1.assignment().await.is_some(), "big-1 held back");
    assert!(
        log.has(&[
            "placed over predicted memory",
            "load_mem=12884901888",
            "memory=8589934592"
        ]),
        "big-1 over the worker's memory, unsaid"
    );
}

/// Worker `name`, as take_job sees it with no session loop: its
/// declaration (capacity, free slots), its size, and the cores and GiB
/// its builds in flight are expected to use. Returns its registration,
/// whose id is its session's, and its capabilities.
fn seated(
    state: &Arc<HubState>,
    name: &str,
    declared: (u32, u32),
    size: Size,
    load: (f64, f64),
) -> (CapsGuard, WorkerCaps) {
    let caps = WorkerCaps {
        name: name.into(),
        connected_at: Instant::now(),
        systems: [(
            "x86_64-linux".to_owned(),
            ["big-parallel".to_owned()].into(),
        )]
        .into(),
        max_jobs: 2,
    };
    let guard = CapsGuard::new(state.clone(), caps.clone());
    let mut a = Admission::default();
    a.declare(&RequestJob {
        capacity: declared.0,
        assignments: Some(0),
        free_slots: Some(declared.1),
    });
    let mut placement = state.placement.lock().unwrap();
    placement.open(guard.id, Arc::new(Mutex::new(a)), size);
    let session = placement.sessions.get_mut(&guard.id).unwrap();
    let in_flight = Demand {
        cores: load.0,
        memory: load.1 * GIB,
    };
    session.committed.insert("in-flight".into(), in_flight);
    drop(placement);
    (guard, caps)
}

/// Four cores and 8 GiB.
const SMALL: Size = Size {
    cores: 4.0,
    memory: Some(8.0 * GIB),
};

fn id(n: usize) -> String {
    format!("{n:032x}")
}

/// Whether `woken`, enabled before, has been notified.
async fn notified(woken: Pin<&mut Notified<'_>>) -> bool {
    tokio::time::timeout(Duration::ZERO, woken).await.is_ok()
}

/// A worker whose cores its builds fill leaves a build that history
/// says fills them too, though it declares nothing, to a worker with an
/// idle agent and nothing in flight, and wakes it once; it takes the
/// light build behind, which is as full on either worker once the heavy
/// one is spoken for. The woken worker takes the heavy one.
#[tokio::test]
async fn a_learned_heavy_build_goes_to_the_less_full_worker() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (4.0, 1.0));
    let (b, b_caps) = seated(&state, "b", (3, 2), SMALL, (0.0, 0.0));
    let _heavy = queue(&state, 0, "heavy-0", expecting(4.0, 1.0)).await;
    let _light = queue(&state, 1, "light-1", expecting(0.1, 0.1)).await;
    for (n, took) in [(0, Some(id(1))), (1, None)] {
        let mut woken = pin!(state.notify.notified());
        woken.as_mut().enable();
        let job = state.take_job(&a_caps, Some(a.id)).await;
        assert_eq!(job.map(|j| j.id), took, "ask {n} of the full worker");
        assert_eq!(notified(woken).await, n == 0, "woken on ask {n}");
    }
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)));
}

/// The asker takes a build when no other worker with an idle agent would
/// be less full with it (a lookahead is no idle agent), and when it was
/// first passed over GRACE ago.
#[tokio::test]
async fn a_build_stays_with_the_asker_unless_another_is_less_full() {
    let ago = |d| Instant::now().checked_sub(d);
    let rows = [
        ("fuller elsewhere", (3, 2), (6.0, 1.0), None),
        ("as full elsewhere", (3, 2), (2.0, 1.0), None),
        ("lookahead elsewhere", (1, 0), (0.0, 0.0), None),
        ("grace over", (3, 2), (0.0, 0.0), ago(GRACE)),
    ];
    for (row, declared, load, deferred) in rows {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (2.0, 1.0));
        let _b = seated(&state, "b", declared, SMALL, load);
        let _client = queue(&state, 0, "build-0", expecting(1.0, 1.0)).await;
        state.queue.lock().await[0].deferred = deferred;
        let job = state.take_job(&a_caps, Some(a.id)).await;
        assert_eq!(job.map(|j| j.id), Some(id(0)), "{row}");
    }
}

/// Rooms::passes, at an explicit `now`: within GRACE of the first pass,
/// to the room with an idle agent that is least full, if less full than
/// the asker's (an asker with only its lookahead is fuller than any room
/// with an idle agent, and a room without one is none to leave it to);
/// each room spoken for, an agent and the build.
#[test]
fn a_build_is_passed_over_within_grace_to_the_least_full_room() {
    let t0 = Instant::now();
    let room = |id, idle, cores| Room {
        id,
        idle,
        size: SMALL,
        load: Demand { cores, memory: 0.0 },
    };
    let build = Demand {
        cores: 2.0,
        memory: 0.0,
    };
    let (none, short) = (
        Duration::ZERO,
        GRACE.saturating_sub(Duration::from_nanos(1)),
    );
    let rows = [
        (
            "first",
            None,
            none,
            room(1, 1, 2.0),
            vec![room(2, 1, 0.0)],
            vec![Some(2), None],
        ),
        (
            "in grace",
            Some(t0),
            short,
            room(1, 1, 2.0),
            vec![room(2, 1, 0.0)],
            vec![Some(2)],
        ),
        (
            "grace over",
            Some(t0),
            GRACE,
            room(1, 1, 2.0),
            vec![room(2, 1, 0.0)],
            vec![None],
        ),
        (
            "a tie",
            None,
            none,
            room(1, 1, 2.0),
            vec![room(2, 3, 2.0)],
            vec![None],
        ),
        (
            "spoken for",
            None,
            none,
            room(1, 1, 3.0),
            vec![room(2, 3, 0.0)],
            vec![Some(2), Some(2), None],
        ),
        (
            "least full",
            None,
            none,
            room(1, 1, 8.0),
            vec![room(2, 1, 1.0), room(3, 1, 0.0)],
            vec![Some(3), Some(2), None],
        ),
        (
            "only a lookahead",
            None,
            none,
            room(1, 0, 0.0),
            vec![room(2, 1, 3.0)],
            vec![Some(2)],
        ),
        (
            "no idle agent anywhere",
            None,
            none,
            room(1, 0, 4.0),
            vec![room(2, 0, 0.0)],
            vec![None],
        ),
    ];
    for (row, first, at, mine, others, passes) in rows {
        let mut rooms = Rooms {
            asker: 1,
            rooms: [vec![mine], others].concat(),
        };
        for (n, want) in passes.into_iter().enumerate() {
            let mut since = first;
            let got = rooms.passes(build, &mut since, |_| true, t0 + at);
            assert_eq!(got, want, "{row}, pass {n}");
            let kept = first.or(got.map(|_| t0 + at));
            assert_eq!(since, kept, "{row}, pass {n}: when first passed over");
        }
    }
}

/// Fullness weighs a room's size and the build, its cores and memory
/// both (the rows say how), and memory only where every room compared,
/// the asker's too, reported its own.
#[test]
fn fullness_weighs_size_and_memory_where_every_room_reported_it() {
    let room = |id, cores, memory: Option<f64>, used, gib| Room {
        id,
        idle: 1,
        size: Size {
            cores,
            memory: memory.map(|m| m * GIB),
        },
        load: Demand {
            cores: used,
            memory: gib * GIB,
        },
    };
    let build = Demand {
        cores: 0.5,
        memory: 2.0 * GIB,
    };
    let (a4, a32) = (room(1, 4.0, None, 2.0, 0.0), room(1, 32.0, None, 2.0, 0.0));
    let (b4, b32) = (room(2, 4.0, None, 2.0, 0.0), room(2, 32.0, None, 2.0, 0.0));
    let full = room(1, 4.0, Some(8.0), 1.0, 7.0);
    let (a64, b8) = (room(1, 64.0, None, 3.0, 0.0), room(2, 8.0, None, 0.0, 0.0));
    let (a_half, b_busy) = (
        room(1, 4.0, Some(8.0), 0.0, 3.0),
        room(2, 4.0, Some(8.0), 1.5, 0.0),
    );
    let (b_free, b_full) = (
        room(2, 4.0, Some(8.0), 2.0, 0.0),
        room(2, 4.0, Some(8.0), 1.0, 7.0),
    );
    let rows = [
        ("4 cores ask", a4.clone(), b32, Some(2)),
        ("32 cores ask", a32, b4.clone(), None),
        ("reported", full.clone(), b_free, Some(2)),
        ("not reported", full, b4, None),
        ("asker reported none", a4, b_full, Some(2)),
        ("this one counted", a64, b8, None),
        ("this one's memory counted", a_half, b_busy, Some(2)),
    ];
    for (row, mine, other, want) in rows {
        let mut rooms = Rooms {
            asker: 1,
            rooms: vec![mine, other],
        };
        let got = rooms.passes(build, &mut None, |_| true, Instant::now());
        assert_eq!(got, want, "{row}");
    }
}

/// A requeued build is passed over afresh: its grace starts again.
#[tokio::test]
async fn a_requeued_build_is_passed_over_afresh() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (4.0, 1.0));
    let _b = seated(&state, "b", (3, 2), SMALL, (0.0, 0.0));
    let mut j = job(0, Arc::new(Replay::default()));
    j.features = vec!["big-parallel".into()];
    j.deferred = Instant::now().checked_sub(GRACE * 5);
    let _client = j.replay.subscribe().await;
    state.requeue(j).await;
    let taken = state.take_job(&a_caps, Some(a.id)).await;
    assert!(taken.is_none(), "taken: its grace counted from before");
}

/// A build counts against its worker from its dispatch until its
/// supervision settles it.
#[tokio::test]
async fn a_build_counts_against_its_worker_until_it_settles() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = sized_worker(&state, "w1", 4, 8);
    w.to_hub.send(declare(3, 2, 0)).await.unwrap();
    let mut client = queue(&state, 0, "heavy-0", expecting(3.0, 1.0)).await;
    let build_id = w.assignment().await.expect("not assigned").build_id;
    assert_eq!(committed_cores(&state), 3.0, "dispatched");
    let result = BuildResult {
        build_id,
        exit_code: 1,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    assert_eq!(committed_cores(&state), 0.0, "settled");
}

/// A queued build dropped during the scan, its client gone, makes take_job
/// scan again: the build it left to another worker stays left, and the
/// asker takes the light build behind it.
#[tokio::test]
async fn a_rescan_still_leaves_the_build() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (4.0, 1.0));
    let _b = seated(&state, "b", (3, 2), SMALL, (0.0, 0.0));
    let _heavy = queue(&state, 0, "heavy-0", expecting(4.0, 1.0)).await;
    drop(queue(&state, 1, "light-1", expecting(0.1, 0.1)).await);
    let _light = queue(&state, 2, "light-2", expecting(0.1, 0.1)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(
        job.map(|j| j.id),
        Some(id(2)),
        "the rescan took the heavy build"
    );
}

/// A build is left only to a worker that serves it: one without
/// big-parallel has idle agents and nothing in flight, and is no room
/// for a build that declares it.
#[tokio::test]
async fn a_build_is_not_left_to_a_worker_that_does_not_serve_it() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (4.0, 1.0));
    let b = register(&state, "b", &[]);
    let mut idle = Admission::default();
    idle.declare(&RequestJob {
        capacity: 3,
        assignments: Some(0),
        free_slots: Some(2),
    });
    (state.placement.lock().unwrap()).open(b.id, Arc::new(Mutex::new(idle)), SMALL);
    let mut j = job(0, Arc::new(Replay::default()));
    (j.features, j.expected) = (vec!["big-parallel".into()], expecting(4.0, 1.0));
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(
        job.map(|j| j.id),
        Some(id(0)),
        "left to a worker without big-parallel"
    );
}

/// A session leaves placement when it ends, with its room.
#[tokio::test]
async fn a_session_that_ends_leaves_placement() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let b = sized_worker(&state, "b", 4, 8);
    b.to_hub.send(declare(3, 2, 0)).await.unwrap();
    tokio::time::sleep(QUIET).await;
    let sessions = || state.placement.lock().unwrap().sessions.len();
    assert_eq!(sessions(), 1, "open");
    let Worker {
        to_hub,
        from_hub,
        hub,
    } = b;
    drop((to_hub, from_hub));
    hub.await.unwrap();
    assert_eq!(sessions(), 0, "ended");
}
