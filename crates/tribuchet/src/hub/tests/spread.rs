//! Heavy builds (placement::heavy) and the workers' room for them.

use super::*;
use std::pin::{Pin, pin};

use tokio::sync::futures::Notified;

use crate::proto::{BuildResult, Resumed};
use placement::{GRACE, Room, Spread};

/// Worker `name` serving x86_64-linux with big-parallel and two slots.
fn heavy_worker(state: &Arc<HubState>, name: &str) -> Worker {
    let register = Register {
        worker_name: name.into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec!["big-parallel".into()],
        }],
        resumable_keys: vec![],
        max_jobs: 2,
        ..Register::default()
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
/// seen no assignment.
fn declare(capacity: u32, free_slots: u32) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::RequestJob(RequestJob {
            capacity,
            assignments: Some(0),
            free_slots: Some(free_slots),
        })),
    }
}

/// Queue build `n`, named `name`, heavy or not, with a client attached.
async fn queue(state: &HubState, n: usize, name: &str, heavy: bool) -> Client {
    let mut j = job(n, Arc::new(Replay::default()));
    j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
    if heavy {
        j.features = vec!["big-parallel".into()];
    }
    let client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    state.notify.notify_waiters();
    client
}

type Client = mpsc::Receiver<Result<AttachEvent, Status>>;

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

/// A heavy build its worker resumes by key counts against that worker
/// too, and stops counting once its supervision ends in a hub-side
/// error: here, the worker no longer lists it, while its session lives.
#[tokio::test(start_paused = true)]
async fn a_resumed_heavy_build_counts_until_its_supervision_ends() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut j = job(0, Arc::new(Replay::default()));
    j.features = vec!["big-parallel".into()];
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w", 1, &["k0"]);
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let resumed = Resumed { build_id };
    w.send(worker_message::Msg::Resumed(resumed)).await;
    let heavy = || {
        let placement = state.placement.lock().unwrap();
        (placement.sessions.values())
            .map(|a| a.lock().unwrap().heavy)
            .sum::<u32>()
    };
    assert_eq!(heavy(), 1, "resumed");
    for keys in [vec!["k0".to_owned()], vec![]] {
        let listing = Heartbeat {
            resumable_keys: keys,
            ..Default::default()
        };
        w.send(worker_message::Msg::Heartbeat(listing)).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert_eq!(state.metrics.requeued.load(Ordering::Relaxed), 1);
    assert_eq!(heavy(), 0, "requeued");
    w.hub.abort();
}

/// A heavy build whose assignment is never sent stops counting against
/// its worker: here the hub cannot prepare it (its daemon does not answer
/// for a cached input) and gives it up, its client gone, for a build that
/// wants the slot.
#[tokio::test(start_paused = true)]
async fn a_heavy_build_never_sent_stops_counting() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let input = format!("/nix/store/{H}-cached");
    let mut unsendable = job(0, Arc::new(Replay::default()));
    (unsendable.req.input_paths, unsendable.features) =
        (vec![input.clone()], vec!["big-parallel".into()]);
    let mut client = unsendable.replay.subscribe().await;
    let wanting = job(1, Arc::new(Replay::default()));
    let _wanting = wanting.replay.subscribe().await;
    state.queue.lock().await.extend([unsendable, wanting]);
    state
        .chunks
        .store_recipe(input, vec![0], Arc::new(Vec::new()));
    let w = heavy_worker(&state, "w");
    keep_alive(&w.to_hub);
    w.to_hub.send(declare(1, 1)).await.unwrap();
    while let Some(Ok(AttachEvent { event })) = client.recv().await {
        if matches!(event, Some(attach_event::Event::Dispatched(_))) {
            break;
        }
    }
    let heavy = || {
        let placement = state.placement.lock().unwrap();
        (placement.sessions.values())
            .map(|a| a.lock().unwrap().heavy)
            .sum::<u32>()
    };
    assert_eq!(heavy(), 1, "dispatched");
    drop(client);
    tokio::time::sleep(FLOOR + Duration::from_secs(6)).await;
    assert_eq!(heavy(), 0, "given up unsent");
}

/// The dispatch line says what a build declares, whether it is heavy,
/// how many other workers that serve it have an idle agent, and each
/// worker's heavy builds in flight.
#[tokio::test]
async fn the_dispatch_line_counts_heavy_builds_and_idle_agents_elsewhere() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w1 = heavy_worker(&state, "w1");
    w1.to_hub.send(declare(3, 2)).await.unwrap();
    let _c0 = queue(&state, 0, "heavy-0", true).await;
    assert!(w1.assignment().await.is_some(), "heavy-0 not assigned");
    let mut w2 = heavy_worker(&state, "w2");
    w2.to_hub.send(declare(3, 2)).await.unwrap();
    tokio::time::sleep(QUIET).await;
    // Whichever takes it, the other has an idle agent.
    let _c1 = queue(&state, 1, "light-1", false).await;
    let (a1, a2) = tokio::join!(w1.assignment(), w2.assignment());
    assert!(a1.is_some() || a2.is_some(), "light-1 not assigned");
    for (drv, fields) in [
        (
            "heavy-0",
            [
                r#"features="big-parallel""#,
                "heavy=true",
                "idle_elsewhere=0",
                r#"heavy_builds="w1:1""#,
            ],
        ),
        (
            "light-1",
            [
                r#"features="""#,
                "heavy=false",
                "idle_elsewhere=1",
                r#"heavy_builds="w1:1,w2:0""#,
            ],
        ),
    ] {
        let line = log.line(&format!(r#"drv="{drv}""#));
        for field in fields {
            assert!(line.contains(field), "{field} missing from {line}");
        }
    }
}

/// Worker `name`, serving big-parallel, as take_job sees it with no
/// session loop: its declaration (capacity, free slots), and `heavy`
/// heavy builds in flight. Returns its registration, whose id is its
/// session's, and its capabilities.
fn seated(
    state: &Arc<HubState>,
    name: &str,
    declared: (u32, u32),
    heavy: u32,
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
    let mut a = Admission {
        heavy,
        ..Admission::default()
    };
    a.declare(&RequestJob {
        capacity: declared.0,
        assignments: Some(0),
        free_slots: Some(declared.1),
    });
    (state.placement.lock().unwrap()).open(guard.id, Arc::new(Mutex::new(a)));
    (guard, caps)
}

fn id(n: usize) -> String {
    format!("{n:032x}")
}

/// Whether `woken`, enabled before, has been notified.
async fn notified(woken: Pin<&mut Notified<'_>>) -> bool {
    tokio::time::timeout(Duration::ZERO, woken).await.is_ok()
}

/// A worker running a heavy build leaves the next heavy one to a worker
/// with an idle agent and fewer heavy builds, wakes it once, and takes
/// the light build behind; the woken worker takes the heavy one.
#[tokio::test]
async fn a_second_heavy_build_goes_to_a_worker_with_an_idle_agent() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), 1);
    let (b, b_caps) = seated(&state, "b", (3, 2), 0);
    let _heavy = queue(&state, 0, "heavy-0", true).await;
    let _light = queue(&state, 1, "light-1", false).await;
    for (n, took) in [(0, Some(id(1))), (1, None)] {
        let mut woken = pin!(state.notify.notified());
        woken.as_mut().enable();
        let job = state.take_job(&a_caps, Some(a.id)).await;
        assert_eq!(job.map(|j| j.id), took, "ask {n} of the worker running one");
        assert_eq!(notified(woken).await, n == 0, "woken on ask {n}");
    }
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)));
}

/// The asker takes a heavy build when no other worker has both an idle
/// agent and fewer heavy builds (a lookahead is no idle agent), when it
/// was first passed over GRACE ago, and always a light build.
#[tokio::test]
async fn a_heavy_build_stays_with_the_asker_without_room_elsewhere() {
    let ago = |d| Instant::now().checked_sub(d);
    let rows = [
        ("busy elsewhere", (3, 2), 1, true, None),
        ("lookahead elsewhere", (1, 0), 0, true, None),
        ("grace over", (3, 2), 0, true, ago(GRACE)),
        ("light", (3, 2), 0, false, None),
    ];
    for (row, declared, heavy, is_heavy, deferred) in rows {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (2, 1), 1);
        let _b = seated(&state, "b", declared, heavy);
        let _client = queue(&state, 0, "build-0", is_heavy).await;
        state.queue.lock().await[0].deferred = deferred;
        let job = state.take_job(&a_caps, Some(a.id)).await;
        assert_eq!(job.map(|j| j.id), Some(id(0)), "{row}");
    }
}

/// A requeued build is passed over afresh: its grace starts again.
#[tokio::test]
async fn a_requeued_heavy_build_is_passed_over_afresh() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), 1);
    let _b = seated(&state, "b", (3, 2), 0);
    let mut j = job(0, Arc::new(Replay::default()));
    j.features = vec!["big-parallel".into()];
    j.deferred = Instant::now().checked_sub(GRACE * 5);
    let _client = j.replay.subscribe().await;
    state.requeue(j).await;
    let taken = state.take_job(&a_caps, Some(a.id)).await;
    assert!(taken.is_none(), "taken: its grace counted from before");
}

/// Passing over is left within GRACE of the first pass, to the room with
/// the fewest heavy builds, fewer than the asker's, each idle agent once.
#[test]
fn a_heavy_build_is_passed_over_within_grace_to_the_least_loaded_room() {
    let t0 = Instant::now();
    let room = |id, idle, heavy| Room { id, idle, heavy };
    let rows = [
        (
            "first",
            None,
            Duration::ZERO,
            1,
            vec![room(2, 1, 0)],
            vec![Some(2), None],
        ),
        (
            "in grace",
            Some(t0),
            GRACE.saturating_sub(Duration::from_nanos(1)),
            1,
            vec![room(2, 1, 0)],
            vec![Some(2)],
        ),
        (
            "grace over",
            Some(t0),
            GRACE,
            1,
            vec![room(2, 1, 0)],
            vec![None],
        ),
        (
            "as many",
            None,
            Duration::ZERO,
            1,
            vec![room(2, 3, 1)],
            vec![None],
        ),
        (
            "up to the asker's",
            None,
            Duration::ZERO,
            1,
            vec![room(2, 3, 0)],
            vec![Some(2), None],
        ),
        (
            "fewest",
            None,
            Duration::ZERO,
            2,
            vec![room(2, 1, 1), room(3, 1, 0)],
            vec![Some(3), Some(2), None],
        ),
    ];
    for (row, first, at, mine, rooms, passes) in rows {
        let mut spread = Spread { mine, rooms };
        for (n, want) in passes.into_iter().enumerate() {
            let mut since = first;
            let got = spread.passes(&mut since, |_| true, t0 + at);
            assert_eq!(got, want, "{row}, pass {n}");
            let kept = first.or(got.map(|_| t0 + at));
            assert_eq!(since, kept, "{row}, pass {n}: when first passed over");
        }
    }
}

/// A heavy build counts against its worker from its dispatch until its
/// supervision settles it.
#[tokio::test]
async fn a_heavy_build_counts_against_its_worker_until_it_settles() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = heavy_worker(&state, "w1");
    w.to_hub.send(declare(3, 2)).await.unwrap();
    let mut client = queue(&state, 0, "heavy-0", true).await;
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let heavy = || {
        let placement = state.placement.lock().unwrap();
        (placement.sessions.values())
            .map(|a| a.lock().unwrap().heavy)
            .sum::<u32>()
    };
    assert_eq!(heavy(), 1, "dispatched");
    let result = BuildResult {
        build_id,
        exit_code: 1,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    assert_eq!(heavy(), 0, "settled");
}

/// A queued build dropped during the scan, its client gone, makes take_job
/// scan again: the heavy build it left to another worker stays left, and
/// the asker takes the light build behind it.
#[tokio::test]
async fn a_rescan_still_leaves_the_heavy_build() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), 1);
    let _b = seated(&state, "b", (3, 2), 0);
    let _heavy = queue(&state, 0, "heavy-0", true).await;
    drop(queue(&state, 1, "light-1", false).await);
    let _light = queue(&state, 2, "light-2", false).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(
        job.map(|j| j.id),
        Some(id(2)),
        "the rescan took the heavy build"
    );
}

/// A heavy build is left only to a worker that serves it: one without
/// big-parallel has idle agents and no heavy build, and is no room for it.
#[tokio::test]
async fn a_heavy_build_is_not_left_to_a_worker_that_does_not_serve_it() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), 1);
    let b = register(&state, "b", &[]);
    let mut idle = Admission::default();
    idle.declare(&RequestJob {
        capacity: 3,
        assignments: Some(0),
        free_slots: Some(2),
    });
    (state.placement.lock().unwrap()).open(b.id, Arc::new(Mutex::new(idle)));
    let _heavy = queue(&state, 0, "heavy-0", true).await;
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
    let b = heavy_worker(&state, "b");
    b.to_hub.send(declare(3, 2)).await.unwrap();
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
