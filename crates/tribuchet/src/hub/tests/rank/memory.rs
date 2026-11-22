//! Admission by memory: a worker takes a build its memory has room for,
//! and a key with no run of its own runs one build at a time.

use super::*;
use crate::proto::BuildUsage;

/// The dispatch line says what a build declares and is expected to use,
/// how many other workers that serve it have an idle agent, and the
/// worker's size and expected load with it. A build larger than every
/// worker that serves it waits for the largest to be empty, and its load
/// over the worker's memory is said once more.
#[tokio::test]
async fn the_dispatch_line_names_the_expectation_and_the_room() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w1 = sized_worker(&state, "w1", 4, 8);
    w1.to_hub.send(declare(3, 2, 0)).await.unwrap();
    let mut c0 = queue(&state, 0, "big-0", expecting(2.0, 6.0)).await;
    let build_id = w1.assignment().await.expect("big-0 not assigned").build_id;
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
    let _c1 = queue(&state, 1, "huge-1", expecting(1.0, 10.0)).await;
    let result = BuildResult {
        build_id,
        exit_code: 0,
        ..Default::default()
    };
    w1.send(worker_message::Msg::Result(result)).await;
    while c0.recv().await.is_some() {}
    assert!(w1.assignment().await.is_some(), "huge-1 never assigned");
    assert!(
        log.has(&[
            "placed over predicted memory",
            "load_mem=10737418240",
            "memory=8589934592"
        ]),
        "huge-1 not alone, or over the worker's memory unsaid"
    );
}

/// A room admits a build by memory only: its expected load and the
/// build's within the memory its builds may use; or, holding nothing, it
/// is the largest room serving a build larger than every one of them,
/// which waits for it, never fails; and a room that reported no memory
/// admits any.
#[test]
fn a_room_admits_a_build_by_its_memory() {
    let room = |memory: Option<f64>, gib: f64, empty: bool| Room {
        id: 1,
        idle: 1,
        size: Size {
            cores: 4.0,
            memory: memory.map(|m| m * GIB),
        },
        load: Demand {
            cores: 64.0,
            memory: gib * GIB,
        },
        clientless: Demand::default(),
        empty,
        only_clientless: false,
    };
    let rows = [
        ("fits", room(Some(8.0), 2.0, false), 5.0, Some(8.0), true),
        (
            "fills it",
            room(Some(8.0), 2.0, false),
            6.0,
            Some(8.0),
            true,
        ),
        ("over", room(Some(8.0), 3.0, false), 6.0, Some(8.0), false),
        (
            "empty, the largest",
            room(Some(8.0), 0.0, true),
            10.0,
            Some(8.0),
            true,
        ),
        (
            "empty, a larger serves",
            room(Some(8.0), 0.0, true),
            10.0,
            Some(16.0),
            false,
        ),
        (
            "larger than all, held",
            room(Some(8.0), 1.0, false),
            10.0,
            Some(8.0),
            false,
        ),
        (
            "reported none",
            room(None, 99.0, false),
            10.0,
            Some(8.0),
            true,
        ),
    ];
    for (row, room, gib, largest, admits) in rows {
        let d = Demand {
            cores: 1.0,
            memory: gib * GIB,
        };
        let largest = largest.map(|l| l * GIB);
        assert_eq!(room.admits(d, largest), admits, "{row}");
    }
}

/// A build its asker has no memory for stays queued, and is taken once
/// A room a scan has spoken for holds that build, so it is no longer
/// empty: a second build larger than every room is not passed to it too.
#[test]
fn a_room_spoken_for_is_empty_no_more() {
    let room = |id: u64, gib: f64, used: f64| Room {
        id,
        idle: 2,
        size: Size {
            cores: 4.0,
            memory: Some(gib * GIB),
        },
        load: Demand {
            cores: 0.0,
            memory: used * GIB,
        },
        clientless: Demand::default(),
        empty: used == 0.0,
        only_clientless: false,
    };
    let mut rooms = Rooms::new(1, vec![room(1, 8.0, 1.0), room(2, 16.0, 0.0)]);
    let d = Demand {
        cores: 1.0,
        memory: 32.0 * GIB,
    };
    let now = Instant::now();
    let passed = [(); 2].map(|()| rooms.passes(d, &mut None, |_| true, now));
    assert_eq!(passed, [Some(2), None]);
}

/// the build in flight there settles.
#[tokio::test]
async fn a_build_that_does_not_fit_stays_queued_until_memory_frees() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 6.0));
    let _c = queue(&state, 0, "big-0", expecting(1.0, 4.0)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert!(job.is_none(), "taken over its memory");
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(
        job.map(|j| j.id),
        Some(id(0)),
        "not taken once memory freed"
    );
}

/// What history guesses of a key with no run of its own: the prior.
fn unseen() -> Expected {
    Expected::prior(false)
}

/// The run of `j` on session `on` ended, failed, peaking at `gib` GiB:
/// history learns it, and it settles.
fn failed(state: &HubState, on: u64, j: &Job, gib: u64) {
    let usage = BuildUsage {
        wall_ms: 1000,
        peak_memory_bytes: gib << 30,
        ..BuildUsage::default()
    };
    let mut p = state.placement.lock().unwrap();
    p.history.record(&placement::build_of(j), 1, &usage, None);
    p.settled(on, &j.id);
}

/// Two builds of a key with no run of its own: the second waits while
/// the first is in flight. The first fails; what its run taught (its
/// peak, over the guess) is what the second is then taken with.
#[tokio::test]
async fn a_key_with_no_run_runs_one_at_a_time() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let _c = [
        queue(&state, 0, "onnx-1.0", unseen()).await,
        queue(&state, 1, "onnx-1.0", unseen()).await,
    ];
    let first = state.take_job(&a_caps, Some(a.id)).await.expect("first");
    state
        .placement
        .lock()
        .unwrap()
        .dispatched(a.id, &first, false);
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert!(job.is_none(), "taken while its key's first run is out");
    failed(&state, a.id, &first, 30);
    let second = state.take_job(&a_caps, Some(a.id)).await.expect("second");
    let e = second.expected;
    assert_eq!((e.level, e.demand.memory), (Level::Key, 30.0 * GIB));
}

/// A key's builds wait for one in flight on a session only while that
/// session lives: its end lets them go to another.
#[tokio::test]
async fn a_closed_session_releases_its_key_hold() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let (b, b_caps) = seated(&state, "b", (2, 1), BIG, (0.0, 0.0));
    let _c = [
        queue(&state, 0, "onnx-1.0", unseen()).await,
        queue(&state, 1, "onnx-1.0", unseen()).await,
    ];
    let first = state.take_job(&a_caps, Some(a.id)).await.expect("first");
    state
        .placement
        .lock()
        .unwrap()
        .dispatched(a.id, &first, false);
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert!(job.is_none(), "taken while its key's first run is out");
    state.placement.lock().unwrap().close(a.id);
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "held past its session");
}

/// The hold begins at the take, under the queue's lock: a second
/// worker's take before the first's dispatch finds the key held. A key
/// with a measured success of its own is held by none; one whose run
/// only failed still is.
#[tokio::test]
async fn a_take_holds_its_key_until_the_run_settles() {
    for (row, seen, held) in [("no success", false, true), ("a success", true, false)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
        let (b, b_caps) = seated(&state, "b", (2, 1), BIG, (0.0, 0.0));
        let e = if seen { expecting(1.0, 1.0) } else { unseen() };
        let _c = [
            queue(&state, 0, "onnx-1.0", e).await,
            queue(&state, 1, "onnx-1.0", e).await,
        ];
        failed(&state, a.id, &state.queue.lock().await[0], 1);
        assert!(state.take_job(&a_caps, Some(a.id)).await.is_some(), "{row}");
        let second = state.take_job(&b_caps, Some(b.id)).await;
        assert_eq!(second.is_none(), held, "{row}");
    }
}

/// A build is taken with what history expects of it now, whatever it
/// was queued with: its key learned 2 GiB, a run of it then failed at
/// 30 GiB, and the next is taken at 30.
#[tokio::test]
async fn a_queued_build_is_taken_with_what_its_key_learned_since() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let _c = [
        queue(&state, 0, "onnx-1.0", expecting(1.0, 2.0)).await,
        queue(&state, 1, "onnx-1.0", expecting(1.0, 2.0)).await,
    ];
    let first = state.take_job(&a_caps, Some(a.id)).await.expect("first");
    failed(&state, a.id, &first, 30);
    let second = state.take_job(&a_caps, Some(a.id)).await.expect("second");
    assert_eq!(second.expected.demand.memory, 30.0 * GIB);
}

/// A build taken and dropped for want of a client lets its key go: the
/// take held it, and the next of its key is taken.
#[tokio::test]
async fn a_build_dropped_at_its_take_releases_its_key() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    drop(queue(&state, 0, "onnx-1.0", unseen()).await);
    let _c = queue(&state, 1, "onnx-1.0", unseen()).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "held by a dropped build");
}

/// A build is not left to a less full worker without memory for it: the
/// asker, with room, takes it. Ranked by cores alone (the asker reported
/// no memory), the other would win it, and not take it.
#[tokio::test]
async fn a_build_is_left_only_to_a_worker_with_memory_for_it() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let unreported = Size {
        cores: 4.0,
        memory: None,
    };
    let (a, a_caps) = seated(&state, "a", (2, 1), unreported, (4.0, 0.0));
    let _b = seated(&state, "b", (2, 2), SMALL, (0.0, 0.0));
    let _c = queue(&state, 0, "big-0", expecting(1.0, 10.0)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)), "left to b");
}

/// A key whose only run the worker could not measure, as a worker reports
/// a run it could not set up, is held still: nothing was learned of it.
#[tokio::test]
async fn a_key_whose_run_was_not_measured_still_holds() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let _c = [
        queue(&state, 0, "onnx-1.0", unseen()).await,
        queue(&state, 1, "onnx-1.0", unseen()).await,
    ];
    {
        let q = state.queue.lock().await;
        let mut p = state.placement.lock().unwrap();
        p.history
            .record(&placement::build_of(&q[0]), 0, &BuildUsage::default(), None);
    }
    assert!(state.take_job(&a_caps, Some(a.id)).await.is_some(), "first");
    let second = state.take_job(&a_caps, Some(a.id)).await;
    assert!(second.is_none(), "taken beside it");
}

/// A key's first run that fails early, at 200 MiB, neither frees its key
/// nor shrinks it: its next build is taken with the guess it had (the
/// prior of a build that declares big-parallel), and alone on a worker
/// with room for two, where both were once taken together at 200 MiB
/// each.
#[tokio::test]
async fn an_early_failure_neither_frees_nor_shrinks_its_key() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let room_for_two = Size {
        memory: Some(128.0 * GIB),
        ..BIG
    };
    let (a, a_caps) = seated(&state, "a", (3, 3), room_for_two, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let mut clients = Vec::new();
    for n in 0..3 {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-torch-2.5"))].into();
        j.features = vec!["big-parallel".into()];
        clients.push(j.replay.subscribe().await);
        state.queue.lock().await.push_back(j);
    }
    let first = state.take_job(&a_caps, Some(a.id)).await.expect("first");
    assert!(state.take_job(&a_caps, Some(a.id)).await.is_none(), "held");
    let usage = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 200 << 20,
        ..BuildUsage::default()
    };
    {
        let mut p = state.placement.lock().unwrap();
        p.history
            .record(&placement::build_of(&first), 1, &usage, None);
        p.settled(a.id, &first.id);
    }
    let second = state.take_job(&a_caps, Some(a.id)).await.expect("second");
    assert_eq!(second.expected.demand.memory, 38.5 * GIB, "shrunk");
    assert!(state.take_job(&a_caps, Some(a.id)).await.is_none(), "freed");
}

/// A lost key's build runs alone on an empty worker though it is larger
/// than that worker's memory, and another larger serves it: not tested
/// for memory (state/take.rs), lest the worker drained for it stay empty.
#[tokio::test]
async fn a_lost_keys_build_is_not_tested_for_memory() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let _b = seated(&state, "b", (2, 1), BIG, (1.0, 60.0));
    let _c = queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await;
    let key = state.queue.lock().await[0].key.clone();
    state.lost.mark(&key);
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)));
}

/// A worker that has no placement session, closed or not yet opened,
/// takes nothing: nothing is known of its memory.
#[tokio::test]
async fn a_worker_without_a_placement_session_takes_nothing() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let _c = queue(&state, 0, "foo-1", expecting(1.0, 1.0)).await;
    state.placement.lock().unwrap().close(a.id);
    assert!(state.take_job(&a_caps, Some(a.id)).await.is_none());
}

/// An aarch64 worker with 128 GiB, larger than any x86 one,
/// holding nothing: it serves no x86 build.
fn aarch64_worker(state: &Arc<HubState>) -> CapsGuard {
    let caps = WorkerCaps {
        name: "arm".into(),
        connected_at: Instant::now(),
        systems: [("aarch64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 2,
    };
    let guard = CapsGuard::new(state.clone(), caps);
    let size = Size {
        cores: 20.0,
        memory: Some(128.0 * GIB),
    };
    let a = Arc::new(Mutex::new(Admission::default()));
    state.placement.lock().unwrap().open(guard.id, a, size);
    guard
}

/// The largest worker is the largest of those serving the build: a
/// larger worker of another system, holding nothing, changes nothing.
/// Empty, the largest x86 worker takes, or is left, an x86 build larger
/// than it; one too small and not the largest keeps itself for none, and
/// takes a later build.
#[tokio::test]
async fn the_largest_worker_is_the_largest_serving_the_build() {
    // Taken by the largest x86 worker, empty.
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let _s = aarch64_worker(&state);
    let _c = queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)), "taken");
    // Left by an asker that reported no memory to it, less full.
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let unreported = Size {
        cores: 4.0,
        memory: None,
    };
    let (a, a_caps) = seated(&state, "a", (2, 1), unreported, (4.0, 0.0));
    let (b, _b_caps) = seated(&state, "b", (2, 2), SMALL, (0.0, 0.0));
    state.placement.lock().unwrap().settled(b.id, "in-flight");
    let _s = aarch64_worker(&state);
    let _c = queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert!(job.is_none(), "not left to b");
    // Not kept by a worker too small that is not the largest, past AGE.
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 1.0));
    let _b = seated(&state, "b", (2, 0), BIG, (0.0, 0.0));
    let _c = [
        queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "a kept itself");
}

/// 32 cores and 64 GiB.
pub(super) const BIG: Size = Size {
    cores: 32.0,
    memory: Some(64.0 * GIB),
};

/// A lost key's build, taken to run alone, is taken with what its key
/// learned since it queued (2 GiB then, 9 GiB since), as any take reads
/// it.
#[tokio::test]
async fn a_lost_keys_build_is_taken_with_what_its_key_learned() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let _c = queue(&state, 0, "grow-0", expecting(1.0, 2.0)).await;
    let mut later = job(50, Arc::new(Replay::default()));
    taught(&state, &mut later, "grow-1", expecting(1.0, 9.0));
    let key = state.queue.lock().await[0].key.clone();
    state.lost.mark(&key);
    let job = state.take_job(&a_caps, Some(a.id)).await.expect("taken");
    assert_eq!(job.expected.demand.memory, 9.0 * GIB, "stale");
}

/// A build a worker resumes by its key is taken with what its key learned
/// since it queued: its worker's room counts that.
#[tokio::test]
async fn a_resumed_build_is_taken_with_what_its_key_learned() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, _) = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    let _c = queue(&state, 0, "grow-0", expecting(1.0, 2.0)).await;
    let mut later = job(50, Arc::new(Replay::default()));
    taught(&state, &mut later, "grow-1", expecting(1.0, 9.0));
    let key = state.queue.lock().await[0].key.clone();
    state.hold(a.id, &[key], false);
    let job = state.take_job_by_key(a.id).await.expect("taken");
    assert_eq!(job.expected.demand.memory, 9.0 * GIB, "stale");
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

/// The queue stays in due order (queued less path, objective.rs) after a
/// take reads a queued build's expectation afresh: the path it was queued
/// with stays, though what follows its key was learned since.
#[tokio::test]
async fn a_refresh_keeps_the_queue_in_due_order() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 0), SMALL, (0.0, 4.0));
    let out = |name: &str| format!("/nix/store/{H}-{name}");
    let _c = [
        queue(&state, 0, "a-0", expecting(1.0, 6.0)).await,
        queue(&state, 1, "b-0", expecting(1.0, 6.0)).await,
    ];
    {
        let mut p = state.placement.lock().unwrap();
        // A run of b's key (touching its history), then a dependant of it.
        let u = BuildUsage {
            wall_ms: 1000,
            cpu_usec: 1_000_000,
            peak_memory_bytes: 6 << 30,
            ..Default::default()
        };
        let mut b = job(50, Arc::new(Replay::default()));
        b.req.outputs = [("out".to_owned(), out("b-1"))].into();
        p.history.record(&placement::build_of(&b), 0, &u, None);
        p.downstream.learn("x86_64-linux/b", 1.0, [out("b-1")], &[]);
        p.downstream
            .learn("x86_64-linux/d", 1000.0, [out("d-1")], &[out("b-1")]);
    }
    assert!(
        state
            .placement
            .lock()
            .unwrap()
            .downstream
            .below("x86_64-linux/b")
            > 0.0
    );
    // A take that admits nothing still reads every queued build afresh.
    assert!(state.take_job(&a_caps, Some(a.id)).await.is_none(), "taken");
    let q = state.queue.lock().await;
    let due = |j: &Job| {
        let back = Duration::from_secs_f64(j.expected.below);
        j.queued_at.checked_sub(back).unwrap()
    };
    let dues: Vec<_> = q.iter().map(due).collect();
    assert!(
        dues.windows(2).all(|w| w[0] <= w[1]),
        "queue out of due order: {:?}",
        q.iter()
            .map(|j| (j.id.clone(), j.expected.below))
            .collect::<Vec<_>>()
    );
}
