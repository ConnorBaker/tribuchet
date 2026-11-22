//! Reservations: a build waiting for memory past AGE keeps one worker,
//! the one with room for it soonest, which takes a later build only as
//! EASY's backfill: before that time, and ending by it or fitting beside.

use super::memory::BIG;
use super::*;

/// A build larger than every worker serving it waits for the largest to
/// hold nothing; once it has waited AGE, that worker takes nothing queued
/// after it: were it to take each later build that fits, it might never
/// empty. Before AGE it does. Its run in flight settles; it takes the
/// build.
#[tokio::test]
async fn the_largest_worker_keeps_itself_for_a_build_larger_than_any() {
    for (row, waited) in [("before AGE", false), ("past AGE", true)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 1.0));
        let _c = [
            queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await,
            queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
        ];
        if waited {
            aged(&state, 0).await;
        }
        let job = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
        let later = if waited { None } else { Some(id(1)) };
        assert_eq!(job, later, "{row}");
        if waited {
            state.placement.lock().unwrap().settled(a.id, "in-flight");
            let job = state.take_job(&a_caps, Some(a.id)).await;
            assert_eq!(job.map(|j| j.id), Some(id(0)), "not taken once empty");
        }
    }
}

/// The run in flight on `s`, named "in-flight", is expected to end
/// `secs` from now (negative: that long ago).
fn ends_in(state: &HubState, s: u64, build: &str, secs: f64) {
    let mut p = state.placement.lock().unwrap();
    let c = p.sessions.get_mut(&s).unwrap().committed.get_mut(build);
    let now = Instant::now();
    let d = Duration::from_secs_f64(secs.abs());
    c.unwrap().ends = Some(if secs < 0.0 { now - d } else { now + d });
}

/// A 7 GiB build that fits its 8 GiB worker only once the 4 GiB run in
/// flight ends, at 100 s, past AGE, keeps the worker until then. A later
/// build goes there first only if it is expected to end by then (short-2,
/// 10 s) or fits beside the kept build, in the 1 GiB it leaves (tiny-3):
/// not long-1, 2 GiB for 1,000 s. Once the run ends the kept build is
/// taken first.
#[tokio::test]
async fn a_kept_worker_backfills_only_what_ends_first_or_fits_beside() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (4, 4), SMALL, (1.0, 4.0));
    ends_in(&state, a.id, "in-flight", 100.0);
    let secs = |s, e: Expected| Expected { secs: Some(s), ..e };
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 7.0)).await,
        queue(&state, 1, "long-1", secs(1000.0, expecting(1.0, 2.0))).await,
        queue(&state, 2, "short-2", secs(10.0, expecting(1.0, 2.0))).await,
        queue(&state, 3, "tiny-3", secs(1000.0, expecting(1.0, 1.0))).await,
    ];
    aged(&state, 0).await;
    let mut taken = Vec::new();
    while let Some(j) = state.take_job(&a_caps, Some(a.id)).await {
        taken.push(j.id);
    }
    assert_eq!(taken, [id(2), id(3)], "backfilled");
    let mut p = state.placement.lock().unwrap();
    for b in ["in-flight", &id(2), &id(3)] {
        p.settled(a.id, b);
    }
    drop(p);
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)), "the kept build, first");
}

/// The bound: a backfill that runs past its expected end delays the kept
/// build by its overrun alone. short-1, expected to end at 50 s, before
/// the run in flight, is backfilled; at 3x its estimate it is still
/// running when that run has ended, so the time the worker was to have
/// room has passed, and the worker takes no new backfill, though tiny-2
/// fits in the 1 GiB beside: the kept build waits for short-1 alone.
#[tokio::test]
async fn a_backfill_that_overruns_delays_the_kept_build_by_its_overrun_alone() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (4, 4), SMALL, (1.0, 4.0));
    ends_in(&state, a.id, "in-flight", 100.0);
    let secs = |s, e: Expected| Expected { secs: Some(s), ..e };
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 7.0)).await,
        queue(&state, 1, "short-1", secs(50.0, expecting(1.0, 2.0))).await,
    ];
    aged(&state, 0).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "backfilled");
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    ends_in(&state, a.id, &id(1), -100.0);
    let _d = queue(&state, 2, "tiny-2", secs(1.0, expecting(1.0, 1.0))).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), None, "a backfill past the start");
    state.placement.lock().unwrap().settled(a.id, &id(1));
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)), "the kept build");
}

/// A 6 GiB build behind a stream of 3 GiB builds, each 500 s, on an
/// 8 GiB worker with a 3 GiB run in flight to 100 s: before AGE each
/// stream build is taken, so while one runs the 6 GiB build never fits.
/// Past AGE the worker is kept: a stream build neither ends by 100 s nor
/// fits beside it, and once the run ends the 6 GiB build is taken.
#[tokio::test]
async fn a_build_behind_a_stream_of_smaller_ones_is_taken_past_age() {
    for (row, waited) in [("before AGE", false), ("past AGE", true)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (4, 4), SMALL, (1.0, 3.0));
        ends_in(&state, a.id, "in-flight", 100.0);
        let secs = |s, e: Expected| Expected { secs: Some(s), ..e };
        let _c = [
            queue(&state, 0, "big-0", expecting(1.0, 6.0)).await,
            queue(&state, 1, "stream-1", secs(500.0, expecting(1.0, 3.0))).await,
        ];
        if waited {
            aged(&state, 0).await;
        }
        let job = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
        assert_eq!(job, (!waited).then(|| id(1)), "{row}");
        if waited {
            state.placement.lock().unwrap().settled(a.id, "in-flight");
            let job = state.take_job(&a_caps, Some(a.id)).await;
            assert_eq!(job.map(|j| j.id), Some(id(0)), "{row}: starved");
        }
    }
}

/// Of workers tied for the soonest room, one alone, the first by id, is
/// kept for a build larger than all: the other takes the later build.
#[tokio::test]
async fn one_of_the_largest_workers_keeps_itself() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 1.0));
    let (b, b_caps) = seated(&state, "b", (2, 1), SMALL, (1.0, 1.0));
    let _c = [
        queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    assert!(a.id < b.id);
    let by_a = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
    let by_b = state.take_job(&b_caps, Some(b.id)).await.map(|j| j.id);
    assert_eq!((by_a, by_b), (None, Some(id(1))), "a kept");
}

/// Two builds past AGE keep a worker each, not both the one with room
/// soonest (a tie, the first by id): a later build that fits either is
/// taken by neither.
#[tokio::test]
async fn a_worker_is_kept_for_one_build() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 6.0));
    let (b, b_caps) = seated(&state, "b", (2, 1), SMALL, (1.0, 6.0));
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 4.0)).await,
        queue(&state, 1, "big-1", expecting(1.0, 4.0)).await,
        queue(&state, 2, "small-2", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    aged(&state, 1).await;
    let by_b = state.take_job(&b_caps, Some(b.id)).await.map(|j| j.id);
    let by_a = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
    assert_eq!((by_a, by_b), (None, None), "one worker kept for both");
}

/// A build past AGE keeps the worker whose runs are expected to end
/// first, here b, though a is first by id: a takes a later build.
#[tokio::test]
async fn the_worker_kept_is_the_one_expected_to_have_room_first() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 6.0));
    let (b, _b_caps) = seated(&state, "b", (2, 1), SMALL, (1.0, 6.0));
    for (s, secs) in [(a.id, 1000), (b.id, 10)] {
        let mut p = state.placement.lock().unwrap();
        let c = p
            .sessions
            .get_mut(&s)
            .unwrap()
            .committed
            .get_mut("in-flight");
        c.unwrap().ends = Some(Instant::now() + Duration::from_secs(secs));
    }
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 4.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "a kept");
}

/// One build past AGE keeps one worker in a scan that asks of each worker
/// in turn, as a wait for a slot does: of three it fits none of, it
/// keeps the first, and a later build waits at that one alone.
#[tokio::test]
async fn a_build_keeps_one_worker_in_a_scan_of_every_worker() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let ids = ["a", "b", "c"].map(|n| seated(&state, n, (2, 1), SMALL, (1.0, 6.0)));
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 4.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let q = state.queue.lock().await;
    let p = state.placement.lock().unwrap();
    let mut scan = p.scan();
    let ids = ids.map(|(g, _)| g.id);
    for id in ids {
        assert!(
            p.waits(&q[0], Some(id), |_| true, &mut scan),
            "big-0 at {id}"
        );
    }
    let kept: Vec<u64> = (ids.iter().copied())
        .filter(|&id| p.waits(&q[1], Some(id), |_| true, &mut scan))
        .collect();
    assert_eq!(kept, vec![ids[0]]);
}

/// The worker a build keeps does not hold that build off: kept at a's
/// ask for big-0, b, with room, is the one it goes to at b's.
#[tokio::test]
async fn a_build_goes_to_the_worker_kept_for_it() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, _) = seated(&state, "a", (2, 1), SMALL, (1.0, 6.0));
    let (b, _) = seated(&state, "b", (2, 1), SMALL, (1.0, 0.0));
    let _c = queue(&state, 0, "big-0", expecting(1.0, 4.0)).await;
    aged(&state, 0).await;
    let q = state.queue.lock().await;
    let p = state.placement.lock().unwrap();
    let mut scan = p.scan();
    assert!(p.waits(&q[0], Some(a.id), |_| true, &mut scan), "at a");
    assert!(!p.waits(&q[0], Some(b.id), |_| true, &mut scan), "at b");
}

/// A worker too small for a build, and not the largest, is never kept
/// for it: past AGE the one it fits is, takes no later build, and takes
/// it once its run settles; the too-small one goes on taking later ones.
#[tokio::test]
async fn a_worker_too_small_for_a_build_is_never_kept() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let (b, b_caps) = seated(&state, "b", (2, 1), BIG, (1.0, 60.0));
    let _c = [
        queue(&state, 0, "huge-0", expecting(1.0, 10.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
        queue(&state, 2, "small-2", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let by_b = state.take_job(&b_caps, Some(b.id)).await.map(|j| j.id);
    assert_eq!(by_b, None, "b, the only worker huge-0 fits, not kept");
    let by_a = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
    assert_eq!(by_a, Some(id(1)), "a kept for a build it can never take");
    state.placement.lock().unwrap().settled(b.id, "in-flight");
    let by_b = state.take_job(&b_caps, Some(b.id)).await.map(|j| j.id);
    assert_eq!(by_b, Some(id(0)), "huge-0 not taken once b emptied");
}

/// A worker with a run of no expected end is kept last: when it will
/// have room is unknown, so b, whose run ends at 500 s, is kept, though
/// a is first by id, and a takes a later build.
#[tokio::test]
async fn a_worker_whose_run_has_no_expected_end_is_kept_last() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (1.0, 6.0));
    let (b, _) = seated(&state, "b", (2, 1), SMALL, (1.0, 6.0));
    {
        let mut p = state.placement.lock().unwrap();
        let on_b = &mut p.sessions.get_mut(&b.id).unwrap().committed;
        on_b.get_mut("in-flight").unwrap().ends = Some(Instant::now() + Duration::from_secs(500));
    }
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 4.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(1)), "a kept");
}

/// The worker kept is the one with room soonest by enough of its runs'
/// ends, the earliest first, not all of them: a, whose first run frees
/// room for the 7 GiB build at 10 s, though its other ends at 1000 s,
/// before b at 500 s. b, not kept, takes a 2 GiB build that would
/// neither end by b's start nor fit beside the kept build there.
#[tokio::test]
async fn the_worker_kept_has_room_once_enough_of_its_runs_end() {
    let secs = |s, e: Expected| Expected { secs: Some(s), ..e };
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, _) = seated(&state, "a", (2, 1), SMALL, (1.0, 5.0));
    let (b, b_caps) = seated(&state, "b", (2, 1), SMALL, (1.0, 6.0));
    let at = |s| Some(Instant::now() + Duration::from_secs(s));
    {
        let mut p = state.placement.lock().unwrap();
        let on_a = &mut p.sessions.get_mut(&a.id).unwrap().committed;
        on_a.get_mut("in-flight").unwrap().ends = at(10);
        let demand = Demand {
            cores: 1.0,
            memory: 1.0 * GIB,
        };
        let later = placement::Placed {
            demand,
            owners: Vec::new(),
            key: None,
            ends: at(1000),
        };
        on_a.insert("later".into(), later);
        let on_b = &mut p.sessions.get_mut(&b.id).unwrap().committed;
        on_b.get_mut("in-flight").unwrap().ends = at(500);
    }
    let _c = [
        queue(&state, 0, "big-0", expecting(1.0, 7.0)).await,
        queue(&state, 1, "long-1", secs(2000.0, expecting(1.0, 2.0))).await,
    ];
    aged(&state, 0).await;
    let by_b = state.take_job(&b_caps, Some(b.id)).await.map(|j| j.id);
    assert_eq!(by_b, Some(id(1)), "b kept");
}

/// A build taken is committed to its worker expected to end its expected
/// seconds after the take (Placed.ends, which Placement::frees reads).
#[tokio::test]
async fn a_taken_build_is_expected_to_end_after_its_expected_seconds() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    let e = Expected {
        secs: Some(100.0),
        ..expecting(1.0, 1.0)
    };
    let _c = queue(&state, 0, "foo-1", e).await;
    let before = Instant::now();
    let job = state.take_job(&a_caps, Some(a.id)).await.expect("taken");
    let after = Instant::now();
    let p = state.placement.lock().unwrap();
    let ends = p.sessions[&a.id].committed[&job.id].ends;
    let secs = Duration::from_secs(100);
    assert!(
        ends.is_some_and(|t| before + secs <= t && t <= after + secs),
        "{ends:?}"
    );
}

/// A build past AGE held for its key (another build of its unseen key
/// runs) keeps no worker: the hold has no bound, and a kept worker would
/// take nothing else for as long. Guessed at 7 GiB from its class, k-0
/// would otherwise keep a, which takes small-1 instead.
#[tokio::test]
async fn a_build_held_for_its_key_keeps_no_worker() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (4, 4), SMALL, (1.0, 4.0));
    let guess = |gib| Expected {
        level: Level::Prefix,
        ..expecting(1.0, gib)
    };
    let mut other = job(101, Arc::new(Replay::default()));
    taught(&state, &mut other, "other-1", expecting(1.0, 7.0));
    let mut sibling = job(100, Arc::new(Replay::default()));
    taught(&state, &mut sibling, "k-1", guess(1.0));
    state
        .placement
        .lock()
        .unwrap()
        .dispatched(a.id, &sibling, false);
    let _c = [
        queue(&state, 0, "k-0", guess(7.0)).await,
        queue(&state, 1, "small-1", expecting(1.0, 1.0)).await,
    ];
    aged(&state, 0).await;
    let e = {
        let queue = state.queue.lock().await;
        state.placement.lock().unwrap().expect(&queue[0])
    };
    assert!(e.demand.memory > 6.0 * GIB, "k-0 guessed small: {e:?}");
    let job = state.take_job(&a_caps, Some(a.id)).await.map(|j| j.id);
    assert_eq!(
        job,
        Some(id(1)),
        "a worker kept for a build held for its key"
    );
}
