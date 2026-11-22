//! Clientless builds given up, or not, for memory and keys.

use super::*;

/// One 8 GiB worker, one slot, and on it the first run of a key, its
/// client gone; queued, a build with a client of another key, which waits
/// for the slot: the run is given up; or of that key, held for it: the
/// run is kept, as giving it up would teach the key only a failure, and
/// its end, which it reaches as any run does, lets that build go.
#[tokio::test(start_paused = true)]
async fn a_clientless_run_is_given_up_for_a_slot_not_for_its_key() {
    const GIB: f64 = 1_073_741_824.0;
    for (row, name, given_up) in [("its key", "q-1", false), ("another key", "other-1", true)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        state.deciding.store(true, Ordering::Relaxed);
        let caps = WorkerCaps {
            name: "w0".into(),
            connected_at: Now::now(),
            systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
            max_jobs: 1,
        };
        state.worker_caps.lock().unwrap().insert(0, caps);
        let size = Size {
            cores: 4.0,
            memory: Some(8.0 * GIB),
        };
        let admission = Arc::new(StdMutex::new(Admission::default()));
        state.placement.lock().unwrap().open(0, admission, size);
        let named = |n: usize, name: &str| {
            let mut j = job(n, Arc::new(Replay::default()));
            j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
            j.expected = Expected::prior(false);
            j
        };
        let first = named(0, "q-1");
        let first_client = first.replay.subscribe().await;
        let _listed = state.list_dispatch(&first, 0);
        state.placement.lock().unwrap().dispatched(0, &first, false);
        let queued = named(100, name);
        let _client = queued.replay.subscribe().await;
        state.queue.lock().await.push_back(queued);
        drop(first_client);
        first.replay.checked(false, Instant::now());
        tokio::time::advance(FLOOR + Duration::from_secs(1)).await;
        state.give_up_clientless().await;
        assert_eq!(state.given_up(&first.id).is_cancelled(), given_up, "{row}");
        let queue = state.queue.lock().await;
        let mut p = state.placement.lock().unwrap();
        assert_eq!(p.held_for_key(&queue[0]), !given_up, "{row}");
        p.settled(0, &first.id);
        let held = p.held_for_key(&queue[0]);
        assert!(!held, "{row}: held past the run's end");
    }
}

/// One queued 4 GiB build short of memory on two 8 GiB workers, each
/// short only for a clientless 4 GiB run beside a 2 GiB run with a
/// client, no slot free: one clientless run is given up, enough for the
/// one build, not one on each worker.
#[tokio::test(start_paused = true)]
async fn one_build_short_of_memory_gives_up_one_clientless_run() {
    const GIB: f64 = 1_073_741_824.0;
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    let size = Size {
        cores: 4.0,
        memory: Some(8.0 * GIB),
    };
    for w in 0..2u64 {
        let caps = WorkerCaps {
            name: format!("w{w}"),
            connected_at: Now::now(),
            systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
            max_jobs: 2,
        };
        state.worker_caps.lock().unwrap().insert(w, caps);
        let admission = Arc::new(StdMutex::new(Admission::default()));
        state.placement.lock().unwrap().open(w, admission, size);
    }
    let named = |n: usize, name: &str, gib: f64| {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
        j.expected = Expected {
            demand: Demand {
                cores: 1.0,
                memory: gib * GIB,
            },
            secs: None,
            level: Level::Key,
            oom: 0,
            below: 0.0,
        };
        j
    };
    let q = named(100, "q-1", 4.0);
    let u = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 4 << 30,
        ..Default::default()
    };
    (state.placement.lock().unwrap().history).record(&build_of(&q), 0, &u, None);
    let _client = q.replay.subscribe().await;
    state.queue.lock().await.push_back(q);
    let (mut listed, mut gone, mut kept) = (Vec::new(), Vec::new(), Vec::new());
    for w in 0..2u64 {
        let n = usize::try_from(w).unwrap();
        let (g, h) = (named(n * 2, "c-1", 4.0), named(n * 2 + 1, "f-1", 2.0));
        let g_client = g.replay.subscribe().await;
        kept.push(h.replay.subscribe().await);
        for j in [&g, &h] {
            listed.push(state.list_dispatch(j, w));
            state.placement.lock().unwrap().dispatched(w, j, false);
        }
        drop(g_client);
        g.replay.checked(false, Instant::now());
        gone.push(g);
    }
    tokio::time::advance(FLOOR + Duration::from_secs(1)).await;
    state.give_up_clientless().await;
    let n = (gone.iter()).filter(|g| state.given_up(&g.id).is_cancelled());
    assert_eq!(n.count(), 1);
}

/// A 2 GiB build waits for a slot at an 8 GiB worker with one slot, held
/// by a clientless 1 GiB run; another 8 GiB worker has a free slot, but a
/// clientless 7 GiB run there leaves it short of memory. The slot rule
/// gives up the first run, which lets the build in; the build wants no
/// memory freed as well, so the second is kept.
#[tokio::test(start_paused = true)]
async fn a_build_waiting_for_a_slot_wants_no_memory_freed_too() {
    const GIB: f64 = 1_073_741_824.0;
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    let size = Size {
        cores: 4.0,
        memory: Some(8.0 * GIB),
    };
    for (w, slots, free) in [(0u64, 1, 0), (1, 2, 1)] {
        let caps = WorkerCaps {
            name: format!("w{w}"),
            connected_at: Now::now(),
            systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
            max_jobs: slots,
        };
        state.worker_caps.lock().unwrap().insert(w, caps);
        let admission = Admission {
            free,
            ..Default::default()
        };
        let admission = Arc::new(StdMutex::new(admission));
        state.placement.lock().unwrap().open(w, admission, size);
    }
    let named = |n: usize, name: &str, gib: f64| {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
        j.expected = Expected {
            demand: Demand {
                cores: 1.0,
                memory: gib * GIB,
            },
            secs: None,
            level: Level::Key,
            oom: 0,
            below: 0.0,
        };
        j
    };
    let q = named(100, "q-1", 2.0);
    let u = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 2 << 30,
        ..Default::default()
    };
    (state.placement.lock().unwrap().history).record(&build_of(&q), 0, &u, None);
    let _client = q.replay.subscribe().await;
    state.queue.lock().await.push_back(q);
    let (mut listed, mut gone) = (Vec::new(), Vec::new());
    for (w, gib) in [(0u64, 1.0), (1, 7.0)] {
        let g = named(usize::try_from(w).unwrap(), &format!("c{w}-1"), gib);
        let g_client = g.replay.subscribe().await;
        listed.push(state.list_dispatch(&g, w));
        state.placement.lock().unwrap().dispatched(w, &g, false);
        drop(g_client);
        g.replay.checked(false, Instant::now());
        gone.push(g);
    }
    tokio::time::advance(FLOOR + Duration::from_secs(1)).await;
    state.give_up_clientless().await;
    let given_up = gone.iter().map(|g| state.given_up(&g.id).is_cancelled());
    assert_eq!(given_up.collect::<Vec<_>>(), [true, false]);
}

/// A 16 GiB build, larger than the one 8 GiB worker, waits for it to be
/// empty, a slot free there; a clientless 1 GiB run is all it holds. It
/// wants that run given up: freed, the worker is empty and admits it, so
/// its wait is not unbounded.
#[tokio::test(start_paused = true)]
async fn a_build_larger_than_every_worker_wants_a_clientless_run_given_up() {
    const GIB: f64 = 1_073_741_824.0;
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    let caps = WorkerCaps {
        name: "w0".into(),
        connected_at: Now::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 2,
    };
    state.worker_caps.lock().unwrap().insert(0, caps);
    let size = Size {
        cores: 4.0,
        memory: Some(8.0 * GIB),
    };
    let admission = Admission {
        free: 1,
        ..Default::default()
    };
    let admission = Arc::new(StdMutex::new(admission));
    state.placement.lock().unwrap().open(0, admission, size);
    let named = |n: usize, name: &str| {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
        j.expected = Expected::prior(false);
        j
    };
    let big = named(100, "big-1");
    let u = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 16 << 30,
        ..Default::default()
    };
    (state.placement.lock().unwrap().history).record(&build_of(&big), 0, &u, None);
    let _client = big.replay.subscribe().await;
    state.queue.lock().await.push_back(big);
    let gone = named(0, "c-1");
    let gone_client = gone.replay.subscribe().await;
    let _listed = state.list_dispatch(&gone, 0);
    state.placement.lock().unwrap().dispatched(0, &gone, false);
    drop(gone_client);
    gone.replay.checked(false, Instant::now());
    tokio::time::advance(FLOOR + Duration::from_secs(1)).await;
    state.give_up_clientless().await;
    assert!(state.given_up(&gone.id).is_cancelled());
}

/// What a queued build's key learns between its submission and a check
/// decides that check. Submitted at 1 GiB, it would wait only for the
/// slot a clientless 1 GiB run holds on an 8 GiB worker beside a 2 GiB
/// run with a client, and that run would be given up; its key has since
/// learned 7 GiB, which fits neither now nor with the clientless run
/// given up, so the run is kept.
#[tokio::test(start_paused = true)]
async fn a_check_reads_what_a_queued_builds_key_learned_since() {
    const GIB: f64 = 1_073_741_824.0;
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    state.deciding.store(true, Ordering::Relaxed);
    let caps = WorkerCaps {
        name: "w0".into(),
        connected_at: Now::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 2,
    };
    state.worker_caps.lock().unwrap().insert(0, caps);
    let size = Size {
        cores: 4.0,
        memory: Some(8.0 * GIB),
    };
    let admission = Arc::new(StdMutex::new(Admission::default()));
    state.placement.lock().unwrap().open(0, admission, size);
    let named = |n: usize, name: &str, gib: f64| {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
        j.expected = Expected {
            demand: Demand {
                cores: 1.0,
                memory: gib * GIB,
            },
            secs: None,
            level: Level::Key,
            oom: 0,
            below: 0.0,
        };
        j
    };
    let ran = |gib: u64| BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: gib << 30,
        ..Default::default()
    };
    let mut q = named(100, "q-1", 1.0);
    {
        let mut p = state.placement.lock().unwrap();
        p.history.record(&build_of(&q), 0, &ran(1), None);
        p.refresh(&mut q);
        p.history.record(&build_of(&q), 0, &ran(7), None);
    }
    let _client = q.replay.subscribe().await;
    state.queue.lock().await.push_back(q);
    let (gone, here) = (named(0, "c-1", 1.0), named(1, "f-1", 2.0));
    let gone_client = gone.replay.subscribe().await;
    let _here_client = here.replay.subscribe().await;
    let mut listed = Vec::new();
    for j in [&gone, &here] {
        listed.push(state.list_dispatch(j, 0));
        state.placement.lock().unwrap().dispatched(0, j, false);
    }
    drop(gone_client);
    gone.replay.checked(false, Instant::now());
    tokio::time::advance(FLOOR + Duration::from_secs(1)).await;
    state.give_up_clientless().await;
    assert!(
        !state.given_up(&gone.id).is_cancelled(),
        "given up on a stale guess"
    );
}
