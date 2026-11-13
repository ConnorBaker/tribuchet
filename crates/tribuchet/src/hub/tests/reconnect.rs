//! A build requeued because its worker's session died is kept for that
//! worker while it reconnects (Loss::awaited), but not for the dead
//! session itself, and not for a worker its assignment never reached.

use super::fake_daemon::{Daemon, cached_input, hub_with};
use super::*;

/// Supervise `job`, dispatched in `session`, whose router worker_loop
/// has closed: the session died. With `reached` false its stream to the
/// worker is closed too, so the assignment's send fails.
async fn supervise_dead(state: &Arc<HubState>, job: Job, session: (u64, String), reached: bool) {
    let router = Router::default();
    let in_rx = router.register(&job.id);
    let (out_tx, out_rx) = mpsc::channel(64);
    let _out_rx = reached.then_some(out_rx);
    router.close_all();
    let sess = Arc::new(WorkerSession::new("w".into(), out_tx.clone()));
    let admission = Arc::new(Mutex::new(Admission::default()));
    supervise_job(
        state.clone(),
        job,
        router,
        out_tx,
        in_rx,
        sess,
        session,
        admission,
    )
    .await;
}

/// worker_loop drops a dead session's registration only after its
/// builds' channels close, so the requeue can see that session still
/// registered under the worker's name. It is not the worker back; the
/// worker registering again without the build is, and releases it.
#[tokio::test]
async fn a_dead_session_still_registered_is_not_its_worker_back() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let _other = register(&state, "w2", &[]);
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe().await;
    let dead = register(&state, "w1", &[]);
    let session = (dead.id, dead.caps.name.clone());
    supervise_dead(&state, job(1, replay), session, true).await;
    assert!(
        state.take_job(&caps("w2"), None).await.is_none(),
        "the dead session counted as its worker back"
    );
    drop(dead);
    assert!(state.take_job(&caps("w2"), None).await.is_none());
    let _back = register(&state, "w1", &[]);
    assert!(
        state.take_job(&caps("w2"), None).await.is_some(),
        "the build was not kept queued for its worker"
    );
}

/// A build whose assignment was never sent (the hub's daemon lacks an
/// input the hub looked up to prepare it) is no loss of that worker's:
/// it is not kept for it, though its session died.
#[tokio::test]
async fn an_assignment_never_sent_is_kept_for_no_worker() {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Daemon::Lacks, 1);
    let _other = register(&state, "w2", &[]);
    let (j, _) = cached_input(&state, 1).await;
    let _client = j.replay.subscribe().await;
    lose_session(&state, j, register(&state, "w1", &[]), None).await;
    assert!(
        state.take_job(&caps("w2"), None).await.is_some(),
        "kept for a worker that never got the assignment"
    );
}

/// Each dispatch of a build is a run of its own: its worker, back after its
/// session died and holding the build, is assigned it under a new run id.
#[tokio::test]
async fn a_build_dispatched_again_is_a_new_run() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe().await;
    state.queue.lock().await.push_back(job(0, replay));
    let mut runs = Vec::new();
    for n in 1..=2 {
        let mut w = connect(&state, "w", 1, &["k0"]);
        runs.push(w.assignment().await.expect("not assigned").run);
        drop(w.to_hub);
        let requeued = || state.metrics.requeued.load(Ordering::Relaxed);
        for _ in (0..500).take_while(|_| requeued() < n) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    assert!(runs.iter().all(Option::is_some), "{runs:?}");
    assert_ne!(runs[0], runs[1], "one run id for two dispatches");
}

/// The session is gone when the assignment is sent: the send fails, so
/// the worker never got the build, and it is kept for no worker.
#[tokio::test]
async fn an_assignment_whose_send_fails_is_kept_for_no_worker() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let _other = register(&state, "w2", &[]);
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe().await;
    let w1 = register(&state, "w1", &[]);
    let session = (w1.id, w1.caps.name.clone());
    drop(w1);
    supervise_dead(&state, job(1, replay), session, false).await;
    assert!(
        state.take_job(&caps("w2"), None).await.is_some(),
        "kept for a worker whose assignment was never sent"
    );
}
