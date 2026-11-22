//! A worker that resumed a build by key and later leaves the key out of
//! its heartbeat no longer has the build: its agent, held for at worker
//! start, answered at last without it (worker/resume.rs).

use super::*;
use crate::proto::{BuildResult, Manifest, ResultGone};

fn listing(keys: &[&str]) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::Heartbeat(Heartbeat {
            resumable_keys: keys.iter().map(|k| (*k).to_owned()).collect(),
            ..Default::default()
        })),
    }
}

/// The build is requeued once a heartbeat after its Resumed leaves its
/// key out, and waited for no longer by its worker, so that the next
/// dispatch starts it over. Its run ended without an outcome: a genuine
/// loss, counted once, its worker's session gone or not, and nothing in
/// transport. Heartbeats that list it change nothing. The
/// hub does not wait for its result for as long as its client stays.
#[tokio::test(start_paused = true)]
async fn a_resumed_build_its_worker_stops_listing_is_requeued() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe().await;
    state.queue.lock().await.push_back(job(0, replay));
    let mut w = connect(&state, "w", 1, &["k0"]);
    let build_id = w.assignment().await.expect("not assigned").build_id;
    w.send(resumed(&build_id, false)).await;
    w.to_hub.send(listing(&["k0"])).await.unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(state.metrics.requeued.load(Ordering::Relaxed), 0);
    w.to_hub.send(listing(&[])).await.unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(state.metrics.requeued.load(Ordering::Relaxed), 1);
    let queue = state.queue.lock().await;
    let lost = queue.front().expect("not requeued");
    assert_eq!((lost.attempts, lost.transport_losses), (1, 0));
    assert!(
        lost.loss
            .as_ref()
            .is_some_and(|r| r.until <= Instant::now())
    );
    drop(queue);
    w.hub.abort();
    let _other = register(&state, "w2", &[]);
    let taken = state.take_job(&caps("w2"), None).await.expect("not taken");
    assert_eq!((taken.attempts, taken.transport_losses), (1, 0));
}

/// A worker that answers the hub's chunk request ResultGone has dropped
/// the build's result: the build is requeued at once, its session alive,
/// lost in transport and waited for no longer, whether a chunk or only
/// the end of them was due. Before, the hub took the answer for a broken
/// delivery and failed the build.
#[tokio::test]
async fn a_result_its_worker_dropped_is_requeued_at_once() {
    for chunks in [0, 1] {
        result_dropped(chunks).await;
    }
}

/// A build with `chunks` output chunks, none cached, whose worker resumes
/// it finished and answers the chunk request ResultGone.
async fn result_dropped(chunks: usize) {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe().await;
    let (mut j, out) = (job(0, replay), format!("/nix/store/{H}-out"));
    j.req.outputs = [("out".to_owned(), out.clone())].into();
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w", 1, &["k0"]);
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let manifest = Manifest {
        store_path: out,
        hashes: vec![7; 32 * chunks],
        sizes: vec![1; chunks],
        ..Default::default()
    };
    let result = BuildResult {
        build_id: build_id.clone(),
        outputs: vec![manifest],
        ..Default::default()
    };
    w.send(resumed(&build_id, true)).await;
    w.send(worker_message::Msg::Result(result)).await;
    let gone = ResultGone {
        build_id,
        run: None,
    };
    w.send(worker_message::Msg::ResultGone(gone)).await;
    let requeued = || state.metrics.requeued.load(Ordering::Relaxed);
    for _ in (0..200).take_while(|_| requeued() == 0) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let failed = state.metrics.failed.load(Ordering::Relaxed);
    assert_eq!((failed, requeued()), (0, 1), "{chunks} chunks");
    let queue = state.queue.lock().await;
    let lost = queue.front().expect("not requeued");
    let until = lost.loss.as_ref().map(|r| r.until);
    assert_eq!(lost.transport_losses, 1);
    assert!(until.is_some_and(|u| u <= Instant::now()), "waited for");
    drop(queue);
    w.hub.abort();
}

/// A key a worker session lists as held is honoured once: no second
/// build of it goes to the session until a heartbeat lists the key
/// again. A listed key is never read as unlisted, taken or not.
#[tokio::test]
async fn a_held_key_is_honoured_once_per_listing() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let w = register(&state, "w1", &["k1"]);
    for n in [1, 2, 3] {
        let mut j = job(n, Arc::new(Replay::default()));
        j.key = "k1".into();
        state.queue.lock().await.push_back(j);
    }
    assert!(state.take_job_by_key(w.id).await.is_some());
    assert!(
        state.take_job_by_key(w.id).await.is_none(),
        "one held build handed out twice"
    );
    state.hold(w.id, &["k1".to_owned()], true);
    assert!(
        state.take_job_by_key(w.id).await.is_some(),
        "listed again by a heartbeat, not honoured"
    );
    assert!(
        !state.unlisted(w.id, "k1", 0),
        "a listed key read as unlisted"
    );
}
