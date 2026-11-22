//! A hold pass (state/hold.rs) against builds whose log is busy: what it
//! decides holds even if it is dropped or a client comes after (L11′,
//! cut-over 7).

use super::*;
use crate::hub::state::Inflight;
use crate::hub::state::queue_tests::hub_knowing;

/// A hold pass dropped midway, as hyper drops a Build handler whose client
/// went, loses no build: one a known worker serves stays queued, and one
/// no known worker serves is still answered once out of the queue (L11′).
#[tokio::test]
async fn a_dropped_hold_pass_loses_no_build() {
    for known in [true, false] {
        let state = hub_knowing(known);
        let (r1, r2) = (Arc::new(Replay::default()), Arc::new(Replay::default()));
        let mut first = r1.subscribe_as(Client::untracked(false)).await;
        let _second = r2.subscribe_as(Client::untracked(false)).await;
        state
            .queue
            .lock()
            .await
            .extend([job(1, r1.clone()), job(2, r2)]);
        // The pass stops at the first build's log, busy, and is dropped.
        let busy = r1.inner.lock().await;
        let mut pass = Box::pin(state.hold_unservable());
        let wait = tokio::time::timeout(Duration::from_millis(100), &mut pass).await;
        assert!(wait.is_err(), "known {known}: the pass did not stop");
        drop((pass, busy));
        let queued = state.queue.lock().await.len();
        assert_eq!(queued, if known { 2 } else { 0 }, "known {known}");
        let mut refused = false;
        while let Ok(Some(Ok(ev))) =
            tokio::time::timeout(Duration::from_secs(1), first.recv()).await
        {
            refused |= matches!(ev.event, Some(attach_event::Event::Error(_)));
        }
        assert_eq!(refused, !known, "known {known}: the first client's answer");
    }
}

/// A build no known worker serves is unlisted at the instant that is
/// decided, so a client that comes after starts a build of its own rather
/// than join one whose answer was decided before it came (cut-over 7).
#[tokio::test]
async fn an_ending_build_takes_no_client_after_its_decision() {
    let state = hub_knowing(false);
    let replay = Arc::new(Replay::default());
    let _client = replay.subscribe_as(Client::untracked(false)).await;
    let job = job(1, replay.clone());
    let listing = Inflight::list(&state.inflight, &job.key, vec![], &replay);
    *job.listing.lock().unwrap() = listing;
    state.queue.lock().await.push_back(job);
    // Its end waits on its busy log, after the decision.
    let busy = replay.inner.lock().await;
    let mut pass = Box::pin(state.hold_unservable());
    let wait = tokio::time::timeout(Duration::from_millis(100), &mut pass).await;
    assert!(wait.is_err(), "the pass did not stop");
    let listed = state.inflight.lock().unwrap().by_key.contains_key("k1");
    assert!(!listed, "joinable after the decision");
    drop(busy);
    pass.await;
}
