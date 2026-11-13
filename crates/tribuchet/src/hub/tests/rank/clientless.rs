//! A build with no client given up for the load placement charges it
//! (hub/state/clientless.rs).

use super::*;

/// The brev-5 incident: a build with no client, charged 3 of worker a's
/// 4 cores, sent builds with a client to b (2 of 4 cores busy) while a
/// kept an idle agent. Once its clients have been gone FLOOR, a build
/// with a client that a would win without that load, whether a passes
/// it to b or b keeps it from a, has it given up. It is kept, however
/// long, where a loses the build anyway, nothing arrives, the arriving
/// build has no client either, or its own client is still there.
#[tokio::test(start_paused = true)]
async fn a_build_with_no_client_is_given_up_when_its_load_sends_work_elsewhere() {
    // a's other load, its client stays, who asks, the asking build's
    // client stays, given up.
    let rows = [
        ("a passes to b", 0.0, false, Some("a"), true, true),
        ("b keeps it from a", 0.0, false, Some("b"), true, true),
        ("a loses anyway", 3.0, false, Some("a"), true, false),
        ("nothing arrives", 0.0, false, None, true, false),
        ("no client arrives", 0.0, false, Some("a"), false, false),
        ("its client is here", 0.0, true, Some("a"), true, false),
    ];
    for (row, other, stays, asks, asking, given_up) in rows {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let (a, a_caps) = seated(&state, "a", (2, 1), SMALL, (other, 0.0));
        let (b, b_caps) = seated(&state, "b", (2, 1), SMALL, (2.0, 0.0));
        let mut held = job(9, Arc::new(Replay::default()));
        held.expected = expecting(3.0, 0.0);
        let _held_client = if stays {
            Some(held.replay.subscribe().await)
        } else {
            None
        };
        state.placement.lock().unwrap().dispatched(a.id, &held);
        let _listed = state.list_dispatch(&held, a.id);
        tokio::time::sleep(2 * FLOOR).await;
        let client = queue(&state, 0, "build-0", expecting(1.0, 0.0)).await;
        let _client = asking.then_some(client);
        let asker = [("a", (&a_caps, a.id)), ("b", (&b_caps, b.id))].into_iter();
        if let Some((_, (caps, id))) = asker.into_iter().find(|(n, _)| Some(*n) == asks) {
            state.take_job(caps, Some(id)).await;
        }
        tokio::time::sleep(Duration::from_secs(3600)).await;
        let got = state.given_up(&held.id).is_cancelled();
        assert_eq!(got, given_up, "{row}");
    }
}
