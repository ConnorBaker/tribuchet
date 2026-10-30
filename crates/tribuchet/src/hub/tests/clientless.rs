//! Builds with no attach client (hub/state/clientless.rs), end to end.
//! Which ones the decider gives up is state/clientless/tests.rs's table.

use super::*;

/// What the hub sent a worker so far: the builds it assigned, and the
/// builds it told it to cancel, each with its key.
pub(super) fn sent(
    from_hub: &mut mpsc::Receiver<Result<HubMessage, Status>>,
) -> (Vec<String>, Vec<(String, String)>) {
    let (mut assigned, mut cancels) = (Vec::new(), Vec::new());
    while let Ok(m) = from_hub.try_recv() {
        match m.map(|m| m.msg) {
            Ok(Some(hub_message::Msg::Assignment(a))) => assigned.push(a.build_id),
            Ok(Some(hub_message::Msg::Cancel(c))) => cancels.push((c.build_id, c.dedupe_key)),
            _ => {}
        }
    }
    (assigned, cancels)
}

/// The builds the hub told a worker to cancel so far.
pub(super) fn cancelled(from_hub: &mut mpsc::Receiver<Result<HubMessage, Status>>) -> Vec<String> {
    sent(from_hub).1.into_iter().map(|(id, _)| id).collect()
}

/// With a build waiting for its slot, a client back 10 s after the last
/// one left keeps a build, one back 30 s after finds it given up (FLOOR
/// and a CHECK later). With none waiting it is kept for hours, not
/// cancelled a fixed time after its client left, and a client back then
/// joins it.
#[tokio::test(start_paused = true)]
async fn a_build_with_no_client_is_given_up_only_for_waiting_work() {
    // The hours-long row first: each row's hub runs on through the rows
    // after it.
    for (waiting, back_after, kept) in [(false, 10_800, true), (true, 10, true), (true, 30, false)]
    {
        let replay = Arc::new(Replay::default());
        let mut s = session(vec![job(0, replay.clone())]).await;
        s.to_hub.send(request_job(1, Some(0))).await.unwrap();
        assert_eq!(assignments(&mut s.from_hub, QUIET).await.len(), 1);
        let mut _wanting = None;
        if waiting {
            _wanting = Some(wanting_a_slot(&s.state, 1).await);
        } else {
            keep_alive(&s.to_hub);
        }
        drop(s.clients.remove(0));
        tokio::time::sleep(Duration::from_secs(back_after)).await;
        let _back = replay.subscribe().await;
        s.to_hub.send(heartbeat(None)).await.unwrap();
        tokio::time::sleep(Duration::from_mins(1)).await;
        let cancels = cancelled(&mut s.from_hub);
        s.hub.abort();
        assert_eq!(cancels.is_empty(), kept, "back after {back_after} s");
    }
}
