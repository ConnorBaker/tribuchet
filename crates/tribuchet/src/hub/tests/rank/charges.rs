//! Charges (placement/objective.rs): what a placed build holds for its
//! clients, from its take until it settles.

use super::memory::BIG;
use super::*;

/// A build is committed to its worker as it is taken (state/take.rs) and
/// again as it is dispatched (placement::dispatch): it holds one charge,
/// which its settling releases, leaving no client charged.
#[tokio::test]
async fn a_build_taken_and_dispatched_holds_one_charge_until_it_settles() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (2, 1), BIG, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let before = state.placement.lock().unwrap().charges.clients();
    let _c = queue(&state, 0, "solo-1", expecting(1.0, 2.0)).await;
    let job = state.take_job(&a_caps, Some(a.id)).await.expect("taken");
    let admission = state.placement.lock().unwrap().sessions[&a.id]
        .admission
        .clone();
    placement::dispatch(&state, (a.id, "a"), &job, false, &admission);
    state.placement.lock().unwrap().settled(a.id, &job.id);
    let after = state.placement.lock().unwrap().charges.clients();
    assert_eq!(after, before, "a settled build left a charge");
}

/// A build run through its worker's session, from its take to its result,
/// leaves no client charged once it settles.
#[tokio::test]
async fn a_build_run_to_its_result_leaves_no_client_charged() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = sized_worker(&state, "w1", 4, 8);
    w.to_hub.send(declare(3, 2, 0)).await.unwrap();
    let mut client = queue(&state, 0, "heavy-0", expecting(3.0, 1.0)).await;
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let result = BuildResult {
        build_id,
        exit_code: 0,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    assert_eq!(committed_cores(&state), 0.0, "settled");
    let clients = state.placement.lock().unwrap().charges.clients();
    assert_eq!(clients, 0, "a client still charged for a settled build");
}

/// The share a fresh take's dispatch logs is its clients' as the build
/// waited: the charge its take committed (state/take.rs) is not in it, as
/// a resume's dispatch, which follows no take, has none to count.
#[tokio::test]
async fn a_fresh_take_logs_the_share_its_clients_held_as_it_waited() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (a, a_caps) = seated(&state, "a", (3, 2), BIG, (0.0, 0.0));
    state.placement.lock().unwrap().settled(a.id, "in-flight");
    let _first = queue(&state, 0, "solo-1", expecting(4.0, 2.0)).await;
    let _held = state.take_job(&a_caps, Some(a.id)).await.expect("taken");
    let _c = queue(&state, 1, "solo-2", expecting(1.0, 2.0)).await;
    let waited = {
        let q = state.queue.lock().await;
        state.placement.lock().unwrap().share(&q[0])
    };
    assert_eq!(waited, 4.0 / 32.0, "the first build's charge, as it waited");
    let job = state.take_job(&a_caps, Some(a.id)).await.expect("taken");
    let admission = state.placement.lock().unwrap().sessions[&a.id]
        .admission
        .clone();
    placement::dispatch(&state, (a.id, "a"), &job, false, &admission);
    let line = log.line("dispatching build");
    let share = line
        .split_whitespace()
        .find_map(|t| t.strip_prefix("share="));
    let share: f64 = share.expect("a share").parse().unwrap();
    assert_eq!(share, placement::round3(waited), "{line}");
}
