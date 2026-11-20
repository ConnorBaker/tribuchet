//! Each client's dominant share of the farm (placement::objective's
//! Charges), as a dispatch logs it: kept as builds are placed and settle.
#![allow(clippy::float_cmp)]

use super::order::{Reader, open_session, queued, reads, terms};
use super::*;
use std::time::Duration;

use crate::hub::placement::{self as placement, Size};
use crate::hub::shadow::Client;

/// Build `n` placed on session 7 for `owners`.
pub(super) async fn placed(state: &HubState, n: usize, owners: &[&str]) -> (Job, Vec<Reader>) {
    let held = job(n, Arc::new(Replay::default()));
    let mut readers = Vec::new();
    for o in owners {
        readers.push(reads(&held.replay, o).await);
    }
    state.placement.lock().unwrap().dispatched(7, &held, false);
    (held, readers)
}

/// A client whose reader of a queued build is gone holds no share of it:
/// the build takes the least share of the clients still waiting.
#[tokio::test]
async fn a_client_gone_holds_no_share() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let _big = [
        placed(&state, 10, &["big"]).await,
        placed(&state, 11, &["big"]).await,
    ];
    let _r = queued(&state, 0, 1.0, 0, "big").await;
    let gone = reads(&state.queue.lock().await[0].replay.clone(), "gone").await;
    assert_eq!(terms(&state, 0).await.share, 0.0, "the waiting client's");
    drop(gone);
    assert_eq!(terms(&state, 0).await.share, 2.0 / 32.0, "a client gone");
}

/// A share is dominant: of a client's cores and memory, the larger part.
#[tokio::test]
async fn a_share_is_the_dominant_resource() {
    const GIB: f64 = 1_073_741_824.0;
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let mut readers = Vec::new();
    // "mem" holds 1 core and 32 GiB (share 1/2); "cpu" 8 cores and 1 GiB (1/4).
    for (n, owner, cores, gib) in [(10, "mem", 1.0, 32.0), (11, "cpu", 8.0, 1.0)] {
        let mut held = job(n, Arc::new(Replay::default()));
        held.expected.demand = placement::Demand {
            cores,
            memory: gib * GIB,
        };
        readers.push(reads(&held.replay, owner).await);
        state.placement.lock().unwrap().dispatched(7, &held, false);
    }
    let _r = [
        queued(&state, 0, 1.0, 0, "mem").await,
        queued(&state, 1, 1.0, 0, "cpu").await,
    ];
    assert_eq!(terms(&state, 0).await.share, 0.5);
    assert_eq!(terms(&state, 1).await.share, 0.25);
}

/// A placed build holds a share for each client waiting for it as it was
/// placed, counts once in its worker's load, and holds none once it
/// settles or its worker's session closes.
#[tokio::test]
async fn a_placed_build_holds_for_its_clients_until_it_settles() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let (held, _xy) = placed(&state, 9, &["x", "y"]).await;
    let load = state.placement.lock().unwrap().sessions[&7].load();
    assert_eq!(load, held.expected.demand, "counted once a client");
    let _r = [
        queued(&state, 0, 1.0, 0, "x").await,
        queued(&state, 1, 1.0, 0, "y").await,
        queued(&state, 2, 1.0, 0, "z").await,
    ];
    let one = 1.0 / 32.0;
    assert_eq!(terms(&state, 0).await.share, one, "the first client");
    assert_eq!(terms(&state, 1).await.share, one, "the second client");
    assert_eq!(terms(&state, 2).await.share, 0.0, "another");
    state.placement.lock().unwrap().settled(7, &held.id);
    assert_eq!(terms(&state, 0).await.share, 0.0, "settled");
    let _p = placed(&state, 8, &["x"]).await;
    assert_eq!(terms(&state, 0).await.share, one);
    // Another worker keeps the farm's total from 0 when session 7 closes.
    let size = Size {
        cores: 32.0,
        memory: Some(64.0 * 1_073_741_824.0),
    };
    state
        .placement
        .lock()
        .unwrap()
        .open(8, Arc::default(), size);
    state.placement.lock().unwrap().close(7);
    assert_eq!(terms(&state, 0).await.share, 0.0, "its session closed");
}

/// A placed build holds for the clients waiting for it as it was placed:
/// one joining it after holds nothing for it, and one leaving it still
/// does, until it settles.
#[tokio::test]
async fn a_client_joining_or_leaving_a_placed_build_counts_from_its_placing() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let (held, mut x) = placed(&state, 9, &["x"]).await;
    let _y = reads(&held.replay, "y").await;
    drop(x.pop());
    let _r = [
        queued(&state, 0, 1.0, 0, "x").await,
        queued(&state, 1, 1.0, 0, "y").await,
    ];
    assert_eq!(terms(&state, 0).await.share, 1.0 / 32.0, "x, gone since");
    assert_eq!(terms(&state, 1).await.share, 0.0, "y, joined since");
    state.placement.lock().unwrap().settled(7, &held.id);
    assert_eq!(terms(&state, 0).await.share, 0.0, "settled");
}

/// A build two clients wait for takes the lesser of their shares.
#[tokio::test]
async fn a_build_takes_its_least_clients_share() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let held = job(9, Arc::new(Replay::default()));
    let _hi = reads(&held.replay, "hi").await;
    state.placement.lock().unwrap().dispatched(7, &held, false);
    let _a = queued(&state, 0, 1.0, 0, "hi").await;
    let _b = reads(&state.queue.lock().await[0].replay.clone(), "lo").await;
    assert!(terms(&state, 0).await.share == 0.0, "not the least");
}

/// Clients their requests do not name (attaches older than the field)
/// are one client, charged: one holding a placed build gives another's
/// queued build its share.
#[tokio::test]
async fn unnamed_clients_are_one_charged_client() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let held = job(9, Arc::new(Replay::default()));
    let _a = held.replay.subscribe_as(Client::untracked(false)).await;
    state.placement.lock().unwrap().dispatched(7, &held, false);
    (state.queue.lock().await).push_back(job(0, Arc::new(Replay::default())));
    let r = state.queue.lock().await[0].replay.clone();
    let _b = r.subscribe_as(Client::untracked(false)).await;
    assert_eq!(terms(&state, 0).await.share, 1.0 / 32.0);
}

/// A client reading a placed build twice is charged for it once.
#[tokio::test]
async fn a_client_waiting_twice_is_charged_once() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let held = job(9, Arc::new(Replay::default()));
    let _x = [
        reads(&held.replay, "x").await,
        reads(&held.replay, "y").await,
        reads(&held.replay, "x").await,
    ];
    state.placement.lock().unwrap().dispatched(7, &held, false);
    let _r = queued(&state, 0, 1.0, 0, "x").await;
    assert_eq!(terms(&state, 0).await.share, 1.0 / 32.0);
}

/// A build no client waits for any more has share 0.
#[tokio::test]
async fn a_build_no_client_waits_for_has_share_zero() {
    let state = HubState::for_test(Duration::ZERO);
    state
        .queue
        .lock()
        .await
        .push_back(job(0, Arc::new(Replay::default())));
    assert_eq!(terms(&state, 0).await.share, 0.0);
}

/// A worker that reports no memory counts none, in the total or in its
/// builds' shares: a client holding 1 core and 32 GiB there holds 1 of
/// the farm's 64 cores.
#[tokio::test]
async fn a_worker_without_memory_counts_none() {
    const GIB: f64 = 1_073_741_824.0;
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let size = Size {
        cores: 32.0,
        memory: None,
    };
    state
        .placement
        .lock()
        .unwrap()
        .open(8, Arc::default(), size);
    let mut held = job(9, Arc::new(Replay::default()));
    held.expected.demand = placement::Demand {
        cores: 1.0,
        memory: 32.0 * GIB,
    };
    let _c = reads(&held.replay, "c").await;
    state.placement.lock().unwrap().dispatched(8, &held, false);
    let _a = queued(&state, 0, 1.0, 0, "c").await;
    assert_eq!(terms(&state, 0).await.share, 1.0 / 64.0);
}

/// On a farm whose workers report no memory, or none to use (a cgroup's
/// memory.max of 0), a share is of cores alone.
#[tokio::test]
async fn a_farm_without_memory_shares_cores() {
    for memory in [None, Some(0.0)] {
        let state = HubState::for_test(Duration::ZERO);
        let size = Size {
            cores: 32.0,
            memory,
        };
        state
            .placement
            .lock()
            .unwrap()
            .open(8, Arc::default(), size);
        let mut held = job(9, Arc::new(Replay::default()));
        held.expected.demand.memory = 1_073_741_824.0;
        let _c = reads(&held.replay, "c").await;
        state.placement.lock().unwrap().dispatched(8, &held, false);
        let _a = queued(&state, 0, 1.0, 0, "c").await;
        assert_eq!(terms(&state, 0).await.share, 1.0 / 32.0, "{memory:?}");
    }
}

/// A client holding two placed builds holds half as much once one settles,
/// and nothing, nor any entry, once both have.
#[tokio::test]
async fn a_settled_build_releases_its_part_of_a_share() {
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let (a, _ra) = placed(&state, 9, &["x"]).await;
    let (b, _rb) = placed(&state, 10, &["x"]).await;
    let _q = queued(&state, 0, 1.0, 0, "x").await;
    assert_eq!(terms(&state, 0).await.share, 2.0 / 32.0, "both placed");
    state.placement.lock().unwrap().settled(7, &a.id);
    assert_eq!(terms(&state, 0).await.share, 1.0 / 32.0, "one settled");
    state.placement.lock().unwrap().settled(7, &b.id);
    assert_eq!(terms(&state, 0).await.share, 0.0, "both settled");
    // Behind a proxy each build is its own client: an entry left for each
    // would grow without bound.
    assert_eq!(state.placement.lock().unwrap().charges.clients(), 0);
}
