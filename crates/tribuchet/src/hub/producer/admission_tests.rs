//! DT19 (R1): drvPath changes nothing the hub decides. For a producer and a
//! plain build, each with every canLocal a client may send, set and unset
//! drvPath give the same dedupe key, the same canLocal as the hub records
//! it, the same admission verdict in each state of the routing table
//! (DD12), the same queue and the same placement expectation. The core's
//! events are a function of the key, the system and canLocal (shadow
//! hooks `submit` and `declined`), which this compares.

use std::time::{Duration, Instant};

use super::super::super::placement::Expected;
use super::super::super::submit::dedupe_key;
use super::super::super::tests::try_submit;
use super::*;
use crate::proto::BUILDER_RPC;

/// How the hub answered a submission.
#[derive(Debug, PartialEq)]
enum Admit {
    /// Queued (to run now, or held for a known worker).
    Queued,
    /// Still waiting for a worker expected back, unanswered.
    Waits,
    /// Declined (222): its client's Nix builds it.
    Declined,
    /// Refused with this status code.
    Refused(tonic::Code),
}

/// The state of the producer's worker (DESIGN.md, DD12).
#[derive(Clone, Copy, Debug)]
enum Routing {
    Connected,
    DepartedWithinGrace,
    KnownNotExpected,
    Decommissioned,
}

/// A worker's capabilities: serving producers and builds with network.
fn serving() -> WorkerCaps {
    worker_caps("w")
}

fn worker_caps(name: &str) -> WorkerCaps {
    WorkerCaps {
        name: name.into(),
        connected_at: Instant::now(),
        systems: [(
            "x86_64-linux".to_owned(),
            HashSet::from([BUILDER_RPC.to_owned(), NETWORK.to_owned()]),
        )]
        .into(),
        max_jobs: 1,
    }
}

/// A hub whose producer worker is in state `routing`.
fn hub(routing: Routing) -> (Arc<HubState>, Option<CapsGuard>) {
    let grace = match routing {
        Routing::DepartedWithinGrace => Duration::from_secs(30),
        _ => Duration::ZERO,
    };
    let mut state = HubState::for_test(grace);
    state.started_at = Instant::now().checked_sub(Duration::from_mins(5)).unwrap();
    let state = Arc::new(state);
    let guard = match routing {
        Routing::Connected => Some(CapsGuard::new(state.clone(), serving())),
        Routing::DepartedWithinGrace => {
            state.record_departed(serving());
            None
        }
        Routing::KnownNotExpected => {
            state.known.learn(&serving());
            None
        }
        Routing::Decommissioned => None,
    };
    (state, guard)
}

/// Everything the hub decides of `req` in `routing`: the key, the answer,
/// and, where it queued the build, canLocal as recorded, the queued keys
/// and the placement expectation.
type Decided = (String, Admit, Option<(Vec<bool>, Vec<String>, Expected)>);

async fn decide(req: BuildRequest, routing: Routing) -> Decided {
    let (state, _worker) = hub(routing);
    let key = dedupe_key(&req, b"");
    let answer = tokio::time::timeout(Duration::from_millis(500), try_submit(&state, req)).await;
    let (admit, _events) = match answer {
        Err(_) => (Admit::Waits, None),
        Ok(Err(status)) => (Admit::Refused(status.code()), None),
        Ok(Ok(mut events)) => {
            let first = tokio::time::timeout(Duration::from_millis(500), events.message()).await;
            let declined = matches!(
                first,
                Ok(Ok(Some(AttachEvent {
                    event: Some(Event::ExitCode(crate::proto::DECLINE_EXIT_CODE))
                })))
            );
            let admit = if declined {
                Admit::Declined
            } else {
                Admit::Queued
            };
            (admit, Some(events))
        }
    };
    let queue = state.queue.lock().await;
    let queued = queue.front().map(|j| {
        let expected = state.placement.lock().unwrap().expect(j);
        let keys = queue.iter().map(|j| j.key.clone()).collect();
        (j.replay.can_local(), keys, expected)
    });
    (key, admit, queued)
}

fn plain(can_local: Option<bool>) -> BuildRequest {
    BuildRequest {
        required_features: vec![],
        attach_id: vec![],
        ..producer_request(can_local)
    }
}

#[tokio::test]
async fn drv_path_changes_no_key_no_admission_no_placement() {
    let routings = [
        Routing::Connected,
        Routing::DepartedWithinGrace,
        Routing::KnownNotExpected,
        Routing::Decommissioned,
    ];
    let fixtures = [producer_request, plain];
    for fixture in fixtures {
        for can_local in [Some(true), Some(false), None] {
            for routing in routings {
                let unset = fixture(can_local);
                let set = BuildRequest {
                    drv_path: Some(format!("/nix/store/{H}-producer.drv")),
                    ..unset.clone()
                };
                let (a, b) = (decide(unset, routing).await, decide(set, routing).await);
                assert_eq!(a, b, "{routing:?}, can_local {can_local:?}");
            }
        }
    }
}

/// OQ1's fallback (a) is a relayed loss's alone (producer::lost_relayed):
/// admitted, a producer whose Nix did not say (no canBuildLocally) is
/// taken for one whose Nix can, as any build without network is: declined
/// where no worker serves producers, canLocal recorded true where one does.
#[tokio::test]
async fn a_producer_whose_nix_did_not_say_is_admitted_as_one_that_can() {
    for routing in [Routing::Connected, Routing::Decommissioned] {
        let unsaid = decide(producer_request(None), routing).await;
        let can = decide(producer_request(Some(true)), routing).await;
        assert_eq!(unsaid, can, "{routing:?}");
    }
}
