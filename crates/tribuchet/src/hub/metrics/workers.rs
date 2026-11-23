//! The `/workers` page: connected workers as indented JSON.

use std::collections::{BTreeMap, BTreeSet};

use crate::hub::state::HubState;

/// The connected workers as JSON, for a person at an incident. Per-worker
/// detail lives here, which keeps the series count independent of worker churn.
pub(super) fn render_workers_json(state: &HubState) -> String {
    let mut workers: Vec<_> = state
        .worker_caps
        .lock()
        .unwrap()
        .values()
        .map(|c| {
            let systems: BTreeMap<_, _> = c
                .systems
                .iter()
                .map(|(s, f)| (s.clone(), f.iter().cloned().collect::<BTreeSet<_>>()))
                .collect();
            serde_json::json!({
                "name": c.name,
                "systems": systems,
                "max_jobs": c.max_jobs,
                "connected_seconds": c.connected_at.elapsed().as_secs(),
            })
        })
        .collect();
    workers.sort_by_key(|w| w["name"].as_str().map(str::to_owned));
    serde_json::to_string_pretty(&workers).unwrap_or_default() + "\n"
}

/// Whether the request line asks for the worker list. Anything else
/// gets the metrics.
pub(super) fn wants_workers(request: &[u8]) -> bool {
    let line = request.split(|b| *b == b'\r').next().unwrap_or_default();
    let mut parts = line.split(|b| *b == b' ');
    parts.next() == Some(b"GET")
        && parts
            .next()
            .is_some_and(|p| p == b"/workers" || p.starts_with(b"/workers?"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;

    use super::*;
    use crate::hub::metrics::render;
    use crate::hub::state::WorkerCaps;
    use crate::hub::tests::caps;
    use crate::hub::{CapsGuard, Router, route_loop};
    use crate::proto::{Heartbeat, WorkerMessage, worker_message};

    #[test]
    fn only_a_get_of_workers_lists_the_workers() {
        assert!(wants_workers(b"GET /workers HTTP/1.1\r\nHost: x\r\n\r\n"));
        assert!(wants_workers(b"GET /workers?pretty HTTP/1.1\r\n\r\n"));
        assert!(!wants_workers(b"GET /metrics HTTP/1.1\r\n\r\n"));
        assert!(!wants_workers(b"GET /workersx HTTP/1.1\r\n\r\n"));
        assert!(!wants_workers(b"POST /workers HTTP/1.1\r\n\r\n"));
        assert!(!wants_workers(b""));
    }

    #[test]
    fn workers_json_lists_name_systems_and_slots_sorted() {
        let state = HubState::for_test(Duration::ZERO);
        for (id, name, system, features, max_jobs) in [
            (1, "b-worker", "x86_64-linux", vec!["kvm"], 8),
            (2, "a-worker", "aarch64-linux", vec![], 4),
        ] {
            state.worker_caps.lock().unwrap().insert(
                id,
                WorkerCaps {
                    name: name.into(),
                    connected_at: Instant::now(),
                    systems: [(
                        system.to_owned(),
                        features.iter().map(|f| (*f).to_owned()).collect(),
                    )]
                    .into(),
                    max_jobs,
                },
            );
        }
        let v: serde_json::Value = serde_json::from_str(&render_workers_json(&state)).unwrap();
        assert_eq!(v[0]["name"], "a-worker");
        assert_eq!(v[1]["name"], "b-worker");
        assert_eq!(v[1]["systems"]["x86_64-linux"], serde_json::json!(["kvm"]));
        assert_eq!(v[1]["max_jobs"], 8);
        assert!(v[0]["connected_seconds"].is_u64());
    }

    /// A heartbeat that gives its worker `slots`.
    fn heartbeat(slots: u32) -> WorkerMessage {
        let beat = Heartbeat {
            max_jobs: Some(slots),
            ..Heartbeat::default()
        };
        WorkerMessage {
            msg: Some(worker_message::Msg::Heartbeat(beat)),
        }
    }

    /// A worker whose heartbeat says it has no slot, as one does that
    /// reaches no build agent, counts as without agents until a heartbeat
    /// gives it a slot.
    #[tokio::test]
    async fn a_heartbeat_with_no_slot_counts_its_worker_as_without_agents() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let guard = CapsGuard::new(state.clone(), caps("w"));
        let id = guard.id;
        let without = async || {
            let out = render(&state).await;
            let name = "tribuchet_workers_without_agents ";
            (out.lines().find_map(|l| l.strip_prefix(name))).map(str::to_owned)
        };
        assert_eq!(without().await.as_deref(), Some("0"));
        for (slots, want) in [(0, "1"), (2, "0")] {
            let (tx, rx) = mpsc::channel(1);
            tx.send(heartbeat(slots)).await.unwrap();
            drop(tx);
            route_loop(rx, Router::default(), Arc::default(), state.clone(), id).await;
            assert_eq!(without().await.as_deref(), Some(want), "{slots}");
        }
    }
}
