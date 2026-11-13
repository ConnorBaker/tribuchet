//! A worker session over a slow link (crate::slowlink).

use super::*;
use crate::slowlink;

/// A worker sending outputs over a slow link to an otherwise idle hub
/// keeps its session. Its keepalive would PING behind the outputs and
/// miss the timeout, but the hub gives it something to read first.
#[tokio::test(start_paused = true)]
async fn a_worker_sending_outputs_over_a_slow_link_keeps_its_session() {
    let hub = WorkerSvc {
        state: Arc::new(HubState::for_test(Duration::from_secs(30))),
        auth: Arc::new(PeerAuth::Mtls),
    };
    let register = Register {
        worker_name: "w".into(),
        ..Default::default()
    };
    let register = WorkerMessage {
        msg: Some(worker_message::Msg::Register(register)),
    };
    let (to_hub, mut from_hub) = slowlink::session(hub, register).await;
    slowlink::flood(to_hub, || WorkerMessage {
        msg: Some(worker_message::Msg::Chunk(slowlink::bulk())),
    });
    let start = tokio::time::Instant::now();
    while let Ok(m) = tokio::time::timeout_at(start + slowlink::LONG, from_hub.message()).await {
        let lived = start.elapsed();
        match m {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the hub ended the session after {lived:?}"),
            Err(e) => panic!("the session died after {lived:?}: {}", chain(&e)),
        }
    }
}
