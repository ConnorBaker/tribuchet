//! A worker session's Control and Session, each on a connection of its
//! own, here over a slow link (crate::slowlink).

use super::*;
use crate::{chunkio, slowlink};

/// worker_loop on one stream each way, which the test reads and writes
/// as both the session's Control and its Session.
pub(super) fn spawn_session(
    state: Arc<HubState>,
    register: Register,
    out_tx: mpsc::Sender<Result<HubMessage, Status>>,
    in_rx: mpsc::Receiver<WorkerMessage>,
) -> tokio::task::JoinHandle<()> {
    let (bulk, bulk_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _bulk = bulk;
        worker_loop(state, register, (out_tx.clone(), in_rx), (out_tx, bulk_rx)).await;
    })
}

/// A worker's heartbeats reach the hub at once while its outputs fill
/// the link: its Control waits behind none of them. Each says a new
/// capacity, which the hub takes up within the time the link takes to
/// carry one output chunk, as long as the hub would wait for a silent
/// worker and more.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_crosses_a_link_full_of_outputs() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let hub = WorkerSvc {
        state: state.clone(),
        auth: Arc::new(PeerAuth::Mtls),
        pending: Default::default(),
    };
    let ((to_hub, _from_hub), (to_bulk, _bulk)) = slowlink::session(hub).await;
    slowlink::flood(to_bulk, || WorkerMessage {
        msg: Some(worker_message::Msg::Chunk(slowlink::bulk())),
    });
    let max_jobs = || {
        (state.worker_caps.lock().unwrap().values())
            .map(|c| c.max_jobs)
            .next()
    };
    let start = tokio::time::Instant::now();
    for jobs in (1..).take_while(|_| start.elapsed() < slowlink::LONG) {
        tokio::time::sleep(chunkio::HEARTBEAT).await;
        to_hub.send(heartbeat(Some(jobs))).await.unwrap();
        tokio::time::sleep(slowlink::one_chunk()).await;
        assert_eq!(max_jobs(), Some(jobs), "after {:?}", start.elapsed());
    }
}

/// A build's tmp dir waits for the worker's first Need: it goes on the
/// worker's Session, where it could overtake an assignment, on its
/// Control, that the worker has not read yet.
#[tokio::test]
async fn a_tmp_dir_follows_the_workers_first_need() {
    let mut s = session(queued(1)).await;
    s.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let tmp_dir = async |from_hub: &mut mpsc::Receiver<Result<HubMessage, Status>>| {
        while let Ok(Some(Ok(m))) = tokio::time::timeout(QUIET, from_hub.recv()).await {
            if let Some(hub_message::Msg::TmpDir(t)) = m.msg {
                return Some(t.build_id);
            }
        }
        None
    };
    assert_eq!(tmp_dir(&mut s.from_hub).await, None, "before the Need");
    let build_id = format!("{:032x}", 0);
    let need = Need {
        build_id: build_id.clone(),
        ..Need::default()
    };
    let need = worker_message::Msg::Need(need);
    s.to_hub
        .send(WorkerMessage { msg: Some(need) })
        .await
        .unwrap();
    assert_eq!(tmp_dir(&mut s.from_hub).await, Some(build_id));
    s.hub.abort();
}
