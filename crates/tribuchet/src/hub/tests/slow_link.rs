//! A worker session's Control and Session, each on a connection of its
//! own, here over a slow link (crate::slowlink).

use tokio_stream::wrappers::TcpListenerStream;

use super::*;
use crate::proto::worker_hub_client::WorkerHubClient;
use crate::proto::worker_hub_server::WorkerHubServer;
use crate::{chunkio, slowlink};

/// A worker predating Control opens its Session alone, its Register naming
/// no session: the hub refuses it, FAILED_PRECONDITION, and serves the
/// next worker, which opens both.
#[tokio::test]
async fn a_worker_predating_control_is_refused_and_the_hub_serves_on() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let svc = WorkerSvc {
        state: state.clone(),
        auth: Arc::new(PeerAuth::Mtls),
        pending: Default::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        Server::builder()
            .add_service(WorkerHubServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let register = |worker_name: &str, session: &[u8]| {
        let r = Register {
            worker_name: worker_name.into(),
            session: session.to_vec(),
            ..Register::default()
        };
        WorkerMessage {
            msg: Some(worker_message::Msg::Register(r)),
        }
    };
    let mut client = WorkerHubClient::connect(hub).await.unwrap();
    let old = client
        .session(tokio_stream::iter([register("old", &[])]))
        .await;
    let code = old.map(|_| ()).map_err(|s| s.code());
    assert_eq!(code, Err(tonic::Code::FailedPrecondition));
    let (ctl, ctl_rx) = mpsc::channel(1);
    let (bulk, bulk_rx) = mpsc::channel(1);
    ctl.send(register("new", &[1])).await.unwrap();
    bulk.send(register("", &[1])).await.unwrap();
    let _ctl = client.control(ReceiverStream::new(ctl_rx)).await.unwrap();
    let _bulk = client.session(ReceiverStream::new(bulk_rx)).await.unwrap();
    let served = || (state.worker_caps.lock().unwrap().values()).any(|c| c.name == "new");
    for _ in (0..100).take_while(|_| !served()) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(served(), "the next worker was not served");
}

/// The hub's worker service, for a worker's tests.
pub(crate) fn service() -> impl WorkerHub {
    WorkerSvc {
        state: Arc::new(HubState::for_test(Duration::from_secs(30))),
        auth: Arc::new(PeerAuth::Mtls),
        pending: Default::default(),
    }
}

/// The hub beats on each stream of a session, and ends one whose Session
/// says nothing for SILENCE_TIMEOUT, its Control beating: a peer silent on
/// either stream may be gone, to the hub as to the worker.
#[tokio::test(start_paused = true)]
async fn the_hub_beats_and_ends_a_session_silent_on_its_session() {
    let start = tokio::time::Instant::now();
    let ((to_hub, mut from_hub), (_to_bulk, mut from_bulk)) = slowlink::session(service()).await;
    keep_alive(&to_hub);
    for stream in [&mut from_hub, &mut from_bulk] {
        let beat = tokio::time::timeout(chunkio::HEARTBEAT * 2, stream.message()).await;
        assert!(
            matches!(beat, Ok(Ok(Some(HubMessage { msg: None })))),
            "{beat:?}"
        );
    }
    while let Ok(Some(_)) = from_hub.message().await {}
    let lived = start.elapsed();
    let silence = SILENCE_TIMEOUT..SILENCE_TIMEOUT + slowlink::one_chunk();
    assert!(silence.contains(&lived), "lived {lived:?}");
}

/// worker_loop on one stream each way, which the test reads and writes
/// as both the session's Control and its Session.
pub(super) fn spawn_session(
    state: Arc<HubState>,
    register: Register,
    out_tx: mpsc::Sender<Result<HubMessage, Status>>,
    in_rx: mpsc::Receiver<WorkerMessage>,
) -> tokio::task::JoinHandle<()> {
    let (bulk, bulk_rx) = mpsc::channel(1);
    // Its Session beats, as a worker's does.
    chunkio::beat(&bulk, WorkerMessage::default);
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
