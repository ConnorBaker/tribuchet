use std::time::{Duration, Instant};

use tokio::net::UnixListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Code;
use tonic::transport::Server;

use super::*;
use crate::errors::chain;
use crate::hub::shadow::Client;
use crate::hub::state::Replay;
use crate::hub::tests::{job, register};
use crate::proto::hub_admin_server::HubAdminServer;
use crate::proto::{AttachEvent, DECLINE_EXIT_CODE, attach_event};

type Rx = tokio::sync::mpsc::Receiver<Result<AttachEvent, Status>>;

/// A hub past its startup grace, which awaits a departed worker 30 s.
fn hub() -> Arc<HubState> {
    let mut state = HubState::for_test(Duration::from_secs(30));
    state.started_at = Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
    Arc::new(state)
}

/// The hub's answer to decommissioning `name`.
async fn retire(state: &Arc<HubState>, name: &str) -> Result<(), Code> {
    let svc = AdminSvc {
        state: state.clone(),
    };
    let worker_name = name.to_owned();
    let request = Request::new(DecommissionRequest { worker_name });
    (svc.decommission(request).await)
        .map(|_| ())
        .map_err(|s| s.code())
}

/// `tribuchet decommission` reaches the hub's HubAdmin over its socket and
/// says what the hub answered.
#[test]
fn the_command_reaches_the_hub_over_its_socket() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hub.sock");
    let rt = Runtime::new().unwrap();
    let uds = (rt.block_on(async { UnixListener::bind(&socket) })).unwrap();
    let svc = HubAdminServer::new(AdminSvc { state: hub() });
    let server = Server::builder().add_service(svc);
    rt.spawn(server.serve_with_incoming(UnixListenerStream::new(uds)));
    let said = chain(&decommission(&socket, "w").unwrap_err());
    assert!(said.contains("no worker w is known"), "{said}");
}

/// U4: an unknown worker, or a connected one, is not retired; a connected
/// one stays known.
#[tokio::test]
async fn only_a_known_absent_worker_is_decommissioned() {
    let state = hub();
    assert_eq!(retire(&state, "w").await, Err(Code::NotFound));
    let _w = register(&state, "w", &[]);
    assert_eq!(retire(&state, "w").await, Err(Code::FailedPrecondition));
    assert!(state.known.serves("x86_64-linux", &[]), "forgotten");
}

/// U4: a retired worker is no longer known or awaited, so a build only it
/// served is answered at once (L11′): declined to a client whose Nix
/// builds it, refused to one whose Nix cannot.
#[tokio::test]
async fn a_build_only_a_decommissioned_worker_served_is_answered() {
    let state = hub();
    drop(register(&state, "w", &[]));
    let replay = Arc::new(Replay::default());
    let mut local = replay.subscribe_as(Client::untracked(true)).await;
    let mut remote = replay.subscribe_as(Client::untracked(false)).await;
    state.queue.lock().await.push_back(job(1, replay));
    state.hold_unservable().await;
    assert_eq!(state.queue.lock().await.len(), 1, "not awaiting w");
    assert_eq!(retire(&state, "w").await, Ok(()));
    assert!(!state.known.serves("x86_64-linux", &[]), "still known");
    assert!(state.queue.lock().await.is_empty(), "still queued");
    let first = async |rx: &mut Rx| {
        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
        ev.ok().flatten().and_then(|e| e.ok()?.event)
    };
    let declined = attach_event::Event::ExitCode(DECLINE_EXIT_CODE);
    assert_eq!(first(&mut local).await, Some(declined));
    let refused = first(&mut remote).await;
    let said = |e: &str| e.contains("no worker this hub knows builds x86_64-linux");
    assert!(matches!(&refused, Some(attach_event::Event::Error(e)) if said(e)));
}
