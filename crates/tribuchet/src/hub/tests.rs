use super::*;
use std::collections::HashSet;
use std::io;

use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Server, Uri};
use tower::service_fn;

use chunkcache::ChunkCache;
use state::{FLOOR, Replay};
use submit::AttachSvc;
use testlog::Captured;

use crate::chunkstore::{Hash, io_hook};
use crate::proto::attach_hub_client::AttachHubClient;
use crate::proto::{
    AttachEvent, BuildAssignment, BuildMessage, BuildRequest, Heartbeat, SystemCaps, TmpDirChunk,
    attach_hub_server, build_message,
};

mod attempts;
mod clientless;
mod clientless_chain;
mod clientless_farm;
mod daemon;
pub(super) mod fake_daemon;
mod unlisted;
mod usage;

/// 32-char base32 hash part for synthetic store paths.
pub(super) const H: &str = "00000000000000000000000000000000";

fn heartbeat(_max_jobs: Option<u32>) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::Heartbeat(Heartbeat::default())),
    }
}

/// Submit `req` through the attach service on a connection of its own,
/// as the attach program does: the build's events, as its client reads
/// them.
pub(super) async fn submit(state: &Arc<HubState>, req: BuildRequest) -> Streaming<AttachEvent> {
    try_submit(state, req).await.unwrap()
}

/// What the attach service answers `req` with.
pub(super) async fn try_submit(
    state: &Arc<HubState>,
    req: BuildRequest,
) -> Result<Streaming<AttachEvent>, Status> {
    let svc = attach_hub_server::AttachHubServer::new(AttachSvc {
        state: state.clone(),
    });
    let (client, server) = tokio::io::duplex(1 << 16);
    tokio::spawn(
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::once(Ok::<_, io::Error>(server))),
    );
    let mut client = Some(client);
    let channel = Endpoint::from_static("http://hub.invalid")
        .connect_with_connector(service_fn(move |_: Uri| {
            let io = client.take().map(TokioIo::new);
            async move { io.ok_or_else(|| io::Error::other("connected once")) }
        }))
        .await
        .unwrap();
    let submission = [
        build_message::Msg::Request(req),
        build_message::Msg::TmpDir(TmpDirChunk {
            zstd_chunk: Vec::new(),
            eof: true,
        }),
    ]
    .map(|msg| BuildMessage { msg: Some(msg) });
    AttachHubClient::new(channel)
        .build(tokio_stream::iter(submission))
        .await
        .map(tonic::Response::into_inner)
}
/// `n` chunks of 16 KiB, admitted to `cache` off the runtime as an
/// output's: its recipe and their frames.
pub(super) fn cached_output(cache: &ChunkCache, n: u8) -> (Vec<(Hash, usize)>, Vec<Vec<u8>>) {
    io_hook::off_runtime(|| {
        (0..n)
            .map(|i| {
                let mut raw = vec![0u8; 16 * 1024];
                blake3::Hasher::new()
                    .update(&[i])
                    .finalize_xof()
                    .fill(&mut raw);
                let hash = *blake3::hash(&raw).as_bytes();
                let frame = zstd::bulk::compress(&raw, 3).unwrap();
                cache.admit(hash, &frame).unwrap();
                ((hash, raw.len()), frame)
            })
            .unzip()
    })
}

/// Heartbeats on `to_hub` every minute, as a worker sends them every
/// 30 s. Without them the hub ends the session at WORKER_SILENCE_TIMEOUT,
/// which long waits outlast, and a build whose session died is requeued.
fn keep_alive(to_hub: &mpsc::Sender<WorkerMessage>) {
    let to_hub = to_hub.clone();
    tokio::spawn(async move {
        while to_hub.send(heartbeat(None)).await.is_ok() {
            tokio::time::sleep(Duration::from_mins(1)).await;
        }
    });
}

fn caps(name: &str) -> WorkerCaps {
    WorkerCaps {
        name: name.into(),
        connected_at: Instant::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 1,
    }
}

/// A worker session as worker_loop registers it.
fn register(state: &Arc<HubState>, name: &str, held: &[&str]) -> CapsGuard {
    let guard = CapsGuard::new(state.clone(), caps(name));
    state.hold(
        guard.id,
        held.iter().map(|k| (*k).to_owned()).collect(),
        false,
    );
    guard
}

pub(super) fn job(n: usize, replay: Arc<Replay>) -> Job {
    Job {
        id: format!("{n:032x}"),
        key: format!("k{n}"),
        req: BuildRequest {
            system: "x86_64-linux".into(),
            ..Default::default()
        },
        tmp_dir_pack: Arc::new(Vec::new()),
        features: vec![],
        replay,
        listing: Arc::default(),
        attempts: 0,
        resumed_losses: 0,
        queued_at: Instant::now(),
        loss: None,
    }
}

/// Dispatch `job` to `worker`, which says `said` about it, if anything,
/// before its session dies, as when the worker unit restarts while the
/// build runs.
async fn lose_session(
    state: &Arc<HubState>,
    job: Job,
    worker: CapsGuard,
    said: Option<worker_message::Msg>,
) {
    let router = Router::default();
    let in_rx = router.register(&job.id);
    if let Some(m) = said {
        let tx = router.builds.lock().unwrap()[&job.id].clone();
        tx.send(m).await.unwrap();
    }
    let (out_tx, _out_rx) = mpsc::channel(64);
    let session = (worker.id, worker.caps.name.clone());
    router.close_all();
    drop(worker);
    let sess = Arc::new(WorkerSession::new("w".into()));
    let admission = Arc::new(Mutex::new(Admission::default()));
    supervise_job(
        state.clone(),
        job,
        router,
        out_tx,
        in_rx,
        sess,
        session,
        admission,
    )
    .await;
}

/// A queued build with a client attached, which a test session's worker
/// has no free slot for: work that wants a slot. Returns its client.
async fn wanting_a_slot(state: &HubState, n: usize) -> mpsc::Receiver<Result<AttachEvent, Status>> {
    let j = job(n, Arc::new(Replay::default()));
    let client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    client
}

fn request_job(capacity: u32, assignments: Option<u64>) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::RequestJob(RequestJob {
            capacity,
            assignments,
        })),
    }
}

/// A hub session the test drives as a worker.
pub(super) struct Worker {
    to_hub: mpsc::Sender<WorkerMessage>,
    from_hub: mpsc::Receiver<Result<HubMessage, Status>>,
    hub: tokio::task::JoinHandle<()>,
}

/// Worker `name`, with `slots`, registered holding the builds `held`.
pub(super) fn connect(state: &Arc<HubState>, name: &str, slots: u32, held: &[&str]) -> Worker {
    let register = Register {
        worker_name: name.into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec![],
        }],
        resumable_keys: held.iter().map(|k| (*k).to_owned()).collect(),
        max_jobs: slots,
    };
    let (out_tx, from_hub) = mpsc::channel(256);
    let (to_hub, in_rx) = mpsc::channel(64);
    let hub = tokio::spawn(worker_loop(state.clone(), register, out_tx, in_rx));
    Worker {
        to_hub,
        from_hub,
        hub,
    }
}

impl Worker {
    /// The next assignment, if one comes within two seconds.
    async fn assignment(&mut self) -> Option<BuildAssignment> {
        let wait = Duration::from_secs(2);
        while let Ok(Some(m)) = tokio::time::timeout(wait, self.from_hub.recv()).await {
            if let Ok(HubMessage {
                msg: Some(hub_message::Msg::Assignment(a)),
            }) = m
            {
                return Some(a);
            }
        }
        None
    }

    async fn send(&self, msg: worker_message::Msg) {
        self.to_hub
            .send(WorkerMessage { msg: Some(msg) })
            .await
            .unwrap();
    }
}

/// A hub session for a two-slot worker with `jobs` queued builds,
/// each with a client attached.
struct Session {
    state: Arc<HubState>,
    to_hub: mpsc::Sender<WorkerMessage>,
    from_hub: mpsc::Receiver<Result<HubMessage, Status>>,
    hub: tokio::task::JoinHandle<()>,
    clients: Vec<mpsc::Receiver<Result<AttachEvent, Status>>>,
}

async fn session(jobs: Vec<Job>) -> Session {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut clients = Vec::new();
    for j in jobs {
        clients.push(j.replay.subscribe().await);
        state.queue.lock().await.push_back(j);
    }
    let Worker {
        to_hub,
        from_hub,
        hub,
    } = connect(&state, "w", 2, &[]);
    Session {
        state,
        to_hub,
        from_hub,
        hub,
        clients,
    }
}

fn queued(n: usize) -> Vec<Job> {
    (0..n)
        .map(|i| job(i, Arc::new(Replay::default())))
        .collect()
}

/// Assignments the hub sends until it is quiet for `quiet`.
async fn assignments(
    from_hub: &mut mpsc::Receiver<Result<HubMessage, Status>>,
    quiet: Duration,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(Some(m)) = tokio::time::timeout(quiet, from_hub.recv()).await {
        if let Ok(HubMessage {
            msg: Some(hub_message::Msg::Assignment(a)),
        }) = m
        {
            ids.push(a.build_id);
        }
    }
    ids
}

const QUIET: Duration = Duration::from_millis(500);

/// What is left of a worker's declaration (worker/session.rs
/// declare_capacity): its capacity less the assignments sent that it
/// had not seen. A worker re-declares as the first assignment takes a
/// slot, and again on its first heartbeat tick, before the others in
/// flight reach it; neither may be granted on top of what was sent. A
/// worker predating the count declares what is left as is.
#[test]
fn a_declaration_leaves_free_what_the_worker_has_not_been_sent() {
    let rows = [
        (0, 3, Some(0), 3),
        (3, 2, Some(1), 0),
        (3, 3, Some(0), 0),
        (3, 1, Some(3), 1),
        (5, 1, Some(0), 0),
        (2, 1, None, 1),
    ];
    for (sent, capacity, assignments, free) in rows {
        let mut a = Admission {
            sent,
            ..Admission::default()
        };
        a.declare(&RequestJob {
            capacity,
            assignments,
        });
        assert_eq!(
            a.free, free,
            "{sent} sent; {capacity} declared, {assignments:?} seen"
        );
    }
}

/// A build whose assignment could not be prepared (here: the local
/// daemon cannot be asked about a cached input, and the client left
/// while the hub waited for it) is given up unsent. The worker never
/// counted it, so it must not keep holding one of the worker's slots.
#[tokio::test(start_paused = true)]
async fn an_assignment_never_sent_holds_no_capacity() {
    let input = format!("/nix/store/{H}-cached");
    let mut unsendable = job(0, Arc::new(Replay::default()));
    unsendable.req.input_paths = vec![input.clone()];
    let sendable = job(1, Arc::new(Replay::default()));
    let mut s = session(vec![unsendable, sendable]).await;
    keep_alive(&s.to_hub);
    s.state
        .chunks
        .store_recipe(input, vec![0], Arc::new(Vec::new()));
    s.to_hub.send(request_job(1, Some(0))).await.unwrap();
    // Dispatched, so the hub is past the abandoned-client check and
    // waits for its daemon; the client leaves.
    let mut client = s.clients.remove(0);
    while let Some(Ok(AttachEvent { event })) = client.recv().await {
        if matches!(event, Some(attach_event::Event::Dispatched(_))) {
            break;
        }
    }
    drop(client);
    // Given up once the client has been gone FLOOR, the other build
    // wanting the slot, checked every 2 s.
    tokio::time::sleep(FLOOR + Duration::from_secs(6)).await;
    s.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let sent = assignments(&mut s.from_hub, QUIET).await;
    s.hub.abort();
    assert_eq!(sent, vec![format!("{:032x}", 1)]);
}

/// The dispatch line names the derivation and carries the session's
/// last capacity declaration and what is left of it.
#[tokio::test]
async fn the_dispatch_line_names_the_derivation_and_the_declaration() {
    let log = Captured::default();
    let _guard = log.install();
    let mut named = job(0, Arc::new(Replay::default()));
    named.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-hello-2.12"))].into();
    let mut s = session(vec![named]).await;
    s.to_hub.send(request_job(3, Some(0))).await.unwrap();
    assert_eq!(assignments(&mut s.from_hub, QUIET).await.len(), 1);
    s.hub.abort();
    let line = log.line("dispatching build");
    for field in [
        r#"drv="hello-2.12""#,
        "worker=",
        "resumed=false",
        "capacity=3",
        "assignments=0",
        "free=2",
        "sent=1",
    ] {
        assert!(line.contains(field), "{field} missing from {line}");
    }
}
