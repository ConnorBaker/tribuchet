//! A worker session over a slow link, for tests on tokio's paused
//! clock: the hub's server and the worker's endpoints as deployed, its
//! Control and its Session each on a connection of its own, whose bytes
//! share one bottleneck that passes them on at RATE, a slice of each
//! connection's in turn; and scripted hubs to hold sessions with.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Endpoint, Uri};
use tonic::{Request, Response, Status, Streaming};
use tower::service_fn;

use crate::chunkio::{self, H2_CONNECTION_WINDOW};
use crate::proto::worker_hub_client::WorkerHubClient;
use crate::proto::worker_hub_server::{WorkerHub, WorkerHubServer};
use crate::proto::{ChunkFrame, HubMessage, Register, WorkerMessage, worker_message};

/// The link's rate in bytes per second, about that of the relayed paths
/// of 2026-11-12.
pub(crate) const RATE: f64 = 1e6;

/// Longer than the hub waits for a silent worker.
pub(crate) const LONG: Duration = Duration::from_mins(6);

/// What the link takes to carry one bulk message: control waits for less.
pub(crate) fn one_chunk() -> Duration {
    Duration::from_secs_f64(chunkio::CHUNK_SIZE as f64 / RATE)
}

/// What one end of a stream sends and reads.
pub(crate) type End<S, R> = (mpsc::Sender<S>, Streaming<R>);

/// A worker session on `hub` over the link: the worker's end of its
/// Control, then of its Session, each Register already sent.
pub(crate) async fn session(
    hub: impl WorkerHub,
) -> (
    End<WorkerMessage, HubMessage>,
    End<WorkerMessage, HubMessage>,
) {
    let [(ctl, hub_ctl), (bulk, hub_bulk)] = links();
    tokio::spawn(
        chunkio::session_server()
            .add_service(WorkerHubServer::new(hub))
            .serve_with_incoming(tokio_stream::iter(
                [hub_ctl, hub_bulk].map(Ok::<_, io::Error>),
            )),
    );
    let register = |worker_name: &str| {
        let r = Register {
            worker_name: worker_name.into(),
            session: vec![1],
            ..Register::default()
        };
        WorkerMessage {
            msg: Some(worker_message::Msg::Register(r)),
        }
    };
    let (ctl_tx, ctl_rx) = mpsc::channel(64);
    ctl_tx.send(register("w")).await.unwrap();
    let ctl_in = (client(ctl).await.control(ReceiverStream::new(ctl_rx)).await)
        .unwrap()
        .into_inner();
    let (bulk_tx, bulk_rx) = mpsc::channel(64);
    bulk_tx.send(register("")).await.unwrap();
    let bulk_in = (client(bulk)
        .await
        .session(ReceiverStream::new(bulk_rx))
        .await)
        .unwrap()
        .into_inner();
    ((ctl_tx, ctl_in), (bulk_tx, bulk_in))
}

/// A worker's endpoint as deployed, connected over `end`.
async fn client(end: DuplexStream) -> WorkerHubClient<tonic::transport::Channel> {
    let mut end = Some(end);
    let channel = chunkio::session_endpoint(Endpoint::from_static("http://hub.invalid"))
        .connect_with_connector(service_fn(move |_: Uri| {
            let end = end.take().map(TokioIo::new);
            async move { end.ok_or_else(|| io::Error::other("connected once")) }
        }))
        .await
        .unwrap();
    WorkerHubClient::new(channel)
}

/// Send `bulk()` on `tx` until it closes, as a task serving chunks does.
pub(crate) fn flood<T: Send + 'static>(tx: mpsc::Sender<T>, bulk: fn() -> T) {
    tokio::spawn(async move { while tx.send(bulk()).await.is_ok() {} });
}

/// A chunk frame of CHUNK_SIZE, as bulk on either side of a session.
pub(crate) fn bulk() -> ChunkFrame {
    ChunkFrame {
        build_id: "b".into(),
        zstd: vec![0; chunkio::CHUNK_SIZE],
        ..Default::default()
    }
}

/// What the hub reads of a stream and sends on it.
pub(crate) type HubEnd = (
    Streaming<WorkerMessage>,
    mpsc::Sender<Result<HubMessage, Status>>,
);

/// A hub whose every session is `serve`, run as a task of its own on the
/// session's Control and Session.
pub(crate) fn hub<F, Fut>(serve: F) -> Hub<F>
where
    F: Fn(HubEnd, HubEnd) -> Fut,
{
    Hub(serve, Mutex::default())
}

/// The Control open, waiting for its Session.
pub(crate) struct Hub<F>(F, Mutex<Option<HubEnd>>);

#[tonic::async_trait]
impl<F, Fut> WorkerHub for Hub<F>
where
    F: Fn(HubEnd, HubEnd) -> Fut,
    F: Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type ControlStream = ReceiverStream<Result<HubMessage, Status>>;
    type SessionStream = ReceiverStream<Result<HubMessage, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        let (tx, rx) = mpsc::channel(64);
        *self.1.lock().unwrap() = Some((request.into_inner(), tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let (tx, rx) = mpsc::channel(64);
        let ctl = self.1.lock().unwrap().take().expect("Control first");
        tokio::spawn((self.0)(ctl, (request.into_inner(), tx)));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// The two ends of each of two connections, whose bytes share one
/// bottleneck in each direction. A connection's queue holds all that h2
/// lets in flight, as a send buffer would (on 2026-11-12 a close took up
/// to 20 s to cross a relayed path).
fn links() -> [(DuplexStream, DuplexStream); 2] {
    let free = || Arc::new(Mutex::new(Instant::now()));
    let (up, down) = (free(), free());
    [(); 2].map(|()| {
        let queue = 2 * H2_CONNECTION_WINDOW as usize;
        let (a, a_link) = tokio::io::duplex(queue);
        let (b, b_link) = tokio::io::duplex(queue);
        let (a_in, a_out) = tokio::io::split(a_link);
        let (b_in, b_out) = tokio::io::split(b_link);
        tokio::spawn(pump(a_in, b_out, Arc::clone(&up)));
        tokio::spawn(pump(b_in, a_out, Arc::clone(&down)));
        (a, b)
    })
}

/// One direction of one connection: what `from` reads leaves on `to` in
/// order, a slice at a time through the `bottleneck` (when it is next
/// free), at RATE.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    bottleneck: Arc<Mutex<Instant>>,
) {
    let mut buf = vec![0; 1 << 14];
    while let Ok(n @ 1..) = from.read(&mut buf).await {
        let free = {
            let mut free = bottleneck.lock().unwrap();
            *free = (*free).max(Instant::now()) + Duration::from_secs_f64(n as f64 / RATE);
            *free
        };
        tokio::time::sleep_until(free).await;
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    drop(to.shutdown().await);
}
