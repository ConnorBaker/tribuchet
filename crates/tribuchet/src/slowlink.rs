//! A worker session over a slow link, for tests on tokio's paused
//! clock: the hub's session server and the worker's endpoint as
//! deployed, joined by a queue that passes bytes on in order at RATE;
//! and scripted hubs to hold sessions with.

use std::io;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Endpoint, Uri};
use tonic::{Request, Response, Status, Streaming};
use tower::service_fn;

use crate::chunkio::{self, H2_CONNECTION_WINDOW, H2_STREAM_WINDOW, KEEPALIVE_TIMEOUT};
use crate::proto::worker_hub_client::WorkerHubClient;
use crate::proto::worker_hub_server::{WorkerHub, WorkerHubServer};
use crate::proto::{ChunkFrame, HubMessage, WorkerMessage};

/// The link's rate in bytes per second. h2 hands a window back to the
/// sender once the receiver has consumed half of what remained of it
/// (h2 0.4.15 flow_control.rs:93-107), so a sender with more to send
/// keeps over 2/3 of H2_STREAM_WINDOW in flight. At this rate that
/// takes KEEPALIVE_TIMEOUT to drain: a PING sent behind it misses the
/// timeout whatever the phase of the window updates.
pub(crate) const RATE: f64 = 2.0 / 3.0 * H2_STREAM_WINDOW as f64 / KEEPALIVE_TIMEOUT.as_secs_f64();

/// Ten keepalive intervals.
pub(crate) const LONG: Duration = Duration::from_secs(10 * chunkio::KEEPALIVE_INTERVAL.as_secs());

/// A worker session on `hub` over the link: the worker's sender, its
/// first message `first` already queued, and what the worker reads.
pub(crate) async fn session(
    hub: impl WorkerHub,
    first: WorkerMessage,
) -> (mpsc::Sender<WorkerMessage>, Streaming<HubMessage>) {
    let (worker_end, hub_end) = link();
    tokio::spawn(
        chunkio::session_server()
            .add_service(WorkerHubServer::new(hub))
            .serve_with_incoming(tokio_stream::once(Ok::<_, io::Error>(hub_end))),
    );
    let mut end = Some(worker_end);
    let channel = chunkio::session_endpoint(Endpoint::from_static("http://hub.invalid"))
        .connect_with_connector(service_fn(move |_: Uri| {
            let end = end.take().map(TokioIo::new);
            async move { end.ok_or_else(|| io::Error::other("connected once")) }
        }))
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(64);
    tx.send(first).await.unwrap();
    let inbound = WorkerHubClient::new(channel)
        .session(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    (tx, inbound)
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

/// A hub whose every session is `serve`, run as a task of its own on
/// what the worker sends and what the hub sends it.
pub(crate) fn hub<F, Fut>(serve: F) -> Hub<F>
where
    F: Fn(Streaming<WorkerMessage>, mpsc::Sender<Result<HubMessage, Status>>) -> Fut,
{
    Hub(serve)
}

pub(crate) struct Hub<F>(F);

#[tonic::async_trait]
impl<F, Fut> WorkerHub for Hub<F>
where
    F: Fn(Streaming<WorkerMessage>, mpsc::Sender<Result<HubMessage, Status>>) -> Fut,
    F: Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type SessionStream = ReceiverStream<Result<HubMessage, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn((self.0)(request.into_inner(), tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// The two ends of the link. Its queue holds all that h2 lets in flight
/// (on 2026-11-12 a close took up to 20 s to cross a relayed path).
fn link() -> (DuplexStream, DuplexStream) {
    let queue = 2 * H2_CONNECTION_WINDOW as usize;
    let (a, a_link) = tokio::io::duplex(queue);
    let (b, b_link) = tokio::io::duplex(queue);
    let (a_in, a_out) = tokio::io::split(a_link);
    let (b_in, b_out) = tokio::io::split(b_link);
    tokio::spawn(pump(a_in, b_out));
    tokio::spawn(pump(b_in, a_out));
    (a, b)
}

/// One direction: what `from` reads leaves on `to` in order, at RATE.
async fn pump(mut from: impl AsyncRead + Unpin, mut to: impl AsyncWrite + Unpin) {
    let mut buf = vec![0; 1 << 14];
    let mut free = Instant::now();
    while let Ok(n @ 1..) = from.read(&mut buf).await {
        free = free.max(Instant::now()) + Duration::from_secs_f64(n as f64 / RATE);
        tokio::time::sleep_until(free).await;
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    drop(to.shutdown().await);
}
