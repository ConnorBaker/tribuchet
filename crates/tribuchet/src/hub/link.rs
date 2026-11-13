//! A worker session's two streams, each on a connection of its own: its
//! Control, and its Session for bulk, which the worker opens second and
//! names by the session its Control registered.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::{Router, SILENCE_TIMEOUT};
use crate::errors::chain;
use crate::proto::{HubMessage, Register, WorkerMessage, worker_message};

/// What the hub sends on a stream of a worker session, what it reads.
pub(super) type Half = (
    mpsc::Sender<Result<HubMessage, Status>>,
    mpsc::Receiver<WorkerMessage>,
);

/// A Session's streams, handed to its Control.
type Bulk = (
    mpsc::Sender<Result<HubMessage, Status>>,
    Streaming<WorkerMessage>,
);

/// Open Controls, by session, waiting for their Session.
#[derive(Default)]
pub(super) struct Pending(Arc<Mutex<HashMap<Vec<u8>, oneshot::Sender<Bulk>>>>);

impl Pending {
    /// `register`'s Control, open on `inbound` and `out_tx`: with its
    /// Session once the worker opens that, which it does at once; never
    /// without.
    pub(super) fn control(
        &self,
        register: &Register,
        inbound: Streaming<WorkerMessage>,
        out_tx: mpsc::Sender<Result<HubMessage, Status>>,
    ) -> impl Future<Output = Option<(Half, Half)>> + use<> {
        let (bulk_tx, bulk) = oneshot::channel();
        let (pending, session) = (self.0.clone(), register.session.clone());
        pending.lock().unwrap().insert(session.clone(), bulk_tx);
        let worker = register.worker_name.clone();
        async move {
            let bulk = tokio::time::timeout(SILENCE_TIMEOUT, bulk).await;
            pending.lock().unwrap().remove(&session);
            let Ok(Ok((bulk_tx, bulk))) = bulk else {
                tracing::warn!(worker, "worker opened no Session for its Control");
                return None;
            };
            let ctl = (out_tx, read(inbound, worker.clone()));
            Some((ctl, (bulk_tx, read(bulk, worker))))
        }
    }

    /// A Session, authenticated by its session, which only its worker's
    /// Control tells the hub.
    pub(super) async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<ReceiverStream<Result<HubMessage, Status>>>, Status> {
        let mut inbound = request.into_inner();
        let Some(WorkerMessage {
            msg: Some(worker_message::Msg::Register(register)),
        }) = inbound.message().await?
        else {
            return Err(Status::invalid_argument("first message must be Register"));
        };
        let control = self.0.lock().unwrap().remove(&register.session);
        // None for a worker predating Control: its Register names no
        // session, and Control takes none without one.
        let Some(control) = control else {
            let worker = register.worker_name;
            tracing::warn!(worker, "refusing a Session without a Control");
            return Err(Status::failed_precondition(
                "open Control first: this hub takes a worker's control on its own connection",
            ));
        };
        let (out_tx, out_rx) = mpsc::channel(64);
        (control.send((out_tx, inbound)))
            .map_err(|_| Status::aborted("the session's Control is gone"))?;
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

/// Route the session's bulk. Either of its halves closing ends the
/// session, as does its silence: the worker beats on it as on its Control.
pub(super) async fn route_bulk(
    mut in_rx: mpsc::Receiver<WorkerMessage>,
    router: Router,
    out: mpsc::Sender<Result<HubMessage, Status>>,
) {
    loop {
        let m = tokio::select! {
            m = tokio::time::timeout(SILENCE_TIMEOUT, in_rx.recv()) => m.ok().flatten(),
            () = out.closed() => None,
        };
        let Some(m) = m else { break };
        if let Some(m) = m.msg {
            router.route(m).await;
        }
    }
    router.close_all();
}

/// The messages of `inbound`, as they come; closed with it.
fn read(mut inbound: Streaming<WorkerMessage>, worker: String) -> mpsc::Receiver<WorkerMessage> {
    let (in_tx, in_rx) = mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            match inbound.message().await {
                Ok(Some(m)) => {
                    if in_tx.send(m).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(worker, "worker stream failed: {}", chain(&e));
                    break;
                }
            }
        }
    });
    in_rx
}
