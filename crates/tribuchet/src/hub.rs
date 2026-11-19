//! `tribuchet hub`: scheduler and NAR relay, colocated with nix-daemon.
//!
//! - accepts build submissions from `attach` over a unix socket (gRPC/UDS)
//! - dedupes in-flight builds by scratch-output set; later identical
//!   submissions replay buffered events and then follow live
//! - queues per system type; submitters block until a worker is free
//! - serves the WorkerHub gRPC service over mTLS; workers dial in
//! - reads input store paths directly from local disk
//! - verifies output chunks against their manifest while relaying

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpConnectInfo;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::worker_hub_server::WorkerHub;
use crate::tailscale;

use crate::proto::{
    CancelBuild, HubMessage, Register, RequestJob, WorkerMessage, attach_event, hub_message,
    worker_message,
};

mod chunkcache;
#[cfg(test)]
pub(crate) use chunkcache::ChunkCache;
mod losses;
mod metrics;
mod naming;
mod placement;
mod relay;
mod serve;
mod state;
mod submit;
#[cfg(test)]
mod testlog;
pub use serve::run;

use crate::errors::{Error, Result, chain};
use losses::Dispatch;
use metrics::Metrics;
use relay::{WorkerSession, run_job, send};
use state::{Holding, HubState, Job, WorkerCaps};
use std::mem;

/// No worker message for this long tears the session down and fails
/// its builds: heartbeats flow every 30s, so silence means a dead
/// worker that would otherwise pin its builds (and dedupe keys) forever.
const WORKER_SILENCE_TIMEOUT: Duration = Duration::from_mins(3);

/// In tailscale mode the hub asks tailscaled who the peer is on every
/// session and uses that as the worker name; mTLS mode trusts the
/// transport layer (only certs signed by our CA can connect) and the
/// self-reported name.
enum PeerAuth {
    Mtls,
    Tailscale {
        socket: PathBuf,
        allowed_tags: Vec<String>,
    },
}

struct WorkerSvc {
    state: Arc<HubState>,
    auth: Arc<PeerAuth>,
}

/// Registers the worker's capabilities while alive; removes them on
/// drop so admission control tracks actual capacity.
struct CapsGuard {
    state: Arc<HubState>,
    id: u64,
    caps: WorkerCaps,
}

impl CapsGuard {
    fn new(state: Arc<HubState>, caps: WorkerCaps) -> Self {
        let id = state.next_worker_id.fetch_add(1, Ordering::Relaxed);
        state.worker_caps.lock().unwrap().insert(id, caps.clone());
        // Wake submissions waiting for a capable worker to appear.
        state.caps_changed.notify_waiters();
        state.regen_nix_config();
        Self { state, id, caps }
    }
}

impl Drop for CapsGuard {
    fn drop(&mut self) {
        self.state.worker_caps.lock().unwrap().remove(&self.id);
        self.state.holding.lock().unwrap().remove(&self.id);
        self.state.regen_nix_config();
        // Remember the platform so a build for it briefly waits for the
        // worker to reconnect instead of declining immediately.
        self.state.record_departed(self.caps.clone());
    }
}

/// Routes the single worker stream to per-build channels so multiple
/// jobs share one session. Dropping a sender closes the job's receiver,
/// which it observes as the worker going away.
#[derive(Default, Clone)]
struct Router {
    builds: Arc<Mutex<HashMap<String, mpsc::Sender<worker_message::Msg>>>>,
    closed: Arc<AtomicBool>,
}

impl Router {
    fn register(&self, build_id: &str) -> mpsc::Receiver<worker_message::Msg> {
        let (tx, rx) = mpsc::channel(64);
        self.builds.lock().unwrap().insert(build_id.to_string(), tx);
        rx
    }

    fn unregister(&self, build_id: &str) {
        self.builds.lock().unwrap().remove(build_id);
    }

    fn close_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.builds.lock().unwrap().clear();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// What the hub may still send one worker session. The worker alone
/// decides its capacity; the hub only takes off the assignments it sent
/// that a declaration had not counted yet.
#[derive(Default)]
struct Admission {
    /// Left of the worker's last declaration.
    free: u32,
    /// Builds handed to this session, each sent as one assignment.
    sent: u64,
    /// The last declaration as received, for the dispatch log.
    capacity: u32,
    assignments: Option<u64>,
}

impl Admission {
    /// A declaration replaces the count, less what it had not seen.
    /// A worker predating `assignments` sends none and its value
    /// stands as is, as before.
    fn declare(&mut self, r: &RequestJob) {
        self.capacity = r.capacity;
        self.assignments = r.assignments;
        let unseen = r
            .assignments
            .map_or(0, |seen| self.sent.saturating_sub(seen));
        self.free = r
            .capacity
            .saturating_sub(u32::try_from(unseen).unwrap_or(u32::MAX));
    }
}

fn msg_build_id(msg: &worker_message::Msg) -> Option<&str> {
    match msg {
        worker_message::Msg::Log(l) => Some(&l.build_id),
        worker_message::Msg::Result(r) => Some(&r.build_id),
        worker_message::Msg::Chunk(c) => Some(&c.build_id),
        worker_message::Msg::Resumed(r) => Some(&r.build_id),
        worker_message::Msg::Need(n) => Some(&n.build_id),
        worker_message::Msg::Register(_)
        | worker_message::Msg::Heartbeat(_)
        | worker_message::Msg::RequestJob(_) => None,
    }
}

/// Demux worker messages to their builds and enforce the session-wide
/// silence deadline; closes every build channel on the way out.
async fn route_loop(
    mut in_rx: mpsc::Receiver<WorkerMessage>,
    router: Router,
    admission: Arc<Mutex<Admission>>,
    state: Arc<HubState>,
    worker: u64,
) {
    loop {
        let m = match tokio::time::timeout(WORKER_SILENCE_TIMEOUT, in_rx.recv()).await {
            Err(_) => {
                tracing::warn!(
                    "worker sent nothing for {}s; assuming it is dead",
                    WORKER_SILENCE_TIMEOUT.as_secs()
                );
                break;
            }
            Ok(None) => break,
            Ok(Some(WorkerMessage { msg: Some(m) })) => m,
            Ok(Some(WorkerMessage { msg: None })) => continue,
        };
        state
            .metrics
            .count_msg(relay::msg_name(&m), m.encoded_len());
        if let worker_message::Msg::RequestJob(r) = &m {
            let free = {
                let mut a = admission.lock().unwrap();
                a.declare(r);
                a.free
            };
            // Several per build: debug, the dispatch line carries the last.
            tracing::debug!(
                session = worker,
                capacity = r.capacity,
                assignments = r.assignments,
                free,
                "capacity declared"
            );
            state.notify.notify_waiters();
            continue;
        }
        if let worker_message::Msg::Heartbeat(h) = &m {
            state.hold(worker, h.resumable_keys.iter().cloned().collect(), true);
        }
        let Some(id) = msg_build_id(&m).map(str::to_string) else {
            continue;
        };
        // clone outside the lock: a send must not block other routing
        let tx = router.builds.lock().unwrap().get(&id).cloned();
        if let Some(tx) = tx {
            // send error = job already ended; drop the message
            drop(tx.send(m).await);
        } else {
            tracing::warn!(id, "dropping worker message for unknown build");
        }
    }
    router.close_all();
}

#[tonic::async_trait]
impl WorkerHub for WorkerSvc {
    type SessionStream = ReceiverStream<Result<HubMessage, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let remote = request
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(|i| i.remote_addr);
        let mut inbound = request.into_inner();
        let Some(WorkerMessage {
            msg: Some(worker_message::Msg::Register(mut register)),
        }) = inbound.message().await?
        else {
            return Err(Status::invalid_argument("first message must be Register"));
        };
        if let PeerAuth::Tailscale {
            socket,
            allowed_tags,
        } = self.auth.as_ref()
        {
            let Some(addr) = remote else {
                return Err(Status::unauthenticated("no peer address"));
            };
            let who = tailscale::whois(socket, addr).await.map_err(|e| {
                tracing::warn!(%addr, "tailscale whois failed: {}", chain(&e));
                Status::unauthenticated("peer is not on the tailnet")
            })?;
            if !allowed_tags.is_empty() && !who.tags.iter().any(|t| allowed_tags.contains(t)) {
                tracing::warn!(
                    node = who.node_name,
                    tags = ?who.tags,
                    "rejecting worker without an allowed tailscale tag"
                );
                return Err(Status::permission_denied(
                    "tailscale node tag not in tailscale-allowed-tags",
                ));
            }
            // The tailnet-asserted name is authoritative; the worker's
            // self-reported one would let any tailnet peer impersonate
            // another in logs and metrics.
            register.worker_name = who.node_name;
        }
        tracing::info!(
            worker = register.worker_name,
            caps = ?register.caps,
            "worker registered"
        );

        let (out_tx, out_rx) = mpsc::channel::<Result<HubMessage, Status>>(64);
        let (in_tx, in_rx) = mpsc::channel::<WorkerMessage>(64);
        tokio::spawn(async move {
            while let Ok(Some(m)) = inbound.message().await {
                if in_tx.send(m).await.is_err() {
                    break;
                }
            }
        });
        tokio::spawn(worker_loop(self.state.clone(), register, out_tx, in_rx));
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

async fn worker_loop(
    state: Arc<HubState>,
    register: Register,
    out_tx: mpsc::Sender<Result<HubMessage, Status>>,
    in_rx: mpsc::Receiver<WorkerMessage>,
) {
    let caps = WorkerCaps {
        name: register.worker_name.clone(),
        connected_at: Instant::now(),
        systems: register
            .caps
            .iter()
            .map(|c| (c.system.clone(), c.features.iter().cloned().collect()))
            .collect(),
        max_jobs: register.max_jobs,
    };
    let caps_guard = CapsGuard::new(state.clone(), caps.clone());
    let router = Router::default();
    // Builds this worker still holds from before a hub restart; jobs
    // with these keys go to it regardless of capacity (their slots are
    // already occupied by them).
    let worker_id = caps_guard.id;
    state.hold(
        worker_id,
        register.resumable_keys.iter().cloned().collect(),
        false,
    );
    let admission = Arc::new(Mutex::new(Admission::default()));
    state
        .placement
        .lock()
        .unwrap()
        .open(worker_id, admission.clone());
    let mut route = tokio::spawn(route_loop(
        in_rx,
        router.clone(),
        admission.clone(),
        state.clone(),
        worker_id,
    ));
    let sess = Arc::new(WorkerSession::new(register.worker_name.clone()));

    'outer: loop {
        let (job, resumed) = loop {
            if out_tx.is_closed() || route.is_finished() {
                break 'outer;
            }
            // resumed builds already occupy their slot on the worker
            if let Some(job) = state.take_job_by_key(worker_id).await {
                admission.lock().unwrap().sent += 1;
                break (job, true);
            }
            let free = admission.lock().unwrap().free;
            if free > 0
                && let Some(job) = state.take_job(&caps).await
            {
                let mut a = admission.lock().unwrap();
                a.free = a.free.saturating_sub(1);
                a.sent += 1;
                break (job, false);
            }
            // notify_waiters() wakes only current waiters; the timeout
            // closes the race between checking the queue and awaiting.
            tokio::select! {
                () = state.notify.notified() => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = &mut route => break 'outer,
            }
        };
        placement::dispatch(&register.worker_name, &job, resumed, &admission);
        // Tell the attach client where its build runs.
        job.replay
            .publish(attach_event::Event::Dispatched(
                register.worker_name.clone(),
            ))
            .await;
        state
            .metrics
            .observe_queue(&job.req.system, job.queued_at.elapsed());
        let in_rx = router.register(&job.id);
        tokio::spawn(supervise_job(
            state.clone(),
            job,
            router.clone(),
            out_tx.clone(),
            in_rx,
            sess.clone(),
            (worker_id, register.worker_name.clone()),
            admission.clone(),
        ));
    }
    // Builds in flight fail through their closed router channels.
    route.abort();
    router.close_all();
    state.placement.lock().unwrap().close(worker_id);
    drop(caps_guard);
    state.hold_unservable().await;
    Metrics::inc(&state.metrics.disconnected);
    tracing::info!(worker = register.worker_name, "worker disconnected");
}

/// The loss of a build with the session of the worker that ran it. The
/// worker likely still runs it and re-registers shortly: until `until`,
/// the build is kept for it. A worker that said it no longer has the
/// build gets no grace; its loss is still the next dispatch's to settle.
struct Loss {
    /// Name the worker registers with, the identity a reconnect keeps.
    worker: String,
    /// Id of the session that died.
    session: u64,
    until: Instant,
}

impl Loss {
    /// Within the grace and the worker is not back. Back means
    /// registered with its held keys: from then on those route the job.
    fn awaited(&self, workers: &HashMap<u64, WorkerCaps>, holding: &HashMap<u64, Holding>) -> bool {
        Instant::now() < self.until
            && !workers.iter().any(|(id, c)| {
                *id != self.session && c.name == self.worker && holding.contains_key(id)
            })
    }
}

/// Run one dispatched job and settle it: finish, requeue or fail.
/// `worker` is the session's id and worker name.
async fn supervise_job(
    state: Arc<HubState>,
    mut job: Job,
    router: Router,
    out_tx: mpsc::Sender<Result<HubMessage, Status>>,
    in_rx: mpsc::Receiver<worker_message::Msg>,
    sess: Arc<WorkerSession>,
    worker: (u64, String),
    admission: Arc<Mutex<Admission>>,
) {
    let _running = state.metrics.running(&job.req.system);
    let dispatch_listed = state.list_dispatch(&job, worker.0);
    let mut dispatch = Dispatch::default();
    let res = run_job(
        &state,
        &job,
        (&out_tx, &worker.1, worker.0),
        in_rx,
        sess,
        &mut dispatch,
    )
    .await;
    // Before a requeue lists the build again elsewhere.
    drop(dispatch_listed);
    let dispatched = dispatch.sent;
    router.unregister(&job.id);
    if !dispatched {
        // The worker will never count an assignment that was not sent.
        let mut a = admission.lock().unwrap();
        a.sent = a.sent.saturating_sub(1);
    }
    // run_job counts the build verdict. Only session/hub-side errors
    // reach the branches below.
    let Err(err) = res else {
        state.finish(&job).await;
        return;
    };
    // A worker that no longer lists a build it resumed lost it as much
    // as one whose session died, though the session lives on.
    let unlisted = matches!(err, Error::Unlisted);
    let session_dead = unlisted || router.is_closed() || out_tx.is_closed();
    if matches!(err, Error::Superseded) {
        // Its worker delivers to the dispatch that took its key over.
        tracing::info!(
            id = job.id,
            "given up and taken over by a later dispatch of its key"
        );
        state.finish(&job).await;
        return;
    }
    if matches!(err, Error::GivenUp) {
        // Its clients left while it waited on the hub's daemon. Nobody
        // gets a verdict: a client resubmitting just now would get a
        // stream that ends without one, and resubmit again.
        tracing::info!(id = job.id, "no attach client left; giving the build up");
        let _ = cancel(&out_tx, &job).await;
        state.finish(&job).await;
        return;
    }
    let err = chain(&err);
    if !dispatched && session_dead {
        // The worker never got the assignment: no loss to count, and
        // any loss pending from before is still pending.
        tracing::warn!(id = job.id, "assignment not sent, requeueing build: {err}");
        Metrics::inc(&state.metrics.requeued);
        state.requeue(job).await;
        return;
    }
    if !dispatched {
        // The hub could not prepare the assignment: its own failure,
        // never a loss of the worker's (an input it does not have, or
        // that its daemon refused). A worker that took the build back by
        // key runs it already: cancel that.
        let _ = cancel(&out_tx, &job).await;
        if job.replay.has_subscribers().await {
            tracing::warn!(id = job.id, "build failed before dispatch: {err}");
            Metrics::inc(&state.metrics.failed);
            job.replay.publish(attach_event::Event::Error(err)).await;
        } else {
            tracing::info!(id = job.id, "dropping build before dispatch: {err}");
        }
        state.finish(&job).await;
        return;
    }
    // A dead session is no build verdict: requeue so this worker (by
    // dedupe key) or another one picks it up.
    if session_dead {
        let Some(j) = losses::lost(&state, job, &dispatch).await else {
            return;
        };
        job = j;
        tracing::warn!(id = job.id, "dispatch lost, requeueing build: {err}");
        Metrics::inc(&state.metrics.requeued);
        // A worker restart drops the session but not the build: keep
        // it for that worker until it re-registers, or the grace ends,
        // instead of running it a second time elsewhere. One that said
        // it no longer has it is waited for no longer; its next
        // dispatch starts it over.
        let (session, worker) = worker;
        let grace = if unlisted {
            Duration::ZERO
        } else {
            state.worker_grace
        };
        job.loss = Some(Loss {
            worker,
            session,
            until: Instant::now() + grace,
        });
        state.requeue(job).await;
    } else {
        tracing::warn!(id = job.id, "build failed: {err}");
        Metrics::inc(&state.metrics.failed);
        // The session is still up and may hold a half-staged or
        // running build for this id. Cancel frees that slot.
        let _ = cancel(&out_tx, &job).await;
        job.replay.publish(attach_event::Event::Error(err)).await;
        state.finish(&job).await;
    }
}

/// Tell the worker to cancel `job`; Err if its session is gone. A worker
/// that does not hold the build ignores it (worker/session.rs,
/// handle_cancel).
async fn cancel(out_tx: &mpsc::Sender<Result<HubMessage, Status>>, job: &Job) -> Result<()> {
    send(
        out_tx,
        hub_message::Msg::Cancel(CancelBuild {
            build_id: job.id.clone(),
            dedupe_key: job.key.clone(),
        }),
    )
    .await
}

#[cfg(test)]
mod tests;
