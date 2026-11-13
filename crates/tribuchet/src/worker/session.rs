//! Hub session: connect, register, and drive the per-build message loop.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::poll_fn;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use super::build::{ActiveBuild, StagingStatus, Wake, validate_assignment};
use super::caps::{build_memory, cores, own_cgroup, system_caps};
use super::lease::Lease;
use super::resume::{
    CANCELLED, ResumableBuild, ack_delivery, execute_to_finished, record_finished, serve_chunks,
    try_deliver,
};
use super::{WorkerCtx, hostname, loadavg1, msg};
use crate::chunkio;
use crate::config::{Auth, WorkerConfig};
use crate::errors::{Error, Result, chain, err_ctx, err_msg};
use crate::proto::{
    BuildAssignment, BuildResult, CancelBuild, Heartbeat, HubMessage, MAX_MSG_SIZE, Need, Register,
    Resumed, Started, WorkerMessage, hub_message, worker_hub_client::WorkerHubClient,
    worker_message,
};
use crate::sd;

/// nix/flakelet-worker.nix greps the unit's StatusText for this.
pub const CONNECTED_STATUS: &str = "connected to hub";

pub(super) async fn session(opts: &WorkerConfig, ctx: &Arc<WorkerCtx>) -> Result<()> {
    let mut endpoint = Endpoint::from_shared(opts.hub.clone())?;
    if matches!(opts.auth, Auth::Mtls) {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(
                fs::read(&opts.ca_cert).map_err(err_ctx("reading CA cert"))?,
            ))
            .identity(Identity::from_pem(
                fs::read(&opts.cert).map_err(err_ctx("reading worker cert"))?,
                fs::read(&opts.key).map_err(err_ctx("reading worker key"))?,
            ));
        endpoint = endpoint.tls_config(tls)?;
    }
    let channel = chunkio::session_endpoint(endpoint)
        .connect()
        .await
        .map_err(err_ctx("connecting to hub"))?;
    let mut client = WorkerHubClient::new(channel)
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE);

    let (out_tx, out_rx) = mpsc::channel::<WorkerMessage>(64);
    // Register must be the first message the hub reads; it fits in the
    // channel buffer, so queue it before the stream is consumed.
    out_tx
        .send(msg(worker_message::Msg::Register(Register {
            worker_name: hostname(),
            caps: system_caps(opts, ctx),
            resumable_keys: ctx.resumable_keys(),
            max_jobs: u32::try_from(ctx.agents.room().capacity).unwrap_or(u32::MAX),
            cores: cores(),
            build_memory: build_memory(
                Path::new("/sys/fs/cgroup"),
                Path::new("/proc/meminfo"),
                &own_cgroup().unwrap_or_default(),
            ),
        })))
        .await?;

    let mut inbound = client
        .session(ReceiverStream::new(out_rx))
        .await?
        .into_inner();
    tracing::info!(hub = opts.hub, systems = ?opts.systems, "connected to hub");
    sd::notify_status(CONNECTED_STATUS);

    let mut active: HashMap<String, ActiveBuild> = HashMap::new();
    let result = session_loop(
        &mut inbound,
        &mut active,
        &out_tx,
        ctx,
        Duration::from_secs(opts.build_timeout_secs.get()),
    )
    .await;
    // Builds still staging when the session dies must not keep their
    // unpackers writing; executing builds finish on their own threads.
    for (_, build) in active.drain() {
        build.abort().await;
    }
    result
}

async fn session_loop(
    inbound: &mut tonic::Streaming<HubMessage>,
    active: &mut HashMap<String, ActiveBuild>,
    out_tx: &mpsc::Sender<WorkerMessage>,
    ctx: &Arc<WorkerCtx>,
    build_timeout: Duration,
) -> Result<()> {
    let env = LaunchEnv {
        ctx,
        out_tx,
        build_timeout,
        assignments: AtomicU64::default(),
        outbox: Outbox::default(),
    };
    // Builds staged to completion but waiting for a free slot.
    let mut ready: VecDeque<String> = VecDeque::new();
    let mut heartbeat = tokio::time::interval(chunkio::HEARTBEAT);
    let mut sender = PollSender::new(out_tx.clone());
    let (wake_tx, mut wake_rx) = mpsc::unbounded_channel::<Wake>();
    env.outbox.declare.set(true);
    loop {
        let m = tokio::select! {
            room = poll_fn(|cx| sender.poll_reserve(cx)), if env.outbox.pending() => {
                let lost = |_| err_msg("hub connection lost");
                room.map_err(lost)?;
                let queued = env.outbox.queue.borrow_mut().pop_front();
                let (m, sent) = queued.unwrap_or_else(|| {
                    if env.outbox.declare.take() {
                        (capacity(active, &ready, &env), None)
                    } else {
                        env.outbox.beat.set(false);
                        (beat(active, ctx), None)
                    }
                });
                sender.send_item(m).map_err(lost)?;
                if let Some(sent) = sent {
                    let _ = sent.send(());
                }
                continue;
            }
            () = ctx.agents.changed.notified() => {
                grant_slots(active, &mut ready, &env);
                env.outbox.declare.set(true);
                continue;
            }
            _ = heartbeat.tick() => {
                env.outbox.beat.set(true);
                env.outbox.declare.set(true);
                continue;
            }
            Some(w) = wake_rx.recv() => {
                if let Some(build) = active.get_mut(&w.build_id) {
                    let res = build.woken(&w.path);
                    advance_staging(active, &mut ready, &w.build_id, res, &env).await;
                }
                continue;
            }
            m = inbound.message() => {
                m?.ok_or_else(|| err_msg("hub closed the session"))?
            }
        };
        let Some(m) = m.msg else { continue };
        match m {
            hub_message::Msg::Assignment(a) => {
                env.assignments.fetch_add(1, Ordering::Relaxed);
                if let Some(id) = handle_assignment(a, active, &wake_tx, &env).await {
                    let res = active.get_mut(&id).unwrap().negotiate().await;
                    advance_staging(active, &mut ready, &id, res, &env).await;
                }
                env.outbox.declare.set(true);
            }
            hub_message::Msg::TmpDir(t) => {
                let id = t.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_tmp_dir(t).await.map(|s| (None, s));
                    advance_staging(active, &mut ready, &id, res, &env).await;
                }
            }
            hub_message::Msg::Manifest(m) => {
                let id = m.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_manifest(&m);
                    advance_staging(active, &mut ready, &id, res, &env).await;
                }
            }
            hub_message::Msg::Chunk(c) => {
                let id = c.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_chunk(&c);
                    advance_staging(active, &mut ready, &id, res, &env).await;
                }
            }
            hub_message::Msg::Cancel(c) => {
                handle_cancel(c, active, &env.outbox, ctx).await;
                env.outbox.declare.set(true);
            }
            hub_message::Msg::Need(n) => {
                serve_chunks(ctx, n.build_id, &n.hashes, out_tx.clone());
            }
            hub_message::Msg::ResultAck(a) => {
                ack_delivery(ctx, &a.dedupe_key, &a.build_id);
            }
        }
    }
}

/// Free slots, plus one lookahead build staged while every slot is
/// busy so a finishing build hands over without a round trip. None
/// with no agent reachable.
fn capacity(
    active: &HashMap<String, ActiveBuild>,
    ready: &VecDeque<String>,
    env: &LaunchEnv<'_>,
) -> WorkerMessage {
    let lookahead = ready.is_empty() && active.values().all(|b| b.lease.is_some());
    let assignments = env.assignments.load(Ordering::Relaxed);
    let asked = env.ctx.agents.room().request(lookahead, assignments);
    msg(worker_message::Msg::RequestJob(asked))
}

/// The worker's heartbeat: its state now.
fn beat(active: &HashMap<String, ActiveBuild>, ctx: &WorkerCtx) -> WorkerMessage {
    let staging = active.values().filter(|b| b.lease.is_some()).count();
    let room = ctx.agents.room();
    let running = room.leases.saturating_sub(staging);
    msg(worker_message::Msg::Heartbeat(Heartbeat {
        running_jobs: u32::try_from(running).unwrap_or(u32::MAX),
        load1: loadavg1(),
        resumable_keys: ctx.resumable_keys(),
        max_jobs: u32::try_from(room.capacity).ok(),
    }))
}

/// What session_loop sends, held until the session channel has room.
/// Outputs can fill the channel for minutes on a slow link; a loop
/// waiting there would stop reading the hub, whose messages are what
/// keeps the worker's keepalive from PINGing behind those outputs.
#[derive(Default)]
pub(super) struct Outbox {
    /// Sent in order, each told to its waiter (if any) once in the channel.
    queue: RefCell<VecDeque<(WorkerMessage, Option<oneshot::Sender<()>>)>>,
    /// A capacity declaration, a heartbeat due. Each is read as it is
    /// sent: it says the worker's state, so the latest is all that counts.
    declare: Cell<bool>,
    beat: Cell<bool>,
}

impl Outbox {
    fn send(&self, m: WorkerMessage) {
        self.queue.borrow_mut().push_back((m, None));
    }

    /// Queue `m`; the receiver hears once it is in the session channel,
    /// or errs if the session ends first.
    fn send_told(&self, m: WorkerMessage) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.queue.borrow_mut().push_back((m, Some(tx)));
        rx
    }

    fn pending(&self) -> bool {
        self.declare.get() || self.beat.get() || !self.queue.borrow().is_empty()
    }
}

/// Act on a staging step: forward a Need, launch, or abort.
async fn advance_staging(
    active: &mut HashMap<String, ActiveBuild>,
    ready: &mut VecDeque<String>,
    id: &str,
    res: Result<(Option<Need>, StagingStatus)>,
    env: &LaunchEnv<'_>,
) {
    let status = match res {
        Err(e) => {
            abort_active(active, id, &env.outbox, &e).await;
            env.outbox.declare.set(true);
            return;
        }
        Ok((need, status)) => {
            if let Some(n) = need {
                env.outbox.send(msg(worker_message::Msg::Need(n)));
            }
            status
        }
    };
    match status {
        StagingStatus::InProgress => {}
        StagingStatus::Ready => {
            let build = active.get_mut(id).unwrap();
            let Some(lease) = build.lease.take().or_else(|| env.ctx.agents.lease()) else {
                ready.push_back(id.to_string());
                return;
            };
            let build = active.remove(id).unwrap();
            launch_build(build, lease, env);
            env.outbox.declare.set(true);
        }
    }
}

/// Cancel a build. Still staging: tear it down right here. Already
/// executing: flag its dedupe key for the supervising loop. The key
/// is the stable identity, the build_id the hub knows may predate a
/// concurrent resume.
pub(super) async fn handle_cancel(
    c: CancelBuild,
    active: &mut HashMap<String, ActiveBuild>,
    outbox: &Outbox,
    ctx: &Arc<WorkerCtx>,
) {
    tracing::info!(id = c.build_id, "hub cancelled the build");
    if let Some(build) = active.remove(&c.build_id) {
        build.abort().await;
        fail_build(outbox, &c.build_id, &err_msg(CANCELLED));
    } else {
        // Only flag builds that are still running: a key flagged for
        // an already-finished build would never be consumed and would
        // kill the next build sharing that dedupe key. The flag is
        // set while holding the registry lock so a build finishing
        // concurrently in record_finished cannot slip between the
        // check and the insert. The entry remembers the cancel, which a
        // later assignment of its key voids (adopt_assignment).
        let mut map = ctx.resumable.lock().unwrap();
        let open = |e: &&mut ResumableBuild| e.finished.is_none() && !(e.cancelled && !e.adopting);
        if let Some(e) = map.get_mut(&c.dedupe_key).filter(open) {
            e.cancelled = true;
            ctx.cancelled.lock().unwrap().insert(c.dedupe_key);
        }
        drop(map);
        // A build waiting for an agent looks at its cancel now.
        ctx.agents.wake();
    }
}

/// Everything needed to launch a staged build.
struct LaunchEnv<'a> {
    ctx: &'a Arc<WorkerCtx>,
    out_tx: &'a mpsc::Sender<WorkerMessage>,
    build_timeout: Duration,
    /// Assignments received this session, declared with the capacity.
    assignments: AtomicU64,
    outbox: Outbox,
}

/// Hand freed slots to staged builds waiting for one.
fn grant_slots(
    active: &mut HashMap<String, ActiveBuild>,
    ready: &mut VecDeque<String>,
    env: &LaunchEnv<'_>,
) {
    while let Some(id) = ready.front() {
        // cancel may have removed it from active
        if !active.contains_key(id) {
            ready.pop_front();
            continue;
        }
        let Some(lease) = env.ctx.agents.lease() else {
            break;
        };
        let build = active.remove(&ready.pop_front().unwrap()).unwrap();
        launch_build(build, lease, env);
    }
}

/// Adopt a re-dispatched build or register a fresh one for staging.
async fn handle_assignment(
    a: BuildAssignment,
    active: &mut HashMap<String, ActiveBuild>,
    wake: &mpsc::UnboundedSender<Wake>,
    env: &LaunchEnv<'_>,
) -> Option<String> {
    let ctx = env.ctx;
    // A key we already hold means a hub (likely freshly restarted)
    // re-dispatched a build we are running or have finished: adopt
    // the new build_id and deliver the result when there is one,
    // instead of building again.
    if let Some(adoption) = ctx.adopt_assignment(&a, env.out_tx) {
        tracing::info!(id = a.build_id, key = a.dedupe_key, "build resumed");
        let (ctx, out_tx) = (ctx.clone(), env.out_tx.clone());
        // Told first, off the loop. Read after the adoption: a run
        // finished by then has its result delivered to it.
        tokio::task::spawn_blocking(move || {
            let finished = (ctx.resumable.lock().unwrap().get(&a.dedupe_key))
                .is_some_and(|e| e.finished.is_some());
            let build_id = a.build_id.clone();
            let resumed = msg(worker_message::Msg::Resumed(Resumed { build_id, finished }));
            if out_tx.blocking_send(resumed).is_ok() {
                try_deliver(&ctx, &a.dedupe_key, Some(adoption));
            }
        });
        return None;
    }
    tracing::info!(id = a.build_id, "build assigned");
    // build ids are never reused; a duplicate is a confused hub
    if let Some(old) = active.remove(&a.build_id) {
        tracing::warn!(id = old.assignment.build_id, "discarding duplicate build");
        old.abort().await;
    }
    let build_id = a.build_id.clone();
    match validate_assignment(&a).and_then(|()| ActiveBuild::new(a, ctx.clone(), wake.clone())) {
        Ok(mut b) => {
            b.lease = ctx.agents.lease();
            active.insert(build_id.clone(), b);
            Some(build_id)
        }
        Err(e) => {
            fail_build(&env.outbox, &build_id, &e);
            None
        }
    }
}

/// Register a fully-staged build as resumable and run it on a blocking
/// thread; the result is delivered via the resumable registry, so the
/// build outlives this session.
fn launch_build(build: ActiveBuild, mut lease: Lease, env: &LaunchEnv<'_>) {
    let (ctx, out_tx, build_timeout) = (env.ctx.clone(), env.out_tx.clone(), env.build_timeout);
    let key = build.assignment.dedupe_key.clone();
    ResumableBuild::launch(
        &ctx,
        key.clone(),
        // No log tail: execute() streams the log live itself.
        ResumableBuild {
            build_id: build.assignment.build_id.clone(),
            out_tx: Some(out_tx.clone()),
            dir: build.dir.clone(),
            worker_name: build.assignment.worker_name.clone(),
            ..ResumableBuild::default()
        },
    );
    // The hub is told that the run starts after all the loop queued for
    // the build (its Need) and before the run sends anything: the run
    // waits for its Started to enter the channel. A session gone by then
    // misses it, as one gone just after it would.
    let build_id = build.assignment.build_id.clone();
    let started = (env.outbox).send_told(msg(worker_message::Msg::Started(Started { build_id })));
    tokio::task::spawn_blocking(move || {
        let _ = started.blocking_recv();
        let fin = execute_to_finished(&build, &mut lease, &out_tx, build_timeout);
        drop((build, lease));
        record_finished(&ctx, &key, fin);
    });
}

/// Tear down a still-staging build and report the error to the hub.
async fn abort_active(
    active: &mut HashMap<String, ActiveBuild>,
    id: &str,
    outbox: &Outbox,
    e: &Error,
) {
    if let Some(build) = active.remove(id) {
        build.abort().await;
    }
    fail_build(outbox, id, e);
}

/// Report a per-build failure to the hub without tearing the session down.
fn fail_build(outbox: &Outbox, build_id: &str, err: &Error) {
    let err = chain(err);
    tracing::error!(id = build_id, "build setup failed: {err}");
    outbox.send(msg(worker_message::Msg::Result(BuildResult {
        build_id: build_id.into(),
        exit_code: 1,
        outputs: vec![],
        error: err,
        usage: None,
    })));
}

#[cfg(test)]
mod slow_link_tests;
#[cfg(test)]
mod started_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// A declaration tells the hub which of its capacity are free slots,
    /// apart from the one build staged ahead while every build holds a lease
    /// and none waits for one: none ahead with a build ready.
    #[tokio::test]
    async fn a_declaration_tells_free_slots_from_the_lookahead() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let agents = ["1.sock", "2.sock"].map(|s| dir.path().join(s)).to_vec();
        let ctx = WorkerCtx::for_test(dir.path(), 4, agents)?;
        let _lease = ctx.agents.lease();
        let (out_tx, _) = mpsc::channel(1);
        let env = LaunchEnv {
            ctx: &ctx,
            out_tx: &out_tx,
            build_timeout: Duration::ZERO,
            assignments: AtomicU64::default(),
            outbox: Outbox::default(),
        };
        for (ready, want) in [(vec![], (2, Some(1))), (vec!["b".into()], (1, Some(1)))] {
            let Some(worker_message::Msg::RequestJob(r)) =
                capacity(&HashMap::new(), &VecDeque::from(ready), &env).msg
            else {
                panic!("expected RequestJob");
            };
            assert_eq!((r.capacity, r.free_slots), want);
        }
        Ok(())
    }
}
