//! Hub session: connect, register, and drive the per-build message loop.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use super::build::{ActiveBuild, Negotiation, StagingStatus, Wake, validate_assignment};
use super::caps::{build_memory, cores, own_cgroup, system_caps};
use super::lease::Lease;
use super::resume::{
    CANCELLED, ResumableBuild, ack_delivery, execute_to_finished, record_finished, release,
    serve_chunks, try_deliver,
};
use super::{WorkerCtx, hostname, loadavg1, msg};
use crate::chunkio;
use crate::config::{Auth, WorkerConfig};
use crate::errors::{Error, Result, chain, err_ctx, err_msg};
use crate::proto::{
    BuildAssignment, BuildResult, CancelBuild, Heartbeat, HeldRun, HubMessage, MAX_MSG_SIZE, Need,
    Register, Resumed, RunId, Started, WorkerMessage, hub_message,
    worker_hub_client::WorkerHubClient, worker_message,
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
    // Control and bulk each on a connection of its own, so that no chunk
    // is queued ahead of control in a send buffer.
    let connect = async |endpoint| {
        let channel = (chunkio::session_endpoint(endpoint).connect().await)
            .map_err(err_ctx("connecting to hub"))?;
        Ok::<_, Error>(
            WorkerHubClient::new(channel)
                .max_decoding_message_size(MAX_MSG_SIZE)
                .max_encoding_message_size(MAX_MSG_SIZE),
        )
    };
    let (mut control, mut bulk) = (connect(endpoint.clone()).await?, connect(endpoint).await?);
    let mut session = vec![0; 16];
    getrandom::fill(&mut session).map_err(|e| err_msg(format!("randomness: {e}")))?;

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
            session: session.clone(),
            held: ctx.held(),
            lost: ctx.lost.lock().unwrap().clone(),
        })))
        .await?;
    let (bulk_tx, bulk_rx) = mpsc::channel::<WorkerMessage>(64);
    let register = Register {
        session,
        ..Register::default()
    };
    bulk_tx
        .send(msg(worker_message::Msg::Register(register)))
        .await?;

    // Control first: the hub then expects its Session.
    let mut inbound = (control.control(ReceiverStream::new(out_rx)).await?).into_inner();
    let mut bulk_in = (bulk.session(ReceiverStream::new(bulk_rx)).await?).into_inner();
    tracing::info!(hub = opts.hub, systems = ?opts.systems, "connected to hub");
    sd::notify_status(CONNECTED_STATUS);

    let mut active: HashMap<String, ActiveBuild> = HashMap::new();
    let result = session_loop(
        (&mut inbound, &mut bulk_in),
        &mut active,
        (&out_tx, &bulk_tx),
        ctx,
        Duration::from_secs(opts.build_timeout_secs),
    )
    .await;
    // Builds still staging when the session dies must not keep their
    // unpackers writing; executing builds finish on their own threads.
    for (_, build) in active.drain() {
        build.abort().await;
    }
    release(ctx, &out_tx);
    result
}

/// Drive a session: `inbound` and `out_tx` are its Control, `bulk_in`
/// and `bulk_tx` its Session.
async fn session_loop(
    (inbound, bulk_in): (&mut Streaming<HubMessage>, &mut Streaming<HubMessage>),
    active: &mut HashMap<String, ActiveBuild>,
    (out_tx, bulk_tx): (&mpsc::Sender<WorkerMessage>, &mpsc::Sender<WorkerMessage>),
    ctx: &Arc<WorkerCtx>,
    build_timeout: Duration,
) -> Result<()> {
    let env = LaunchEnv {
        ctx,
        out_tx,
        build_timeout,
        assignments: AtomicU64::default(),
    };
    // Builds staged to completion but waiting for a free slot.
    let mut ready: VecDeque<String> = VecDeque::new();
    let mut heartbeat = tokio::time::interval(chunkio::HEARTBEAT);
    let (wake_tx, mut wake_rx) = mpsc::unbounded_channel::<Wake>();
    // Negotiations with the daemon, off the loop: it reads the hub meanwhile.
    let mut negotiations = tokio::task::JoinSet::<Negotiation>::new();
    // The loop's sends wait on control alone: the bulk has its own stream.
    out_tx.send(capacity(active, &ready, &env)).await?;
    loop {
        let m = tokio::select! {
            () = ctx.agents.changed.notified() => {
                grant_slots(active, &mut ready, &env);
                out_tx.send(capacity(active, &ready, &env)).await?;
                continue;
            }
            _ = heartbeat.tick() => {
                out_tx.send(beat(active, ctx)).await?;
                out_tx.send(capacity(active, &ready, &env)).await?;
                continue;
            }
            Some(w) = wake_rx.recv() => {
                if let Some(build) = active.get_mut(&w.build_id) {
                    let res = build.woken(&w.path);
                    advance_staging(active, &mut ready, &w.build_id, res, &env).await?;
                }
                continue;
            }
            Some(done) = negotiations.join_next() => {
                let (id, dir, checked) = done?;
                // Not cancelled, nor its id assigned again, meanwhile.
                if let Some(build) = active.get_mut(&id).filter(|b| b.dir == dir) {
                    let res = build.negotiated(checked);
                    advance_staging(active, &mut ready, &id, res, &env).await?;
                }
                continue;
            }
            m = inbound.message() => {
                m?.ok_or_else(|| err_msg("hub closed the session"))?
            }
            m = bulk_in.message() => {
                m?.ok_or_else(|| err_msg("hub closed the session"))?
            }
        };
        let Some(m) = m.msg else { continue };
        match m {
            hub_message::Msg::Assignment(a) => {
                env.assignments.fetch_add(1, Ordering::Relaxed);
                if let Some(id) = handle_assignment(a, active, &wake_tx, &env).await? {
                    negotiations.spawn(active[&id].negotiation());
                }
                out_tx.send(capacity(active, &ready, &env)).await?;
            }
            hub_message::Msg::TmpDir(t) => {
                let id = t.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_tmp_dir(t).await.map(|s| (None, s));
                    advance_staging(active, &mut ready, &id, res, &env).await?;
                }
            }
            hub_message::Msg::Manifest(m) => {
                let id = m.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_manifest(&m);
                    advance_staging(active, &mut ready, &id, res, &env).await?;
                }
            }
            hub_message::Msg::Chunk(c) => {
                let id = c.build_id.clone();
                if let Some(build) = active.get_mut(&id) {
                    let res = build.feed_chunk(&c);
                    advance_staging(active, &mut ready, &id, res, &env).await?;
                }
            }
            hub_message::Msg::Cancel(c) => {
                handle_cancel(c, active, out_tx, ctx).await?;
                out_tx.send(capacity(active, &ready, &env)).await?;
            }
            hub_message::Msg::Need(n) => {
                serve_chunks(
                    ctx,
                    n.build_id,
                    &n.hashes,
                    (out_tx.clone(), bulk_tx.clone()),
                );
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
    let mut held = ctx.held();
    // Staging, and so not started.
    held.extend(active.values().filter_map(|b| {
        Some(HeldRun {
            run: Some(b.assignment.run?),
            dedupe_key: b.assignment.dedupe_key.clone(),
            ..HeldRun::default()
        })
    }));
    msg(worker_message::Msg::Heartbeat(Heartbeat {
        running_jobs: u32::try_from(running).unwrap_or(u32::MAX),
        load1: loadavg1(),
        resumable_keys: ctx.resumable_keys(),
        max_jobs: u32::try_from(room.capacity).ok(),
        held,
        lost: ctx.lost.lock().unwrap().clone(),
        ..ctx.arbiter.frozen_by()
    }))
}

/// Act on a staging step: forward a Need, launch, or abort.
async fn advance_staging(
    active: &mut HashMap<String, ActiveBuild>,
    ready: &mut VecDeque<String>,
    id: &str,
    res: Result<(Option<Need>, StagingStatus)>,
    env: &LaunchEnv<'_>,
) -> Result<()> {
    let status = match res {
        Err(e) => {
            abort_active(active, id, env.out_tx, &e).await?;
            return Ok(env.out_tx.send(capacity(active, ready, env)).await?);
        }
        Ok((need, status)) => {
            if let Some(n) = need {
                env.out_tx.send(msg(worker_message::Msg::Need(n))).await?;
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
                return Ok(());
            };
            let build = active.remove(id).unwrap();
            launch_build(build, lease, env);
            env.out_tx.send(capacity(active, ready, env)).await?;
        }
    }
    Ok(())
}

/// Cancel a build. Still staging: tear it down right here. Already
/// executing: flag its dedupe key for the supervising loop. The key
/// is the stable identity, the build_id the hub knows may predate a
/// concurrent resume.
pub(super) async fn handle_cancel(
    c: CancelBuild,
    active: &mut HashMap<String, ActiveBuild>,
    out_tx: &mpsc::Sender<WorkerMessage>,
    ctx: &Arc<WorkerCtx>,
) -> Result<()> {
    tracing::info!(id = c.build_id, "hub cancelled the build");
    if let Some(build) = active.remove(&c.build_id) {
        let run = build.assignment.run;
        build.abort().await;
        fail_build(out_tx, (&c.build_id, run), &err_msg(CANCELLED)).await?;
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
            ctx.cancelled.lock().unwrap().insert(c.dedupe_key.clone());
        }
        // A finished result is its run's outcome: the cancel says that its
        // dispatch wants it no longer, not that no later one will. Kept for
        // those (adopt_assignment), it is reaped a ttl on, as when its
        // session closes; a cancel for an earlier adoption leaves it.
        let held = |e: &&mut ResumableBuild| e.finished.is_some() && e.build_id == c.build_id;
        if let Some(e) = map.get_mut(&c.dedupe_key).filter(held) {
            e.released.get_or_insert_with(Instant::now);
        }
        drop(map);
        // A build waiting for an agent looks at its cancel now.
        ctx.agents.wake();
    }
    Ok(())
}

/// Everything needed to launch a staged build.
struct LaunchEnv<'a> {
    ctx: &'a Arc<WorkerCtx>,
    out_tx: &'a mpsc::Sender<WorkerMessage>,
    build_timeout: Duration,
    /// Assignments received this session, declared with the capacity.
    assignments: AtomicU64,
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
) -> Result<Option<String>> {
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
            let held = (ctx.resumable.lock().unwrap().get(&a.dedupe_key))
                .map(|e| (e.finished.is_some(), e.run));
            let (finished, run) = held.unwrap_or_default();
            let build_id = a.build_id.clone();
            let resumed = Resumed {
                build_id,
                finished,
                run,
            };
            let resumed = msg(worker_message::Msg::Resumed(resumed));
            if out_tx.blocking_send(resumed).is_ok() {
                try_deliver(&ctx, &a.dedupe_key, Some(adoption));
            }
        });
        return Ok(None);
    }
    tracing::info!(id = a.build_id, "build assigned");
    // build ids are never reused; a duplicate is a confused hub
    if let Some(old) = active.remove(&a.build_id) {
        tracing::warn!(id = old.assignment.build_id, "discarding duplicate build");
        old.abort().await;
    }
    let (build_id, run) = (a.build_id.clone(), a.run);
    match validate_assignment(&a).and_then(|()| ActiveBuild::new(a, ctx.clone(), wake.clone())) {
        Ok(mut b) => {
            b.lease = ctx.agents.lease();
            active.insert(build_id.clone(), b);
            Ok(Some(build_id))
        }
        Err(e) => {
            fail_build(env.out_tx, (&build_id, run), &e).await?;
            Ok(None)
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
            run: build.assignment.run,
            out_tx: Some(out_tx.clone()),
            dir: build.dir.clone(),
            worker_name: build.assignment.worker_name.clone(),
            ..ResumableBuild::default()
        },
    );
    // The run's first message, after the build's Needs, which the loop has
    // sent on the same channel, and before its builder spawns. A session
    // gone by then misses it, as one gone just after it would.
    let (build_id, run) = (build.assignment.build_id.clone(), build.assignment.run);
    let started = msg(worker_message::Msg::Started(Started { build_id, run }));
    tokio::task::spawn_blocking(move || {
        let _ = out_tx.blocking_send(started);
        let fin = execute_to_finished(&build, &mut lease, &out_tx, build_timeout);
        drop((build, lease));
        record_finished(&ctx, &key, fin);
    });
}

/// Tear down a still-staging build and report the error to the hub.
async fn abort_active(
    active: &mut HashMap<String, ActiveBuild>,
    id: &str,
    out_tx: &mpsc::Sender<WorkerMessage>,
    e: &Error,
) -> Result<()> {
    let build = active.remove(id);
    let run = build.as_ref().and_then(|b| b.assignment.run);
    if let Some(build) = build {
        build.abort().await;
    }
    fail_build(out_tx, (id, run), e).await
}

/// Report a per-build failure to the hub without tearing the session down.
async fn fail_build(
    out_tx: &mpsc::Sender<WorkerMessage>,
    (build_id, run): (&str, Option<RunId>),
    err: &Error,
) -> Result<()> {
    let err = chain(err);
    tracing::error!(id = build_id, "build setup failed: {err}");
    out_tx
        .send(msg(worker_message::Msg::Result(BuildResult {
            build_id: build_id.into(),
            run,
            exit_code: 1,
            outputs: vec![],
            error: err,
            usage: None,
        })))
        .await?;
    Ok(())
}

#[cfg(test)]
mod negotiate_tests;
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
