//! Finished-result persistence and delivery: results survive worker
//! restarts and dropped hub sessions until the hub acknowledges them.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::FileExt;
use std::panic;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::{BuildState, unrecord};
use crate::chunker::parse_hashes;
use crate::chunkstore::Hash;
use crate::errors::{Result, chain, err_msg};
use crate::fsutil::io_ctx;
use crate::proto::{BuildResult, BuildUsage, ChunkFrame, Manifest, WorkerMessage, worker_message};
use crate::worker::build::ActiveBuild;
use crate::worker::logtail::LogTail;
use crate::worker::{WorkerCtx, msg, remove_build_dir};

/// Forget finished builds nobody resumed. Without a client
/// resubmitting (it gave up or died), the result has no taker; the
/// entry would otherwise pin the build dir forever.
pub(in crate::worker) fn spawn_resumable_reaper(ctx: Arc<WorkerCtx>) {
    const TTL: Duration = Duration::from_mins(5);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_mins(1)).await;
            let ctx = ctx.clone();
            // Its file I/O never runs on a runtime worker.
            let _ = tokio::task::spawn_blocking(move || reap(&ctx, TTL)).await;
        }
    });
}

/// The dir of a result finished over `ttl` ago that no session is sent.
fn expired(e: &ResumableBuild, ttl: Duration) -> Option<&PathBuf> {
    let fin = e.finished.as_ref()?;
    let idle = !e.delivering && Arc::strong_count(fin) == 1;
    (idle && fin.finished_at.elapsed() > ttl).then_some(&fin.dir)
}

/// Drop the results `expired` names, with no file I/O under the registry
/// lock. Each one's state.json goes before its key: the hub, told by the
/// next heartbeat without the key, may run the build again under the same
/// build id, which a worker adopting from that state would take for this
/// result. A result a session starts to deliver meanwhile keeps its key.
pub(super) fn reap(ctx: &WorkerCtx, ttl: Duration) {
    let candidates: Vec<(String, PathBuf)> = ctx
        .resumable
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(key, e)| Some((key.clone(), expired(e, ttl)?.clone())))
        .collect();
    for (key, dir) in candidates {
        unrecord(&dir);
        let mut map = ctx.resumable.lock().unwrap();
        if map.get(&key).and_then(|e| expired(e, ttl)) != Some(&dir) {
            continue;
        }
        map.remove(&key);
        drop(map);
        remove_build_dir(&dir);
        tracing::warn!(
            key,
            "dropping undelivered build result (no resume within TTL)"
        );
    }
}

/// A build past staging: running, or finished with its result not yet
/// delivered to any hub. Keyed by the assignment's dedupe_key, which
/// survives hub restarts (build ids do not).
#[derive(Default)]
pub(in crate::worker) struct ResumableBuild {
    /// From the latest assignment; result messages carry this id.
    pub(in crate::worker) build_id: String,
    /// Sender of the session that issued that assignment. Kept here,
    /// not captured by the build thread: the session alive when the
    /// build *finishes* may not be the one that assigned it. None for
    /// a freshly re-adopted build no session has assigned yet.
    pub(in crate::worker) out_tx: Option<mpsc::Sender<WorkerMessage>>,
    pub(in crate::worker) finished: Option<Arc<FinishedBuild>>,
    /// A delivery is in flight; a concurrent re-assignment must not
    /// start a second one.
    pub(in crate::worker) delivering: bool,
    /// Build dir holding build.log, for log replay on resume.
    pub(in crate::worker) dir: PathBuf,
    /// Replays the log to the resumed session; joined before the
    /// result is delivered so logs arrive first.
    pub(in crate::worker) log_tail: Option<LogTail>,
    /// It waits for a build agent (WorkerCtx::wait_for_agent).
    pub(in crate::worker) waiting_for_agent: bool,
    /// The number of its latest wait's start or end or takeover during a
    /// wait, unique in the worker: a note is told for it only (tell_wait).
    pub(in crate::worker) wait: u64,
    /// The agent it waits for is the one running it, which did not
    /// answer when this worker started (resume::wait_for_agent).
    pub(in crate::worker) adopting: bool,
    /// The hub cancelled the run for its latest assignment
    /// (session::handle_cancel); a later assignment voids that while the
    /// run goes on, held for its agent included (adopt_assignment).
    pub(in crate::worker) cancelled: bool,
    /// The worker's name as the latest assignment's hub knows it;
    /// empty when unknown.
    pub(in crate::worker) worker_name: String,
}

impl ResumableBuild {
    /// Register a build no session has assigned yet.
    pub(in crate::worker) fn insert(
        ctx: &Arc<WorkerCtx>,
        key: String,
        build_id: String,
        dir: PathBuf,
        finished: Option<FinishedBuild>,
    ) {
        let finished = finished.map(Arc::new);
        ctx.resumable.lock().unwrap().insert(
            key,
            Self {
                build_id,
                finished,
                dir,
                ..Self::default()
            },
        );
    }
}

/// What a build used; all 0 when unknown (adopted after an agent restart,
/// no cgroup, setup failure).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(in crate::worker) struct Usage {
    pub(in crate::worker) wall_ms: u64,
    pub(in crate::worker) cpu_usec: u64,
    pub(in crate::worker) peak_memory_bytes: u64,
    pub(in crate::worker) oom_killed: bool,
    /// Of wall_ms; absent from a delivery persisted before it was kept.
    #[serde(default)]
    pub(in crate::worker) frozen_ms: u64,
}

impl From<sandbox_proto::agent::ExitNotice> for Usage {
    fn from(n: sandbox_proto::agent::ExitNotice) -> Self {
        Self {
            wall_ms: n.wall_ms,
            cpu_usec: n.cpu_usec,
            peak_memory_bytes: n.peak_memory_bytes,
            oom_killed: n.oom_killed,
            frozen_ms: n.frozen_ms,
        }
    }
}

impl From<Usage> for BuildUsage {
    fn from(u: Usage) -> Self {
        Self {
            wall_ms: u.wall_ms,
            cpu_usec: u.cpu_usec,
            peak_memory_bytes: u.peak_memory_bytes,
            oom_killed: u.oom_killed,
            frozen_ms: u.frozen_ms,
        }
    }
}

pub(in crate::worker) struct FinishedBuild {
    pub(in crate::worker) exit_code: i32,
    pub(in crate::worker) usage: Usage,
    pub(in crate::worker) error: String,
    pub(in crate::worker) outputs: Vec<PackedOutput>,
    /// Build dir holding the chunk frames, removed after delivery.
    pub(in crate::worker) dir: PathBuf,
    pub(in crate::worker) finished_at: Instant,
}

/// One NAR chunk as a zstd frame at `off..off+len` in a frames file.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(in crate::worker) struct OutChunk {
    pub(in crate::worker) hash: Hash,
    pub(in crate::worker) size: u32,
    pub(in crate::worker) off: u64,
    pub(in crate::worker) len: u32,
}

fn recipe(chunks: &[OutChunk]) -> (Vec<u8>, Vec<u64>) {
    let hashes = chunks.iter().flat_map(|c| c.hash).collect();
    let sizes = chunks.iter().map(|c| u64::from(c.size)).collect();
    (hashes, sizes)
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(in crate::worker) struct PackedOutput {
    pub(in crate::worker) scratch: String,
    pub(in crate::worker) frames_file: PathBuf,
    pub(in crate::worker) chunks: Vec<OutChunk>,
}

/// Record a build's result in the registry (persisted for redelivery
/// across worker generations) and start delivering it. Shared by the
/// normal execute path and re-adopted builds.
pub(in crate::worker) fn record_finished(ctx: &Arc<WorkerCtx>, key: &str, fin: FinishedBuild) {
    // build_id may have changed via a resume assignment meanwhile
    let held = match ctx.resumable.lock().unwrap().get(key) {
        Some(e) if e.dir == fin.dir => Some(e.build_id.clone()),
        _ => None,
    };
    // Written under no lock, before the registry has the
    // result: what removes a result's state.json (the reaper, an ack)
    // waits for that, so this write never follows it.
    if let Some(build_id) = held {
        persist_finished(key, &build_id, &fin);
    }
    {
        let mut map = ctx.resumable.lock().unwrap();
        match map.get_mut(key) {
            // A run killed for a cancel, its key since run anew.
            Some(e) if e.dir != fin.dir => {
                drop(map);
                remove_build_dir(&fin.dir);
                return;
            }
            Some(e) => e.finished = Some(Arc::new(fin)),
            None => {}
        }
    }
    // A cancel flag the abort loop did not get to consume (the build
    // beat it to the finish line) must not linger and kill the next
    // build with this dedupe key. Cleared after `finished` is set: the
    // Cancel handler only adds the flag while the entry is unfinished
    // (under the registry lock), so no new flag can appear afterwards.
    ctx.cancelled.lock().unwrap().remove(key);
    try_deliver(ctx, key);
}

/// Persist for redelivery by a replacement worker; the rename
/// atomically supersedes the running state.
pub(in crate::worker) fn persist_finished(key: &str, build_id: &str, fin: &FinishedBuild) {
    let state = BuildState::Finished {
        dedupe_key: key.to_string(),
        build_id: build_id.to_string(),
        exit_code: fin.exit_code,
        usage: fin.usage,
        error: fin.error.clone(),
        outputs: fin.outputs.clone(),
    };
    let tmp = fin.dir.join("state.json.tmp");
    if let Ok(json) = serde_json::to_vec(&state)
        && fs::write(&tmp, json).is_ok()
    {
        let _ = fs::rename(&tmp, fin.dir.join("state.json"));
    }
}

/// Run a build to a FinishedBuild, whatever happens: errors and even
/// panics become a failed result. Nothing else reports it -- the
/// JoinHandle is dropped, so a leaked panic would leave the registry
/// entry unfinished and the client waiting forever.
pub(in crate::worker) fn execute_to_finished(
    build: &ActiveBuild,
    out_tx: &mpsc::Sender<WorkerMessage>,
    timeout: Duration,
) -> FinishedBuild {
    panic::catch_unwind(panic::AssertUnwindSafe(|| build.execute(out_tx, timeout)))
        .unwrap_or_else(|_| Err(err_msg("build execution panicked")))
        .unwrap_or_else(|e| {
            let e = chain(&e);
            tracing::error!("build execution failed: {e}");
            FinishedBuild {
                exit_code: 1,
                usage: Usage::default(),
                error: e,
                outputs: vec![],
                dir: build.dir.clone(),
                finished_at: Instant::now(),
            }
        })
}

/// Send a finished build's result. The hub follows up with
/// NeedChunks. Blocking.
fn deliver(
    fin: &FinishedBuild,
    build_id: &str,
    out_tx: &mpsc::Sender<WorkerMessage>,
) -> Result<()> {
    out_tx.blocking_send(msg(worker_message::Msg::Result(BuildResult {
        build_id: build_id.into(),
        exit_code: fin.exit_code,
        usage: Some(fin.usage.into()),
        outputs: fin
            .outputs
            .iter()
            .map(|o| {
                let (hashes, sizes) = recipe(&o.chunks);
                Manifest {
                    build_id: String::new(),
                    store_path: o.scratch.clone(),
                    info: None,
                    hashes,
                    sizes,
                }
            })
            .collect(),
        error: fin.error.clone(),
    })))?;
    Ok(())
}

/// Answer the hub's Need: every needed chunk once, in recipe
/// order across outputs, then eof. Blocking.
fn send_chunks(
    fin: &FinishedBuild,
    build_id: &str,
    mut needed: HashSet<Hash>,
    out_tx: &mpsc::Sender<WorkerMessage>,
) -> Result<()> {
    for o in &fin.outputs {
        let path = &o.frames_file;
        let f = fs::File::open(path).map_err(io_ctx("opening", path))?;
        for c in &o.chunks {
            if !needed.remove(&c.hash) {
                continue;
            }
            let mut frame = vec![0u8; c.len as usize];
            f.read_exact_at(&mut frame, c.off)
                .map_err(io_ctx("reading", path))?;
            out_tx.blocking_send(msg(worker_message::Msg::Chunk(ChunkFrame {
                build_id: build_id.into(),
                hash: c.hash.to_vec(),
                zstd: frame,
                eof: false,
            })))?;
        }
    }
    if !needed.is_empty() {
        return Err(err_msg("hub asked for chunks outside the result recipes"));
    }
    out_tx.blocking_send(msg(worker_message::Msg::Chunk(ChunkFrame {
        build_id: build_id.into(),
        eof: true,
        ..Default::default()
    })))?;
    Ok(())
}

/// Serve a NeedChunks for a finished build on a blocking thread.
pub(in crate::worker) fn serve_chunks(
    ctx: &Arc<WorkerCtx>,
    build_id: String,
    hashes: &[u8],
    out_tx: mpsc::Sender<WorkerMessage>,
) {
    // holding the Arc keeps the reaper off the build dir
    let fin = ctx
        .resumable
        .lock()
        .unwrap()
        .values()
        .find(|e| e.build_id == build_id)
        .and_then(|e| e.finished.clone());
    let Some(fin) = fin else {
        tracing::warn!(id = build_id, "chunk request for an unknown build");
        return;
    };
    let hashes = hashes.to_vec();
    tokio::task::spawn_blocking(move || {
        let res = parse_hashes(&hashes)
            .and_then(|needed| send_chunks(&fin, &build_id, needed.into_iter().collect(), &out_tx));
        if let Err(e) = res {
            let err = chain(&e);
            tracing::warn!(id = build_id, "sending output chunks failed: {err}");
            let _ = out_tx.blocking_send(msg(worker_message::Msg::Result(BuildResult {
                build_id,
                exit_code: 1,
                error: format!("worker could not serve output chunks: {err}"),
                ..Default::default()
            })));
        }
    });
}

/// Drop a build whose result the hub confirmed: only now is it safe
/// to forget it, a result merely handed to a dying session would
/// otherwise be lost and cost a rebuild. Matched by dedupe key (the
/// stable identity); the ack's build_id may predate a concurrent
/// resume that rotated the entry's id.
pub(in crate::worker) fn ack_delivery(ctx: &Arc<WorkerCtx>, key: &str, build_id: &str) {
    let removed = {
        let mut map = ctx.resumable.lock().unwrap();
        match map.get(key) {
            Some(e) if e.finished.is_some() => map.remove(key),
            _ => None,
        }
    };
    if let Some(e) = removed {
        remove_build_dir(&e.dir);
        tracing::info!(id = build_id, "build result acknowledged");
    }
}

/// Deliver `key`'s finished result if there is one and no other
/// delivery is running, over the session that issued its latest
/// assignment. The build is kept until the hub acknowledges the
/// result; a failed or unacknowledged delivery is retried on the next
/// assignment of the same key.
pub(in crate::worker) fn try_deliver(ctx: &Arc<WorkerCtx>, key: &str) {
    let (build_id, out_tx, fin, log_tail) = {
        let mut map = ctx.resumable.lock().unwrap();
        let Some(e) = map.get_mut(key) else { return };
        if e.delivering {
            return;
        }
        let (Some(fin), Some(out_tx)) = (e.finished.clone(), e.out_tx.clone()) else {
            return;
        };
        e.delivering = true;
        (e.build_id.clone(), out_tx, fin, e.log_tail.take())
    };
    // Flush any log replay first so the result arrives after the log.
    if let Some(t) = log_tail {
        t.stop();
    }
    let result = deliver(&fin, &build_id, &out_tx);
    let mut map = ctx.resumable.lock().unwrap();
    // The ack may already have removed the entry; nothing to update then.
    if let Some(entry) = map.get_mut(key) {
        entry.delivering = false;
    }
    match result {
        Ok(()) => {
            tracing::info!(id = build_id, "build result sent, awaiting chunk request");
        }
        Err(e) => {
            tracing::warn!(
                id = build_id,
                "result delivery failed, keeping for resume: {}",
                chain(&e)
            );
        }
    }
}
