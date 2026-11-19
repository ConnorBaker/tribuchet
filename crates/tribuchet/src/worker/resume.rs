//! Resumable builds: re-adopting still-running builds from their
//! agents after a worker restart.

mod delivery;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use harmonia_store_path::StoreDir;
use harmonia_store_remote::{DaemonClient, DaemonStore};

use super::build::supervise_agent;
use super::lease::Lease;
use super::logtail::LogTail;
use super::{DaemonConn, WorkerCtx, agent_sockets_in, agents, msg, remove_build_dir, sandbox};
use crate::errors::{Result, chain};
use crate::fsutil::io_ctx;
use crate::proto::{BuildResult, worker_message};

/// How often a held build's agent is asked again: each call waits up to
/// AGENT_ANSWER for it.
const AGENT_RETRY: Duration = Duration::from_secs(3);

/// What a build the hub cancelled fails with.
pub(super) const CANCELLED: &str = "build cancelled";

pub(super) use delivery::{
    FinishedBuild, OutChunk, PackedOutput, ResumableBuild, Usage, ack_delivery,
    execute_to_finished, persist_finished, record_finished, serve_chunks, spawn_resumable_reaper,
    try_deliver,
};

/// Pick up builds a previous worker instance left behind: still
/// running (their agent outlives the worker) or finished but
/// undelivered. Anything stale is swept. Each running build's agent is
/// asked at once and for AGENT_ANSWER at most, so the worker registers
/// within that whatever its agents do; a build whose agent does not
/// answer in it is held and waited for (wait_for_agent).
pub(super) async fn adopt_builds(ctx: &Arc<WorkerCtx>) {
    let Ok(entries) = fs::read_dir(ctx.state_dir.join("builds")) else {
        return;
    };
    let mut running = Vec::new();
    for entry in entries.flatten() {
        let dir = entry.path();
        let Ok(s) = fs::read_to_string(dir.join("state.json")) else {
            continue; // already swept by sweep_state_dir
        };
        match serde_json::from_str::<BuildState>(&s) {
            Ok(BuildState::Running(st)) => running.push((dir, st)),
            Ok(BuildState::Finished {
                dedupe_key,
                build_id,
                exit_code,
                usage,
                error,
                outputs,
            }) => {
                tracing::info!(id = build_id, "adopted finished build awaiting delivery");
                let fin = FinishedBuild {
                    exit_code,
                    usage,
                    error,
                    outputs,
                    dir: dir.clone(),
                    finished_at: Instant::now(),
                };
                ResumableBuild::insert(ctx, dedupe_key, build_id, dir, Some(fin));
            }
            Err(_) => remove_build_dir(&dir),
        }
    }
    // Something must tie the persisted state to live processes,
    // otherwise a recycled pid could be supervised as a build. That tie
    // is the agent that still knows the build.
    let asked: Vec<_> = thread::scope(|s| {
        let calls: Vec<_> = running
            .iter()
            .map(|(_, st)| s.spawn(|| agents::AgentBuild::adopt(&st.agent_socket, &st.build_id)))
            .collect();
        calls
            .into_iter()
            .map(|c| c.join().expect("an adoption call panicked"))
            .collect()
    });
    for ((dir, st), answer) in running.into_iter().zip(asked) {
        settle(ctx, *st, dir, answer).await;
    }
}

/// What an adoption call's `answer` means for the build `st`: adopted;
/// held while its agent does not answer; dropped once the agent says it
/// no longer has it.
async fn settle(
    ctx: &Arc<WorkerCtx>,
    st: ResumeState,
    dir: PathBuf,
    answer: Result<(agents::AgentBuild, Option<i32>)>,
) {
    match answer {
        Ok((build, _)) => {
            let lease = ctx.agents.lease_this(&st.agent_socket);
            adopt(ctx, st, dir, build, lease).await;
        }
        Err(e) if agents::unreachable(&e) => {
            tracing::warn!(
                id = st.build_id,
                "its agent does not answer; holding the build until it does: {}",
                chain(&e)
            );
            wait_for_agent(ctx, st, dir);
        }
        Err(e) => {
            // The agent answered: it no longer has the build.
            tracing::warn!(
                id = st.build_id,
                "re-adopting build from its agent: {}",
                chain(&e)
            );
            ctx.agents.probe_up(&st.agent_socket);
            remove_build_dir(&dir);
        }
    }
}

/// How a run (key?, dir, lease, log tailer) ends, in this order: its
/// state.json goes (if it stays, so do its key and dir); its key, if its
/// entry is still in `dir` and nothing reads its result; the run is killed
/// (`build`); its lease handed back; its tailer stopped; its dir removed.
pub(super) struct Ending(Option<String>, PathBuf, Option<Lease>, Option<LogTail>);

impl Ending {
    /// Blocking.
    pub(super) fn end(self, ctx: &WorkerCtx, build: Option<(ResumeState, agents::AgentBuild)>) {
        let Self(key, dir, lease, tail) = self;
        let mut gone = unrecord(&dir);
        if !gone {
            tracing::warn!("keeping {}: its state.json stays", dir.display());
        }
        if let Some(key) = key.filter(|_| gone) {
            let mut map = ctx.resumable.lock().unwrap();
            let ours = map.get(&key).filter(|e| e.dir == dir);
            let fin = ours.and_then(|e| e.finished.as_ref());
            let read =
                ours.is_some_and(|e| e.delivering) || fin.is_some_and(|f| Arc::strong_count(f) > 1);
            gone = !read;
            if gone && ours.is_some() {
                map.remove(&key);
            }
        }
        if let Some((st, build)) = build {
            let socket = &st.agent_socket;
            if let Err(e) = agents::kill(socket, &st.build_id) {
                tracing::warn!(id = st.build_id, "killing the build: {}", chain(&e));
            }
            if let Err(e) = build.wait_exit() {
                tracing::warn!(
                    id = st.build_id,
                    "waiting for the killed build: {}",
                    chain(&e)
                );
            }
            agents::retire(socket, &st.build_id, "agent cleanup");
        }
        drop(lease);
        if let Some(t) = tail {
            t.stop();
        }
        if gone {
            remove_build_dir(&dir);
        }
    }
}

/// Ask `st`'s agent to adopt its build every AGENT_RETRY until it
/// answers, doing `meanwhile` before each call.
fn answered(st: &ResumeState, meanwhile: impl Fn()) -> Result<(agents::AgentBuild, Option<i32>)> {
    loop {
        thread::sleep(AGENT_RETRY);
        meanwhile();
        match agents::AgentBuild::adopt(&st.agent_socket, &st.build_id) {
            Err(e) if agents::unreachable(&e) => {}
            answer => return answer,
        }
    }
}

/// Supervise an adopted build to its end in `lease`, then record it.
async fn adopt(
    ctx: &Arc<WorkerCtx>,
    st: ResumeState,
    dir: PathBuf,
    build: agents::AgentBuild,
    lease: Lease,
) {
    tracing::info!(id = st.build_id, pid = st.pid, "adopted running build");
    let socket = st.agent_socket.clone();
    // The temp roots taken at negotiation died with the previous
    // generation's daemon connection; without new ones a GC could
    // delete inputs under the still-running build.
    let gc_roots = re_root_inputs(&st.spec).await;
    let held = ctx.resumable.lock().unwrap().contains_key(&st.dedupe_key);
    if !held {
        ResumableBuild::insert(
            ctx,
            st.dedupe_key.clone(),
            st.build_id.clone(),
            dir.clone(),
            None,
        );
    }
    let task_ctx = ctx.clone();
    tokio::task::spawn_blocking(move || {
        let ctx = task_ctx;
        let key = st.dedupe_key.clone();
        let fin = supervise_agent(&ctx, &st, dir, &socket, build);
        // Roots live until the outputs are packed.
        drop((gc_roots, lease));
        record_finished(&ctx, &key, fin);
    });
}

/// A running build whose agent did not answer at startup (stopped, or
/// busy in a long Finish or Cleanup) waits, visibly: its key offered to
/// the hub, its agent and a slot kept,
/// its clients told; adopted once the agent answers, dropped if the
/// agent then no longer has it. A hub cancel meanwhile fails it at once
/// (fail_cancelled); once the agent answers it is killed, unless a later
/// assignment of its key wanted it again (adopt_assignment).
fn wait_for_agent(ctx: &Arc<WorkerCtx>, st: ResumeState, dir: PathBuf) {
    let key = st.dedupe_key.clone();
    ResumableBuild::insert(ctx, key.clone(), st.build_id.clone(), dir.clone(), None);
    if let Some(e) = ctx.resumable.lock().unwrap().get_mut(&key) {
        e.adopting = true;
    }
    ctx.wait_for_agent(&key, true);
    let lease = ctx.agents.lease_this(&st.agent_socket);
    let ctx = ctx.clone();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let answer = answered(&st, || fail_cancelled(&ctx, &key));
        fail_cancelled(&ctx, &key);
        ctx.wait_for_agent(&key, false);
        // Held no more. One that ends takes its log tailer and any cancel
        // taken since fail_cancelled; no cancel or assignment takes it after.
        let (cancelled, tail) = {
            let mut map = ctx.resumable.lock().unwrap();
            let e = map.get_mut(&key);
            let ends = answer.is_err() || e.as_ref().is_some_and(|e| e.cancelled);
            if ends {
                ctx.cancelled.lock().unwrap().remove(&key);
            }
            let tail = e.and_then(|e| {
                (e.adopting, e.cancelled) = (false, ends);
                ends.then(|| e.log_tail.take()).flatten()
            });
            (ends, tail)
        };
        match answer {
            Ok((build, _)) if !cancelled => rt.block_on(adopt(&ctx, st, dir, build, lease)),
            answer => {
                if let Err(e) = &answer {
                    let e = chain(e);
                    let why = "its agent answered at last, without the build";
                    tracing::warn!(id = st.build_id, "{why}: {e}");
                }
                let build = answer.ok().map(|(build, _)| (st, build));
                Ending(Some(key), dir, Some(lease), tail).end(&ctx, build);
            }
        }
    });
}

/// Remove `dir`'s state.json, which a restarted worker adopts its build
/// from; the rest of the dir can go later. False if it is still there.
pub(super) fn unrecord(dir: &Path) -> bool {
    // Whatever fails here fails the removal of the rest, which warns.
    let gone = fs::remove_file(dir.join("state.json"));
    gone.map_or_else(|e| e.kind() == io::ErrorKind::NotFound, |()| true)
}

/// If the hub cancelled the held build keyed `key`, fail it to the hub
/// under its build id, once: the hub's supervision of it ends and its
/// slot no longer counts as freeing there. It stays held, its key
/// offered, so its run is not started again beside it.
fn fail_cancelled(ctx: &WorkerCtx, key: &str) {
    let failed = {
        let map = ctx.resumable.lock().unwrap();
        let Some(e) = map.get(key) else { return };
        if !ctx.cancelled.lock().unwrap().remove(key) {
            return;
        }
        e.out_tx.clone().map(|tx| (tx, e.build_id.clone()))
    };
    tracing::info!(
        key,
        "the hub cancelled a build held for its agent; it is killed once the agent answers"
    );
    if let Some((tx, build_id)) = failed {
        let result = BuildResult {
            build_id,
            exit_code: 1,
            error: CANCELLED.into(),
            ..Default::default()
        };
        // A session gone meanwhile needs no verdict.
        let _ = tx.blocking_send(msg(worker_message::Msg::Result(result)));
    }
}

/// Clean up agent builds without an on-disk record, left by a Start
/// that raced the previous worker's shutdown. They fail every later
/// Start on their agent with Busy. Runs after adoption, so adopted
/// agents are already out of the pool. The query activates every agent,
/// so idle ones are told to exit again. `sockets` are idle or down;
/// those that answer join the free list, the others are marked down.
pub(super) fn sweep_orphaned_agent_builds(ctx: &Arc<WorkerCtx>, sockets: Vec<PathBuf>) {
    for socket in sockets {
        let id = match agents::current_build(&socket) {
            Ok(Some(id)) => id,
            Ok(None) => {
                if let Err(e) = agents::shutdown(&socket) {
                    tracing::warn!("agent shutdown {}: {}", socket.display(), chain(&e));
                }
                ctx.agents.probe_up(&socket);
                continue;
            }
            Err(e) if agents::unreachable(&e) => {
                if ctx.agents.mark_down(&socket) {
                    tracing::warn!("{}; not leasing builds to it", chain(&e));
                }
                continue;
            }
            Err(e) => {
                tracing::warn!("querying agent {}: {}", socket.display(), chain(&e));
                continue;
            }
        };
        tracing::warn!(id, "cleaning up an orphaned agent build");
        // Cleanup is slow. Run it in the background, with the agent
        // reserved, so the hub connection is not delayed.
        let lease = ctx.agents.lease_this(&socket);
        tokio::task::spawn_blocking(move || {
            let _ = agents::kill(&socket, &id);
            agents::retire(&socket, &id, "orphaned build cleanup");
            drop(lease);
        });
    }
}

/// Every 10 s, probe the agents marked down and the sockets new in
/// agent-sockets-dir. Those that answer join the pool, so neither a
/// restarted nor an added socket unit is missed.
pub(super) fn spawn_agent_rescan(ctx: Arc<WorkerCtx>, dir: Option<PathBuf>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let (ctx, dir) = (ctx.clone(), dir.clone());
            let res = tokio::task::spawn_blocking(move || rescan_agents(&ctx, dir.as_deref()));
            if let Err(e) = res.await {
                tracing::warn!("rescanning build agents: {e}");
            }
        }
    });
}

fn rescan_agents(ctx: &Arc<WorkerCtx>, dir: Option<&Path>) {
    if let Some(dir) = dir {
        match agent_sockets_in(dir) {
            Ok(found) => ctx.agents.discover(found),
            Err(e) => tracing::warn!("agent-sockets-dir {}: {e}", dir.display()),
        }
    }
    sweep_orphaned_agent_builds(ctx, ctx.agents.down_sockets());
}

/// Take fresh temp roots for an adopted build's inputs on a new daemon
/// connection (returned; the roots die with it). Best effort: adoption
/// must not fail because the daemon is briefly unavailable.
async fn re_root_inputs(spec: &sandbox::SandboxSpec) -> Option<DaemonConn> {
    let store_dir = StoreDir::default();
    let mut daemon = match DaemonClient::builder().connect_daemon().await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                "connecting to nix-daemon for adopted-build GC roots: {}",
                chain(&e)
            );
            return None;
        }
    };
    let outputs = if cfg!(target_os = "macos") {
        &spec.outputs[..]
    } else {
        &[]
    };
    for path in spec.store_inputs.iter().chain(outputs) {
        let sp = match store_dir.parse(path) {
            Ok(sp) => sp,
            Err(e) => {
                tracing::warn!(path, "skipping GC root for unparsable input: {e}");
                continue;
            }
        };
        if let Err(e) = daemon.add_temp_root(&sp).await {
            tracing::warn!(path, "re-adding GC root: {}", chain(&e));
        }
    }
    Some(daemon)
}

/// On-disk `state.json`: one file, so the running-to-finished
/// transition is a single atomic replacement.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub(super) enum BuildState {
    Running(Box<ResumeState>),
    Finished {
        dedupe_key: String,
        build_id: String,
        exit_code: i32,
        #[serde(default)]
        usage: Usage,
        error: String,
        outputs: Vec<PackedOutput>,
    },
}

/// State for re-adopting a running build after a worker restart; the
/// build's identity across restarts is the agent that owns it.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct ResumeState {
    pub(super) dedupe_key: String,
    /// Original assignment id: names the cgroup and the log file.
    pub(super) build_id: String,
    /// Agent-side pid of the builder (Linux: its setup stage).
    pub(super) pid: i32,
    pub(super) spec: sandbox::SandboxSpec,
    /// When its builder started, Unix seconds, by which the arbiter's
    /// oldest build leads; absent from a state persisted before it, 0.
    #[serde(default)]
    pub(super) started_unix: u64,
    pub(super) deadline_unix: u64,
    /// The client's max-silent-time (BuildAssignment's); absent from a
    /// state persisted before it, 0.
    #[serde(default)]
    pub(super) max_silent_secs: u64,
    /// Socket of the agent that owns the build, for re-adoption.
    pub(super) agent_socket: PathBuf,
}

impl ResumeState {
    /// Persist as `state.json`; a restarted worker adopts from it.
    pub(in crate::worker) fn persist(&self, dir: &Path) -> io::Result<()> {
        #[derive(serde::Serialize)]
        #[serde(tag = "phase", rename_all = "snake_case")]
        enum Ref<'a> {
            Running(&'a ResumeState),
        }
        fs::write(
            dir.join("state.json"),
            serde_json::to_vec(&Ref::Running(self))?,
        )
        .map_err(io_ctx("writing", &dir.join("state.json")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod lifecycle;

    #[test]
    fn a_finished_state_written_before_usage_existed_still_loads() {
        let old = r#"{"phase":"finished","dedupe_key":"k","build_id":"b","exit_code":0,"error":"","outputs":[]}"#;
        let Ok(BuildState::Finished {
            usage, exit_code, ..
        }) = serde_json::from_str(old)
        else {
            panic!("old finished state did not load");
        };
        assert_eq!((exit_code, usage), (0, Usage::default()));
    }

    #[test]
    fn a_finished_state_keeps_its_usage() {
        let u = Usage {
            wall_ms: 1500,
            cpu_usec: 2_000_000,
            peak_memory_bytes: 1 << 30,
            oom_killed: true,
            frozen_ms: 500,
        };
        let st = BuildState::Finished {
            dedupe_key: "k".into(),
            build_id: "b".into(),
            exit_code: 1,
            usage: u,
            error: String::new(),
            outputs: vec![],
        };
        let json = serde_json::to_string(&st).unwrap();
        let Ok(BuildState::Finished { usage, .. }) = serde_json::from_str(&json) else {
            panic!("finished state did not round trip");
        };
        assert_eq!(usage, u);
    }
}
