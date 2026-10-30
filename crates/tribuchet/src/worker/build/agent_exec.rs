//! Build execution on the per-uid agents.
//!
//! Both platforms lease one agent per build; the platform difference
//! is confined to the StartRequest: macOS sends a seatbelt profile,
//! Linux the serialized sandbox spec the agent runs the setup stage
//! with.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{self, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::{ActiveBuild, pack_outputs_and_extras, unix_now};
use crate::errors::chain;
use crate::errors::{Result, err_ctx, err_msg};
use crate::fsutil::io_ctx;
use crate::proto::WorkerMessage;
use crate::tmpdir::pack_zstd_dir;
#[cfg(target_os = "linux")]
use crate::worker::caps::requires_uid_range;
#[cfg(target_os = "macos")]
use crate::worker::caps::requires_uid_range;
use crate::worker::logtail::tail_log;
use crate::worker::resume::{FinishedBuild, ResumeState, Usage};
use crate::worker::{WorkerCtx, agents, sandbox};

impl ActiveBuild {
    /// Lease a per-uid agent and run the build there. The agent
    /// unpacks the tmp dir into its own scratch dir, confines and owns
    /// the builder process. The worker tails the log fd, polls its
    /// abort conditions, and packs the outputs once the agent made
    /// them readable.
    pub(in crate::worker) fn execute(
        &self,
        out_tx: &mpsc::Sender<WorkerMessage>,
        timeout: Duration,
    ) -> Result<FinishedBuild> {
        #[cfg(target_os = "macos")]
        if requires_uid_range(&self.assignment) {
            return Err(err_msg(
                "the uid-range feature is only supported on Linux workers",
            ));
        }
        if self.reuse_outputs {
            return Ok(self.finish_reused());
        }
        let socket =
            self.ctx.agents.acquire().ok_or_else(|| {
                err_msg("no free build agent (max-jobs exceeds the agent count?)")
            })?;
        let result = self.execute_on_agent(&socket, out_tx, timeout);
        self.ctx.agents.release(socket);
        result
    }

    /// Pack existing outputs without running a builder. Agent cleanup
    /// is skipped since it would delete them.
    fn finish_reused(&self) -> FinishedBuild {
        let spec = sandbox::SandboxSpec {
            outputs: self.assignment.outputs.values().cloned().collect(),
            store_inputs: self.input_list(),
            recursive_nix: self.recursive_nix(),
            ..sandbox::SandboxSpec::default()
        };
        let deadline = Instant::now() + Duration::from_mins(10);
        let packed = tokio::runtime::Handle::current()
            .block_on(pack_outputs_and_extras(&self.dir, &spec, None, deadline));
        let (exit_code, error, outputs, extras) = match packed {
            Ok((o, e)) => (0, String::new(), o, e),
            Err(e) => (1, chain(&e), vec![], vec![]),
        };
        FinishedBuild {
            exit_code,
            usage: Usage::default(),
            error,
            outputs,
            extras,
            dir: self.dir.clone(),
            finished_at: Instant::now(),
        }
    }

    fn execute_on_agent(
        &self,
        socket: &Path,
        out_tx: &mpsc::Sender<WorkerMessage>,
        timeout: Duration,
    ) -> Result<FinishedBuild> {
        let a = &self.assignment;
        let outputs: Vec<String> = a.outputs.values().cloned().collect();
        // The agent-side confinement of the builder: a seatbelt
        // profile on macOS, the namespace sandbox spec on Linux. The
        // spec doubles as the packing/resume state, so the Linux one
        // carries the full input and network configuration.
        #[cfg(target_os = "macos")]
        let (profile, sandbox_json, spec) = (
            if self.ctx.sandbox {
                agents::seatbelt_profile(
                    &outputs,
                    &self.ctx.secret_paths,
                    a.fixed_output,
                    a.local_networking,
                )?
            } else {
                String::new()
            },
            None,
            sandbox::SandboxSpec {
                outputs: outputs.clone(),
                store_inputs: self.input_list(),
                recursive_nix: self.recursive_nix(),
                ..sandbox::SandboxSpec::default()
            },
        );
        #[cfg(target_os = "linux")]
        let (profile, sandbox_json, mut spec) = {
            let spec = self.build_spec()?;
            (String::new(), Some(serde_json::to_string(&spec)?), spec)
        };
        // Re-pack the staged tmp dir: the agent unpacks it into its own
        // scratch dir, since the worker's copy is not agent-writable.
        fs::write(
            self.dir.join("top.tmpdir.zst"),
            pack_zstd_dir(&self.dir.join("top/build"))?,
        )?;
        let req = sandbox_proto::agent::StartRequest {
            build_id: a.build_id.clone(),
            builder: a.builder.clone(),
            args: a.args.clone(),
            env: a.env.clone().into_iter().collect(),
            tmp_dir_in_sandbox: a.tmp_dir_in_sandbox.clone(),
            profile,
            sandbox_json,
            outputs: outputs.clone(),
            memory_max_bytes: self.ctx.build_memory_max_bytes,
        };
        // The builder writes dir/build.log directly through this fd.
        let log_w = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("build.log"))?;
        let build = agents::AgentBuild::start(
            socket,
            &req,
            &fs::File::open(self.dir.join("top.tmpdir.zst"))
                .map_err(io_ctx("opening", &self.dir.join("top.tmpdir.zst")))?,
            &log_w,
        )?;
        drop(log_w);
        tracing::info!(
            id = a.build_id,
            pid = build.pid,
            agent = %socket.display(),
            scratch = %build.scratch_dir.display(),
            "builder started on agent"
        );
        // The agent placed the sandbox root in its scratch dir; record
        // it so packing and adopted supervision find the outputs.
        #[cfg(target_os = "linux")]
        {
            spec.root = build
                .scratch_dir
                .parent()
                .unwrap_or(&build.scratch_dir)
                .join("root");
        }

        let log_done = Arc::new(atomic::AtomicBool::new(false));
        let tailer = {
            let tx = out_tx.clone();
            let build_id = a.build_id.clone();
            let log_done = log_done.clone();
            let dir = self.dir.clone();
            thread::spawn(move || {
                tail_log(&dir, &build_id, &tx, || log_done.load(Ordering::Relaxed));
            })
        };
        // From here a restarted worker can re-adopt the build from
        // its agent.
        let started_unix = unix_now();
        let resume = ResumeState {
            dedupe_key: a.dedupe_key.clone(),
            build_id: a.build_id.clone(),
            pid: build.pid,
            spec,
            agent_socket: socket.to_path_buf(),
            started_unix,
            deadline_unix: started_unix + limit(timeout, a.timeout_secs).as_secs(),
            max_silent_secs: a.max_silent_secs,
        };
        if let Err(e) = resume.persist(&self.dir) {
            tracing::warn!(id = a.build_id, "persisting resume state: {}", chain(&e));
        }
        let fin = supervise_agent(&self.ctx, &resume, self.dir.clone(), socket, build);
        log_done.store(true, Ordering::Relaxed);
        let _ = tailer.join();
        Ok(fin)
    }

    /// The Linux sandbox spec sent with the StartRequest. The agent
    /// fills in its scratch paths, user namespace and uid block before
    /// spawning the setup stage with it.
    fn recursive_nix(&self) -> bool {
        self.ctx.recursive_nix
            && self
                .assignment
                .required_features
                .iter()
                .any(|f| f == "recursive-nix")
    }

    #[cfg(target_os = "linux")]
    fn build_spec(&self) -> Result<sandbox::SandboxSpec> {
        let a = &self.assignment;
        let uid_count = if requires_uid_range(a) { 65536 } else { 1 };
        let spec = sandbox::prepare(
            a,
            &self.dir,
            &self.input_list(),
            &sandbox::PrepareOpts {
                bin_sh: self.ctx.sandbox_bin_sh.as_deref(),
                secrets: &self.ctx.secret_paths,
                leased_userns: None,
                leased_uid_count: Some(uid_count),
                emulator: self.ctx.emulators.get(&a.system).map(PathBuf::as_path),
                net_isolation: self.ctx.fod_isolation,
                net_policy: self.ctx.fod_network.clone(),
                recursive_nix: self.recursive_nix(),
                nix_daemon_socket: None,
            },
        )?;
        tracing::info!(
            id = a.build_id,
            fixed_output = a.fixed_output,
            network = spec.network,
            net_isolation = spec.net_isolation,
            "sandbox network decision"
        );
        Ok(spec)
    }
}

/// Wait out a build running on an agent (fresh or re-adopted), pack
/// its outputs, and have the agent clean up. Driven off the persisted
/// `ResumeState` so fresh and adopted builds share one path.
pub(in crate::worker) fn supervise_agent(
    ctx: &WorkerCtx,
    st: &ResumeState,
    dir: PathBuf,
    socket: &Path,
    mut build: agents::AgentBuild,
) -> FinishedBuild {
    let log_path = dir.join("build.log");
    let tracking = ctx.arbiter.track(st, build.freeze.take());
    let max_silent = limit(ctx.max_silent_time, st.max_silent_secs);
    // The exit notice arrives on the lease connection. Wait for it on
    // its own thread so the abort conditions keep being polled.
    let waiter = thread::spawn(move || build.wait_exit());
    let mut aborted: Option<String> = None;
    let mut silence = Silence::default();
    while !waiter.is_finished() {
        if aborted.is_none() {
            let frozen = tracking.frozen();
            let timed_out = past_deadline(st.deadline_unix, unix_now(), frozen)
                .then(|| "build timed out".to_string());
            let silent = silence.observe(&log_path, Instant::now(), frozen);
            let silent = (!max_silent.is_zero() && silent > max_silent)
                .then(|| format!("build produced no output for {}s", max_silent.as_secs()));
            if let Some(r) = ctx.abort_reason(&st.dedupe_key, &log_path, timed_out, silent) {
                aborted = Some(r);
                if let Err(e) = agents::kill(socket, &st.build_id) {
                    tracing::warn!(
                        id = st.build_id,
                        "killing the build via its agent: {}",
                        chain(&e)
                    );
                }
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
    let (code, usage) = match waiter.join() {
        Ok(Ok(n)) => (n.exit_code, n.into()),
        Ok(Err(e)) => {
            aborted.get_or_insert(format!("agent connection lost: {}", chain(&e)));
            (1, Usage::default())
        }
        Err(_) => {
            aborted.get_or_insert("agent wait thread panicked".into());
            (1, Usage::default())
        }
    };
    tracing::info!(id = st.build_id, exit_code = code, aborted = ?aborted, "builder finished");
    let (exit_code, error, outputs, extras) = if let Some(reason) = aborted {
        (1, reason, vec![], vec![])
    } else if code != 0 {
        // A Linux setup-stage failure leaves its message under the
        // sandbox root; a plain builder failure leaves none.
        #[cfg(target_os = "linux")]
        let detail = sandbox::setup_error_detail(&st.spec).unwrap_or_default();
        #[cfg(target_os = "macos")]
        let detail = String::new();
        (code, detail, vec![], vec![])
    } else {
        // Finish makes the outputs (at their real store paths on
        // macOS, under the sandbox root on Linux) readable for
        // packing.
        let deadline_unix = st.deadline_unix.saturating_add(tracking.frozen().as_secs());
        let remaining = Duration::from_secs(deadline_unix.saturating_sub(unix_now()));
        let deadline = Instant::now() + remaining.max(Duration::from_mins(10));
        let packed = agents::finish(socket, &st.build_id)
            .map_err(err_ctx("finishing the build on its agent"))
            .and_then(|()| {
                tokio::runtime::Handle::current()
                    .block_on(pack_outputs_and_extras(&dir, &st.spec, None, deadline))
            });
        match packed {
            Ok((o, e)) => (0, String::new(), o, e),
            Err(e) => (1, chain(&e), vec![], vec![]),
        }
    };
    // The agent removes its scratch dir and the scratch outputs
    // (packing above already read them) and forgets the build.
    if let Err(e) = agents::cleanup(socket, &st.build_id) {
        tracing::warn!(id = st.build_id, "agent cleanup failed: {}", chain(&e));
    } else if let Err(e) = agents::shutdown(socket) {
        tracing::warn!(id = st.build_id, "agent shutdown failed: {}", chain(&e));
    }
    FinishedBuild {
        exit_code,
        usage,
        error,
        outputs,
        extras,
        dir,
        finished_at: Instant::now(),
    }
}

/// A build's limit: the lesser of the worker's own and the one its client
/// asked for (Nix's, through attach and the hub), 0 being none.
fn limit(own: Duration, asked: u64) -> Duration {
    let asked = Duration::from_secs(asked);
    if own.is_zero() || (!asked.is_zero() && asked < own) {
        asked
    } else {
        own
    }
}

/// Whether a build has run out its timeout: `deadline_unix` is its start
/// plus the timeout by the wall clock, and only running time counts
/// toward it, so the time it spent frozen moves the deadline on. Time
/// spent waiting for an agent already does not count:
/// the deadline starts once its builder has.
fn past_deadline(deadline_unix: u64, now_unix: u64, frozen: Duration) -> bool {
    now_unix >= deadline_unix.saturating_add(frozen.as_secs())
}

/// A build's silence, for max-silent-time: the running time since its log
/// last grew, the time it was frozen left out. It starts at the first
/// look, so a silence restarts once per worker restart (an adopted build's
/// earlier freezes cannot be told from its silence). Read each supervision
/// tick, so to 200 ms.
#[derive(Default)]
struct Silence {
    /// The log's size at the last look.
    seen: u64,
    /// When it last grew, and the build's frozen time then.
    output: Option<(Instant, Duration)>,
}

impl Silence {
    /// The silence at `now`, `frozen` being the build's frozen time then.
    fn observe(&mut self, log: &Path, now: Instant, frozen: Duration) -> Duration {
        let Ok(meta) = fs::metadata(log) else {
            return Duration::ZERO;
        };
        if self.output.is_none() || meta.len() != self.seen {
            self.output = Some((now, frozen));
        }
        self.seen = meta.len();
        let (at, then) = self.output.unwrap_or((now, frozen));
        now.saturating_duration_since(at)
            .saturating_sub(frozen.saturating_sub(then))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::SystemTime;

    use super::*;

    /// Silence is the running time since the log last grew, from the first
    /// look: time frozen does not add to it, and a log last written before
    /// that look (an adopted build's) starts it then.
    #[test]
    fn silence_is_running_time_since_the_log_last_grew() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("build.log");
        fs::write(&log, "started\n").unwrap();
        let hour_ago = SystemTime::now() - Duration::from_secs(3600);
        fs::File::options()
            .append(true)
            .open(&log)
            .unwrap()
            .set_modified(hour_ago)
            .unwrap();
        let t0 = Instant::now();
        let mut s = Silence::default();
        let mut at = |t: u64, frozen: u64, append: &[u8]| {
            fs::File::options()
                .append(true)
                .open(&log)
                .unwrap()
                .write_all(append)
                .unwrap();
            let secs = Duration::from_secs;
            s.observe(&log, t0 + secs(t), secs(frozen)).as_secs()
        };
        assert_eq!(at(0, 0, b""), 0, "a log written before the first look");
        assert_eq!(at(5, 0, b""), 5);
        assert_eq!(at(10, 3, b""), 7, "frozen time added to it");
        assert_eq!(at(11, 3, b"\n"), 0, "output did not end it");
        assert_eq!(at(13, 4, b""), 1);
    }

    /// A build's limit is the lesser of the worker's own and its client's,
    /// 0 being none, as from a client or hub predating the fields.
    #[test]
    fn a_build_is_limited_by_the_lesser_of_the_workers_and_its_clients() {
        for (own, asked, lesser) in [
            (600, 0, 600),
            (600, 60, 60),
            (60, 600, 60),
            (0, 60, 60),
            (0, 0, 0),
        ] {
            let secs = Duration::from_secs;
            assert_eq!(limit(secs(own), asked), secs(lesser), "{own} {asked}");
        }
    }

    /// 100 s of timeout from 1000: a build never frozen has run it out at
    /// 1100; one frozen 30 s of it, at 1130.
    #[test]
    fn frozen_time_moves_the_deadline_on() {
        assert!(!past_deadline(1100, 1099, Duration::ZERO));
        assert!(past_deadline(1100, 1100, Duration::ZERO));
        assert!(!past_deadline(1100, 1129, Duration::from_secs(30)));
        assert!(past_deadline(1100, 1130, Duration::from_secs(30)));
    }
}
