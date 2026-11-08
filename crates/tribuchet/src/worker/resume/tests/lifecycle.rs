use super::*;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{OnceLock, mpsc};
use std::thread;

use crate::errors::Result;
use crate::proto::{BuildAssignment, CancelBuild, WorkerMessage, worker_message};
use crate::sockpath;
use crate::worker::agents::tests::pool::{Calls, fake_agent};
use crate::worker::agents::tests::spawn_agent;
use crate::worker::session::handle_cancel;
use crate::worker::unix_now;

mod cancel;
mod slots;
mod teardown;

/// A worker starting up waits on no agent that never answers, as a
/// stopped one: every agent is asked at once for AGENT_ANSWER (7 s), the
/// unanswered one's build held with its agent and a slot, the idle one
/// set aside. Startup does not wait for the agent (a worker that did
/// took 30.4 s and 71.4 s in VM runs), and the build is not dropped for
/// the hub to run again.
#[test]
fn startup_waits_on_no_agent_that_does_not_answer() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (stuck, idle) = (dir.path().join("1.sock"), dir.path().join("2.sock"));
    let _stuck = sockpath::bind(&stuck)?;
    let _idle = sockpath::bind(&idle)?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![stuck.clone(), idle.clone()])?;
    let build = dir.path().join("builds/b1");
    fs::create_dir_all(&build)?;
    resume_state("b1", stuck).persist(&build)?;
    let (done, finished) = mpsc::channel();
    let (started, task_ctx) = (Instant::now(), ctx.clone());
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(adopt_builds(&task_ctx));
        sweep_orphaned_agent_builds(&task_ctx, task_ctx.agents.idle_sockets());
        done.send(()).unwrap();
        // The held build's wait runs on this runtime.
        thread::sleep(Duration::from_mins(1));
    });
    let finished = finished.recv_timeout(Duration::from_secs(30));
    assert!(
        finished.is_ok(),
        "startup still waits on an agent after 30 s"
    );
    assert!(started.elapsed() >= agents::AGENT_ANSWER);
    assert_eq!(ctx.agents.down_sockets(), vec![idle]);
    assert!(
        ctx.agents.idle_sockets().is_empty(),
        "the held agent is free"
    );
    assert!(build.exists(), "the held build's state is gone");
    assert_eq!(held(&ctx, "k"), Some((true, false)));
    assert_eq!(
        ctx.agents.room().free,
        0,
        "the held build left its slot free"
    );
    Ok(())
}

/// The persisted state of a build `id` keyed "k" on the agent behind
/// `socket`.
fn resume_state(id: &str, socket: PathBuf) -> ResumeState {
    ResumeState {
        dedupe_key: "k".into(),
        build_id: id.into(),
        pid: 42,
        spec: sandbox::SandboxSpec::default(),
        started_unix: 0,
        deadline_unix: unix_now() + 3600,
        max_silent_secs: 0,
        agent_socket: socket,
    }
}

/// Whether the registry holds `key` as waiting for an agent, and as
/// finished.
fn held(ctx: &WorkerCtx, key: &str) -> Option<(bool, bool)> {
    let map = ctx.resumable.lock().unwrap();
    map.get(key)
        .map(|e| (e.waiting_for_agent && e.adopting, e.finished.is_some()))
}

/// Until `done`, for 60 s, asked every millisecond so a step after the
/// change cannot finish unseen.
fn until(what: &str, mut done: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_mins(1);
    while !done() {
        assert!(Instant::now() < end, "not {what} in 60 s");
        thread::sleep(Duration::from_millis(1));
    }
}

/// Make the build dir `build` slow to remove, so that what goes before it
/// shows: 20000 files in `sub`.
fn fill(build: &Path, sub: &str) -> Result<()> {
    let d = build.join(sub);
    fs::create_dir_all(&d)?;
    for i in 0..20_000 {
        fs::File::create(d.join(format!("f{i}")))?;
    }
    Ok(())
}

/// The assignment `id` of the build keyed "k".
fn assignment(id: &str) -> BuildAssignment {
    BuildAssignment {
        build_id: id.into(),
        dedupe_key: "k".into(),
        ..Default::default()
    }
}

/// The build id of a held build.
const HELD: &str = "00000000000000000000000000000020";

/// A worker restarted with the running build HELD keyed "k" in builds/b1,
/// its agent a stand-in that holds the build (or no longer has it) and
/// answers nothing until `allow` grows: the build is held. Each call to the
/// agent is logged with what b1's state.json said (running, finished or
/// none) and whether "k" was still registered.
struct Held {
    _dir: tempfile::TempDir,
    build: PathBuf,
    allow: Arc<AtomicUsize>,
    calls: Arc<Calls>,
    ctx: Arc<WorkerCtx>,
    rt: Option<tokio::runtime::Runtime>,
}

impl Held {
    fn cancel(&self, session: &str, out: &tokio::sync::mpsc::Sender<WorkerMessage>) -> Result<()> {
        let c = CancelBuild {
            build_id: session.into(),
            dedupe_key: "k".into(),
        };
        let rt = self.rt.as_ref().unwrap();
        rt.block_on(handle_cancel(c, &mut HashMap::new(), out, &self.ctx))
    }

    /// Assign the build to `session`, on the runtime, as a session does.
    /// ... and, adopted, delivered to.
    fn assign(&self, session: &str, out: &tokio::sync::mpsc::Sender<WorkerMessage>) -> bool {
        let adoption = {
            let _rt = self.rt.as_ref().unwrap().enter();
            self.ctx.adopt_assignment(&assignment(session), out)
        };
        if let Some(n) = adoption {
            try_deliver(&self.ctx, "k", Some(n));
        }
        adoption.is_some()
    }
}

impl Drop for Held {
    /// A held build's wait runs on its runtime until its agent answers:
    /// shut it down without waiting on one a failed test left.
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

fn held_build(has: bool, exit: Option<i32>, network: bool) -> Result<Held> {
    let dir = tempfile::tempdir()?;
    let (build, socket) = (dir.path().join("builds/b1"), dir.path().join("agent.sock"));
    fs::create_dir_all(&build)?;
    fs::write(build.join("build.log"), "")?;
    let (allow, cell) = (Arc::new(AtomicUsize::new(0)), Arc::new(OnceLock::new()));
    let (state, ctx_of) = (build.join("state.json"), cell.clone());
    let seen = move || {
        let state = fs::read_to_string(&state).map(|s| s.contains("\"finished\""));
        let state = state.map_or("none", |f| if f { "finished" } else { "running" });
        let ctx: Option<&Arc<WorkerCtx>> = ctx_of.get();
        let keyed = ctx.is_some_and(|c| c.resumable.lock().unwrap().contains_key("k"));
        format!("{state}{}", if keyed { " k" } else { "" })
    };
    let id = if has { HELD } else { "" };
    let calls = fake_agent(&socket, id, exit, allow.clone(), seen)?;
    let mut st = resume_state(HELD, socket.clone());
    st.spec.network = network;
    st.persist(&build)?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![socket])?;
    let _ = cell.set(ctx.clone());
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(adopt_builds(&ctx));
    Ok(Held {
        _dir: dir,
        build,
        allow,
        calls,
        ctx,
        rt: Some(rt),
    })
}

/// Scenarios of a held build, "flags | steps | calls": flags set up the
/// stand-in agent (`held_build`; "cap1": sessions take one message at a
/// time), each step is an arm of `run`, and calls are what the agent was
/// asked (repeats once), with what state.json said and whether "k" was
/// registered then.
const SCENARIOS: &[&str] = &[
    // Held however many of its calls time out (not dropped after 7 s),
    // adopted once its agent answers: run to its end and recorded, on
    // disk before its agent cleans it up (not after), delivered.
    "has exit | assign:old held asked:3 open done ok:old | adopt running k, finish running k, cleanup finished k, other finished k",
    // Cancelled while held, then assigned again (a new client asked), it
    // runs on for the new assignment: the cancel does not survive it.
    "has exit | assign:old cancel:old assign:new open done ok:new | adopt running k, finish running k, cleanup finished k, other finished k",
    // Cancelled while held, it is failed to the hub at once and stays held;
    // once its agent answers, its key goes as the kill is decided and its
    // state before the kill, then its agent is handed back: the cancel
    // does not wait unseen, nor the state outlive the kill.
    "has | assign:old cancel:old cancelled:old held open gone idle removed | adopt running k, kill none, cleanup none, other none",
    // ... also while its log replay waits on a session that takes nothing,
    // killed and its agent handed back before it drains, not the replay
    // stopped first, which would keep the run alive and its agent taken.
    "has cap1 | assign:old cancel:old cancelled:old log:65536 replayed:8192 open gone killed replayed:8192 idle drain:old removed | adopt running k, kill none, cleanup none, other none",
    // Once adopted, a cancel kills it like any run.
    "has | assign:old open running cancel:old cancelled:old | adopt running k, kill running k, cleanup finished k, other finished k",
    // Its agent no longer has it: its agent handed back, then dropped, its
    // state, then its key: not after 7 s, nor its key first.
    "lost | fill assign:old open gone nodir idle | adopt running k",
    // ... its agent handed back while its log replay waits on a session
    // that takes nothing; a cancel taken meanwhile leaves no flag, nor does
    // the replay keep its state (which fails its removal: "directory not
    // empty").
    "lost cap1 | fill log:1638400 assign:old open running idle cancel:old flagged drain:old gone nodir noflag | adopt running k",
    // Cancelled and its agent without it: its key goes as that is decided,
    // and its teardown leaves a new run of the key alone.
    "lost | fill assign:old cancel:old open gone dir anew idle removed running | adopt running k",
    // ... while its log replay waits on a session that takes nothing, its
    // agent is handed back and its state goes, not after the drain, and the
    // replay is stopped before its dir goes, not left to write an offset
    // there meanwhile ("directory not empty").
    "lost cap1 | fill assign:old cancel:old cancelled:old log:1638400 replayed:8192 open gone idle unrecorded drain:old removed stopped:old | adopt running k",
    // Left by an older worker with network in the host's network: killed,
    // not adopted, its key never offered.
    "has net | gone open idle removed gone | adopt running, kill none, cleanup none, other none",
];

/// Each held build's scenario, all at once (each waits on its agent).
#[test]
fn every_held_build_ends_as_the_table_says() {
    thread::scope(|s| {
        let runs: Vec<_> = (SCENARIOS.iter())
            .map(|row| s.spawn(move || run(row)))
            .collect();
        let failed: Vec<String> = (runs.into_iter().enumerate())
            .filter_map(|(i, r)| match r.join() {
                Ok(r) => r.err().map(|e| format!("{i}: {}", chain(&e))),
                Err(_) => Some(format!("{i}: panicked, as said above")),
            })
            .collect();
        assert!(failed.is_empty(), "scenarios {failed:?} failed");
    });
}

fn run(row: &str) -> Result<()> {
    let [flags, steps, calls] = row.split(" | ").collect::<Vec<_>>()[..] else {
        panic!("{row}");
    };
    let flag = |f| flags.split(' ').any(|x| x == f);
    // The agent "has" the build or has "lost" it; it "exit"s 0 once adopted
    // (or runs until killed); its spec had network in the host's ("net").
    let h = held_build(flag("has"), flag("exit").then_some(0), flag("net"))?;
    let (ctx, build, log) = (&h.ctx, &h.build, h.build.join("build.log"));
    let mut sessions = HashMap::new();
    let mut results: HashMap<&str, Vec<(String, String)>> = HashMap::new();
    let mut drained = HashMap::new();
    for step in steps.split(' ') {
        let (what, arg) = step.split_once(':').unwrap_or((step, ""));
        let cap = if flag("cap1") { 1 } else { 64 };
        let state = |want| until(&format!("{want:?}: {row}"), || held(ctx, "k") == want);
        match what {
            "assign" | "cancel" => {
                let tx = sessions
                    .entry(arg)
                    .or_insert_with(|| tokio::sync::mpsc::channel(cap));
                let tx = tx.0.clone();
                if what == "cancel" {
                    h.cancel(arg, &tx)?;
                } else {
                    assert!(h.assign(arg, &tx), "{row}");
                }
            }
            // Until the agent was asked n times, answered or not; it
            // answers; until "k" is held, running, done or gone, until its
            // agent and its slot are handed back, or its agent asked to kill
            // the run.
            "asked" => until(row, || h.calls.asked.load(SeqCst) >= arg.parse().unwrap()),
            "open" => h.allow.store(usize::MAX, SeqCst),
            "held" => state(Some((true, false))),
            "running" => state(Some((false, false))),
            "done" => state(Some((false, true))),
            "gone" => state(None),
            "idle" => until(row, || {
                !ctx.agents.idle_sockets().is_empty() && ctx.agents.room().free > 0
            }),
            "killed" => until(&format!("killed while the replay waits: {row}"), || {
                (h.calls.answered.lock().unwrap().iter()).any(|c| c.starts_with("kill"))
            }),
            // Until session s's one result is ok or cancelled.
            "ok" | "cancelled" => until(&format!("{step}: {row}"), || {
                let rx = &mut sessions.get_mut(arg).unwrap().1;
                let got = results.entry(arg).or_default();
                while let Ok(m) = rx.try_recv() {
                    if let Some(worker_message::Msg::Result(r)) = m.msg {
                        got.push((r.build_id, r.error));
                    }
                }
                let error = if what == "ok" { "" } else { CANCELLED };
                *got == [(arg.to_owned(), error.to_owned())]
            }),
            // b1 is there or not; until it is not; until its state is not.
            "dir" | "nodir" => assert_eq!(build.exists(), what == "dir", "{step}: {row}"),
            "removed" => until(&format!("{step}: {row}"), || !build.exists()),
            "unrecorded" => until(&format!("{step}: {row}"), || {
                !build.join("state.json").exists()
            }),
            "fill" => fill(build, "top/build")?,
            // The run logs n bytes; until n were replayed; session s takes
            // what it is sent, slowly; "k" runs anew in b2.
            "log" => {
                let mut f = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&log)?;
                f.write_all(&vec![b'x'; arg.parse().unwrap()])?;
            }
            "replayed" => until(&format!("{step}: {row}"), || {
                fs::read_to_string(build.join("log.offset")).is_ok_and(|o| o == arg)
            }),
            "drain" => {
                let (tx, mut rx) = sessions.remove(arg).unwrap();
                drained.insert(arg, tx.downgrade());
                thread::spawn(move || {
                    while rx.blocking_recv().is_some() {
                        thread::sleep(Duration::from_millis(5));
                    }
                });
            }
            // Nothing sends to drained session s any more: its replay ended.
            "stopped" => assert!(drained[arg].upgrade().is_none(), "{step}: {row}"),
            "anew" => {
                let anew = build.with_file_name("b2");
                fs::create_dir_all(&anew)?;
                ResumableBuild::insert(ctx, "k".into(), "new".into(), anew, None);
            }
            // A cancel is flagged; the key's next run is not failed for it.
            "flagged" => assert!(ctx.cancelled.lock().unwrap().contains("k"), "{row}"),
            "noflag" => assert_eq!(ctx.abort_reason("k", &log, None, None), None),
            _ => panic!("no step {step}"),
        }
    }
    let mut asked = h.calls.answered.lock().unwrap().clone();
    asked.dedup();
    assert_eq!(asked.join(", "), calls, "{row}");
    Ok(())
}

/// The startup sweep sets a stopped socket unit's file aside, so
/// it counts neither for the hub nor for slots, and a rescan
/// returns it once its agent answers again.
#[test]
fn unreachable_agents_do_not_count_until_they_answer() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (live, stale) = (dir.path().join("1.sock"), dir.path().join("2.sock"));
    spawn_agent(&live, dir.path().join("a1"))?;
    drop(sockpath::bind(&stale)?);
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![live, stale.clone()])?;
    sweep_orphaned_agent_builds(&ctx, ctx.agents.idle_sockets());
    assert_eq!(ctx.agents.room().capacity, 1);
    let _lease = ctx.agents.lease().unwrap();
    assert!(ctx.agents.lease().is_none());

    fs::remove_file(&stale)?;
    spawn_agent(&stale, dir.path().join("a2"))?;
    rescan_agents(&ctx, None);
    let room = ctx.agents.room();
    assert_eq!((room.capacity, room.free), (2, 1));
    Ok(())
}

/// Run one rescan on a thread of its own: whether it returned within
/// 30 s.
fn rescan_returns(ctx: &Arc<WorkerCtx>) -> bool {
    let (done, finished) = mpsc::channel();
    let task_ctx = ctx.clone();
    thread::spawn(move || {
        rescan_agents(&task_ctx, None);
        let _ = done.send(());
    });
    finished.recv_timeout(Duration::from_secs(30)).is_ok()
}

/// A down agent that accepts a connection and never answers holds up
/// no rescan: each call to it gives up after AGENT_ANSWER, so the
/// rescan still returns another agent that answers again.
#[test]
fn a_silent_agent_does_not_stop_the_rescan() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (silent, back) = (dir.path().join("1.sock"), dir.path().join("2.sock"));
    let _silent = sockpath::bind(&silent)?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![silent.clone(), back.clone()])?;
    ctx.agents.mark_down(&silent);
    ctx.agents.mark_down(&back);
    spawn_agent(&back, dir.path().join("a2"))?;
    assert!(
        rescan_returns(&ctx),
        "the rescan still waits on the silent agent after 30 s"
    );
    assert_eq!(ctx.agents.down_sockets(), vec![silent]);
    assert_eq!(ctx.agents.room().capacity, 1);
    Ok(())
}

/// An agent that answers Status, then never answers Shutdown, holds
/// up no rescan either: Shutdown gives up after AGENT_ANSWER too, and
/// the agent still joins the pool.
#[test]
fn an_agent_silent_on_shutdown_does_not_stop_the_rescan() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("1.sock");
    // It answers the first call, Status; the next, Shutdown, waits.
    let allow = Arc::new(AtomicUsize::new(1));
    let calls = fake_agent(&socket, "", None, allow, String::new)?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![socket.clone()])?;
    ctx.agents.mark_down(&socket);
    assert!(
        rescan_returns(&ctx),
        "the rescan still waits on Shutdown after 30 s"
    );
    assert_eq!(calls.asked.load(SeqCst), 2, "Status and Shutdown");
    assert_eq!(ctx.agents.room().capacity, 1);
    Ok(())
}

/// A socket added to agent-sockets-dir after startup joins the
/// pool, up to max-jobs.
#[test]
fn rescan_adds_new_agent_sockets() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let agents = dir.path().join("agents");
    fs::create_dir(&agents)?;
    spawn_agent(&agents.join("1.sock"), dir.path().join("a1"))?;
    let ctx = WorkerCtx::for_test(dir.path(), 2, agent_sockets_in(&agents)?)?;
    assert_eq!(ctx.agents.room().capacity, 1);
    for i in 2..=3 {
        spawn_agent(
            &agents.join(format!("{i}.sock")),
            dir.path().join(format!("a{i}")),
        )?;
    }
    rescan_agents(&ctx, Some(&agents));
    assert_eq!(ctx.agents.idle_sockets().len(), 3);
    assert_eq!(ctx.agents.room().capacity, 2);
    Ok(())
}

/// persist() serializes through a borrowing mirror of BuildState.
#[test]
fn persisted_running_state_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let st = ResumeState {
        dedupe_key: "k".into(),
        build_id: "b1".into(),
        pid: 42,
        spec: sandbox::SandboxSpec::default(),
        started_unix: 11,
        deadline_unix: 7,
        max_silent_secs: 13,
        agent_socket: PathBuf::from("/run/agent.sock"),
    };
    st.persist(dir.path()).unwrap();
    let json = fs::read_to_string(dir.path().join("state.json")).unwrap();
    let BuildState::Running(back) = serde_json::from_str(&json).unwrap() else {
        panic!("expected Running");
    };
    assert_eq!(back.build_id, "b1");
    assert_eq!(back.agent_socket, st.agent_socket);
    assert_eq!(back.deadline_unix, 7);
    assert_eq!((back.started_unix, back.max_silent_secs), (11, 13));
    // As persisted before started_unix and max_silent_secs: 0 each.
    let before = json
        .replace("\"started_unix\":11,", "")
        .replace("\"max_silent_secs\":13,", "");
    assert!(!before.contains("started_unix") && !before.contains("max_silent_secs"));
    let Ok(BuildState::Running(old)) = serde_json::from_str(&before) else {
        panic!("a state persisted before the limits does not read");
    };
    assert_eq!((old.started_unix, old.max_silent_secs), (0, 0));
}
