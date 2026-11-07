//! The pool's reachability: builds skip and wait for agents that do not
//! answer, and a stand-in agent for the tests that need one.

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, mpsc};

use sandbox_proto::agent::{AdoptReply, ERROR_UNKNOWN_BUILD, Empty, StatusReply};

/// A build adopted while it runs, as a restarted worker adopts it, gets its
/// exit on the adoption's lease, and its agent's Kill ends it: 137.
#[test]
fn an_adopted_running_build_ends_at_its_agents_kill() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (socket, pack_path) = spawn_test_agent(dir.path())?;
    let build_id = "0123456789abcdef0123456789abcdee";
    let log = fs::File::create(dir.path().join("build.log"))?;
    let req = start_request(build_id, "sleep 600".into(), vec![]);
    let pack = fs::File::open(&pack_path)?;
    drop(AgentBuild::start(&socket, &req, &pack, &log)?);
    let (adopted, exit) = AgentBuild::adopt(&socket, build_id)?;
    assert_eq!(exit, None);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || tx.send(adopted.wait_exit().map(|e| e.exit_code).ok()));
    kill(&socket, build_id)?;
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)), Ok(Some(137)));
    cleanup(&socket, build_id)
}

/// A stopped socket unit's file, a missing socket and one that
/// drops the call unanswered (a failing agent service) sit on top
/// of the free list. The build skips them and lands on the live
/// agent. An error the agent replies with leaves it in the pool.
#[test]
fn start_skips_unreachable_agents() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (live, pack_path) = spawn_test_agent(dir.path())?;
    let stale = dir.path().join("stale.sock");
    drop(sockpath::bind(&stale)?);
    let missing = dir.path().join("missing.sock");
    let dead = dir.path().join("dead.sock");
    let listener = sockpath::bind(&dead)?;
    thread::spawn(move || drop(listener.accept()));
    let mut sockets = vec![live.clone(), missing.clone(), stale.clone()];
    sockets.push(dead.clone());
    let pool = AgentPool::new(sockets);
    let build_id = "00000000000000000000000000000002";
    let req = start_request(build_id, "true".into(), vec![]);
    let pack = fs::File::open(&pack_path)?;
    let log = fs::File::create(dir.path().join("build.log"))?;
    let (socket, build) = pool.start(&req, &pack, &log, || false, || {})?;
    assert_eq!(socket, live);
    assert_eq!(build.wait_exit()?.exit_code, 0);
    assert_eq!(pool.down_sockets(), vec![dead, stale, missing]);
    assert_eq!(pool.usable(), 1);
    pool.release(live.clone());
    let busy = pool.start(&req, &pack, &log, || false, || {});
    assert!(busy.is_err());
    assert_eq!(pool.usable(), 1);
    cleanup(&live, build_id)
}

/// With no agent reachable a build waits instead of failing, until
/// it is cancelled or an agent is back.
#[test]
fn start_waits_for_an_agent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_, pack_path) = spawn_test_agent(dir.path())?;
    let socket = dir.path().join("down.sock");
    let pool = AgentPool::new(vec![socket.clone()]);
    let build_id = "00000000000000000000000000000003";
    let req = start_request(build_id, "true".into(), vec![]);
    let pack = fs::File::open(&pack_path)?;
    let log = fs::File::create(dir.path().join("build.log"))?;
    let Err(e) = pool.start(&req, &pack, &log, || true, || {}) else {
        panic!("started on an unreachable agent");
    };
    assert_eq!(chain(&e), "build cancelled");
    thread::scope(|s| {
        let started = s.spawn(|| pool.start(&req, &pack, &log, || false, || {}));
        thread::sleep(Duration::from_millis(1500));
        assert!(!started.is_finished(), "the build did not wait");
        spawn_agent(&socket, dir.path().join("down"))?;
        pool.release(socket.clone());
        let (leased, build) = started.join().unwrap()?;
        assert_eq!(leased, socket);
        assert_eq!(build.wait_exit()?.exit_code, 0);
        cleanup(&socket, build_id)
    })
}

/// What a stand-in agent was asked: how many calls reached it, and each
/// answered with what its `seen` said then ("adopt <seen>", ...).
#[derive(Default)]
pub(in crate::worker) struct Calls {
    pub(in crate::worker) asked: AtomicUsize,
    pub(in crate::worker) answered: Mutex<Vec<String>>,
}

/// A stand-in agent at `socket` holding the build `id` (none if empty):
/// it answers calls in order, as many as `allow` lets through, the rest
/// waiting unanswered as on an agent stopped or busy on its one accept
/// thread; a call its caller gave up on meanwhile is still answered, as one
/// in a stopped agent's backlog. Its build runs until killed, or ends with
/// `exit` once adopted.
pub(in crate::worker) fn fake_agent(
    socket: &Path,
    id: &str,
    exit: Option<i32>,
    allow: Arc<AtomicUsize>,
    seen: impl Fn() -> String + Send + 'static,
) -> Result<Arc<Calls>> {
    let listener = sockpath::bind(socket)?;
    listener.set_nonblocking(true)?;
    let calls = Arc::new(Calls::default());
    let (log, mut build) = (calls.clone(), (!id.is_empty()).then(|| id.to_owned()));
    let notice = |conn: &UnixStream, exit_code| {
        let n = reply::Reply::Exit(ExitNotice {
            exit_code,
            ..ExitNotice::default()
        });
        let _ = framing::send_reply(conn, n, &[]);
    };
    thread::spawn(move || {
        let (mut waiting, mut leases) = (VecDeque::new(), Vec::new());
        loop {
            if let Ok((conn, _)) = listener.accept() {
                log.asked.fetch_add(1, SeqCst);
                waiting.push_back(conn);
            }
            if waiting.is_empty()
                || allow
                    .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                    .is_err()
            {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            let conn: UnixStream = waiting.pop_front().unwrap();
            conn.set_nonblocking(false).unwrap();
            let Ok((call, _)) = framing::recv_call(&conn) else {
                continue;
            };
            let (name, of) = match &call {
                call::Call::Adopt(r) => ("adopt", Some(&r.build_id)),
                call::Call::Kill(r) => ("kill", Some(&r.build_id)),
                call::Call::Finish(r) => ("finish", Some(&r.build_id)),
                call::Call::Cleanup(r) => ("cleanup", Some(&r.build_id)),
                call::Call::Status(_) => ("status", None),
                _ => ("other", None),
            };
            log.answered
                .lock()
                .unwrap()
                .push(format!("{name} {}", seen()));
            let ours = of.is_none() || of == build.as_ref();
            let empty = reply::Reply::Empty(Empty {});
            let answer = match name {
                _ if !ours => reply::Reply::Error(ERROR_UNKNOWN_BUILD.into()),
                "adopt" => {
                    let scratch_dir = String::new();
                    reply::Reply::Adopt(AdoptReply {
                        pid: 1,
                        scratch_dir,
                        exit_code: None,
                    })
                }
                "status" => reply::Reply::Status(StatusReply {
                    current: build.clone(),
                }),
                "cleanup" => {
                    build = None;
                    empty
                }
                _ => empty,
            };
            let _ = framing::send_reply(&conn, answer, &[]);
            match (name, ours, exit) {
                ("adopt", true, Some(code)) => notice(&conn, code),
                ("adopt", true, None) => leases.push(conn),
                ("kill", true, _) => leases.drain(..).for_each(|l| notice(&l, 137)),
                _ => {}
            }
        }
    });
    Ok(calls)
}
