//! The pool's reachability: builds skip and wait for agents that do not
//! answer, and a stand-in agent for the tests that need one.

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, mpsc};

use crate::worker::lease::{Lease, Pool};

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

/// A lease started on a thread of its own, with the agent at `dir`'s pack
/// and log: it and its start arrive on the receiver.
fn start(mut lease: Lease, req: &StartRequest, dir: &Path, cancelled: bool) -> Started {
    let (tx, rx) = mpsc::channel();
    let (req, dir) = (req.clone(), dir.to_path_buf());
    thread::spawn(move || {
        let pack = fs::File::open(dir.join("top.tmpdir.zst")).unwrap();
        let log = fs::File::create(dir.join("build.log")).unwrap();
        let started = lease.start(&req, &pack, &log, || cancelled, || {});
        let _ = tx.send((lease, started));
    });
    rx
}

type Started = mpsc::Receiver<(Lease, Result<(PathBuf, AgentBuild)>)>;

/// What `started` gives within 30 s: a start that waits for good fails the
/// test instead of hanging it.
fn within(started: &Started) -> (PathBuf, AgentBuild) {
    let started = started.recv_timeout(Duration::from_secs(30));
    started
        .expect("a start still runs after 30 s: no wake reached its lease")
        .1
        .unwrap()
}

/// A stopped socket unit's file, a missing socket and one that drops the
/// call unanswered (a failing agent service) are leased first. The build
/// skips them, setting each down, and starts on the live agent. Then it is
/// the only agent up: a lease whose start the agent refuses (Busy) keeps
/// it, and one whose agent does not answer is cancelled, still counted,
/// or waits until a probe brings an agent back, and starts there.
#[test]
fn a_lease_skips_unreachable_agents_and_waits_for_one() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (live, _) = spawn_test_agent(dir.path())?;
    let stale = dir.path().join("stale.sock");
    drop(sockpath::bind(&stale)?);
    let missing = dir.path().join("missing.sock");
    let dead = dir.path().join("dead.sock");
    let listener = sockpath::bind(&dead)?;
    thread::spawn(move || drop(listener.accept()));
    let sockets = vec![live.clone(), missing.clone(), stale.clone(), dead.clone()];
    let pool = Pool::new(sockets, 4);
    let build_id = "00000000000000000000000000000002";
    let req = start_request(build_id, "true".into(), vec![]);
    let (socket, build) = within(&start(pool.lease().unwrap(), &req, dir.path(), false));
    assert_eq!((socket, build.wait_exit()?.exit_code), (live.clone(), 0));
    assert_eq!(pool.down_sockets(), vec![dead, stale, missing.clone()]);
    let busy = start(pool.lease().unwrap(), &req, dir.path(), false);
    let (busy, refused) = busy.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(refused.is_err());
    // It keeps the agent: none free, one up.
    assert_eq!((pool.idle_sockets().len(), pool.room().capacity), (0, 1));
    drop(busy);
    cleanup(&live, build_id)?;

    fs::remove_file(&live)?;
    let cancelled = start(pool.lease().unwrap(), &req, dir.path(), true);
    let (lease, cancelled) = cancelled.recv_timeout(Duration::from_secs(30)).unwrap();
    let cancelled = cancelled.err().map(|e| chain(&e));
    assert_eq!(cancelled.as_deref(), Some("build cancelled"));
    assert_eq!((pool.room().capacity, pool.room().leases), (0, 1));
    let started = start(lease, &req, dir.path(), false);
    let early = started.recv_timeout(Duration::from_millis(500));
    assert!(early.is_err(), "the build did not wait");
    spawn_agent(&missing, dir.path().join("back"))?;
    pool.probe_up(&missing);
    let (socket, build) = within(&started);
    assert_eq!((socket, build.wait_exit()?.exit_code), (missing.clone(), 0));
    cleanup(&missing, build_id)
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
