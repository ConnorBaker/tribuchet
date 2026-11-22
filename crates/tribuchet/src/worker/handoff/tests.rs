//! The handoff socket (DD10, DD11): DT8, DT9 and the worker's half of DT15.

use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;

use super::*;
use sandbox_proto::agent::HandoffRequest;

const KEY: &str = "k";
const TOKEN: &[u8] = &[1; 16];

/// A worker serving handoffs in `dir`, one run of KEY ready under TOKEN,
/// whose listener's inode is the second value; the handoff socket third.
fn served(dir: &Path) -> (Arc<Handoffs>, u64, PathBuf) {
    let handoffs = Arc::new(Handoffs::default());
    let path = dir.join("handoff.sock");
    serve(&path, None, handoffs.clone()).unwrap();
    let listener = UnixListener::bind(dir.join("rpc.sock")).unwrap();
    let inode = fs::metadata(format!("/proc/self/fd/{}", listener.as_raw_fd()))
        .unwrap()
        .ino();
    handoffs.ready(KEY, TOKEN, OwnedFd::from(listener), dir);
    (handoffs, inode, path)
}

/// What the handoff socket at `path` answers `token` with: a listener's
/// inode, or the error.
fn fetch(path: &Path, token: &[u8]) -> Result<u64, String> {
    let conn = UnixStream::connect(path).unwrap();
    let req = call::Call::Handoff(HandoffRequest {
        token: token.to_vec(),
    });
    framing::send_call(&conn, req, &[]).unwrap();
    let (_, mut fds) = framing::recv_reply(&conn).map_err(|e| e.to_string())?;
    let fd = fds.pop().ok_or("no listener")?;
    Ok(fs::metadata(format!("/proc/self/fd/{}", fd.as_raw_fd()))
        .unwrap()
        .ino())
}

/// The fds of this process that are the socket `inode`.
fn copies(inode: u64) -> usize {
    let name = format!("socket:[{inode}]");
    (fs::read_dir("/proc/self/fd").unwrap().flatten())
        .filter(|f| fs::read_link(f.path()).is_ok_and(|l| l.to_string_lossy() == name))
        .count()
}

/// DT8 (DD11): the listener goes only to the holder of its run's token, and
/// once; the worker keeps no copy after; the run's dir records it.
#[test]
fn handoff_needs_the_runs_token_and_happens_once() {
    let dir = tempfile::tempdir().unwrap();
    let (handoffs, inode, path) = served(dir.path());
    let wrong = fetch(&path, &[2; 16]).unwrap_err();
    assert!(wrong.contains(NO_SUCH_RUN), "{wrong}");
    assert!(!dir.path().join(HANDED_OFF).exists());
    assert_eq!(fetch(&path, TOKEN), Ok(inode));
    // Recorded on the serving thread, after its copy is gone.
    let recorded = (0..500).any(|_| {
        thread::sleep(Duration::from_millis(10));
        dir.path().join(HANDED_OFF).exists()
    });
    assert!(recorded, "the handoff was not recorded");
    assert_eq!(copies(inode), 0, "the worker kept a copy");
    let again = fetch(&path, TOKEN).unwrap_err();
    assert!(again.contains("handed off already"), "{again}");
    assert!(!handoffs.lapsed(KEY, dir.path()), "a handed-off run lapsed");
}

/// DT8: the hub's dispatch of a resume gives its attach a token of its
/// own, which the worker takes in place of the first.
#[test]
fn a_resume_retokens_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let (handoffs, inode, path) = served(dir.path());
    handoffs.retoken(KEY, &[3; 16]);
    assert!(fetch(&path, TOKEN).is_err(), "the old token still served");
    assert_eq!(fetch(&path, &[3; 16]), Ok(inode));
}

/// DT9 (DD10): a failed send records no handoff and keeps the listener for
/// the next request; a restarted worker (no entry) finds a run lapsed
/// unless its dir records the handoff.
#[test]
fn a_handoff_is_recorded_only_after_its_send() {
    let dir = tempfile::tempdir().unwrap();
    let (handoffs, inode, _) = served(dir.path());
    let (key, listener) = handoffs.take(TOKEN).unwrap();
    handoffs.handed(&key, Err(listener));
    assert!(
        !dir.path().join(HANDED_OFF).exists(),
        "recorded before a send"
    );
    assert_eq!(copies(inode), 1, "the listener was not kept");
    let (key, listener) = handoffs.take(TOKEN).expect("not offered again");
    drop(listener);
    handoffs.handed(&key, Ok(()));
    let restarted = Handoffs::default();
    assert!(
        !restarted.lapsed(KEY, dir.path()),
        "a recorded handoff lapsed"
    );
    let other = tempfile::tempdir().unwrap();
    assert!(
        restarted.lapsed(KEY, other.path()),
        "an unrecorded run did not lapse"
    );
}

/// DT15, the worker's half (DD10): a listener no attach fetched by its
/// deadline is closed, so the producer's connect() is refused rather than
/// queued, and the run lapses; one fetched in time never does.
#[test]
fn the_deadline_closes_an_unfetched_listener() {
    let dir = tempfile::tempdir().unwrap();
    let handoffs = Handoffs::default();
    let path = dir.path().join("rpc.sock");
    let listener = UnixListener::bind(&path).unwrap();
    handoffs.ready(KEY, TOKEN, OwnedFd::from(listener), dir.path());
    assert!(
        !handoffs.lapsed(KEY, dir.path()),
        "lapsed before its deadline"
    );
    assert!(UnixStream::connect(&path).is_ok(), "not listening");
    handoffs.runs.lock().unwrap().get_mut(KEY).unwrap().deadline = Some(Instant::now());
    assert!(
        handoffs.lapsed(KEY, dir.path()),
        "not lapsed at its deadline"
    );
    let refused = UnixStream::connect(&path).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::ConnectionRefused, "{refused}");
}

/// DD11: an empty token names no run, not even one given none (by a hub
/// that predates producers): no listener goes out for it.
#[test]
fn an_empty_token_names_no_run() {
    let dir = tempfile::tempdir().unwrap();
    let handoffs = Arc::new(Handoffs::default());
    let path = dir.path().join("handoff.sock");
    serve(&path, None, handoffs.clone()).unwrap();
    let listener = UnixListener::bind(dir.path().join("rpc.sock")).unwrap();
    handoffs.ready(KEY, &[], OwnedFd::from(listener), dir.path());
    let got = fetch(&path, &[]);
    let refused = got.as_ref().is_err_and(|e| e.contains(NO_SUCH_RUN));
    assert!(refused, "an empty token was served: {got:?}");
}

/// DD10: the hub tells attach a run's token at Started, which the worker
/// sends once it expects the run, before the agent started its builder
/// (which may wait for an agent): a request that comes first waits for
/// the listener, and is served once it came; one for a run that ended
/// first is refused. A token of no run expected here is refused at once.
#[test]
fn a_request_before_the_run_is_ready_waits_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let handoffs = Arc::new(Handoffs::default());
    let path = dir.path().join("handoff.sock");
    serve(&path, None, handoffs.clone()).unwrap();
    let early = |token: &'static [u8]| {
        let path = path.clone();
        thread::spawn(move || fetch(&path, token))
    };
    handoffs.expect(KEY, TOKEN, dir.path());
    handoffs.expect("ended", &[2; 16], dir.path());
    let (asked, ended) = (early(TOKEN), early(&[2; 16]));
    thread::sleep(Duration::from_millis(200));
    let listener = UnixListener::bind(dir.path().join("rpc.sock")).unwrap();
    let inode = fs::metadata(format!("/proc/self/fd/{}", listener.as_raw_fd()))
        .unwrap()
        .ino();
    handoffs.ready(KEY, TOKEN, OwnedFd::from(listener), dir.path());
    handoffs.forget("ended");
    assert_eq!(asked.join().unwrap(), Ok(inode), "the early request");
    let ended = ended.join().unwrap().unwrap_err();
    assert!(ended.contains(NO_SUCH_RUN), "a run that ended: {ended}");
    let unknown = fetch(&path, &[3; 16]).unwrap_err();
    assert!(unknown.contains(NO_SUCH_RUN), "{unknown}");
}

/// DD10, DD11: a resume retokens a run whose listener no attach fetched
/// yet: its attach is told the new token now, so the listener waits
/// HANDOFF_DEADLINE from the resume, not from the run's start. One handed
/// off never lapses either way.
#[test]
fn a_resume_gives_an_unfetched_listener_its_deadline_again() {
    let dir = tempfile::tempdir().unwrap();
    let (handoffs, inode, path) = served(dir.path());
    handoffs.runs.lock().unwrap().get_mut(KEY).unwrap().deadline = Some(Instant::now());
    handoffs.retoken(KEY, &[3; 16]);
    assert!(
        !handoffs.lapsed(KEY, dir.path()),
        "lapsed at the start's deadline after a resume"
    );
    assert_eq!(fetch(&path, &[3; 16]), Ok(inode));
}

/// A connection that sends no request is answered, and so let go, once
/// RECV_TIMEOUT passed: no serving thread waits on it for ever.
#[test]
fn a_silent_connection_is_let_go() {
    let dir = tempfile::tempdir().unwrap();
    let handoffs = Arc::new(Handoffs::default());
    let path = dir.path().join("handoff.sock");
    serve(&path, None, handoffs).unwrap();
    let conn = UnixStream::connect(&path).unwrap();
    conn.set_read_timeout(Some(RECV_TIMEOUT + Duration::from_secs(5)))
        .unwrap();
    let answer = framing::recv_reply(&conn)
        .map(drop)
        .map_err(|e| e.to_string());
    let answered = answer
        .as_ref()
        .is_err_and(|e| e.contains("expected a handoff request"));
    assert!(answered, "the silent connection was held: {answer:?}");
}
