//! A producer's relay in attach: DT1-DT4, DT11, DT16, DD8a's resubmission,
//! DD9 and OQ1 (c).

use std::os::fd::AsRawFd;
use std::result::Result as StdResult;
use std::sync::Mutex;
use std::time::Duration;
use std::{fs, future, thread};

use std::ops::ControlFlow;

use sandbox_proto::agent::Empty;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::super::{request, run_async};
use super::*;
use crate::build_json::tests::doc;
use crate::proto::attach_hub_server::{AttachHub, AttachHubServer};
use crate::proto::{AttachEvent, BuildMessage, DECLINE_EXIT_CODE, attach_event, build_message};

const WAIT: Duration = Duration::from_secs(10);

/// A gate already open (RelayStarted acknowledged).
fn open_gate() -> Arc<Gate> {
    let gate = Arc::new(Gate::default());
    gate.ack();
    gate
}

/// The client's socket in `dir`, and its connections as they come.
fn client(dir: &Path) -> (PathBuf, UnixListener) {
    let path = dir.join("client.sock");
    (path.clone(), UnixListener::bind(&path).unwrap())
}

/// A producer's connection spliced, through `gate`, to `client`: the
/// producer's end, and the client's.
async fn spliced(client: &(PathBuf, UnixListener), gate: Arc<Gate>) -> (UnixStream, UnixStream) {
    let (ours, theirs) = UnixStream::pair().unwrap();
    tokio::spawn(splice(theirs, client.0.clone(), gate));
    let (at_client, _) = tokio::time::timeout(WAIT, client.1.accept())
        .await
        .unwrap()
        .unwrap();
    (ours, at_client)
}

async fn read_all(s: &mut UnixStream) -> Vec<u8> {
    let mut v = Vec::new();
    tokio::time::timeout(WAIT, s.read_to_end(&mut v))
        .await
        .unwrap()
        .unwrap();
    v
}

fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// DT1 (DD2, DD4): each direction arrives byte for byte, in order, and a
/// half-close of either end reaches the other as a half-close: the other
/// direction goes on.
#[tokio::test]
async fn splice_is_byte_exact_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (producer, mut at_client) = spliced(&c, open_gate()).await;
    let (up, down) = (bytes(1, 1 << 20), bytes(2, 3 << 19));
    let (mut p_read, mut p_write) = producer.into_split();
    let sent = up.clone();
    let writer = tokio::spawn(async move {
        p_write.write_all(&sent).await.unwrap();
        p_write.shutdown().await.unwrap();
    });
    assert_eq!(read_all(&mut at_client).await, up, "producer to client");
    writer.await.unwrap();
    // The producer's half-close left the other direction open.
    let reader = tokio::spawn(async move {
        let mut got = Vec::new();
        let read = tokio::time::timeout(WAIT, p_read.read_to_end(&mut got));
        read.await.unwrap().unwrap();
        got
    });
    at_client.write_all(&down).await.unwrap();
    at_client.shutdown().await.unwrap();
    assert_eq!(reader.await.unwrap(), down, "client to producer");
}

/// DT2 (DD4): 64 connections whose client end never reads delay no other,
/// each accepted on the run's listener as the relay does.
#[tokio::test]
async fn n_stalled_connections_delay_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (path, listener) = run_listener(dir.path(), "rpc.sock");
    let to = (c.0.clone(), open_gate(), Arc::default());
    tokio::spawn(accept(listener, to, Arc::new(Notify::new())));
    let mut stalled = Vec::new();
    for _ in 0..64 {
        let mut producer = UnixStream::connect(&path).await.unwrap();
        // More than the socket buffers hold: the splice blocks on it.
        tokio::spawn(async move {
            let _ = producer.write_all(&bytes(3, 8 << 20)).await;
        });
        let accepted = tokio::time::timeout(WAIT, c.1.accept()).await;
        let (at_client, _) = accepted
            .expect("a stalled connection delayed another")
            .unwrap();
        stalled.push(at_client);
    }
    let mut producer = UnixStream::connect(&path).await.unwrap();
    producer.write_all(b"through").await.unwrap();
    producer.shutdown().await.unwrap();
    let (mut at_client, _) = tokio::time::timeout(WAIT, c.1.accept())
        .await
        .expect("a stalled connection delayed another")
        .unwrap();
    assert_eq!(read_all(&mut at_client).await, b"through");
}

/// DT4 (DD8): the first byte for the client waits for the hub's
/// acknowledgement of RelayStarted, which the splice asks for.
#[tokio::test]
async fn the_first_write_waits_for_the_relay_started_ack() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let gate = Arc::new(Gate::default());
    let (mut producer, mut at_client) = spliced(&c, gate.clone()).await;
    producer.write_all(b"first").await.unwrap();
    tokio::time::timeout(WAIT, gate.asked())
        .await
        .expect("RelayStarted not asked for");
    let mut buf = [0u8; 5];
    let early = tokio::time::timeout(Duration::from_millis(300), at_client.read(&mut buf)).await;
    assert!(early.is_err(), "a byte reached the client before the ack");
    gate.ack();
    tokio::time::timeout(WAIT, at_client.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"first");
}

/// A listener for a producer's run, as the worker hands it: its path and
/// the listener.
fn run_listener(dir: &Path, name: &str) -> (PathBuf, UnixListener) {
    let path = dir.join(name);
    (path.clone(), UnixListener::bind(&path).unwrap())
}

/// DT3 (DD7): at the exit code, a connection queued on the listener and
/// not yet accepted is spliced, and drain returns only once each splice
/// carried all it had to the client.
#[tokio::test]
async fn exit_drains_the_backlog_and_waits_for_every_splice() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (path, listener) = run_listener(dir.path(), "rpc.sock");
    let splices: Arc<tokio::sync::Mutex<JoinSet<()>>> = Arc::default();
    let ended = Arc::new(Notify::new());
    // The run ended before any connection was accepted.
    ended.notify_one();
    let payload = bytes(4, 1 << 20);
    let mut queued = Vec::new();
    for _ in 0..3 {
        let mut p = UnixStream::connect(&path).await.unwrap();
        let payload = payload.clone();
        queued.push(tokio::spawn(async move {
            p.write_all(&payload).await.unwrap();
            p.shutdown().await.unwrap();
            let mut rest = Vec::new();
            let _ = p.read_to_end(&mut rest).await;
        }));
    }
    let to = (c.0.clone(), open_gate(), splices.clone());
    let reader = tokio::spawn(async move {
        let mut got = Vec::new();
        for _ in 0..3 {
            let (mut s, _) = c.1.accept().await.unwrap();
            got.push(tokio::spawn(async move { read_all(&mut s).await }));
        }
        let mut lens = Vec::new();
        for g in got {
            lens.push(g.await.unwrap().len());
        }
        lens
    });
    tokio::time::timeout(WAIT, accept(listener, to, ended))
        .await
        .unwrap();
    let mut s = splices.lock().await;
    tokio::time::timeout(WAIT, async { while s.join_next().await.is_some() {} })
        .await
        .unwrap();
    let lens = tokio::time::timeout(WAIT, reader)
        .await
        .expect("the backlog was not spliced");
    assert_eq!(lens.unwrap(), [1 << 20; 3], "the backlog's bytes");
}

/// DT11 (DD5): the client's socket is Nix's tmpDir and NIX_REMOTE's
/// basename, whatever that is.
#[test]
fn client_socket_from_tmp_dir_and_nix_remote() {
    let build = doc(&serde_json::json!({"NIX_REMOTE": "unix:///build/not-the-usual-name"}));
    assert_eq!(
        client_socket(&build),
        Some(PathBuf::from("/tmp/x/build/not-the-usual-name"))
    );
    assert_eq!(client_socket(&doc(&serde_json::json!({}))), None);
}

/// A producer's build.json, its Nix saying it can build it itself or not.
fn producer_build(can_local: Option<bool>) -> BuildJson {
    let mut build = doc(&serde_json::json!({
        "requiredSystemFeatures": "builder-rpc-v0",
        "NIX_REMOTE": "unix:///build/.nix-socket",
    }));
    build.can_build_locally = can_local;
    build
}

/// OQ1, decided (c): a producer whose Nix cannot build it itself is
/// refused before anything is submitted; one whose Nix can, or does not
/// say (the hub then answers a loss by the fallback (a)), is relayed. Any
/// other build has no relay.
#[test]
fn a_producer_its_nix_cannot_build_is_refused_before_submission() {
    let handoff = || Some(PathBuf::from("/run/handoff.sock"));
    let relayed = |build, handoff| match of(&build, handoff) {
        Ok(ControlFlow::Continue(r)) => Ok(r.is_some()),
        Ok(ControlFlow::Break(())) => Err("declined".to_owned()),
        Err(e) => Err(e.to_string()),
    };
    let refused = relayed(producer_build(Some(false)), handoff());
    assert_eq!(refused, Err(PRODUCER_REFUSED.to_owned()));
    let refused = relayed(producer_build(Some(false)), None);
    assert_eq!(refused, Err(PRODUCER_REFUSED.to_owned()), "no handoff");
    assert_eq!(relayed(producer_build(Some(true)), handoff()), Ok(true));
    assert_eq!(relayed(producer_build(None), handoff()), Ok(true));
    let plain = || doc(&serde_json::json!({}));
    assert_eq!(relayed(plain(), handoff()), Ok(false));
    assert_eq!(relayed(plain(), None), Ok(false), "no handoff");
}

/// DD12, where no worker on this host serves the handoff socket (the
/// module's default): a producer whose Nix can build it, or does not say,
/// is declined (222) before attach connects to a hub, and Nix builds it
/// itself, as a hub with no worker serving producers would decline it.
#[tokio::test]
async fn a_producer_without_a_handoff_socket_is_declined() {
    for can_local in [Some(true), None] {
        let dir = tempfile::tempdir().unwrap();
        let no_hub = dir.path().join("no-hub.sock");
        let got = run_async(producer_build(can_local), no_hub, None).await;
        let got = got.map_err(|e| e.to_string());
        assert_eq!(got, Ok(DECLINE_EXIT_CODE), "{can_local:?}");
    }
}

/// DD9: a producer's request carries 16 random bytes of its attach's own;
/// any other build's none.
#[test]
fn each_producers_attach_has_an_id_of_its_own() {
    let (a, _) = request(producer_build(Some(true)));
    let (b, _) = request(producer_build(Some(true)));
    assert_eq!(a.attach_id.len(), 16);
    assert_ne!(a.attach_id, b.attach_id, "two attaches share an id");
    let (plain, _) = request(doc(&serde_json::json!({})));
    assert!(plain.attach_id.is_empty());
}

/// A handoff socket in `dir` that hands the listener at each of `runs`
/// to the request with its token.
fn handoff_server(dir: &Path, runs: Vec<(Vec<u8>, PathBuf)>) -> PathBuf {
    let path = dir.join("handoff.sock");
    let server = StdListener::bind(&path).unwrap();
    thread::spawn(move || {
        for conn in server.incoming().flatten() {
            let Ok((call::Call::Handoff(req), _)) = framing::recv_call(&conn) else {
                continue;
            };
            let run = runs.iter().find(|(t, _)| *t == req.token);
            let listener = run.map(|(_, p)| StdListener::bind(p).unwrap());
            match listener {
                Some(l) => {
                    let fd = l.as_raw_fd();
                    let _ = framing::send_reply(&conn, reply::Reply::Empty(Empty {}), &[fd]);
                }
                None => drop(framing::send_error(&conn, "no such run")),
            }
        }
    });
    path
}

/// DT16 (DD10): RelayReady{fresh} closes the listener held, so its run's
/// producer is refused, and aborts its splices, before the new run's is
/// fetched; a resumed run's RelayReady keeps the listener.
#[tokio::test]
async fn relay_ready_fresh_closes_the_old_listener_and_aborts_its_splices() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (first, second) = (
        dir.path().join("first.sock"),
        dir.path().join("second.sock"),
    );
    let runs = vec![(vec![1; 16], first.clone()), (vec![2; 16], second.clone())];
    let handoff = handoff_server(dir.path(), runs);
    let mut relay = Relay::new(c.0.clone(), handoff);
    let ready = |token: u8, fresh| RelayReady {
        handoff_token: vec![token; 16],
        fresh,
    };
    relay.ready(ready(1, true)).await.unwrap();
    let mut producer = UnixStream::connect(&first).await.unwrap();
    let (_at_client, _) = tokio::time::timeout(WAIT, c.1.accept())
        .await
        .unwrap()
        .unwrap();
    relay.ready(ready(3, false)).await.unwrap();
    assert!(
        UnixStream::connect(&first).await.is_ok(),
        "a resume closed the listener"
    );
    let _ = tokio::time::timeout(WAIT, c.1.accept()).await;
    relay.ready(ready(2, true)).await.unwrap();
    let refused = UnixStream::connect(&first).await;
    assert!(refused.is_err(), "the old run's listener is still open");
    let mut rest = Vec::new();
    let ended = tokio::time::timeout(WAIT, producer.read_to_end(&mut rest)).await;
    assert!(ended.is_ok(), "the old run's splice was not aborted");
    assert!(
        UnixStream::connect(&second).await.is_ok(),
        "the new run's listener"
    );
}

/// DT3, DD7's wait: the relay's drain returns only once every splice of the
/// run ended, each having carried all it had to the client.
#[tokio::test]
async fn drain_returns_once_every_splice_ended() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let run = dir.path().join("run.sock");
    let handoff = handoff_server(dir.path(), vec![(vec![1; 16], run.clone())]);
    let mut relay = Relay::new(c.0.clone(), handoff);
    let ready = RelayReady {
        handoff_token: vec![1; 16],
        fresh: true,
    };
    relay.ready(ready).await.unwrap();
    relay.acked(&[1; 16]);
    let payload = bytes(5, 4 << 20);
    let mut producer = UnixStream::connect(&run).await.unwrap();
    let (mut at_client, _) = tokio::time::timeout(WAIT, c.1.accept())
        .await
        .unwrap()
        .unwrap();
    let got = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let into = got.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1 << 16];
        while let Ok(n) = at_client.read(&mut buf).await {
            if n == 0 {
                break;
            }
            into.lock().await.extend_from_slice(&buf[..n]);
            // A slow client: the splice outlasts the run's end.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let sent = payload.clone();
    tokio::spawn(async move {
        producer.write_all(&sent).await.unwrap();
        producer.shutdown().await.unwrap();
        let mut rest = Vec::new();
        let _ = producer.read_to_end(&mut rest).await;
    });
    tokio::time::timeout(WAIT, relay.drain())
        .await
        .expect("drain hung");
    // What the splice wrote before it ended reaches the reader at once.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        got.lock().await.len(),
        payload.len(),
        "drain returned early"
    );
}

/// DD10 on a reconnect: a hub replays the run's RelayReady, fresh or not,
/// to the attach holding that run's listener (here against a handoff
/// socket that, as the worker's, hands a run's listener once): the
/// listener is kept, and the replay is no error.
#[tokio::test]
async fn a_relay_ready_replayed_with_the_held_token_keeps_the_listener() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let run = dir.path().join("run.sock");
    let path = dir.path().join("handoff.sock");
    let server = StdListener::bind(&path).unwrap();
    let run2 = run.clone();
    thread::spawn(move || {
        let mut kept = Some(StdListener::bind(&run2).unwrap());
        for conn in server.incoming().flatten() {
            let Ok((call::Call::Handoff(_), _)) = framing::recv_call(&conn) else {
                continue;
            };
            match kept.take() {
                Some(l) => {
                    let fds = [l.as_raw_fd()];
                    let _ = framing::send_reply(&conn, reply::Reply::Empty(Empty {}), &fds);
                }
                None => drop(framing::send_error(&conn, "handed off already")),
            }
        }
    });
    let mut relay = Relay::new(c.0.clone(), path);
    let ready = |token: u8, fresh| RelayReady {
        handoff_token: vec![token; 16],
        fresh,
    };
    relay.ready(ready(1, true)).await.unwrap();
    relay.ready(ready(2, false)).await.unwrap();
    for again in [ready(1, true), ready(2, true), ready(2, false)] {
        let told = relay.ready(again.clone()).await;
        assert!(told.is_ok(), "replayed {again:?}: {told:?}");
        let open = UnixStream::connect(&run).await.is_ok();
        assert!(open, "replayed {again:?}: the listener was closed");
    }
}

/// DD8: the gate opens only at an acknowledgement naming a token of the
/// run held, its fresh start's or a resume's; what RelayStarted names is
/// the latest; and a run closed takes back what its splices wanted told.
#[tokio::test]
async fn the_gate_opens_only_for_the_run_held() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (first, second) = (dir.path().join("1.sock"), dir.path().join("2.sock"));
    let runs = vec![(vec![1; 16], first), (vec![2; 16], second)];
    let mut relay = Relay::new(c.0.clone(), handoff_server(dir.path(), runs));
    let ready = |token: u8, fresh| RelayReady {
        handoff_token: vec![token; 16],
        fresh,
    };
    relay.ready(ready(1, true)).await.unwrap();
    relay.gate.wanted.store(true, Ordering::SeqCst);
    relay.ready(ready(2, true)).await.unwrap();
    assert!(
        !relay.gate.wanted(),
        "a closed run's splice is still to be told"
    );
    relay.ready(ready(3, false)).await.unwrap();
    assert_eq!(relay.token(), Some(vec![3; 16]), "what RelayStarted names");
    relay.acked(&[1; 16]);
    assert!(
        !relay.gate.acked(),
        "a closed run's acknowledgement opened it"
    );
    relay.acked(&[2; 16]);
    assert!(relay.gate.acked(), "the run's fresh start's token");
}

/// DD8, DD10: a fresh run closes the gate opened for the run before, so
/// its splices write nothing until the hub acknowledged RelayStarted
/// naming it.
#[tokio::test]
async fn a_fresh_run_closes_the_gate_of_the_run_before() {
    let dir = tempfile::tempdir().unwrap();
    let c = client(dir.path());
    let (first, second) = (dir.path().join("1.sock"), dir.path().join("2.sock"));
    let runs = vec![(vec![1; 16], first), (vec![2; 16], second)];
    let mut relay = Relay::new(c.0.clone(), handoff_server(dir.path(), runs));
    let ready = |token: u8, fresh| RelayReady {
        handoff_token: vec![token; 16],
        fresh,
    };
    relay.ready(ready(1, true)).await.unwrap();
    relay.acked(&[1; 16]);
    assert!(relay.gate.acked());
    relay.ready(ready(2, true)).await.unwrap();
    assert!(
        !relay.gate.acked(),
        "a fresh run inherited the gate opened for the run before"
    );
    relay.acked(&[2; 16]);
    assert!(relay.gate.acked(), "the fresh run's acknowledgement");
}

#[path = "relay_loop_tests.rs"]
mod attach_loop;
