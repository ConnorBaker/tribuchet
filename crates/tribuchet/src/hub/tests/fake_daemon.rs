//! Fake nix-daemons for the hub's own daemon, speaking protocol 1.37,
//! and the helpers of the tests that run builds against them.

use std::io;
use std::os::fd::AsFd as _;
use std::path::Path;
use std::sync::atomic::AtomicUsize;

use harmonia_store_remote::{ConnectionPool, PoolConfig};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};

use super::daemon::with_input;
use super::*;
use crate::sockpath;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::hub) enum Daemon {
    /// Closes the connection at the first operation, as a daemon (or
    /// its per-connection process) that dies mid-request.
    Drops,
    /// Closes the connection in the handshake, once it has the client's
    /// version, as a per-connection process that dies there.
    DropsInHandshake,
    /// Never answers an operation.
    Silent,
    /// Answers every QueryPathInfo that the path is not valid.
    Lacks,
    /// As Lacks, 5 s after each query: time for a client to leave.
    LacksSlowly,
    /// Answers every operation with STDERR_ERROR and this message.
    Says(&'static str),
    /// Answers the handshake with another magic number than a
    /// nix-daemon's (harmonia: WrongMagic).
    Foreign,
    /// Ends the handshake with STDERR_ERROR instead of STDERR_LAST.
    RefusesHandshake,
    /// Answers every operation with a frame out of place: STDERR_READ,
    /// asking for data a query has none of (harmonia: NoSourceForLoggerRead).
    Garbles,
    /// Closes every connection before answering the handshake at all, as
    /// Nix's daemon does to a user it does not allow (daemon.cc:234-235).
    Closes,
    /// Answers the handshake with protocol 1.36, older than harmonia
    /// speaks (harmonia: UnsupportedVersion).
    Old,
    /// Answers every operation with a message type no nix-daemon sends
    /// (harmonia: InvalidData, no discriminant in RawLogMessageType).
    Unknown,
    /// Names its version in the handshake with bytes that are not UTF-8
    /// (harmonia: InvalidData, in other words than Unknown's).
    Unreadable,
}

const STDERR_LAST: u64 = 0x616c_7473;
const STDERR_ERROR: u64 = 0x6378_7470;
const STDERR_READ: u64 = 0x6461_7461;
/// Not one of the eight STDERR_ message types of Nix's worker protocol.
const STDERR_UNKNOWN: u64 = 0x6e6f_7065;

/// What Daemon::RefusesHandshake says, as Nix's daemon does of a path
/// it cannot take (BadStorePath, store-dir-config.cc:26-37).
const REFUSAL: &str = "path '/nix/store/x' is not in the Nix store";

async fn write_string(conn: &mut UnixStream, s: &[u8]) -> io::Result<()> {
    conn.write_all(&(s.len() as u64).to_le_bytes()).await?;
    conn.write_all(s).await?;
    let pad = s.len().next_multiple_of(8) - s.len();
    conn.write_all(&[0u8; 8][..pad]).await
}

/// An error as Nix's daemon sends it at protocol 1.26 and later
/// (serialise.cc, operator<< for Error): type, level, name, message, no
/// position, no traces.
async fn refuse(conn: &mut UnixStream, msg: &str) -> io::Result<()> {
    conn.write_all(&STDERR_ERROR.to_le_bytes()).await?;
    write_string(conn, b"Error").await?;
    conn.write_all(&0u64.to_le_bytes()).await?;
    write_string(conn, b"Error").await?;
    write_string(conn, msg.as_bytes()).await?;
    conn.write_all(&[0u8; 16]).await
}

async fn handshake(conn: &mut UnixStream, daemon: Daemon) -> io::Result<()> {
    let mut b = [0u8; 8];
    conn.read_exact(&mut b).await?;
    let magic: u64 = if daemon == Daemon::Foreign {
        0x6478_696e
    } else {
        0x6478_696f
    };
    conn.write_all(&magic.to_le_bytes()).await?;
    if daemon == Daemon::Foreign {
        return conn.read_to_end(&mut Vec::new()).await.map(drop);
    }
    if daemon == Daemon::Old {
        conn.write_all(&0x0124_u64.to_le_bytes()).await?;
        return conn.read_to_end(&mut Vec::new()).await.map(drop);
    }
    conn.write_all(&0x0125_u64.to_le_bytes()).await?;
    for _ in 0..3 {
        conn.read_exact(&mut b).await?;
    }
    if daemon == Daemon::DropsInHandshake {
        return Ok(());
    }
    let version: &[u8] = if daemon == Daemon::Unreadable {
        b"2.\xff\xfe"
    } else {
        b"2.30.0"
    };
    write_string(conn, version).await?;
    conn.write_all(&1u64.to_le_bytes()).await?;
    if daemon == Daemon::RefusesHandshake {
        return refuse(conn, REFUSAL).await;
    }
    conn.write_all(&STDERR_LAST.to_le_bytes()).await
}

async fn serve(conn: &mut UnixStream, daemon: Daemon) -> io::Result<()> {
    if daemon == Daemon::Closes {
        return Ok(());
    }
    handshake(conn, daemon).await?;
    let mut b = [0u8; 8];
    match daemon {
        Daemon::Drops => conn.read_exact(&mut b).await.map(drop),
        Daemon::Silent
        | Daemon::RefusesHandshake
        | Daemon::Foreign
        | Daemon::Old
        | Daemon::Unreadable => conn.read_to_end(&mut Vec::new()).await.map(drop),
        Daemon::Unknown => {
            conn.read_exact(&mut b).await?;
            conn.write_all(&STDERR_UNKNOWN.to_le_bytes()).await?;
            conn.read_to_end(&mut Vec::new()).await.map(drop)
        }
        Daemon::Says(msg) => {
            conn.read_exact(&mut b).await?;
            refuse(conn, msg).await?;
            // What the client still sends (the path) is read and
            // dropped, so its writes do not fail first.
            conn.read_to_end(&mut Vec::new()).await.map(drop)
        }
        Daemon::Garbles => {
            conn.read_exact(&mut b).await?;
            conn.write_all(&STDERR_READ.to_le_bytes()).await?;
            conn.write_all(&8u64.to_le_bytes()).await?;
            conn.read_to_end(&mut Vec::new()).await.map(drop)
        }
        Daemon::Closes | Daemon::DropsInHandshake => Ok(()),
        Daemon::Lacks | Daemon::LacksSlowly => loop {
            // The operation, then the path: a length and padded bytes.
            conn.read_exact(&mut b).await?;
            conn.read_exact(&mut b).await?;
            let padded = u64::from_le_bytes(b).div_ceil(8) * 8;
            let mut path = vec![0; usize::try_from(padded).unwrap()];
            conn.read_exact(&mut path).await?;
            if daemon == Daemon::LacksSlowly {
                tracing::info!("fake daemon: answering in 5 s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            conn.write_all(&STDERR_LAST.to_le_bytes()).await?;
            conn.write_all(&0u64.to_le_bytes()).await?;
        },
    }
}

/// A hub whose nix-daemon behaves as `daemon`, with at most `max_size`
/// connections to it.
pub(in crate::hub) fn hub_with(dir: &Path, daemon: Daemon, max_size: usize) -> Arc<HubState> {
    hub_scripted(dir, move |_| daemon, max_size).0
}

/// A hub whose nix-daemon behaves as `script` says on its `n`th
/// connection, from 0, and the connections it took so far: one per
/// attempt that fails, as harmonia keeps no connection whose operation
/// failed.
pub(super) fn hub_scripted(
    dir: &Path,
    script: impl Fn(usize) -> Daemon + Send + 'static,
    max_size: usize,
) -> (Arc<HubState>, Arc<AtomicUsize>) {
    let socket = dir.join("daemon.sock");
    let listener = sockpath::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = UnixListener::from_std(listener).unwrap();
    let conns = Arc::new(AtomicUsize::new(0));
    let counted = conns.clone();
    // An accept error must not end the daemon: every connect would be
    // refused from then on, a failure no test counts. Another process
    // can hold the daemon's end of a connection too: a child that a
    // concurrent test forked, until it execs. The hub then reads no close
    // until it lets go, in real time, while the paused clock runs out
    // ANSWER. So the daemon holds each end itself, and ends a connection
    // by shutting it down, which no holder delays.
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            match listener.accept().await {
                Ok((mut conn, _)) => {
                    let n = counted.fetch_add(1, Ordering::SeqCst);
                    let daemon = script(n);
                    held.push(conn.as_fd().try_clone_to_owned().unwrap());
                    drop(tokio::spawn(async move {
                        drop(serve(&mut conn, daemon).await);
                        drop(conn.shutdown().await);
                    }));
                }
                Err(e) => {
                    tracing::warn!("fake daemon: accept: {e}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    });
    let mut state = HubState::for_test(Duration::from_secs(30));
    // A paused clock jumps to the next timer whenever the runtime idles,
    // also between the handshake's messages: a short timeout would end
    // handshakes that never stalled.
    let config = PoolConfig {
        max_size,
        connection_timeout: Duration::from_secs(3600),
        ..PoolConfig::default()
    };
    state.daemon_pool = ConnectionPool::new(socket, config);
    (Arc::new(state), conns)
}

pub(in crate::hub) type Client = mpsc::Receiver<Result<AttachEvent, Status>>;

/// The notes on the hub's daemon a client got so far, and its error.
pub(in crate::hub) fn told(client: &mut Client) -> (Vec<String>, Option<String>) {
    let (mut notes, mut error) = (Vec::new(), None);
    while let Ok(Ok(AttachEvent { event: Some(e) })) = client.try_recv() {
        match e {
            attach_event::Event::Log(l) => notes.push(String::from_utf8(l).unwrap()),
            attach_event::Event::Error(e) => error = Some(e),
            _ => {}
        }
    }
    notes.retain(|n| n.contains("the hub's nix-daemon"));
    (notes, error)
}

/// A worker session asking for one build.
pub(super) async fn worker(state: &Arc<HubState>, name: &str) -> Worker {
    let w = connect(state, name, 1, &[]);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    w
}

/// A build whose one input has its recipe cached, so the hub looks
/// it up before it can send the assignment.
pub(super) async fn cached_input(state: &Arc<HubState>, n: usize) -> (Job, String) {
    let mut j = with_input(n);
    let input = format!("/nix/store/{H}-cached");
    j.req.input_paths = vec![input.clone()];
    state
        .chunks
        .store_recipe(input.clone(), vec![0; 32], Arc::new(Vec::new()));
    (j, input)
}

/// The error `client` gets, within an hour.
pub(super) async fn error_of(client: &mut Client) -> String {
    tokio::time::timeout(Duration::from_hours(1), async {
        loop {
            match client.recv().await {
                Some(Ok(AttachEvent {
                    event: Some(attach_event::Event::Error(e)),
                })) => return e,
                Some(_) => {}
                None => panic!("the stream ended without an error"),
            }
        }
    })
    .await
    .expect("no error within an hour")
}
