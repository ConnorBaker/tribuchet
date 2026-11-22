//! A producer's (builder-rpc-v0) side of attach (DESIGN.md,
//! "Dynamic-derivation producers"): its run's listener, fetched from the
//! worker on this host, and each connection on it spliced to the client's
//! socket.
//!
//! - DD5: the client's socket is Nix's tmpDir (topTmpDir/build) joined with
//!   NIX_REMOTE's basename, as Nix names it; never a constant.
//! - DD4: one client connection per accepted connection, each spliced by a
//!   task of its own, both ways, half-close as half-close. Backpressure is
//!   the kernel's, per socket.
//! - DD8: before the first byte of the run reaches the client, the hub has
//!   acknowledged RelayStarted naming a token of the run held, or the run
//!   ended (a worker's exit code), after which no fresh run follows.
//! - DD10: RelayReady{fresh} of another run closes a listener held, aborts
//!   its splices (none of them carried a byte, by DD8) and closes the gate
//!   before the new one is fetched. One with a token of the run held
//!   changes nothing: a hub replays it to an attach that reconnects. A
//!   listener not fetched is the run's loss, which the hub settles once
//!   the worker reports it expired; attach goes on.
//! - DD7: at a worker's exit code, the backlog is accepted and spliced, and
//!   attach exits only once every splice ended. At the hub's decline the
//!   run was lost and may still run: its splices are aborted instead.

use std::ops::ControlFlow;
use std::os::fd::OwnedFd;
use std::os::unix::net::{UnixListener as StdListener, UnixStream as StdStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sandbox_proto::agent::{HandoffRequest, call, reply};
use sandbox_proto::framing;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, watch};
use tokio::task::{JoinHandle, JoinSet};

use super::PRODUCER_REFUSED;
use crate::build_json::{BuildJson, required_system_features};
use crate::errors::{Result, err_ctx, err_msg};
use crate::proto::{RelayReady, producer};

/// Whether the run reached the client, as the hub acknowledged (DD8).
#[derive(Default)]
pub(super) struct Gate {
    /// A splice holds bytes for the client: RelayStarted is to be sent.
    wanted: AtomicBool,
    want: Notify,
    acked: watch::Sender<bool>,
}

impl Gate {
    /// The hub acknowledged RelayStarted.
    pub(super) fn ack(&self) {
        self.acked.send_replace(true);
    }

    pub(super) fn acked(&self) -> bool {
        *self.acked.borrow()
    }

    /// Whether RelayStarted is to be sent, and not yet acknowledged.
    pub(super) fn wanted(&self) -> bool {
        self.wanted.load(Ordering::SeqCst) && !self.acked()
    }

    /// Woken when a splice first wants RelayStarted sent.
    pub(super) async fn asked(&self) {
        let notified = self.want.notified();
        if !self.wanted() {
            notified.await;
        }
    }

    /// Before the first byte reaches the client: ask for RelayStarted,
    /// and wait for the hub's acknowledgement.
    async fn open(&self) {
        let mut acked = self.acked.subscribe();
        if !*acked.borrow() {
            self.wanted.store(true, Ordering::SeqCst);
            self.want.notify_waiters();
        }
        let _ = acked.wait_for(|a| *a).await;
    }
}

/// A producer's relay: where its client's socket and the worker's handoff
/// socket are, the listener held, and its splices.
pub(super) struct Relay {
    client: PathBuf,
    handoff: PathBuf,
    pub(super) gate: Arc<Gate>,
    held: Option<Held>,
    splices: Arc<tokio::sync::Mutex<JoinSet<()>>>,
}

/// The listener held: its run's tokens (its fresh start's, then each
/// resume's), its accept task, and what tells that task the run ended.
struct Held {
    tokens: Vec<Vec<u8>>,
    accept: JoinHandle<()>,
    ended: Arc<Notify>,
}

/// A producer's relay for `build`; None for any other build. Refused if
/// its Nix cannot build it itself (OQ1 (c)); unsaid goes on (OQ1's
/// fallback (a) is the hub's). With no handoff socket here (DD12), Break:
/// declined, as a hub with no worker serving producers would.
pub(super) fn of(
    build: &BuildJson,
    handoff: Option<PathBuf>,
) -> Result<ControlFlow<(), Option<Relay>>> {
    let features = required_system_features(&build.env, build.attrs().as_ref());
    if !producer(&features) {
        return Ok(ControlFlow::Continue(None));
    }
    if build.can_build_locally == Some(false) {
        return Err(err_msg(PRODUCER_REFUSED));
    }
    let Some(handoff) = handoff else {
        return Ok(ControlFlow::Break(()));
    };
    let client = client_socket(build)
        .ok_or_else(|| err_msg("a producer's NIX_REMOTE names no socket in its tmp dir"))?;
    Ok(ControlFlow::Continue(Some(Relay::new(client, handoff))))
}

/// 16 random bytes for a producer's attach (DD9).
pub(super) fn attach_id() -> Vec<u8> {
    let mut id = vec![0u8; 16];
    getrandom::fill(&mut id).expect("read system randomness");
    id
}

/// The client's socket of `build` (DD5): Nix's tmpDir and NIX_REMOTE's
/// basename. None without a NIX_REMOTE of the form unix://DIR/NAME.
pub(super) fn client_socket(build: &BuildJson) -> Option<PathBuf> {
    let remote = build.env.get("NIX_REMOTE")?.strip_prefix("unix://")?;
    let name = Path::new(remote).file_name()?;
    Some(build.top_tmp_dir.join("build").join(name))
}

impl Relay {
    pub(super) fn new(client: PathBuf, handoff: PathBuf) -> Self {
        Self {
            client,
            handoff,
            gate: Arc::default(),
            held: None,
            splices: Arc::default(),
        }
    }

    /// The hub's RelayReady: fetch the run's listener, unless the one held
    /// is that run's.
    pub(super) async fn ready(&mut self, ready: RelayReady) -> Result<()> {
        if let Some(held) = &mut self.held {
            // Told again (replayed to this attach as it reconnected).
            if held.tokens.contains(&ready.handoff_token) {
                return Ok(());
            }
            // The run resumed, under a token of its own: the listener
            // held is the run's.
            if !ready.fresh {
                held.tokens.push(ready.handoff_token);
                return Ok(());
            }
        }
        self.close().await;
        let handoff = self.handoff.clone();
        let token = ready.handoff_token.clone();
        let fd = tokio::task::spawn_blocking(move || fetch(&handoff, token)).await??;
        let std = StdListener::from(fd);
        std.set_nonblocking(true)?;
        let listener = UnixListener::from_std(std)?;
        let ended = Arc::new(Notify::new());
        let to = (self.client.clone(), self.gate.clone(), self.splices.clone());
        let accept = tokio::spawn(accept(listener, to, ended.clone()));
        self.held = Some(Held {
            tokens: vec![ready.handoff_token],
            accept,
            ended,
        });
        Ok(())
    }

    /// The latest token of the run held: what RelayStarted names.
    pub(super) fn token(&self) -> Option<Vec<u8>> {
        self.held.as_ref()?.tokens.last().cloned()
    }

    /// The hub acknowledged RelayStarted naming `token`: the gate opens if
    /// that is a token of the run held.
    pub(super) fn acked(&self, token: &[u8]) {
        if (self.held.as_ref()).is_some_and(|h| h.tokens.iter().any(|t| t == token)) {
            self.gate.ack();
        }
    }

    /// Close the listener held and abort its splices: what a splice of it
    /// wanted told is no longer to be, and what the hub acknowledged of it
    /// opens no gate for the next run.
    pub(super) async fn close(&mut self) {
        if let Some(held) = self.held.take() {
            held.accept.abort();
            let _ = held.accept.await;
        }
        self.splices.lock().await.abort_all();
        self.gate.wanted.store(false, Ordering::SeqCst);
        self.gate.acked.send_replace(false);
    }

    /// The run ended (DD7): splice every connection still queued on the
    /// listener, then wait until every splice ended. No fresh run follows
    /// an ended one, so the gate opens (DD8): a splice waiting there for an
    /// acknowledgement the hub may never send now would hang attach.
    pub(super) async fn drain(&mut self) {
        self.gate.ack();
        if let Some(held) = self.held.take() {
            held.ended.notify_one();
            let _ = held.accept.await;
        }
        let mut splices = self.splices.lock().await;
        while splices.join_next().await.is_some() {}
    }
}

/// Fetch the listener of the run `token` names from the worker's handoff
/// socket at `handoff`. Blocking.
fn fetch(handoff: &Path, token: Vec<u8>) -> Result<OwnedFd> {
    let conn = StdStream::connect(handoff).map_err(err_ctx(format!(
        "connecting to the worker's handoff socket {}",
        handoff.display()
    )))?;
    framing::send_call(&conn, call::Call::Handoff(HandoffRequest { token }), &[])?;
    let (answer, mut fds) =
        framing::recv_reply(&conn).map_err(err_ctx("fetching the producer's listener"))?;
    match (answer, fds.pop()) {
        (reply::Reply::Empty(_), Some(fd)) => Ok(fd),
        _ => Err(err_msg("the worker's handoff answer carried no listener")),
    }
}

/// The splices' destination: the client's socket, the gate before it, and
/// the set the splices run in.
type To = (PathBuf, Arc<Gate>, Arc<tokio::sync::Mutex<JoinSet<()>>>);

/// Accept each connection of the producer's run and splice it to the
/// client's socket; once `ended`, also each still queued (DD7).
async fn accept(listener: UnixListener, (client, gate, splices): To, ended: Arc<Notify>) {
    let spawn = async |conn| {
        let (client, gate) = (client.clone(), gate.clone());
        splices.lock().await.spawn(splice(conn, client, gate));
    };
    loop {
        tokio::select! {
            biased;
            () = ended.notified() => break,
            accepted = listener.accept() => match accepted {
                Ok((conn, _)) => spawn(conn).await,
                Err(_) => return,
            },
        }
    }
    let Ok(listener) = listener.into_std() else {
        return;
    };
    if listener.set_nonblocking(true).is_err() {
        return;
    }
    while let Ok((conn, _)) = listener.accept() {
        if conn.set_nonblocking(true).is_ok()
            && let Ok(conn) = UnixStream::from_std(conn)
        {
            spawn(conn).await;
        }
    }
}

/// One connection of the producer, spliced to one of the client's (DD4):
/// both ways, the first byte to the client after the gate opened (DD8).
async fn splice(mut producer: UnixStream, client: PathBuf, gate: Arc<Gate>) {
    let Ok(mut client) = UnixStream::connect(&client).await else {
        tracing::warn!(socket = %client.display(), "the client's socket refused a producer's connection");
        return;
    };
    let mut first = vec![0u8; 64 * 1024];
    let Ok(n) = producer.read(&mut first).await else {
        return;
    };
    if n > 0 {
        gate.open().await;
        if client.write_all(&first[..n]).await.is_err() {
            return;
        }
    } else if client.shutdown().await.is_err() {
        return;
    }
    let _ = copy_bidirectional(&mut producer, &mut client).await;
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
