//! The handoff socket (DD10, DD11): a producer's (builder-rpc-v0) run gets a
//! listening socket in its sandbox, at the path its NIX_REMOTE names; the
//! agent hands its listener to the worker with StartReply, and the worker
//! to the run's attach, which splices each connection to its client's
//! socket. Served only where `handoff-socket` is set, on the hub's host:
//! there alone does attach run beside the worker (DD12).
//!
//! - The listener goes only to a request that presents the run's token,
//!   which the hub sent the worker in the assignment and the attach in
//!   RelayReady, and nowhere else; at most once per run.
//! - Once the send succeeded, the worker closes its copy and records
//!   `handed_off` in the run's dir; never before it: attach is then the
//!   only holder, so the listener closes exactly when the client goes away.
//!   A send that failed leaves the listener for the next request.
//! - A request may come before the listener does (the hub tells attach
//!   at Started, which the worker sends once it expects the run, before
//!   the agent started the builder): it waits for it.
//! - Before that, the worker lets go at HANDOFF_DEADLINE after the
//!   StartReply, or when a restarted worker finds the run never handed
//!   off: the run is killed and reported `handoff_expired`, a loss.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use sandbox_proto::agent::{Empty, call, reply};
use sandbox_proto::framing;

use crate::config::WorkerConfig;
use crate::errors::{Result, err_ctx};

/// How long a run's listener waits for its attach after the StartReply,
/// or after a resume retokened it. Chosen, not measured: attach's
/// reconnect budget (attach.rs, 30 × 2 s), so an attach reconnecting to a
/// restarting hub is told RelayReady again within it; a live attach
/// fetches within one local round trip of RelayReady. Too short costs a
/// dispatch, counted as a loss.
pub(super) const HANDOFF_DEADLINE: Duration = Duration::from_mins(1);

/// The file in a run's dir that says its listener reached its attach.
const HANDED_OFF: &str = "handed_off";

/// The error of a run whose listener reached no attach: its result is
/// sent `handoff_expired`, which the hub settles as a loss.
pub(in crate::worker) const EXPIRED: &str = "no attach fetched the producer's listener in time";

/// Said to a request whose token names no run here: a producer routed to
/// another worker than its attach's host's (DD12).
pub(super) const NO_SUCH_RUN: &str = "no such run on the local worker";

/// How long a handoff request may take to arrive once connected. Chosen:
/// attach, on this host, sends it as soon as it connected.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// A producer's run, by its dedupe key, until it ends.
struct Run {
    token: Vec<u8>,
    listener: Option<OwnedFd>,
    /// None until the agent's StartReply brought the listener.
    deadline: Option<Instant>,
    dir: PathBuf,
    handed: bool,
}

/// The runs whose listener this worker holds or held.
#[derive(Default)]
pub(in crate::worker) struct Handoffs {
    runs: Mutex<HashMap<String, Run>>,
    changed: Condvar,
}

impl Handoffs {
    /// The run of `key` in `dir` is launched under `token`, before the
    /// worker says Started (from which the hub tells attach): a request
    /// with that token waits for its listener, however long the agent
    /// takes to start the builder. An empty token names no run.
    pub(in crate::worker) fn expect(&self, key: &str, token: &[u8], dir: &Path) {
        if token.is_empty() {
            return;
        }
        let run = Run {
            token: token.to_vec(),
            listener: None,
            deadline: None,
            dir: dir.to_owned(),
            handed: false,
        };
        self.runs.lock().unwrap().insert(key.to_owned(), run);
    }

    /// The run of `key` in `dir` started: its listener, for the holder of
    /// `token`, until `HANDOFF_DEADLINE` from now.
    pub(in crate::worker) fn ready(&self, key: &str, token: &[u8], listener: OwnedFd, dir: &Path) {
        let run = Run {
            token: token.to_vec(),
            listener: Some(listener),
            deadline: Some(Instant::now() + HANDOFF_DEADLINE),
            dir: dir.to_owned(),
            handed: false,
        };
        self.runs.lock().unwrap().insert(key.to_owned(), run);
        self.changed.notify_all();
    }

    /// A later dispatch of `key` resumes its run here: its token is the
    /// one the hub gave that dispatch's attach, which is told it now, so a
    /// listener not handed off yet waits HANDOFF_DEADLINE from now.
    pub(in crate::worker) fn retoken(&self, key: &str, token: &[u8]) {
        if let Some(run) = self.runs.lock().unwrap().get_mut(key) {
            run.token = token.to_vec();
            if !run.handed && run.deadline.is_some() {
                run.deadline = Some(Instant::now() + HANDOFF_DEADLINE);
            }
        }
        self.changed.notify_all();
    }

    /// The run of `key` ended: forget it.
    pub(in crate::worker) fn forget(&self, key: &str) {
        self.runs.lock().unwrap().remove(key);
        self.changed.notify_all();
    }

    /// Whether the producer run of `key` in `dir` lost its chance to reach
    /// its attach: its listener not handed off by its deadline, or, held
    /// by no entry (a restarted worker adopting it), never handed off.
    /// Its listener is then closed.
    pub(in crate::worker) fn lapsed(&self, key: &str, dir: &Path) -> bool {
        let mut runs = self.runs.lock().unwrap();
        let Some(run) = runs.get_mut(key) else {
            return !dir.join(HANDED_OFF).exists();
        };
        if run.handed || run.deadline.is_none_or(|d| Instant::now() < d) {
            return false;
        }
        drop(run.listener.take());
        true
    }

    /// The listener of the run `token` names, once, waiting for it while
    /// that run is launched and its listener not yet here.
    fn take(&self, token: &[u8]) -> Result<(String, OwnedFd), &'static str> {
        let mut runs = self.runs.lock().unwrap();
        loop {
            let found = (runs.iter_mut()).find(|(_, r)| !token.is_empty() && r.token == token);
            let Some((key, run)) = found else {
                return Err(NO_SUCH_RUN);
            };
            if run.handed {
                return Err("the run's listener was handed off already");
            }
            if run.deadline.is_some() {
                let listener = run.listener.take().ok_or("the run's listener is closed")?;
                run.handed = true;
                return Ok((key.clone(), listener));
            }
            runs = self.changed.wait(runs).unwrap();
        }
    }

    /// Record that the run of `key` was handed off, or, the send having
    /// failed, give its listener back.
    fn handed(&self, key: &str, sent: Result<(), OwnedFd>) {
        let mut runs = self.runs.lock().unwrap();
        let Some(run) = runs.get_mut(key) else { return };
        match sent {
            Ok(()) => {
                if let Err(e) = fs::write(run.dir.join(HANDED_OFF), b"") {
                    tracing::warn!(key, "recording a handoff: {e}");
                }
            }
            Err(listener) => (run.listener, run.handed) = (Some(listener), false),
        }
    }
}

/// The handoffs this worker serves, where its config names a socket.
pub(in crate::worker) fn start(opts: &WorkerConfig) -> Result<Option<Arc<Handoffs>>> {
    let Some(path) = &opts.handoff_socket else {
        return Ok(None);
    };
    let handoffs = Arc::new(Handoffs::default());
    serve(path, opts.handoff_gid, handoffs.clone()).map_err(err_ctx(format!(
        "serving the handoff socket {}",
        path.display()
    )))?;
    Ok(Some(handoffs))
}

/// Serve `handoffs` on `path`, reachable by `gid`'s members, on threads.
fn serve(path: &Path, gid: Option<u32>, handoffs: Arc<Handoffs>) -> io::Result<()> {
    let _ = fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    if let Some(gid) = gid {
        rustix::fs::chown(path, None, Some(rustix::fs::Gid::from_raw(gid)))?;
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
    tracing::info!(path = %path.display(), "serving producers' listeners");
    thread::Builder::new()
        .name("handoff".into())
        .spawn(move || {
            for conn in listener.incoming().flatten() {
                let handoffs = handoffs.clone();
                thread::spawn(move || answer(&conn, &handoffs));
            }
        })?;
    Ok(())
}

/// Answer one request on the handoff socket.
fn answer(conn: &UnixStream, handoffs: &Handoffs) {
    let _ = conn.set_read_timeout(Some(RECV_TIMEOUT));
    let Ok((call::Call::Handoff(req), _)) = framing::recv_call(conn) else {
        let _ = framing::send_error(conn, "expected a handoff request");
        return;
    };
    match handoffs.take(&req.token) {
        Err(e) => {
            tracing::warn!("refusing a handoff: {e}");
            let _ = framing::send_error(conn, e);
        }
        Ok((key, listener)) => {
            let empty = reply::Reply::Empty(Empty {});
            let sent = framing::send_reply(conn, empty, &[listener.as_raw_fd()]);
            tracing::info!(key, sent = sent.is_ok(), "handed a producer's listener off");
            handoffs.handed(&key, sent.map_err(|_| listener));
        }
    }
}

#[cfg(test)]
mod tests;
