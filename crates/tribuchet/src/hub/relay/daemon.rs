//! A build's wait for the hub's own nix-daemon. run(c, r) counts the
//! failures in a row of class c on request r that end the wait's
//! attempts: a failure of another class, or at another request, starts
//! it again, and so does a failure waited on, a drop the journal does
//! not explain among them. The build fails, naming the cause, once
//! run(c, r) reaches θ(c); otherwise it waits, its clients told once,
//! and no wait is a loss of its worker, for every failure waiting can
//! clear. θ is 1 for a refusal of the request, a handshake the hub
//! cannot speak, and a new connection the daemon closes before the
//! handshake as its journal says it refuses the hub's user; 2 for a
//! message it cannot read, which a connection left mid-answer gives once
//! (harmonia discards a connection whose operation failed, so the next
//! attempt is on another); 3 for a request the daemon drops as its
//! journal says a worker crashed (evidence.rs); and none for anything
//! else: a shortage, an interruption, an I/O error, a refused handshake,
//! a frame out of place, an error the hub does not know, a drop or close
//! with no such evidence, and no answer or progress in ANSWER, an
//! overloaded or silent daemon's included. Each wait counts its own run
//! and its own crash lines; a build has one per chunk of its query
//! (staging.rs, up to 8). So a request that crashes the daemon's worker
//! every time crashes it 3 times per chunk that asks for it, and one
//! crash line can count in several waits at once.

mod evidence;

use std::collections::HashSet;
use std::io::ErrorKind;
use std::time::Duration;

use harmonia_store_remote::{
    ConnectionPool, DaemonError, DaemonErrorKind, DaemonResult, PooledConnectionGuard,
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

pub(in crate::hub) use evidence::Evidence;

use crate::errors::{Error, Result, err_msg};
use crate::hub::state::{HubState, Job, Wait};
use crate::worker::unix_now;

/// Backoff between attempts.
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
/// How long the daemon may take to answer an operation, or to accept a
/// connection and answer its handshake (the pool's connect timeout,
/// HubState::new): past it the attempt is given up, told, and retried.
/// The wait for a free connection has no bound: past ANSWER the clients
/// are told, and the build keeps its place in the pool's queue.
pub(in crate::hub) const ANSWER: Duration = Duration::from_mins(1);
/// The handshake, the request every attempt makes on a new connection.
const CONNECT: &str = "the handshake";

/// What a failed operation on the daemon means for the build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::hub) enum Failure {
    /// The daemon refused the request: a fact about it, not an outage.
    Refused,
    /// Anything else.
    Retried,
    /// The daemon closed the connection mid-request (end of file, a
    /// reset or a broken pipe), as its worker for the connection does
    /// when it crashes. Counted as such only with its journal's word
    /// that a worker crashed.
    Dropped,
    /// The daemon closed a new connection before answering the
    /// handshake at all, as Nix's daemon does to a user it does not
    /// allow (daemon.cc:234-235), and when it cannot fork a worker or
    /// its worker cannot open the store (daemon.cc:380-401). Counted as
    /// such only with its journal's refusal of the hub's user.
    Closed,
    /// The daemon answered the handshake as no nix-daemon the hub can
    /// speak to: another magic number, or a protocol version older than
    /// the hub speaks.
    Unspoken,
    /// The daemon sent a message, or a value in one, that the hub
    /// cannot read (harmonia: InvalidData, whatever its words).
    Unknown,
}

/// Phrases of the daemon's refusals of a request, from Nix 7587788, the
/// hub's. It sends what an operation throws as STDERR_ERROR named
/// "Error" (daemon.cc:1236-1244, serialise.cc:565-568), so only the
/// message tells them apart, as for Nix's own client
/// (worker-protocol-connection.cc:259). HintFmt wraps a message's
/// arguments in colour codes; no phrase straddles one.
const REFUSALS: &[&str] = &[
    // InvalidPath, "path '%s' is not valid": local-store.cc:858, :1690,
    // store-api.cc:645, :665. BadStorePathName, "name '%s' is not
    // valid": path.cc:22-31.
    "is not valid",
    // BadStorePath: store-dir-config.cc:26-37, store-api.cc:141, :177.
    "is not in the Nix store",
];

/// What the failure `e` of an operation means for the build. Every
/// answer but a refusal is retried alike, so one condition is never
/// two classes: an I/O error on the daemon's disk is retried whether
/// SQLite reports it (SQLITE_IOERR as "disk I/O error", sqlite.cc:30-36)
/// or a system call does (EIO as "Input/output error", SystemError,
/// error.hh:354).
pub(in crate::hub) fn classify(e: &DaemonError) -> Failure {
    match e.kind() {
        DaemonErrorKind::IO(io)
            if matches!(
                io.kind(),
                ErrorKind::UnexpectedEof
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
            ) =>
        {
            // Harmonia names the step of the handshake that failed: the
            // first two are the hub's greeting and the daemon's first
            // answer (harmonia-store-remote client.rs, handshake).
            if matches!(e.fields().first(), Some(&("clientMagic" | "serverMagic"))) {
                Failure::Closed
            } else {
                Failure::Dropped
            }
        }
        DaemonErrorKind::WrongMagic(_) | DaemonErrorKind::UnsupportedVersion(_) => {
            Failure::Unspoken
        }
        // What harmonia's decoder cannot read: an enum's unknown value
        // (daemon_wire/logger.rs RawLogMessageType, num_enum), a string
        // not UTF-8, a length out of range (harmonia-protocol de/).
        DaemonErrorKind::IO(io) if io.kind() == ErrorKind::InvalidData => Failure::Unknown,
        DaemonErrorKind::Remote(r)
            if REFUSALS
                .iter()
                .any(|p| String::from_utf8_lossy(&r.msg).contains(p)) =>
        {
            Failure::Refused
        }
        _ => Failure::Retried,
    }
}

pub(in crate::hub) struct DaemonWait<'a> {
    job: &'a Job,
    state: &'a HubState,
    delay: Duration,
    failures: u32,
    /// run(c, r): the class and request of the failures in a row that
    /// end the attempts so far, and how many there are.
    run: Option<(Failure, String, u32)>,
    /// The journal's crash lines counted so far: an attempt reads the
    /// journal from the second it began, which can reach a crash an
    /// earlier attempt counted.
    crashes: HashSet<String>,
    /// Cancelled when the build is given up (clientless.rs).
    given_up: CancellationToken,
}

impl<'a> DaemonWait<'a> {
    pub(in crate::hub) fn new(job: &'a Job, state: &'a HubState) -> Self {
        Self {
            job,
            state,
            delay: RETRY_FIRST,
            failures: 0,
            run: None,
            crashes: HashSet::new(),
            given_up: state.given_up(&job.id),
        }
    }

    /// A connection, however long the wait for a free one takes, and
    /// when the attempt began (Unix seconds): that wait is not the
    /// attempt's, so it began at most ANSWER, the connect's bound, before
    /// the connection came.
    pub(in crate::hub) async fn connect(
        &mut self,
        pool: &ConnectionPool,
    ) -> Result<(PooledConnectionGuard, u64)> {
        loop {
            let began = unix_now();
            let acquire = pool.acquire();
            tokio::pin!(acquire);
            let acquired = match self.heed(timeout(ANSWER, &mut acquire)).await? {
                Ok(acquired) => acquired,
                Err(_) => {
                    let why = format!("no connection to it in {} s", ANSWER.as_secs());
                    self.tell(&why).await;
                    self.heed(acquire).await?
                }
            };
            let began = began.max(unix_now().saturating_sub(ANSWER.as_secs()));
            let (failure, why) = match acquired.map_err(|e| (classify(&e), e.to_string())) {
                Ok(conn) => return Ok((conn, began)),
                // No request has been made yet: what the daemon refuses
                // in the handshake is about itself.
                Err((Failure::Refused, why)) => (Failure::Retried, why),
                Err(failed) => failed,
            };
            let what = format!("connecting to the local nix-daemon: {why}");
            self.failed(failure, began, CONNECT, &what).await?;
        }
    }

    /// The outcome of `op`, an attempt bounded by ANSWER, its failure
    /// classed (or no answer: Retried), unless the build is given up
    /// meanwhile.
    pub(in crate::hub) async fn ask<T>(
        &self,
        op: impl Future<Output = DaemonResult<T>>,
    ) -> Result<Result<T, (Failure, String)>> {
        Ok(match self.heed(timeout(ANSWER, op)).await? {
            Ok(Ok(out)) => Ok(out),
            Ok(Err(e)) => Err((classify(&e), e.to_string())),
            Err(_) => Err((
                Failure::Retried,
                format!("no answer in {} s", ANSWER.as_secs()),
            )),
        })
    }

    /// The outcome of `op`, unless the build is given up meanwhile: it
    /// has no client and waiting work wants its slot (clientless.rs).
    async fn heed<T>(&self, op: impl Future<Output = T>) -> Result<T> {
        tokio::select! {
            out = op => Ok(out),
            () = self.given_up.cancelled() => Err(Error::GivenUp),
        }
    }

    /// The attempt that began at `began` (Unix seconds) failed at
    /// `request` with `what`, which `failure` classes: fail the build
    /// once the run reaches θ, else tell the clients, warn, and back off
    /// before the next attempt, unless the build is given up meanwhile.
    pub(in crate::hub) async fn failed(
        &mut self,
        failure: Failure,
        began: u64,
        request: &str,
        what: &str,
    ) -> Result<()> {
        let (failure, shown) = self.shown(failure, began, request).await?;
        let run = match &mut self.run {
            Some((f, r, n)) if *f == failure && r == request => {
                *n += 1;
                *n
            }
            run => {
                *run = Some((failure, request.to_owned(), 1));
                1
            }
        };
        if let Some(why) = verdict(failure, run, request, what, &shown) {
            return Err(err_msg(why));
        }
        self.failures += 1;
        self.tell(what).await;
        tracing::warn!(
            id = self.job.id,
            failures = self.failures,
            retry_in_s = self.delay.as_secs(),
            "{what}; retrying"
        );
        self.heed(sleep(self.delay)).await?;
        self.delay = (self.delay * 2).min(RETRY_MAX);
        Ok(())
    }

    /// `failure` as its evidence classes it: a close is one only with the
    /// journal's refusal of the hub's user, a drop only with a crash it
    /// tells of that no earlier attempt counted, each with that line;
    /// without it, either is waited on.
    async fn shown(
        &mut self,
        failure: Failure,
        began: u64,
        request: &str,
    ) -> Result<(Failure, String)> {
        let evidence = &self.state.daemon_evidence;
        let shown = match failure {
            Failure::Closed => self.heed(evidence.refused(&hub_user(), began)).await?,
            Failure::Dropped => self.heed(evidence.crashed(began, &self.crashes)).await?,
            _ => return Ok((failure, String::new())),
        };
        match shown {
            Ok(line) => {
                if failure == Failure::Dropped {
                    self.crashes.insert(line.clone());
                }
                Ok((failure, line))
            }
            Err(why) => {
                tracing::warn!(id = self.job.id, request, "{why}: waiting");
                Ok((Failure::Retried, String::new()))
            }
        }
    }

    async fn tell(&self, why: &str) {
        let note = format!("tribuchet: waiting for the hub's nix-daemon: {why}\n");
        self.job.replay.tell_waiting(Wait::Daemon, note).await;
    }
}

/// The hub's own user, as Nix's daemon names a peer: by its effective
/// uid's name, or the uid when it has none (authPeer, daemon.cc:218-221
/// at Nix 7587788).
pub(in crate::hub) fn hub_user() -> String {
    let uid = nix::unistd::geteuid();
    nix::unistd::User::from_uid(uid)
        .ok()
        .flatten()
        .map_or_else(|| uid.to_string(), |u| u.name)
}

/// What a build whose run of `n` failures of `failure` at `request`
/// reached θ fails with: `what` the last attempt saw, and `shown`, its
/// evidence. θ is the guard of each class's arm, 1 where there is none;
/// below θ, and for a class waited on however many, None.
fn verdict(failure: Failure, n: u32, request: &str, what: &str, shown: &str) -> Option<String> {
    Some(match failure {
        Failure::Refused => what.to_owned(),
        Failure::Unspoken => format!(
            "{what}: the hub cannot speak to this nix-daemon, and waiting does not change that"
        ),
        Failure::Unknown if n >= 2 => format!(
            "{what}: the hub's nix-daemon sent a message the hub does not know, again on \
             another connection, and waiting does not change that"
        ),
        Failure::Closed => format!(
            "{what}: the hub's nix-daemon closed the connection before answering the \
             handshake: it does not let the hub's user {} connect, as its journal says \
             ({shown}); allowed-users and trusted-users decide",
            hub_user()
        ),
        Failure::Dropped if n >= 3 => format!(
            "{what}: the hub's nix-daemon dropped {request} {n} times in a row, each time as its \
             journal tells of a worker that crashed (the last: {shown}): the request crashes \
             its worker every time; its journal (journalctl -u nix-daemon) says why"
        ),
        Failure::Unknown | Failure::Dropped | Failure::Retried => return None,
    })
}

#[cfg(test)]
mod tests;
