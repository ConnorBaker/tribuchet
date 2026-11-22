//! Producers (builder-rpc-v0, proto::producer) at the hub (DESIGN.md,
//! "Dynamic-derivation producers").
//!
//! - DD9: a producer's request carries its attach's `attach_id`, which is
//!   in the dedupe key, so another client's goal never joins its run. A
//!   request without one is refused: its attach relays nothing.
//! - DD6: a producer returns no scratch outputs and claims no scratch path.
//! - DD8: RelayStarted counts, and is acknowledged, only with a token of
//!   the run current now (`RelayRun::tokens`). A run lost is current no
//!   more: every loss and requeue ends it (`Replay::run_lost`).
//! - DD8a: once its attach said the run reached the client (RelayStarted,
//!   acknowledged), no fresh run of it follows a loss. The run may only be
//!   resumed. Anything else is `hub_cannot`'s answer: each client whose
//!   Nix said it builds it is declined (Nix rebuilds it in a fresh
//!   builder), the others fail; a client whose Nix did not say (no
//!   canBuildLocally) is one of the others there (OQ1's fallback (a)), and
//!   nowhere else. A loss before that point follows the loss policy. The
//!   acknowledgement and each loss take one lock (`Replay::relay`), so
//!   either the acknowledgement came first, and the loss settles the build
//!   so, or the loss did, and the run's tokens are acknowledged no more.
//!   The loss is decided at three points: a loss its worker says is of a
//!   started run (`losses::lost`, gone), settled there; a fresh take of
//!   the build (state/take.rs); and a worker staging afresh a build it was
//!   handed by key (relay.rs, Error::Relayed).
//! - R1: build.json's drvPath is logged and recorded, nothing more.

use std::collections::BTreeMap;
use std::sync::Arc;

use tonic::Streaming;

use super::losses;
use super::record::{Row, now_ms, val};
use super::state::{HubState, Job, Replay};
use crate::proto::{BuildMessage, BuildRequest, RelayReady, attach_event, build_message, producer};

/// `hub_cannot`'s point for a relayed producer whose run is lost.
pub(super) const RELAYED: &str = "producer relayed";

/// What the clients of a relayed producer whose run was lost are told,
/// those whose Nix cannot build it or did not say: a farm rule, not Nix's.
pub(super) const RELAYED_LOST: &str = "the producer's run was lost after it reached this \
    client's Nix, and Tribuchet never runs a producer twice behind one build; Nix builds it \
    again itself only where its build.json says it can (canBuildLocally)";

/// What a producer's request without an attach_id is told: its attach
/// predates producers, and relays none of its run (DD9).
pub(super) const NO_ATTACH_ID: &str = "a dynamic-derivation producer (builder-rpc-v0) needs an \
    attach that relays its run: this request carries no attach_id";

/// What a request saying its run reached the client is told when it is no
/// producer's: only a producer's run is relayed (DD8a), and attach says so
/// for no other.
pub(super) const NOT_RELAYED: &str = "relay_started on a build that is not a \
    dynamic-derivation producer's (builder-rpc-v0): only a producer's run reaches its client";

/// Whether `job` is a producer.
pub(super) fn is(job: &Job) -> bool {
    is_request(&job.req)
}

/// Whether `req` is a producer's.
pub(super) fn is_request(req: &BuildRequest) -> bool {
    producer(&req.required_features)
}

/// The scratch outputs `job` returns: a producer none (DD6), its outputs
/// being what it submits to the client's Nix itself.
pub(super) fn outputs(job: &Job) -> BTreeMap<String, String> {
    if is(job) {
        BTreeMap::new()
    } else {
        job.req.outputs.clone()
    }
}

/// The scratch paths a submission claims while in flight: a producer's
/// none, as it writes none (DD6), so no other goal ever waits on it.
pub(super) fn claims(req: &BuildRequest) -> Vec<String> {
    if is_request(req) {
        Vec::new()
    } else {
        req.outputs.values().cloned().collect()
    }
}

impl Replay {
    /// The run reached the client, as a resubmission after a hub restart
    /// says: from now on, no fresh run (DD8a).
    pub(super) fn relay_started(&self) {
        self.relay.lock().unwrap().started = true;
    }

    /// Whether the build's run reached its client (relay_started).
    pub(super) fn relayed(&self) -> bool {
        self.relay.lock().unwrap().started
    }

    /// A RelayStarted naming `token`: recorded if that is a token of the
    /// run current now. Whether it was.
    fn relay_started_by(&self, token: &[u8]) -> bool {
        // Decided under the clients' lock, taken before the relay's as a
        // join does: no loss reads a client's answer between the run
        // recorded reached and the answer switched (Replay::reached_when).
        self.reached_when(|| {
            let mut relay = self.relay.lock().unwrap();
            let current = relay.tokens.iter().any(|t| t == token);
            relay.started |= current;
            current
        })
    }

    /// The run current now was lost (or its build requeued): whether it
    /// had reached the client. It is current no more: a RelayStarted of
    /// it, a splice of a run gone, is not acknowledged after (DD8); one
    /// that reached the client was acknowledged already.
    pub(super) fn run_lost(&self) -> bool {
        let mut relay = self.relay.lock().unwrap();
        relay.tokens.clear();
        relay.started
    }
}

/// Read what a producer's attach sends after its submission: each
/// RelayStarted naming the run current now is recorded on `replay`, then
/// acknowledged to the attach (relay_started_ack), until the attach's
/// stream ends. One naming another run (a splice of a run gone) is not.
pub(super) async fn relay_reader(
    mut inbound: Streaming<BuildMessage>,
    replay: Arc<Replay>,
    key: String,
) {
    while let Ok(Some(m)) = inbound.message().await {
        let Some(build_message::Msg::RelayStarted(started)) = m.msg else {
            continue;
        };
        let token = started.handoff_token;
        if !replay.relay_started_by(&token) {
            tracing::info!(key, "RelayStarted of no current run; not acknowledged");
            continue;
        }
        tracing::info!(key, "producer's run reached its client");
        replay
            .publish(attach_event::Event::RelayStartedAck(token))
            .await;
    }
}

/// A fresh handoff token for one dispatch of a producer (DD11): 16 random
/// bytes, never logged. Empty for every other build.
pub(super) fn token(job: &Job) -> Vec<u8> {
    if !is(job) {
        return Vec::new();
    }
    let mut buf = vec![0u8; 16];
    getrandom::fill(&mut buf).expect("read system randomness");
    buf
}

/// Tell `job`'s attach where its run's listener is, once the worker
/// started the run (`fresh`: the run current now is this one) or resumed
/// it (one more token of the run current now).
pub(super) async fn relay_ready(job: &Job, token: &[u8], fresh: bool) {
    if token.is_empty() {
        return;
    }
    {
        let mut relay = job.replay.relay.lock().unwrap();
        if fresh {
            relay.tokens.clear();
        }
        relay.tokens.push(token.to_vec());
    }
    let ready = RelayReady {
        handoff_token: token.to_vec(),
        fresh,
    };
    job.replay
        .publish(attach_event::Event::RelayReady(ready))
        .await;
}

/// Whether a client is taken to build `req` itself, as its Nix said or
/// not: never one with network (§1, DD1b); one whose Nix does not say,
/// only before the run reached it (`relayed`, OQ1's fallback (a)).
pub(super) fn can_local(req: &BuildRequest, relayed: bool) -> bool {
    !req.fixed_output && req.can_local.unwrap_or(!relayed)
}

/// The loss of a producer's run, at `why`. Relayed, it is settled with no
/// fresh run behind the same attach (DD8a), declining only a client whose
/// Nix builds it, as reached (OQ1's fallback (a), Replay::reached_when), never a
/// fixed-output one (DD1b); else it is current no more (`run_lost`), for
/// the loss policy.
pub(super) async fn lost_relayed(state: &HubState, job: Job, why: &str) -> Option<Job> {
    if !job.replay.run_lost() {
        return Some(job);
    }
    tracing::warn!(
        id = job.id,
        why,
        "a relayed producer's run is lost; no fresh run"
    );
    losses::cannot(state, job, (RELAYED, true), RELAYED_LOST.to_owned()).await;
    None
}

/// The record's row joining the hub's build id to the client's `.drv`
/// (R1): only where build.json carried drvPath.
pub(super) fn drv_path_row(job: &Job) -> Option<Row> {
    let drv_path = job.req.drv_path.clone()?;
    Some(Row {
        table: "drv_path",
        values: vec![
            now_ms().into(),
            job.id.clone().into(),
            val(Some(job.key.clone())),
            drv_path.into(),
        ],
    })
}

#[cfg(test)]
#[path = "producer/tests.rs"]
mod tests;
