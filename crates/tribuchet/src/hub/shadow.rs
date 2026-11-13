//! What the semantic core (semantics.rs) would do where today's code
//! decides, from the facts the wire now carries: logged, not acted on.

use std::fmt::Display;

use super::state::Job;
use crate::proto::{HeldRun, RunId};

/// Today's code answers a build's clients `today` at `point`. The core
/// answers no client Failed for a loss (D1, U1), Defers only a client whose
/// own Nix can build the key (`can_local`, each client's: D2), and keeps the
/// others waiting, or Refuses them if no known worker serves the key.
pub(super) fn answer(id: &str, point: &str, today: &str, can_local: &[bool]) {
    let defer = can_local.iter().filter(|c| **c).count();
    let wait = can_local.len() - defer;
    let core = "the core would Defer `defer` clients, and keep `wait` waiting or Refuse them";
    tracing::debug!(target: "tribuchet::shadow", id, point, today, defer, wait, core);
}

impl Job {
    pub(super) fn shadow(&self, point: &str, today: &str) {
        answer(&self.id, point, today, &self.replay.can_local());
    }
}

/// A worker's full report (Register, Heartbeat): the core settles by it each
/// run the hub knows on the worker, a run lost after it started being
/// isolation evidence (D4).
pub(super) fn report(worker: impl Display, held: &[HeldRun], lost: &[RunId]) {
    tracing::debug!(target: "tribuchet::shadow", %worker, ?held, ?lost, "worker report");
}
