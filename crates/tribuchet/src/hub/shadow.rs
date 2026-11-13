//! What the semantic core (semantics.rs) is told where today's code
//! decides, from the facts the wire now carries: logged, not acted on.

use std::fmt::Display;

use crate::proto::{HeldRun, RunId};

/// A worker's full report (Register, Heartbeat): the core settles by it each
/// run the hub knows on the worker, a run lost after it started being
/// isolation evidence (D4).
pub(super) fn report(worker: impl Display, held: &[HeldRun], lost: &[RunId]) {
    tracing::debug!(target: "tribuchet::shadow", %worker, ?held, ?lost, "worker report");
}
