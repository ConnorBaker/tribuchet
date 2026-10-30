//! Worker-session losses of a dispatched build: which ones count, and
//! what happens to a build lost too often.
//!
//! A loss is settled by the dispatch after it. The worker that lost
//! the build taking it back by key and resuming it (`Resumed`) costs
//! the build nothing and is not counted as genuine. A dispatch that
//! starts it over makes the loss genuine: another worker takes it, or
//! the same one stages it afresh. An assignment the hub could not hand
//! over is no loss: that is the hub's failure (supervise_job). A loss
//! while its worker has the build resumed,
//! or before the worker it went back to says either, is a loss on a
//! resume, with a cap of its own. Such a loss that the next dispatch
//! starts over is genuine as well: the two caps bound different things.

use super::metrics::Metrics;
use super::state::{HubState, Job};
use crate::proto::attach_event;

/// Genuine losses a build survives; the next fails it.
pub(super) const MAX_JOB_ATTEMPTS: u32 = 3;
/// Losses on a resume a build survives, counted per build across
/// workers; the next fails it. In 12 days of one deployed hub's journal
/// no build could have lost more than 12 sessions on a resume within
/// 24 h of one hub process, even moving 3 times; within 48 h, 14.
pub(super) const MAX_RESUMED_LOSSES: u32 = 12;

/// What one dispatch established about the loss before it.
#[derive(Default)]
pub(super) struct Dispatch {
    /// The assignment went out to the worker.
    pub(super) sent: bool,
    /// The worker asked for the build's inputs: it staged it afresh
    /// instead of resuming it.
    pub(super) staged: bool,
    /// The worker said it already holds the build.
    pub(super) resumed: bool,
}

/// Count the loss of a sent dispatch whose session died. The job comes
/// back unless this loss was one too many, in which case
/// `after_too_many_losses` has settled it.
pub(super) async fn lost(state: &HubState, job: Job, dispatch: &Dispatch) -> Option<Job> {
    // The loss of this worker's last session, which this dispatch was
    // to settle.
    let pending = job.loss.is_some();
    if dispatch.staged && pending {
        // Taken back by key and staged afresh: genuine. That settles
        // the pending loss; the session's own loss replaces it
        // (supervise_job).
        return count(state, job, true).await;
    }
    if dispatch.resumed || pending {
        return count(state, job, false).await;
    }
    // A dead session alone is settled by the next dispatch.
    Some(job)
}

/// Count one loss, `genuine` or on a resume (its worker may well still
/// run the build, but a worker that loses its session on every resume
/// would keep it forever), against that count's cap. The job comes back
/// unless this loss was one too many, in which case
/// `after_too_many_losses` has settled it.
pub(super) async fn count(state: &HubState, mut job: Job, genuine: bool) -> Option<Job> {
    let (losses, cap) = if genuine {
        job.attempts += 1;
        (job.attempts, MAX_JOB_ATTEMPTS)
    } else {
        job.resumed_losses += 1;
        (job.resumed_losses, MAX_RESUMED_LOSSES)
    };
    let (lost, how) = if genuine {
        ("without a resume", "without a worker resuming it")
    } else {
        ("on a resume", "while its worker resumed it")
    };
    tracing::warn!(id = job.id, losses, "build lost {lost}");
    if losses <= cap {
        return Some(job);
    }
    let err = format!("build lost {losses} times {how}; giving up");
    after_too_many_losses(state, job, err).await;
    None
}

/// The policy for a build lost too often: more than MAX_JOB_ATTEMPTS
/// times with no worker resuming it (a build that kills its worker,
/// e.g. OOM, or workers that die under it), or more than
/// MAX_RESUMED_LOSSES times on a resume. This is the only
/// place it lives. It fails the build, for both.
async fn after_too_many_losses(state: &HubState, job: Job, err: String) {
    tracing::warn!(id = job.id, "build failed: {err}");
    Metrics::inc(&state.metrics.failed);
    job.replay.publish(attach_event::Event::Error(err)).await;
    state.finish(&job).await;
}
