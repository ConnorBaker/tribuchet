//! Worker-session losses of a dispatched build: which ones count, and
//! what happens to a build lost too often.
//!
//! A session's end is no evidence about the build. Two things are:
//! - Run evidence: a run of the build that started (Started, or Resumed)
//!   is known to have ended without an outcome: its worker is back
//!   without the build, says it no longer has it, or stages it afresh.
//!   That is settled by the dispatch after the loss: the worker taking
//!   the build back by key and resuming it costs nothing.
//! - Transport evidence, all else: the session died while the build was
//!   staged, before the worker answered its assignment, while its run
//!   was resumed, or while its finished result was delivered; the worker
//!   dropped the result (ResultGone); or the run's worker, not back
//!   within its grace or with another worker holding the build, may
//!   still run it. That says how the farm moves the build, not what the
//!   build does, so it never fails it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::metrics::Metrics;
use super::state::{Holding, HubState, Job, WorkerCaps};
use crate::proto::{DECLINE_EXIT_CODE, RunId, attach_event};

/// Genuine losses a build survives; the next fails it.
pub(super) const MAX_JOB_ATTEMPTS: u32 = 3;
/// Losses in transport a build survives, counted per build across
/// workers; the next declines it. In 12 days of one deployed hub's
/// journal no build could have lost more than 12 sessions on a resume
/// within 24 h of one hub process, even moving 3 times; within 48 h, 14.
pub(super) const MAX_TRANSPORT_LOSSES: u32 = 12;

/// What one dispatch established about the build's run.
#[derive(Default)]
pub(super) struct Dispatch {
    /// The assignment went out to the worker.
    pub(super) sent: bool,
    /// The worker asked for the build's inputs: it staged it afresh
    /// instead of resuming it.
    pub(super) staged: bool,
    /// How far a run the worker said it holds got: it started (Started)
    /// or the worker holds one (Resumed); it finished (as Resumed said, or
    /// its successful result arrived).
    pub(super) reached: Run,
    /// The worker said it holds a run of the build already (Resumed).
    pub(super) resumed: bool,
    /// The run it assigned.
    pub(super) run: Option<RunId>,
}

/// How far the build's run is known to have got, across dispatches.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Run {
    #[default]
    Unstarted,
    Started,
    Finished,
}

impl Dispatch {
    /// The run after this dispatch, `before` it: one staged afresh is
    /// a new run.
    pub(super) fn run(&self, before: Run) -> Run {
        if self.staged || self.reached > Run::Unstarted {
            self.reached
        } else {
            before
        }
    }
}

/// The loss of a build with the session of the worker that ran it. The
/// worker likely still runs it and re-registers shortly: until `until`,
/// the build is kept for it. A worker that said it no longer has the
/// build gets no grace; its loss is still the next dispatch's to settle.
pub(super) struct Loss {
    /// Name the worker registers with, the identity a reconnect keeps.
    pub(super) worker: String,
    /// Id of the session that died.
    pub(super) session: u64,
    pub(super) until: Instant,
    /// How far the build's run got, by what the dispatches so far said.
    pub(super) run: Run,
}

impl Loss {
    /// Within the grace and the worker is not back. Back means
    /// registered with its held keys: from then on those route the job.
    pub(super) fn awaited(
        &self,
        workers: &HashMap<u64, WorkerCaps>,
        holding: &HashMap<u64, Holding>,
    ) -> bool {
        Instant::now() < self.until && !self.back(workers, holding)
    }

    /// Its worker is back. Taken fresh, the build is held by none: that
    /// worker is back without it.
    pub(super) fn back(
        &self,
        workers: &HashMap<u64, WorkerCaps>,
        holding: &HashMap<u64, Holding>,
    ) -> bool {
        workers
            .iter()
            .any(|(id, c)| *id != self.session && c.name == self.worker && holding.contains_key(id))
    }
}

/// What a loss counts against.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Charge {
    /// Run evidence: MAX_JOB_ATTEMPTS, then the build fails.
    Genuine,
    /// Transport evidence: MAX_TRANSPORT_LOSSES, then the build is deferred.
    Transport,
}

/// Count the loss of a sent dispatch whose session died, or whose worker
/// said it no longer has the build (`gone`), on `worker`'s session (its
/// id and name). The job comes back with the loss pending, unless this
/// loss was one too many, in which case `count` has settled it.
pub(super) async fn lost(
    state: &HubState,
    mut job: Job,
    dispatch: &Dispatch,
    gone: bool,
    (session, worker): (u64, String),
) -> Option<Job> {
    let before = job.loss.as_ref().map_or(Run::Unstarted, |l| l.run);
    if dispatch.staged && before == Run::Started {
        // Taken back by key and staged afresh: the run before is gone.
        job = count(state, job, Charge::Genuine).await?;
    }
    let run = dispatch.run(before);
    let mut job = match run {
        // Its worker says it no longer has the run (Unlisted).
        Run::Started if gone => count(state, job, Charge::Genuine).await,
        // Its worker likely runs it still; the next dispatch settles that.
        Run::Started if dispatch.resumed => count(state, job, Charge::Transport).await,
        // The run may go on, or ended without an outcome: the next
        // dispatch settles it (take_job).
        Run::Started if dispatch.reached == Run::Started => Some(job),
        // Staged, the assignment unanswered, or the result's delivery cut.
        _ => count(state, job, Charge::Transport).await,
    }?;
    // A worker restart drops the session but not the build: keep
    // it for that worker until it re-registers, or the grace ends,
    // instead of running it a second time elsewhere. One that said
    // it no longer has it is waited for no longer, its run settled
    // here; its next dispatch starts it over.
    let (run, grace) = if gone {
        (Run::Unstarted, Duration::ZERO)
    } else {
        (run, state.worker_grace)
    };
    job.loss = Some(Loss {
        worker,
        session,
        until: Instant::now() + grace,
        run,
    });
    Some(job)
}

/// Count one loss against `charge`'s cap. The job comes back unless this
/// loss was one too many, in which case it is settled here.
pub(super) async fn count(state: &HubState, mut job: Job, charge: Charge) -> Option<Job> {
    let (n, cap, lost) = match charge {
        Charge::Genuine => (&mut job.attempts, MAX_JOB_ATTEMPTS, "without a resume"),
        Charge::Transport => (
            &mut job.transport_losses,
            MAX_TRANSPORT_LOSSES,
            "in transport",
        ),
    };
    *n += 1;
    let losses = *n;
    tracing::warn!(id = job.id, losses, "build lost {lost}");
    if losses <= cap {
        return Some(job);
    }
    if charge == Charge::Transport {
        return defer(state, job, losses).await;
    }
    let err = format!("build lost {losses} times without a worker resuming it; giving up");
    after_too_many_losses(state, job, err).await;
    None
}

/// The policy for a build whose run was lost more than MAX_JOB_ATTEMPTS
/// times (a build that kills its worker, e.g. OOM, or workers that die
/// under it). This is the only place it lives. It fails the build.
async fn after_too_many_losses(state: &HubState, job: Job, err: String) {
    tracing::warn!(id = job.id, "build failed: {err}");
    Metrics::inc(&state.metrics.failed);
    job.fail(state, "genuine-loss cap", None, err).await;
    state.finish(&job).await;
}

/// A build the farm lost in transport too often goes back to its
/// clients' Nix to build itself (DECLINE_EXIT_CODE), as one no worker
/// serves does: their output transfers dropped first. Not one with
/// network, which would run in the client host's own network, outside
/// every fod-network policy: it keeps waiting for a worker. So does one a
/// client's Nix would not build (canLocal false), which would fail it.
async fn defer(state: &HubState, job: Job, losses: u32) -> Option<Job> {
    if job.req.fixed_output || job.replay.can_local().contains(&false) {
        return Some(job);
    }
    state.shadow.deferred(&job);
    tracing::warn!(
        id = job.id,
        losses,
        "build lost in transport too often; declining it"
    );
    Metrics::inc(&state.metrics.declined);
    job.restart_outputs().await;
    let decline = attach_event::Event::ExitCode(DECLINE_EXIT_CODE);
    job.replay.publish(decline).await;
    state.finish(&job).await;
    None
}
