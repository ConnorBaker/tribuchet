//! Queued builds no connected worker serves and none is expected for: held
//! while a known worker serves them, else answered per client as a new
//! client would be (SEMANTICS-7 L11′).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{HubState, Job, Replay, Servability, Wait};
use crate::hub::known::refusal;
use crate::hub::metrics::Metrics;
use crate::proto::attach_event;

/// A queued build no connected worker serves, none expected back, that a
/// known worker serves: it may yet come back.
struct Unservable {
    id: String,
    replay: Arc<Replay>,
    system: String,
    features: String,
}

impl HubState {
    /// Hold or answer queued jobs no connected worker serves, and re-run
    /// when the latest expected worker is due.
    pub(in crate::hub) async fn hold_unservable(self: &Arc<Self>) {
        let Some(mut due) = self.hold_unservable_now().await else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep_until((due + Duration::from_secs(1)).into()).await;
                match state.hold_unservable_now().await {
                    Some(next) => due = next,
                    None => break,
                }
            }
        });
    }

    /// Returns the latest deadline a queued job is still waiting for. A
    /// job a known worker serves stays queued, its clients told why they
    /// wait; one with no client leaves the queue and ends. One that no
    /// known worker serves is taken out and unlisted at the instant that
    /// is decided, so no client joins it after, and ends in a task of its
    /// own, its clients answered (L11′, cut-over 7).
    async fn hold_unservable_now(self: &Arc<Self>) -> Option<Instant> {
        let mut due: Option<Instant> = None;
        let mut unservable = Vec::new();
        let unserved: Vec<Job> = {
            let mut queue = self.queue.lock().await;
            let (unserved, kept) = (queue.drain(..)).partition::<VecDeque<Job>, _>(|j| {
                match self.servability(&j.req.system, &j.features) {
                    Servability::Now => {}
                    Servability::ExpectedBy(at) => due = Some(due.map_or(at, |d| d.max(at))),
                    Servability::Never if !self.known.serves(&j.req.system, &j.features) => {
                        j.unlist();
                        return true;
                    }
                    Servability::Never => unservable.push(Unservable {
                        id: j.id.clone(),
                        replay: j.replay.clone(),
                        system: j.req.system.clone(),
                        features: j.features.join(","),
                    }),
                }
                false
            });
            *queue = kept;
            unserved.into()
        };
        // Before any await: out of the queue, each must end even if the
        // caller is dropped.
        let state = self.clone();
        let ended = tokio::spawn(async move {
            for job in unserved {
                state.end_unservable(job).await;
            }
        });
        // Outside the queue lock: both wait on the job's replay.
        let mut ending = HashSet::new();
        for u in unservable {
            if !u.replay.has_subscribers().await {
                ending.insert(u.id);
                continue;
            }
            let (system, features) = (u.system, u.features);
            let note = format!(
                "tribuchet: no connected worker builds {system} [{features}]; waiting for one\n"
            );
            if u.replay.tell_waiting(Wait::Worker, note).await {
                let id = u.id;
                tracing::warn!(
                    id,
                    system,
                    features,
                    "holding queued build: no capable worker"
                );
            }
        }
        if !ending.is_empty() {
            let gone: Vec<Job> = {
                let mut queue = self.queue.lock().await;
                let (gone, kept) =
                    (queue.drain(..)).partition::<VecDeque<Job>, _>(|j| ending.contains(&j.id));
                *queue = kept;
                gone.into()
            };
            for job in gone {
                tracing::info!(id = job.id, "dropping queued build: no client attached");
                self.finish(&job).await;
            }
        }
        if let Err(e) = ended.await {
            tracing::error!("ending unservable builds: {e}");
        }
        due
    }

    /// End `job`, which no connected or known worker serves: each client
    /// whose Nix builds it is declined, the others refused (L11′).
    async fn end_unservable(&self, job: Job) {
        let (id, system) = (job.id.as_str(), job.req.system.as_str());
        if !job.replay.has_subscribers().await {
            tracing::info!(id, "dropping queued build: no client attached");
            self.finish(&job).await;
            return;
        }
        let features = job.features.join(",");
        if !job.req.fixed_output && job.replay.can_local().contains(&true) {
            job.restart_outputs().await;
            let (declined, waiting) = job.replay.decline_local().await;
            self.shadow.deferred("no known worker", &job, &declined);
            tracing::info!(id, system, features, waiting, "no known worker; declining");
            if !declined.is_empty() {
                Metrics::inc(&self.metrics.declined);
            }
        }
        if !job.replay.can_local().is_empty() {
            tracing::info!(id, system, features, "no known worker; refusing");
            self.shadow.refused(&job);
            let err = refusal(system, &job.features);
            job.replay.publish(attach_event::Event::Error(err)).await;
        }
        self.finish(&job).await;
    }
}
