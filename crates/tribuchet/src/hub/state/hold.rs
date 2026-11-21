//! Queued builds no connected worker serves and none is expected for: held
//! while a known worker serves them, else answered per client as a new
//! client would be (SEMANTICS-7 L11′).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{HubState, Job, Replay, Servability, Wait};
use crate::hub::known::refusal;
use crate::hub::losses;
use crate::hub::metrics::Metrics;
use crate::hub::producer;
use crate::hub::semantics as sem;
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
        let mut answered = HashMap::new();
        let unserved: Vec<Job> = {
            let mut queue = self.queue.lock().await;
            let (unserved, kept) = (queue.drain(..)).partition::<VecDeque<Job>, _>(|j| {
                match self.servability(&j.req.system, &j.features) {
                    Servability::Now => {}
                    Servability::ExpectedBy(at) => due = Some(due.map_or(at, |d| d.max(at))),
                    Servability::Never if !self.known.serves(&j.req.system, &j.features) => {
                        // A relayed producer's one attach is its every
                        // client (DD9): its request says (OQ1's fallback (a)).
                        let local = if j.replay.relayed() {
                            producer::can_local(&j.req, true)
                        } else {
                            !j.req.fixed_output
                        };
                        let decided = j.replay.close(local, |declined, waiting| {
                            self.shadow.deferred("no known worker", j, declined);
                            self.shadow.refused(j, waiting);
                            j.unlist();
                        });
                        answered.insert(j.id.clone(), decided);
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
                let decided = answered.remove(&job.id).unwrap_or_default();
                state.end_unservable(job, decided).await;
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

    /// `job`'s input `path` is not valid in the hub's store, as a query made
    /// once `since` clients had joined says. No fact about the build: its
    /// client's Nix made every input valid in its store, the hub's, before
    /// it submitted, and a client that joins does so again (SEMANTICS-7
    /// cut-over 10). Each client whose Nix builds it is declined (D2); the
    /// others wait, the build held out of placement until a client joins.
    pub(in crate::hub) async fn hold_for_input(
        self: &Arc<Self>,
        job: Job,
        path: &str,
        since: usize,
    ) {
        losses::decline(self, &job, "input missing").await;
        if job.replay.can_local().is_empty() {
            self.finish(&job).await;
            return;
        }
        let held = job.replay.hold(since);
        tracing::warn!(
            id = job.id,
            path,
            held,
            "an input is not in the hub's store"
        );
        let note = format!(
            "tribuchet: the hub's store lacks {path}, an input; waiting for a client to bring it\n"
        );
        (job.replay
            .publish(attach_event::Event::Log(note.into_bytes())))
        .await;
        self.requeue(job).await;
    }

    /// End `job`, which no connected or known worker serves, its answer
    /// decided as it left the queue: each client whose Nix builds it was
    /// `declined`, the others, `waiting`, are refused (L11′). A queued build
    /// has no half-sent output: a requeue restarted them.
    async fn end_unservable(&self, job: Job, (declined, waiting): (Vec<sem::Client>, usize)) {
        let (id, system) = (job.id.as_str(), job.req.system.as_str());
        if declined.is_empty() && waiting == 0 {
            tracing::info!(id, "dropping queued build: no client attached");
            self.finish(&job).await;
            return;
        }
        let features = job.features.join(",");
        if !declined.is_empty() {
            tracing::info!(id, system, features, waiting, "no known worker; declining");
            Metrics::inc(&self.metrics.declined);
        }
        if waiting > 0 {
            tracing::info!(id, system, features, "no known worker; refusing");
            let err = refusal(system, &job.features);
            job.replay.publish(attach_event::Event::Error(err)).await;
        }
        self.finish(&job).await;
    }
}
