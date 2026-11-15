//! Queued builds no connected worker serves: held, told why they wait.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{HubState, Job, Servability, Wait};

impl HubState {
    /// Hold queued jobs no connected worker serves, telling their
    /// clients, and re-run when the latest expected worker is due.
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

    /// Returns the latest deadline a queued job is still waiting for.
    /// A job no worker is expected for stays queued until one
    /// registers: its build is not the failure. Its client leaving
    /// drops it, as at any other point in the queue.
    async fn hold_unservable_now(&self) -> Option<Instant> {
        let mut due: Option<Instant> = None;
        let mut unservable = Vec::new();
        for j in self.queue.lock().await.iter() {
            match self.servability(&j.req.system, &j.features) {
                Servability::Now => {}
                Servability::ExpectedBy(at) => due = Some(due.map_or(at, |d| d.max(at))),
                Servability::Never => unservable.push((
                    j.id.clone(),
                    j.replay.clone(),
                    j.req.system.clone(),
                    j.features.join(","),
                )),
            }
        }
        // Outside the queue lock: both wait on the job's replay.
        let mut abandoned = HashSet::new();
        for (id, replay, system, features) in unservable {
            if !replay.has_subscribers().await {
                abandoned.insert(id);
                continue;
            }
            let note = format!(
                "tribuchet: no connected worker builds {system} [{features}]; waiting for one\n"
            );
            if replay.tell_waiting(Wait::Worker, note).await {
                tracing::warn!(
                    id,
                    system,
                    features,
                    "holding queued build: no capable worker"
                );
            }
        }
        if !abandoned.is_empty() {
            let gone: Vec<Job> = {
                let mut queue = self.queue.lock().await;
                let (gone, kept) = queue
                    .drain(..)
                    .partition::<VecDeque<Job>, _>(|j| abandoned.contains(&j.id));
                *queue = kept;
                gone.into()
            };
            for job in gone {
                tracing::info!(id = job.id, "dropping queued build: no client attached");
                self.finish(&job).await;
            }
        }
        due
    }
}
