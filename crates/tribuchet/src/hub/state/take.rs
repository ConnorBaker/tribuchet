//! Taking a queued job: a fresh take, which a less full worker may be
//! left, and a resume of a build a worker holds from before.

use std::collections::HashSet;
use std::time::Instant;

use super::losses::{Charge, Run};
use super::{HubState, Job, WorkerCaps, losses};
use crate::proto::CancelBuild;

/// The builds a worker session holds from before, by dedupe key: the
/// keys it listed last (Register's, then each Heartbeat's
/// resumable_keys), after `beats` heartbeats, less those a dispatch took
/// since: each is honoured once. A key a later heartbeat leaves out is
/// no longer held, which `unlisted` tells a resumed build.
#[derive(Default)]
pub(in crate::hub) struct Holding {
    beats: u64,
    keys: HashSet<String>,
    taken: HashSet<String>,
}

impl Holding {
    pub(super) fn holds(&self, key: &str) -> bool {
        self.keys.contains(key) && !self.taken.contains(key)
    }
}

impl HubState {
    /// Worker session `worker` lists `keys` as held: at registration,
    /// or in a heartbeat (`beat`), which also counts it.
    pub(in crate::hub) fn hold(&self, worker: u64, keys: &[String], beat: bool) {
        let mut holding = self.holding.lock().unwrap();
        let h = holding.entry(worker).or_default();
        (h.beats, h.keys) = (h.beats + u64::from(beat), keys.iter().cloned().collect());
        h.taken.clear();
    }

    /// The heartbeats worker session `worker` has sent so far.
    pub(in crate::hub) fn heartbeats(&self, worker: u64) -> u64 {
        self.holding
            .lock()
            .unwrap()
            .get(&worker)
            .map_or(0, |h| h.beats)
    }

    /// Build `job` is dispatched to worker `name`: a run of it there that
    /// outlives every build of its key is cancelled (strays).
    pub(in crate::hub) fn ran(&self, name: &str, job: &Job) {
        let mut ran = self.ran.lock().unwrap();
        let on = ran.entry(name.to_owned()).or_default();
        on.insert(job.id.clone(), job.key.clone());
    }

    /// The runs worker session `worker` lists as held, of builds dispatched
    /// to its worker, that no build wants: none of their key is queued,
    /// dispatched or listed for dedupe. The hub ended their builds without
    /// a verdict from that run, or with one from another, maybe while the
    /// worker was away, and no timeout ends a run nobody cancels. Each is
    /// told at each listing until the worker lists it no more.
    pub(in crate::hub) async fn strays(&self, worker: u64) -> Vec<CancelBuild> {
        let key = |j: &Job| j.key.clone();
        let mut wanted: HashSet<String> = self.queue.lock().await.iter().map(key).collect();
        wanted.extend(self.inflight.lock().unwrap().by_key.keys().cloned());
        let dispatched = self.dispatched.lock().unwrap();
        wanted.extend(dispatched.values().map(|d| d.key.clone()));
        drop(dispatched);
        let held = (self.holding.lock().unwrap().get(&worker)).map(|h| h.keys.clone());
        let name = (self.worker_caps.lock().unwrap().get(&worker)).map(|c| c.name.clone());
        let mut ran = self.ran.lock().unwrap();
        let (Some(held), Some(on)) = (held, name.and_then(|n| ran.get_mut(&n))) else {
            return Vec::new();
        };
        on.retain(|_, k| held.contains(k) || wanted.contains(k));
        let stray = on.iter().filter(|(_, k)| !wanted.contains(*k));
        let cancel = |(id, k): (&String, &String)| CancelBuild {
            build_id: id.clone(),
            dedupe_key: k.clone(),
        };
        stray.map(cancel).collect()
    }

    /// Whether a heartbeat of worker session `worker` after its first
    /// `after` left `key` out: the worker no longer holds that build.
    pub(in crate::hub) fn unlisted(&self, worker: u64, key: &str, after: u64) -> bool {
        self.holding
            .lock()
            .unwrap()
            .get(&worker)
            .is_some_and(|h| h.beats > after && !h.keys.contains(key))
    }

    /// The first queued job `caps`'s worker can take, as worker session
    /// `asker`, if any: a build it passes over to a less full worker
    /// (placement::Rooms) stays queued, the session it is left to woken.
    pub(in crate::hub) async fn take_job(
        &self,
        caps: &WorkerCaps,
        asker: Option<u64>,
    ) -> Option<Job> {
        loop {
            // Read before the queue lock, so that nothing more is taken
            // under it; afresh on a rescan, which passes over again.
            let open = asker.map(|_| self.open_clientless()).unwrap_or_default();
            let mut rooms = asker.map(|id| self.placement.lock().unwrap().rooms_for(id, &open));
            let mut wanted = Vec::new();
            let (taken, left) = {
                let mut queue = self.queue.lock().await;
                let holding = self.holding.lock().unwrap();
                // A held key goes to its holder by take_job_by_key, this
                // worker included: a fresh take would count the loss its
                // Resumed is about to clear.
                let workers = self.worker_caps.lock().unwrap();
                let (now, mut left) = (Instant::now(), Vec::new());
                let pos = queue.iter_mut().position(|j| {
                    if !caps.serves(&j.req.system, &j.features) || j.kept(&workers, &holding) {
                        return false;
                    }
                    let (system, features, first) =
                        (&j.req.system, &j.features, j.deferred.is_none());
                    let serves = |id| workers.get(&id).is_some_and(|c| c.serves(system, features));
                    let demand = j.expected.demand;
                    let to = (rooms.as_mut())
                        .and_then(|r| r.passes(demand, &mut j.deferred, serves, now));
                    for s in rooms.iter_mut().flat_map(|r| r.wants.drain(..)) {
                        wanted.push((s, j.id.clone(), j.replay.clone()));
                    }
                    if let Some(to) = to.filter(|_| first) {
                        left.push((j.id.clone(), workers.get(&to).map(|c| c.name.clone())));
                    }
                    to.is_none()
                });
                (pos.and_then(|p| queue.remove(p)), left)
            };
            for (s, id, replay) in wanted {
                if let Some(s) = self.placement.lock().unwrap().sessions.get_mut(&s) {
                    s.wants.insert(id, replay);
                }
            }
            for (id, to) in &left {
                tracing::info!(
                    id,
                    to,
                    "leaving a build to a less full worker with an idle agent"
                );
            }
            if !left.is_empty() {
                self.notify.notify_waiters();
            }
            let mut job = taken?;
            // Abandoned while queued (every attach client gone): drop it
            // here, at the moment it would have occupied a build slot.
            if !job.replay.has_subscribers().await {
                tracing::info!(id = job.id, "dropping queued build: no client attached");
                self.finish(&job).await;
                continue;
            }
            // Its last worker did not resume it: this starts it over. A
            // run it started ended without an outcome if that worker is
            // back without it; one not back may run it still, a loss in
            // transport. A loss before or after the run was counted as it
            // happened (losses::lost).
            if let Some(loss) = &job.loss {
                self.given_up_on(loss);
            }
            if let Some(loss) = job.loss.take().filter(|l| l.run == Run::Started) {
                let back = {
                    let holding = self.holding.lock().unwrap();
                    loss.back(&self.worker_caps.lock().unwrap(), &holding)
                };
                let charge = if back {
                    Charge::Genuine
                } else {
                    Charge::Transport
                };
                let Some(j) = losses::count(self, job, charge).await else {
                    continue;
                };
                job = j;
            }
            return Some(job);
        }
    }

    /// Take a queued job `worker` can resume, regardless of its
    /// capacity. Each held key is honoured once.
    pub(in crate::hub) async fn take_job_by_key(&self, worker: u64) -> Option<Job> {
        let mut job = {
            let mut queue = self.queue.lock().await;
            let mut holding = self.holding.lock().unwrap();
            let held = holding.get_mut(&worker)?;
            let pos = queue.iter().position(|j| held.holds(&j.key))?;
            let job = queue.remove(pos)?;
            held.taken.insert(job.key.clone());
            job
        };
        // Held by a worker other than the one that lost it: a run that
        // one started may go on, a loss in transport. The same worker's
        // resume is settled by run_job.
        let name = self
            .worker_caps
            .lock()
            .unwrap()
            .get(&worker)
            .map(|c| c.name.clone());
        if let Some(loss) = (job.loss.as_ref()).filter(|r| Some(&r.worker) != name.as_ref()) {
            self.given_up_on(loss);
            if job.loss.take().is_some_and(|l| l.run == Run::Started) {
                job = losses::count(self, job, Charge::Transport).await?;
            }
        }
        Some(job)
    }
}
