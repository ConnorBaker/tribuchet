//! Builds with no attach client. A build holds what placement charges
//! it: an agent (its slot) and what it is expected to use (its load).
//! Holding one costs something only when releasing that would change
//! where a build with a client runs, so it is kept for as long as no
//! such build wants it, and a client that comes back however late finds
//! it. A build whose clients have all been gone FLOOR is given up, the
//! one with the least work sunk (its run time) first, when
//! - work waits for a slot (queued at the hub, or staged on a worker
//!   beyond its slots): no more than the waiting builds their slots
//!   would serve, counted hub-wide; or
//! - placement ranked a build with a client away from a worker with an
//!   idle agent, or kept it from one, only for the load of that
//!   worker's builds with no client (placement::Rooms's wants): one of
//!   them per such build; or
//! - as its worker's CPU arbiter's leader, it keeps a build with a
//!   client frozen (the worker's heartbeat says which): it, whatever
//!   its run time.
//! One task decides, every CHECK (decide); a build given up
//! has its dispatch's token cancelled, which its supervision follows.
//! A build a worker holds from before the hub's restart that no dispatch
//! claims is never given up: it runs to completion, an exception to
//! this rule.

use std::collections::HashSet;
use std::mem;
use std::sync::atomic::Ordering;

use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::proto::Heartbeat;

/// How often the decider checks the dispatched builds for clients.
const CHECK: Duration = Duration::from_secs(2);
/// How long a build's clients have all been gone before it may be given
/// up: the hub cancelled a build 10 to 14 s after its last client left
/// before this rule, so no shorter absence was ever seen to end.
pub(in crate::hub) const FLOOR: Duration = Duration::from_secs(14);

/// A build dispatched to a worker session, as giving it up weighs it.
pub(in crate::hub) struct Dispatched {
    session: u64,
    key: String,
    replay: Arc<Replay>,
    /// The job's dedupe listing: given up, a later client starts it anew.
    listing: Arc<StdMutex<Option<Listing>>>,
    /// When its run started (Replay::run_since): the work it has sunk.
    started: Instant,
    /// Cancelled when it is given up: its slot is freeing.
    given_up: CancellationToken,
    /// Given up, then a later dispatch on the same worker took its key:
    /// the worker delivers to that one.
    superseded: bool,
}

impl Dispatched {
    /// If it may be given up at `now`: how long it has been without a
    /// client, and how long it has run.
    fn open(&self, now: Instant) -> Option<(Duration, Duration)> {
        let away = now.duration_since(self.replay.clientless()?);
        let open = !self.given_up.is_cancelled() && !self.superseded && away >= FLOOR;
        open.then(|| (away, now.duration_since(self.started)))
    }
}

/// Lists a dispatched build until its supervision ends.
pub(in crate::hub) struct Listed<'a> {
    state: &'a HubState,
    id: String,
}

impl Drop for Listed<'_> {
    fn drop(&mut self) {
        self.state.dispatched.lock().unwrap().remove(&self.id);
    }
}

impl HubState {
    /// List `job` as dispatched to worker session `session` from now. A
    /// job taken back by the worker that ran it (its loss still
    /// set) goes on with the same run. A given-up build of the same key
    /// there is superseded: the worker hands its run to this one.
    pub(in crate::hub) fn list_dispatch(self: &Arc<Self>, job: &Job, session: u64) -> Listed<'_> {
        if !self.deciding.swap(true, Ordering::Relaxed) {
            tokio::spawn(decide(self.clone()));
        }
        let started = job.replay.run_since(job.loss.is_some(), Instant::now());
        let mut map = self.dispatched.lock().unwrap();
        for d in map.values_mut() {
            if d.session == session && d.key == job.key && d.given_up.is_cancelled() {
                d.superseded = true;
            }
        }
        map.insert(
            job.id.clone(),
            Dispatched {
                session,
                key: job.key.clone(),
                replay: job.replay.clone(),
                listing: job.listing.clone(),
                started,
                given_up: CancellationToken::new(),
                superseded: false,
            },
        );
        Listed {
            state: self,
            id: job.id.clone(),
        }
    }

    /// Whether build `id`, given up, was superseded by a later dispatch
    /// of its key to the same worker (list_dispatch).
    pub(in crate::hub) fn superseded(&self, id: &str) -> bool {
        self.dispatched
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|d| d.superseded)
    }

    /// The dispatched builds that may be given up now.
    pub(in crate::hub) fn open_clientless(&self) -> HashSet<String> {
        let (now, map) = (Instant::now(), self.dispatched.lock().unwrap());
        let open = map.iter().filter(|(_, d)| d.open(now).is_some());
        open.map(|(id, _)| id.clone()).collect()
    }

    /// Record which builds worker session `session`'s CPU arbiter keeps
    /// frozen, and for which leader, as its heartbeat `h` says.
    pub(in crate::hub) fn record_frozen(&self, session: u64, h: &Heartbeat) {
        let mut placement = self.placement.lock().unwrap();
        if let Some(s) = placement.sessions.get_mut(&session) {
            s.frozen_by = h.leader_key.clone().map(|l| (l, h.frozen_keys.clone()));
        }
    }

    /// The token cancelled when build `id` is given up; one never
    /// cancelled if it is not dispatched.
    pub(in crate::hub) fn given_up(&self, id: &str) -> CancellationToken {
        self.dispatched
            .lock()
            .unwrap()
            .get(id)
            .map_or_else(CancellationToken::new, |d| d.given_up.clone())
    }

    /// Give up the dispatched builds that have had no client for FLOOR,
    /// while one slot more lets one more waiting build run, the one with
    /// the least work sunk (run time) first, then the one longest without
    /// a client.
    async fn give_up_clientless(&self) {
        let now = Instant::now();
        let replays: Vec<Arc<Replay>> = self
            .dispatched
            .lock()
            .unwrap()
            .values()
            .map(|d| d.replay.clone())
            .collect();
        for replay in replays {
            let has_client = replay.has_subscribers().await;
            replay.checked(has_client, now);
        }
        let queued = self.waiting_queued().await;
        // The builds with a client that each session's clientless load
        // kept from it: one build given up there for each.
        let wants: Vec<(u64, Arc<Replay>)> = (self.placement.lock().unwrap().sessions)
            .iter_mut()
            .flat_map(|(s, p)| p.wants.drain().map(|(_, r)| (*s, r)))
            .collect();
        let mut load_wanted: HashMap<u64, usize> = HashMap::new();
        for (s, replay) in wants {
            if replay.has_subscribers().await {
                *load_wanted.entry(s).or_default() += 1;
            }
        }
        let frozen_by: Vec<(u64, (String, Vec<String>))> = (self.placement.lock().unwrap())
            .sessions
            .iter()
            .filter_map(|(s, p)| Some((*s, p.frozen_by.clone()?)))
            .collect();
        let workers = self.worker_caps.lock().unwrap().clone();
        let map = self.dispatched.lock().unwrap();
        for (s, (leader, frozen)) in &frozen_by {
            let on = |d: &&Dispatched| d.session == *s && !d.superseded;
            let mut held = map.values().filter(on).filter(|d| frozen.contains(&d.key));
            let wanted = held.any(|d| d.replay.clientless().is_none());
            let lead = map.iter().find(|(_, d)| on(d) && d.key == *leader);
            if let Some((id, d)) = lead.filter(|(_, d)| wanted && d.open(now).is_some()) {
                d.given_up.cancel();
                drop(d.listing.lock().unwrap().take());
                tracing::info!(id, "a build with no client keeps one with a client frozen");
            }
        }
        let next = |load_wanted: &HashMap<u64, usize>| {
            let weighed: HashMap<&String, (Duration, Duration)> = map
                .iter()
                .filter_map(|(id, d)| d.open(now).map(|w| (id, w)))
                .collect();
            let sessions: HashSet<u64> = weighed.keys().map(|id| map[*id].session).collect();
            let wanted: HashMap<u64, usize> = sessions
                .into_iter()
                .filter_map(|s| {
                    let by_load = load_wanted.get(&s).is_some_and(|n| *n > 0);
                    let slots = one_more_runs(&map, &workers, &queued, s);
                    slots.or(by_load.then_some(0)).map(|n| (s, n))
                })
                .collect();
            weighed
                .into_iter()
                .filter_map(|(id, w)| wanted.get(&map[id].session).map(|n| (id, w, *n)))
                .min_by(|(a, (away_a, ran_a), _), (b, (away_b, ran_b), _)| {
                    ran_a.cmp(ran_b).then(away_b.cmp(away_a)).then(a.cmp(b))
                })
        };
        while let Some((id, (away, ran), slots_wanted)) = next(&load_wanted) {
            map[id].given_up.cancel();
            drop(map[id].listing.lock().unwrap().take());
            let load = load_wanted.get_mut(&map[id].session);
            let loads_wanted = load.map(|n| mem::replace(n, n.saturating_sub(1)));
            tracing::info!(
                id,
                clientless_s = away.as_secs(),
                ran_s = ran.as_secs(),
                slots_wanted,
                loads_wanted,
                "builds with a client want the slot or load of a build with no client"
            );
        }
    }

    /// For each queued build waiting for a slot, the worker sessions
    /// that serve it: those with a client, that no session with a free
    /// slot serves (one would take them), nor a worker's key holds, nor a
    /// loss keeps for a worker still awaited. The queue is not held while
    /// it asks the replays for clients.
    async fn waiting_queued(&self) -> Vec<Vec<u64>> {
        let candidates: Vec<(Arc<Replay>, Vec<u64>)> = {
            let queue = self.queue.lock().await;
            let holding = self.holding.lock().unwrap();
            let workers = self.worker_caps.lock().unwrap();
            let placement = self.placement.lock().unwrap();
            let free: Vec<&WorkerCaps> = placement
                .sessions
                .iter()
                .filter(|(_, s)| s.admission.lock().unwrap().free > 0)
                .filter_map(|(id, _)| workers.get(id))
                .collect();
            queue
                .iter()
                .filter(|j| {
                    !j.kept(&workers, &holding)
                        && !free.iter().any(|c| c.serves(&j.req.system, &j.features))
                })
                .map(|j| {
                    let serving = workers
                        .iter()
                        .filter(|(_, c)| c.serves(&j.req.system, &j.features))
                        .map(|(id, _)| *id)
                        .collect();
                    (j.replay.clone(), serving)
                })
                .collect()
        };
        let mut waiting = Vec::new();
        for (replay, serving) in candidates {
            if replay.has_subscribers().await {
                waiting.push(serving);
            }
        }
        waiting
    }
}

/// The hub's clientless decider, every CHECK.
async fn decide(state: Arc<HubState>) {
    let mut tick = interval(CHECK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        state.give_up_clientless().await;
    }
}

/// How many waiting builds the slots already given up leave without a
/// slot, if one slot more freed on worker session `s` lets one more of
/// them run, else None. Builds staged on a worker take its slots first;
/// the queued ones (`queued`, the sessions serving each) take the rest,
/// as a maximum matching. A session gone: Some(0), the build holds no
/// slot.
fn one_more_runs(
    map: &HashMap<String, Dispatched>,
    workers: &HashMap<u64, WorkerCaps>,
    queued: &[Vec<u64>],
    s: u64,
) -> Option<usize> {
    if !workers.contains_key(&s) {
        return Some(0);
    }
    let mut free: HashMap<u64, usize> = HashMap::new();
    let mut staged_left = 0;
    for (t, caps) in workers {
        let freeing = map
            .values()
            .filter(|d| d.session == *t && d.given_up.is_cancelled() && !d.superseded)
            .count();
        let staged = waiting_on(map, *t, caps.max_jobs);
        free.insert(*t, freeing.saturating_sub(staged));
        if *t == s {
            staged_left = staged.saturating_sub(freeing);
        }
    }
    if staged_left > 0 {
        return Some(staged_left);
    }
    let now = matching(queued, &free);
    *free.entry(s).or_default() += 1;
    (matching(queued, &free) > now).then_some(queued.len() - now)
}

/// How many builds dispatched to worker session `s` wait there for one
/// of its `slots` with a client: those beyond the slots that builds
/// take there (a worker with every slot busy stages one build more,
/// worker/session.rs declare_capacity), as many as had a client at
/// their latest check. Such a build left the queue, but wants the slot
/// of a build with no client as much as a queued one does.
fn waiting_on(map: &HashMap<String, Dispatched>, s: u64, slots: u32) -> usize {
    let (on, with_client) = map
        .values()
        .filter(|d| d.session == s && !d.superseded)
        .fold((0usize, 0usize), |(n, c), d| {
            (n + 1, c + usize::from(d.replay.clientless().is_none()))
        });
    on.saturating_sub(usize::try_from(slots).unwrap_or(usize::MAX))
        .min(with_client)
}

/// The size of a maximum matching of the queued builds (each the
/// sessions serving it) to `free` slots per session.
fn matching(queued: &[Vec<u64>], free: &HashMap<u64, usize>) -> usize {
    let mut on: Vec<Option<u64>> = vec![None; queued.len()];
    (0..queued.len())
        .filter(|&b| augment(b, queued, free, &mut on, &mut HashSet::new()))
        .count()
}

/// Place build `b` on a session with a free slot, moving builds placed
/// before it if that makes room (an augmenting path); each session is
/// tried once per path.
fn augment(
    b: usize,
    queued: &[Vec<u64>],
    free: &HashMap<u64, usize>,
    on: &mut [Option<u64>],
    tried: &mut HashSet<u64>,
) -> bool {
    for &t in &queued[b] {
        if !tried.insert(t) {
            continue;
        }
        let load = on.iter().filter(|&&x| x == Some(t)).count();
        if load < free.get(&t).copied().unwrap_or(0) {
            on[b] = Some(t);
            return true;
        }
        for o in 0..queued.len() {
            if on[o] == Some(t) && augment(o, queued, free, on, tried) {
                on[b] = Some(t);
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
#[path = "clientless/tests.rs"]
mod tests;
