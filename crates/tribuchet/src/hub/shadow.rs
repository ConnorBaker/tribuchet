//! Shadow mode: the semantic core (semantics.rs) is fed the hub's events and
//! policy choices where they happen, and each point where it would answer a
//! build's clients otherwise than the hub does, or refuses a choice the hub
//! made, is a warning (target tribuchet::shadow). It decides nothing. A panic
//! in it turns it off for the hub process. Not fed: a session's end (the core
//! needs no Down), required features (its Sys is the system alone), and
//! heartbeat lists (absent may be an assignment in flight; present adds
//! nothing the run's own messages and Register do not).

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};

use super::losses::Loss;
use super::semantics::{self as sem, Answer, Command, Event, Hub, Outcome, Phase};
use super::state::{HubState, Job};
use crate::proto::{HeldRun, Register, RunId, attach_event};

/// The core beside the hub; `None` once it panicked.
pub(super) struct Shadow(Mutex<Option<Mirror>>);

impl Default for Shadow {
    fn default() -> Self {
        Self(Mutex::new(Some(Mirror::default())))
    }
}

#[derive(Default)]
pub(super) struct Mirror {
    hub: Hub,
    /// Worker names and systems, as the core's numbers.
    names: HashMap<String, u32>,
    /// The hub's run -> the core's run it is, or stands for (Resumed).
    runs: HashMap<(u64, u64), sem::Run>,
    /// Runs assigned that no worker answered yet: the answer says whether
    /// the hub placed a new run or adopted one held.
    pending: HashMap<(u64, u64), (sem::Key, sem::Worker)>,
    /// The last number given a run first heard of from a worker: down from
    /// the top, the core's own count up.
    held: sem::Run,
    clients: sem::Client,
    /// Each waiting build's events, for the warnings.
    seen: HashMap<sem::Key, Vec<String>>,
}

/// A client reading a build; dropped once it stops (the core's Leave).
pub(super) struct Client {
    shadow: Option<Arc<Shadow>>,
    key: sem::Key,
    id: sem::Client,
    /// Whether its own Nix can build the key (canLocal).
    pub(super) can_local: bool,
}

impl Drop for Client {
    fn drop(&mut self) {
        let (key, client) = (self.key, self.id);
        if let Some(s) = self.shadow.take() {
            s.with(|m| _ = m.feed("leave", &[], vec![Event::Leave { key, client }]));
        }
    }
}

impl Client {
    pub(super) fn untracked(can_local: bool) -> Self {
        let (shadow, key, id) = (None, 0, 0);
        Self {
            shadow,
            key,
            id,
            can_local,
        }
    }
}

/// A dedupe key as the core's: its first 32 bits (a SHA-256 in hex).
fn key(k: &str) -> Option<sem::Key> {
    let key = k.get(..8).and_then(|h| u32::from_str_radix(h, 16).ok());
    if key.is_none() {
        tracing::warn!(target: "tribuchet::shadow", k, "shadow unmappable: not a dedupe key");
    }
    key
}

/// The core's run for `run`, as the hub numbered it; None from a peer
/// predating run ids.
fn id(point: &str, run: Option<&RunId>) -> Option<(u64, u64)> {
    if run.is_none() {
        tracing::warn!(target: "tribuchet::shadow", point, "shadow unmappable: no run id");
    }
    run.map(|r| (r.epoch, r.seq))
}

/// What the hub answers `job`'s clients still reading at a point.
fn today(job: &Job, a: Answer) -> Vec<(sem::Key, Answer)> {
    let n = job.replay.can_local().len();
    key(&job.key).map_or(vec![], |k| vec![(k, a); n.min(1)])
}

impl Mirror {
    fn name(&mut self, s: &str) -> u32 {
        let n = u32::try_from(self.names.len()).unwrap_or(u32::MAX);
        *self.names.entry(s.to_owned()).or_insert(n)
    }

    fn keys(&self, ev: &Event) -> Vec<sem::Key> {
        let on = |w| self.hub.runs.values().filter(move |r| r.worker == w);
        let of = |run| self.hub.runs.get(run).map(|r| r.key).into_iter().collect();
        match *ev {
            Event::Submit { key, .. } | Event::Leave { key, .. } | Event::Place { key, .. } => {
                vec![key]
            }
            Event::Adopt { key, .. } | Event::Defer { key, .. } | Event::Refuse { key } => {
                vec![key]
            }
            Event::Ran { run, .. } | Event::Fetched { run } | Event::Abort { run } => of(&run),
            Event::Register { worker, .. } | Event::Suspect { worker } => {
                on(worker).map(|r| r.key).collect()
            }
            #[cfg(test)]
            Event::Decommission { .. } => vec![],
        }
    }

    /// Step the core by `evs` at the hub's `point`, where it answers `today`.
    fn feed(&mut self, point: &str, today: &[(sem::Key, Answer)], evs: Vec<Event>) -> Vec<Command> {
        let mut out = Vec::new();
        let mut keys: Vec<_> = today.iter().map(|t| t.0).collect();
        for ev in evs {
            let k = self.keys(&ev);
            for k in &k {
                self.seen
                    .entry(*k)
                    .or_default()
                    .push(format!("{point}: {ev:?}"));
            }
            let choice = matches!(ev, Event::Place { .. } | Event::Abort { .. });
            let did = sem::step(&mut self.hub, ev.clone());
            tracing::debug!(target: "tribuchet::shadow", point, ?ev, ?did, "core step");
            if choice && did.is_empty() {
                self.diverged(point, &k, "the core refuses the hub's choice", &[], &did);
            }
            keys.extend(k);
            out.extend(did);
        }
        let said = |a: &[(sem::Key, Answer)]| {
            let mut s: Vec<_> = a.iter().map(|(k, a)| format!("{k:08x} {a:?}")).collect();
            s.sort();
            s
        };
        let core: Vec<_> = (out.iter())
            .filter_map(|c| match c {
                Command::Answer { key, answer, .. } => Some((*key, *answer)),
                _ => None,
            })
            .collect();
        if said(&core) != said(today) {
            keys.extend(core.iter().map(|c| c.0));
            self.diverged(point, &keys, "answers differ", today, &out);
        }
        self.seen.retain(|k, _| self.hub.builds.contains_key(k));
        self.runs.retain(|_, r| self.hub.runs.contains_key(r));
        out
    }

    fn diverged(
        &self,
        point: &str,
        keys: &[sem::Key],
        what: &str,
        live: &[(u32, Answer)],
        core: &[Command],
    ) {
        let mut keys = keys.to_vec();
        keys.dedup();
        for k in keys {
            let build = self.hub.builds.get(&k);
            let run = build.and_then(|b| b.live);
            let hub_run: Vec<_> = (self.runs.iter())
                .filter(|r| Some(*r.1) == run)
                .map(|r| r.0)
                .collect();
            let (key, seen) = (format!("{k:08x}"), self.seen.get(&k));
            tracing::warn!(target: "tribuchet::shadow", point, key, what, ?live, ?core, ?build,
                ?hub_run, ?seen, "shadow divergence");
        }
    }

    /// The core's run for the hub's `run`; at the worker's first word of a
    /// run assigned and not yet answered, the hub placed it (Place).
    fn bind(&mut self, point: &str, run: (u64, u64)) -> Option<sem::Run> {
        if let Some((key, worker)) = self.pending.remove(&run)
            && !self.runs.contains_key(&run)
            && let Some(Command::Assign { run: r, .. }) = self
                .feed(point, &[], vec![Event::Place { key, worker }])
                .first()
        {
            self.runs.insert(run, *r);
        }
        self.runs.get(&run).copied()
    }

    /// Feed the event `ev` makes of the hub's `run`, where it answers `today`.
    fn on_run(
        &mut self,
        point: &str,
        run: Option<(u64, u64)>,
        today: &[(u32, Answer)],
        ev: impl FnOnce(sem::Run) -> Event,
    ) {
        let ev = run.and_then(|r| self.bind(point, r)).map(ev);
        self.feed(point, today, ev.into_iter().collect());
    }
}

impl Shadow {
    /// Run `f` on the core, unless it panicked before.
    pub(super) fn with(&self, f: impl FnOnce(&mut Mirror)) {
        let mut on = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(m) = on.as_mut()
            && catch_unwind(AssertUnwindSafe(|| f(m))).is_err()
        {
            tracing::error!(target: "tribuchet::shadow", "the semantic core panicked; shadow off");
            *on = None;
        }
    }

    /// A client asks for `key`; it leaves as the returned Client drops.
    pub(super) fn submit(self: &Arc<Self>, k: &str, sys: &str, can_local: bool) -> Client {
        let mut c = Client::untracked(can_local);
        self.with(|m| {
            let (Some(key), sys) = (key(k), m.name(sys)) else {
                return;
            };
            m.clients += 1;
            (c.shadow, c.key, c.id) = (Some(self.clone()), key, m.clients);
            m.feed(
                "submit",
                &[],
                vec![Event::Submit {
                    key,
                    client: c.id,
                    sys,
                    can_local,
                }],
            );
        });
        c
    }

    /// No worker serves `key` and the hub declines the client: the core's
    /// Defer if its Nix can build the key, else its Refuse (policy order).
    pub(super) fn declined(&self, k: &str, sys: &str, can_local: bool) {
        self.with(|m| {
            let (Some(key), sys) = (key(k), m.name(sys)) else {
                return;
            };
            m.clients += 1;
            let client = m.clients;
            let choice = if can_local {
                Event::Defer { key, client }
            } else {
                Event::Refuse { key }
            };
            let evs = vec![
                Event::Submit {
                    key,
                    client,
                    sys,
                    can_local,
                },
                choice,
                Event::Leave { key, client },
            ];
            m.feed("no capable worker", &[(key, Answer::Defer)], evs);
        });
    }

    /// A worker registers with every run it holds; a run it lost is absent.
    pub(super) fn register(&self, r: &Register) {
        self.with(|m| {
            let worker = m.name(&r.worker_name);
            let serves = r.caps.iter().map(|c| m.name(&c.system)).collect();
            let held = (r.held.iter()).filter_map(|h| m.held(h)).collect();
            m.feed(
                "register",
                &[],
                vec![Event::Register {
                    worker,
                    serves,
                    held,
                }],
            );
        });
    }

    pub(super) fn assigned(&self, run: &RunId, k: &str, worker: &str) {
        self.with(|m| {
            let (Some(key), worker) = (key(k), m.name(worker)) else {
                return;
            };
            m.pending.retain(|_, p| p.0 != key);
            m.pending.insert((run.epoch, run.seq), (key, worker));
        });
    }

    /// The worker asks for the inputs of `run`: it stages it afresh.
    pub(super) fn need(&self, run: Option<&RunId>) {
        self.with(|m| _ = id("need", run).and_then(|r| m.bind("need", r)));
    }

    pub(super) fn started(&self, run: Option<&RunId>) {
        let now = Some(Phase::Started);
        self.with(|m| {
            m.on_run("started", id("started", run), &[], |run| Event::Ran {
                run,
                now,
            })
        });
    }

    /// The worker no longer holds `run` (ResultGone, or unlisted).
    pub(super) fn gone(&self, point: &str, run: Option<&RunId>) {
        self.with(|m| {
            m.on_run(point, id(point, run), &[], |run| Event::Ran {
                run,
                now: None,
            })
        });
    }

    pub(super) fn result(&self, job: &Job, run: Option<&RunId>, code: i32) {
        let o = if code == 0 {
            Outcome::Built
        } else {
            Outcome::Failed(code)
        };
        let today = if code == 0 {
            vec![]
        } else {
            today(job, Answer::Give(o))
        };
        let now = Some(Phase::Finished(o));
        self.with(|m| {
            m.on_run("result", id("result", run), &today, |run| Event::Ran {
                run,
                now,
            })
        });
    }

    /// The hub holds `run`'s outputs and answers Built.
    pub(super) fn fetched(&self, job: &Job, run: Option<&RunId>) {
        let today = today(job, Answer::Give(Outcome::Built));
        self.with(|m| {
            m.on_run("fetched", id("fetched", run), &today, |run| {
                Event::Fetched { run }
            })
        });
    }

    /// The worker answers assignment `assigned` with `held`, a run of `k` it
    /// holds already, which the assignment now stands for (Adopt).
    pub(super) fn resumed(&self, assigned: Option<&RunId>, held: Option<&RunId>, k: &str) {
        self.with(|m| {
            let a = assigned.map(|a| (a.epoch, a.seq));
            if let Some(a) = a {
                m.pending.remove(&a);
            }
            let (Some(h), Some(key)) = (id("resumed", held), key(k)) else {
                return;
            };
            let run = m.run(h);
            if let Some(a) = a {
                m.runs.insert(a, run);
            }
            let other = |m: &Mirror| (m.hub.builds.get(&key)).is_some_and(|b| b.live != Some(run));
            let mut evs = vec![Event::Ran {
                run,
                now: Some(Phase::Started),
            }];
            if other(m) {
                evs.insert(0, Event::Adopt { key, run });
            }
            let out = m.feed("resumed", &[], evs);
            if other(m) {
                m.diverged(
                    "resumed",
                    &[key],
                    "the core refuses the adoption",
                    &[],
                    &out,
                );
            }
        });
    }

    /// The hub cancels `run`, a cause it knows (Abort).
    pub(super) fn abort(&self, point: &str, run: Option<&RunId>) {
        self.with(|m| {
            let Some(r) = run.map(|r| (r.epoch, r.seq)) else {
                return;
            };
            m.pending.remove(&r);
            if let Some(&run) = m.runs.get(&r) {
                m.feed(point, &[], vec![Event::Abort { run }]);
            }
        });
    }

    /// The hub fails `job`'s clients at `point`, a lost run's or its own
    /// error, cancelling `run` if it names it.
    pub(super) fn failed(&self, job: &Job, point: &str, run: Option<&RunId>) {
        let today = today(job, Answer::Give(Outcome::Failed(1)));
        self.with(|m| {
            let run = run.and_then(|r| m.runs.get(&(r.epoch, r.seq)).copied());
            m.feed(
                point,
                &today,
                run.map(|run| Event::Abort { run }).into_iter().collect(),
            );
        });
    }

    /// The hub sends every client of `job` back to its own Nix (exit 222).
    pub(super) fn deferred(&self, job: &Job) {
        let n = job.replay.can_local().len();
        self.with(|m| {
            let Some(key) = key(&job.key) else { return };
            let clients = m
                .hub
                .builds
                .get(&key)
                .map(|b| b.clients.keys().copied().collect());
            let defer = |client| Event::Defer { key, client };
            let evs = clients
                .unwrap_or_else(Vec::new)
                .into_iter()
                .map(defer)
                .collect();
            m.feed("transport-loss cap", &vec![(key, Answer::Defer); n], evs);
        });
    }

    pub(super) fn suspect(&self, worker: &str) {
        self.with(|m| {
            let worker = m.name(worker);
            m.feed("grace over", &[], vec![Event::Suspect { worker }]);
        });
    }
}

impl Mirror {
    /// The core's number for the hub's `run`, a new one if unseen.
    fn run(&mut self, run: (u64, u64)) -> sem::Run {
        let held = &mut self.held;
        *self.runs.entry(run).or_insert_with(|| {
            *held = held.wrapping_sub(1);
            *held
        })
    }

    fn held(&mut self, h: &HeldRun) -> Option<(sem::Run, sem::Key, Phase)> {
        let (r, k) = (id("register", h.run.as_ref())?, key(&h.dedupe_key)?);
        let phase = match h.exit_code {
            Some(0) => Phase::Finished(Outcome::Built),
            Some(c) => Phase::Finished(Outcome::Failed(c)),
            None if h.started => Phase::Started,
            None => Phase::Assigned,
        };
        Some((self.run(r), k, phase))
    }
}

impl Job {
    /// Fail the build's clients with `err`, as the hub does at `point`
    /// (attach fails on an Error), cancelling `run` if it names it.
    pub(super) async fn fail(
        &self,
        state: &HubState,
        point: &str,
        run: Option<&RunId>,
        err: String,
    ) {
        state.shadow.failed(self, point, run);
        self.replay.publish(attach_event::Event::Error(err)).await;
    }
}

impl HubState {
    /// The hub stops waiting on `loss`'s worker, its session gone and the
    /// worker not back: the core's Suspect.
    pub(super) fn given_up_on(&self, loss: &Loss) {
        let holding = self.holding.lock().unwrap();
        let workers = self.worker_caps.lock().unwrap();
        if !workers.contains_key(&loss.session) && !loss.back(&workers, &holding) {
            self.shadow.suspect(&loss.worker);
        }
    }
}
