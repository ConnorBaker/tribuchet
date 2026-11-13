//! The hub's semantic core (SEMANTICS-7), pure: `step(hub, event) -> commands`.
//!
//! The farm refines local building: a client gets `Give o` only with the
//! outcome of one run of its key (L1, L5), `Defer` only if its Nix can build
//! the key (D2), and `Refuse` only if no known worker serves the key (static).
//! A lost run is no outcome (D1, D6). Policies enter as events the core applies
//! only if feasible, so the laws hold for every policy (L4).

use std::collections::{BTreeMap, BTreeSet};

pub type Key = u32;
pub type Client = u32;
pub type Worker = u32;
pub type Run = u32;
pub type Sys = u32;

/// U3 (open): answer `Built` once the hub holds the outputs (L2c), or at once.
pub const COMMIT_THEN_STREAM: bool = true;

/// A run's own outcome: the only source of `Give` (§1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Built,
    Failed(i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    Give(Outcome),
    Defer,
    Refuse,
}

/// How far a run is known to have got. It only moves forward, and once
/// Finished it is final.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Assigned,
    Started,
    Finished(Outcome),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Submit {
        key: Key,
        client: Client,
        sys: Sys,
        can_local: bool,
    },
    Leave {
        key: Key,
        client: Client,
    },
    /// A worker (re)connects and lists every run it holds and was not told
    /// to drop, so the farm's own cancel is never taken for a loss (§7).
    Register {
        worker: Worker,
        serves: BTreeSet<Sys>,
        held: Vec<(Run, Key, Phase)>,
    },
    /// What a run's worker says of it; `None`: it no longer holds the run.
    Ran {
        run: Run,
        now: Option<Phase>,
    },
    /// The run's outputs are durable at the hub (L2c).
    Fetched {
        run: Run,
    },
    /// Policy: start a new run of `key` on `worker`.
    Place {
        key: Key,
        worker: Worker,
    },
    /// Policy: let a build wait on a run a worker already holds.
    Adopt {
        key: Key,
        run: Run,
    },
    /// Policy: send `client` back to its own Nix (exit 222).
    Defer {
        key: Key,
        client: Client,
    },
    Refuse {
        key: Key,
    },
    /// Policy: stop waiting on `worker`'s runs, a cause unknown (D4, D5).
    Suspect {
        worker: Worker,
    },
    /// Policy: the farm ends `run` itself, a known cause (§7).
    Abort {
        run: Run,
    },
    /// The operator retires `worker` (U4); suspecting it is the policy's.
    /// The hub has no such command yet.
    #[cfg(test)]
    Decommission {
        worker: Worker,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Assign {
        worker: Worker,
        run: Run,
        key: Key,
    },
    /// Stop `run` and discard what it left: cancel or release.
    Drop {
        worker: Worker,
        run: Run,
    },
    Fetch {
        worker: Worker,
        run: Run,
    },
    /// `run` is the run whose outcome a `Give` is.
    Answer {
        key: Key,
        clients: BTreeSet<Client>,
        answer: Answer,
        run: Option<Run>,
    },
}

/// A key some client waits on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Build {
    pub sys: Sys,
    /// Each client, and whether its Nix can build the key (canLocal).
    pub clients: BTreeMap<Client, bool>,
    /// The run whose outcome will answer it.
    pub live: Option<Run>,
    /// A run of it ended with no known cause: run it alone (D4).
    pub lost: bool,
}

/// A run a worker holds, as far as the hub knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunOf {
    pub key: Key,
    pub worker: Worker,
    pub phase: Phase,
}

#[derive(Clone, Debug, Default)]
pub struct Hub {
    pub builds: BTreeMap<Key, Build>,
    pub runs: BTreeMap<Run, RunOf>,
    /// Known, not decommissioned, workers and the systems they serve.
    pub workers: BTreeMap<Worker, BTreeSet<Sys>>,
    next_run: Run,
}

impl Hub {
    /// After a restart the hub keeps its workers and run ids (on disk);
    /// reports and resubmissions rebuild the rest.
    #[cfg(test)]
    pub fn restarted(&self) -> Hub {
        Hub {
            workers: self.workers.clone(),
            next_run: self.next_run,
            ..Hub::default()
        }
    }

    fn live(&self, run: Run) -> Option<Key> {
        let key = self.runs.get(&run)?.key;
        (self.builds.get(&key)?.live == Some(run)).then_some(key)
    }

    fn answer(&mut self, key: Key, answer: Answer, run: Option<Run>, out: &mut Vec<Command>) {
        let b = self.builds.remove(&key).expect("a waiting build");
        let clients = b.clients.into_keys().collect();
        out.push(Command::Answer {
            key,
            clients,
            answer,
            run,
        });
    }

    fn detach(&mut self, key: Key, evidence: bool) {
        let b = self.builds.get_mut(&key).expect("a waiting build");
        (b.live, b.lost) = (None, b.lost || evidence);
    }

    fn leave(&mut self, key: Key, client: Client) {
        if let Some(b) = self.builds.get_mut(&key) {
            b.clients.remove(&client);
            if b.clients.is_empty() {
                self.builds.remove(&key);
            }
        }
    }

    fn drop_run(&mut self, run: Run, out: &mut Vec<Command>) {
        if let Some(key) = self.live(run) {
            self.detach(key, false);
        }
        let worker = self.runs.remove(&run).expect("a known run").worker;
        out.push(Command::Drop { worker, run });
    }

    fn finished(&mut self, key: Key, run: Run, o: Outcome, out: &mut Vec<Command>) {
        if o == Outcome::Built {
            let worker = self.runs[&run].worker;
            out.push(Command::Fetch { worker, run });
            if !COMMIT_THEN_STREAM {
                self.answer(key, Answer::Give(o), Some(run), out);
            }
        } else {
            self.answer(key, Answer::Give(o), Some(run), out);
            self.drop_run(run, out);
        }
    }

    /// The run's worker now holds it at `now`, or (`None`) no longer. A run
    /// known started that vanishes is isolation evidence (D4).
    fn ran(&mut self, run: Run, now: Option<Phase>, out: &mut Vec<Command>) {
        let key = self.live(run);
        let Some(r) = self.runs.get_mut(&run) else {
            return;
        };
        let Some(now) = now else {
            let started = r.phase == Phase::Started;
            self.runs.remove(&run);
            if let Some(key) = key {
                self.detach(key, started);
            }
            return;
        };
        let was = r.phase;
        if let Phase::Finished(_) = was {
            return; // A run has one outcome: a report against it changes nothing.
        }
        r.phase = was.max(now);
        if let (Some(key), Phase::Finished(o)) = (key, r.phase)
            && was != r.phase
        {
            self.finished(key, run, o, out);
        }
    }

    fn runs_on(&self, worker: Worker) -> Vec<Run> {
        let on = self.runs.iter().filter(|(_, r)| r.worker == worker);
        on.map(|(run, _)| *run).collect()
    }
}

/// The hub's one transition.
pub fn step(hub: &mut Hub, ev: Event) -> Vec<Command> {
    let mut out = Vec::new();
    match ev {
        Event::Submit {
            key,
            client,
            sys,
            can_local,
        } => {
            let b = hub.builds.entry(key).or_insert(Build {
                sys,
                clients: BTreeMap::new(),
                live: None,
                lost: false,
            });
            b.clients.insert(client, can_local);
        }
        Event::Leave { key, client } => hub.leave(key, client),
        Event::Register {
            worker,
            serves,
            held,
        } => {
            hub.workers.insert(worker, serves);
            for run in hub.runs_on(worker) {
                let now = held.iter().find(|h| h.0 == run).map(|h| h.2);
                hub.ran(run, now, &mut out);
            }
            for (run, key, phase) in held {
                let r = RunOf { key, worker, phase };
                hub.runs.entry(run).or_insert(r);
            }
        }
        Event::Ran { run, now } => hub.ran(run, now, &mut out),
        Event::Fetched { run } => {
            let built = Phase::Finished(Outcome::Built);
            if hub.runs.get(&run).is_some_and(|r| r.phase == built) {
                if let Some(key) = hub.live(run) {
                    hub.answer(key, Answer::Give(Outcome::Built), Some(run), &mut out);
                }
                hub.drop_run(run, &mut out);
            }
        }
        Event::Place { key, worker } => {
            let (Some(b), Some(serves)) = (hub.builds.get_mut(&key), hub.workers.get(&worker))
            else {
                return out;
            };
            if b.live.is_none() && serves.contains(&b.sys) {
                let run = hub.next_run;
                hub.next_run += 1;
                b.live = Some(run);
                let phase = Phase::Assigned;
                hub.runs.insert(run, RunOf { key, worker, phase });
                out.push(Command::Assign { worker, run, key });
            }
        }
        Event::Adopt { key, run } => {
            let Some(r) = hub.runs.get(&run).copied() else {
                return out;
            };
            let Some(b) = hub.builds.get_mut(&key) else {
                return out;
            };
            if r.key == key && b.live.is_none() {
                b.live = Some(run);
                if let Phase::Finished(o) = r.phase {
                    hub.finished(key, run, o, &mut out);
                }
            }
        }
        Event::Defer { key, client } => {
            let b = hub.builds.get(&key);
            if b.and_then(|b| b.clients.get(&client)) == Some(&true) {
                let clients = BTreeSet::from([client]);
                let (answer, run) = (Answer::Defer, None);
                out.push(Command::Answer {
                    key,
                    clients,
                    answer,
                    run,
                });
                hub.leave(key, client);
            }
        }
        Event::Refuse { key } => {
            if let Some(b) = hub.builds.get(&key)
                && !hub.workers.values().any(|s| s.contains(&b.sys))
            {
                hub.answer(key, Answer::Refuse, None, &mut out);
            }
        }
        Event::Suspect { worker } => {
            for run in hub.runs_on(worker) {
                if let Some(key) = hub.live(run) {
                    let finished = matches!(hub.runs[&run].phase, Phase::Finished(_));
                    hub.detach(key, !finished);
                }
            }
        }
        Event::Abort { run } => {
            if hub.runs.contains_key(&run) {
                hub.drop_run(run, &mut out);
            }
        }
        #[cfg(test)]
        Event::Decommission { worker } => _ = hub.workers.remove(&worker),
    }
    out
}

#[cfg(test)]
#[path = "semantics/tests.rs"]
mod tests;
