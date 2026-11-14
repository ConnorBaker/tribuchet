//! Where the hub feeds the core: one entry point per event or policy choice.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use super::{CLIENTS, Client, Mirror, Shadow, Sys, id, key, serves, today};
use crate::hub::losses::Loss;
use crate::hub::semantics::{Answer, Event, Outcome, Phase};
use crate::hub::state::{HubState, Job};
use crate::proto::{Register, RunId, attach_event};

impl Shadow {
    /// A client asks for `key`; it leaves as the returned Client drops.
    pub(in crate::hub) fn submit(self: &Arc<Self>, k: &str, sys: Sys, can_local: bool) -> Client {
        let mut c = Client::untracked(can_local);
        let Some(key) = key(k) else { return c };
        (c.key, c.id) = (key, CLIENTS.fetch_add(1, Relaxed) + 1);
        self.with(|m| {
            let sys = m.system(sys.0, sys.1);
            c.shadow = Some(self.clone());
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

    /// No worker serves `key`, and the hub defers the client if its Nix can
    /// build the key, else refuses it (L11): the core's Defer or Refuse.
    pub(in crate::hub) fn declined(&self, k: &str, sys: Sys, can_local: bool) {
        self.with(|m| {
            let (Some(key), sys) = (key(k), m.system(sys.0, sys.1)) else {
                return;
            };
            let client = CLIENTS.fetch_add(1, Relaxed) + 1;
            let (choice, answer) = if can_local {
                (Event::Defer { key, client }, Answer::Defer)
            } else {
                (Event::Refuse { key }, Answer::Refuse)
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
            m.feed("no capable worker", &[(key, answer)], evs);
        });
    }

    /// A worker registers with every run it holds; a run it lost is absent.
    pub(in crate::hub) fn register(&self, r: &Register) {
        self.with(|m| {
            let worker = m.name(&r.worker_name);
            m.caps.insert(worker, r.caps.clone());
            let on = |(s, (system, f)): (&u32, &(String, Vec<String>))| {
                serves(&r.caps, system, f).then_some(*s)
            };
            let serves = m.systems.iter().filter_map(on).collect();
            let held = (r.held.iter()).filter_map(|h| m.held(h)).collect();
            let mut evs = m.lost(&r.lost);
            evs.push(Event::Register {
                worker,
                serves,
                held,
            });
            m.feed("register", &[], evs);
        });
    }

    /// A worker's heartbeat lists the runs it `lost` (Mirror::lost).
    pub(in crate::hub) fn lost(&self, lost: &[RunId]) {
        if !lost.is_empty() {
            self.with(|m| {
                let evs = m.lost(lost);
                m.feed("lost", &[], evs);
            });
        }
    }

    pub(in crate::hub) fn assigned(&self, run: &RunId, k: &str, worker: &str) {
        self.with(|m| {
            let (Some(key), worker) = (key(k), m.name(worker)) else {
                return;
            };
            // With no build waiting, no Place can follow it.
            if !m.hub.builds.contains_key(&key) {
                return;
            }
            let run = (run.epoch, run.seq);
            if let Some(old) = m.pending_of.insert(key, run) {
                m.pending.remove(&old);
            }
            m.pending.insert(run, (key, worker));
        });
    }

    /// The worker asks for the inputs of `run`: it stages it afresh.
    pub(in crate::hub) fn need(&self, run: Option<&RunId>) {
        self.with(|m| _ = id("need", run).and_then(|r| m.bind("need", r)));
    }

    pub(in crate::hub) fn started(&self, run: Option<&RunId>) {
        let now = Some(Phase::Started);
        self.with(|m| {
            m.on_run("started", id("started", run), &[], |run| Event::Ran {
                run,
                now,
            })
        });
    }

    /// The worker no longer holds `run` (ResultGone, or unlisted).
    pub(in crate::hub) fn gone(&self, point: &str, run: Option<&RunId>) {
        self.with(|m| {
            m.on_run(point, id(point, run), &[], |run| Event::Ran {
                run,
                now: None,
            })
        });
    }

    pub(in crate::hub) fn result(&self, job: &Job, run: Option<&RunId>, code: i32) {
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
    pub(in crate::hub) fn fetched(&self, job: &Job, run: Option<&RunId>) {
        let today = today(job, Answer::Give(Outcome::Built));
        self.with(|m| {
            m.on_run("fetched", id("fetched", run), &today, |run| {
                Event::Fetched { run }
            })
        });
    }

    /// The worker answers assignment `assigned` with `held`, a run of `k` it
    /// holds already, which the assignment now stands for (Adopt).
    pub(in crate::hub) fn resumed(&self, assigned: Option<&RunId>, held: Option<&RunId>, k: &str) {
        self.with(|m| {
            let a = assigned.map(|a| (a.epoch, a.seq));
            if let Some(a) = a {
                m.answered(a);
            }
            let (Some(h), Some(key)) = (id("resumed", held), key(k)) else {
                return;
            };
            let run = m.run(h);
            if let Some(a) = a {
                m.link(a, run);
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
    pub(in crate::hub) fn abort(&self, point: &str, run: Option<&RunId>) {
        self.with(|m| {
            let Some(r) = run.map(|r| (r.epoch, r.seq)) else {
                return;
            };
            m.answered(r);
            if let Some(&run) = m.runs.get(&r) {
                m.feed(point, &[], vec![Event::Abort { run }]);
            }
        });
    }

    /// The hub fails `job`'s clients at `point`, a lost run's or its own
    /// error, cancelling `run` if it names it.
    pub(in crate::hub) fn failed(&self, job: &Job, point: &str, run: Option<&RunId>) {
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
    pub(in crate::hub) fn deferred(&self, job: &Job) {
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

    pub(in crate::hub) fn suspect(&self, worker: &str) {
        self.with(|m| {
            let worker = m.name(worker);
            m.feed("grace over", &[], vec![Event::Suspect { worker }]);
        });
    }
}

impl Job {
    /// Fail the build's clients with `err`, as the hub does at `point`
    /// (attach fails on an Error), cancelling `run` if it names it.
    pub(in crate::hub) async fn fail(
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
    pub(in crate::hub) fn given_up_on(&self, loss: &Loss) {
        let holding = self.holding.lock().unwrap();
        let workers = self.worker_caps.lock().unwrap();
        if !workers.contains_key(&loss.session) && !loss.back(&workers, &holding) {
            self.shadow.suspect(&loss.worker);
        }
    }
}
