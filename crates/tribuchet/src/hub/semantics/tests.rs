//! The laws of SEMANTICS-7 after every step of random histories. The world
//! holds the truth (each run's key and outcome, the workers the farm knows);
//! the hub sees only events, and policy choices are random (L4).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use proptest::arbitrary::any;
use proptest::collection::vec;
use proptest::strategy::{Just, Strategy};
use proptest::test_runner::{Config, TestRunner};
use proptest::{prop_assert, prop_oneof};

use super::*;

#[derive(Clone, Debug)]
enum Act {
    Submit(Key, Client, bool),
    Leave(Key, Client),
    Connect(usize),
    Down(usize),
    Crash(usize),
    Deliver(usize),
    Start(usize, usize),
    Finish(usize, usize, bool),
    /// The worker discards a finished or dropped run.
    Reap(usize, usize),
    /// A worker's message about a run arrives again (an old stream, a retry).
    Replay(usize),
    Fetched(usize),
    /// A protocol-breaking worker: any run it holds is "fetched".
    BogusFetched(usize, usize),
    /// A protocol-breaking worker: a finished run's other outcome.
    Contradict(usize),
    /// Every stream ends; workers re-register and clients resubmit.
    HubRestart,
    Place(Key, usize),
    /// Adopt the i-th held run, as `key` (`key` ≥ 4: the run's own key).
    Adopt(usize, Key),
    Abort(usize),
    Defer(Key, Client),
    Refuse(Key),
    Suspect(usize),
    Decommission(usize),
}

/// A worker: its stream, what reached it, and the runs it holds (with
/// whether a `Drop` reached each).
#[derive(Default)]
struct Wk {
    up: bool,
    inbox: Vec<Command>,
    runs: BTreeMap<Run, (Key, Phase, bool)>,
}

#[derive(Default)]
struct World {
    hub: Hub,
    wk: [Wk; 3],
    /// Every run assigned: its key, worker and, once it has one, outcome.
    ran: BTreeMap<Run, (Key, usize, Option<Outcome>)>,
    /// Each waiting client: its canLocal, and the hub it last asked.
    open: BTreeMap<(Key, Client), (bool, usize)>,
    hubs: usize,
    /// Keys a run of which ended with no known cause (D4).
    evidence: BTreeSet<Key>,
    /// Finished runs whose outcome reached the hub.
    told: BTreeSet<Run>,
    fetch: BTreeSet<Run>,
    fetched: BTreeSet<Run>,
    /// Workers registered and not decommissioned since, with what they serve.
    known: BTreeMap<usize, BTreeSet<Sys>>,
    said: Vec<(Run, Option<Phase>)>,
    /// Answers seen: Give Failed, Give Built, Defer, Refuse; then isolation.
    answers: [usize; 5],
    /// The outcome the hub first knew each run finished with.
    finals: BTreeMap<Run, Outcome>,
}

fn serves(w: usize) -> BTreeSet<Sys> {
    [vec![0], vec![1], vec![0, 1]][w].iter().copied().collect()
}

fn nth(w: &Wk, i: usize, want: impl Fn(Phase, bool) -> bool) -> Option<Run> {
    let rs: Vec<Run> = (w.runs.iter())
        .filter(|(_, (_, p, d))| want(*p, *d))
        .map(|(r, _)| *r)
        .collect();
    (!rs.is_empty()).then(|| rs[i % rs.len()])
}

impl World {
    fn send(&mut self, ev: Event) {
        if let Event::Ran { run, now } = ev {
            self.said.push((run, now));
        }
        if let Event::Ran {
            run,
            now: Some(Phase::Finished(_)),
        } = ev
        {
            self.told.insert(run);
        }
        if let Event::Register { held, .. } = &ev {
            let done = held.iter().filter(|h| matches!(h.2, Phase::Finished(_)));
            self.told.extend(done.map(|h| h.0));
        }
        for c in step(&mut self.hub, ev) {
            match c {
                Command::Assign { worker, run, key } => {
                    let serving = serves(worker as usize).contains(&(key % 3));
                    assert!(serving, "a run only on a worker that serves its key");
                    self.ran.insert(run, (key, worker as usize, None));
                    self.to(worker, c);
                }
                Command::Drop { worker, .. } => self.to(worker, c),
                Command::Fetch { run, .. } => _ = self.fetch.insert(run),
                Command::Answer {
                    key,
                    clients,
                    answer,
                    run,
                } => self.answered(key, &clients, answer, run),
            }
        }
    }

    fn to(&mut self, worker: Worker, c: Command) {
        let wk = &mut self.wk[worker as usize];
        if wk.up {
            wk.inbox.push(c);
        }
    }

    fn answered(&mut self, key: Key, clients: &BTreeSet<Client>, answer: Answer, run: Option<Run>) {
        let all: BTreeSet<Client> = (self.open.iter())
            .filter(|((k, _), (_, hub))| *k == key && *hub == self.hubs)
            .map(|((_, c), _)| *c)
            .collect();
        for c in clients {
            let can_local = self.open.remove(&(key, *c)).map(|o| o.0);
            assert!(can_local.is_some(), "L2: one answer per waiting client");
            let defer = answer == Answer::Defer;
            assert!(
                !defer || can_local == Some(true),
                "D2: Defer only to canLocal"
            );
        }
        match answer {
            Answer::Give(o) => {
                assert_eq!(clients, &all, "L2: every client of the build gets it");
                let run = run.expect("a Give names its run");
                let truth = self.ran.get(&run).map(|t| (t.0, t.2));
                assert_eq!(
                    truth,
                    Some((key, Some(o))),
                    "L1/L5/D1/D6: one run's outcome"
                );
                let committed = self.fetched.contains(&run);
                assert!(
                    !COMMIT_THEN_STREAM || o != Outcome::Built || committed,
                    "L2c"
                );
                self.answers[usize::from(o == Outcome::Built)] += 1;
            }
            Answer::Defer => self.answers[2] += 1,
            Answer::Refuse => {
                assert_eq!(clients, &all, "L2: every client of the build gets it");
                let served = self.known.values().any(|s| s.contains(&(key % 3)));
                assert!(!served, "Refuse only if no known worker serves the key");
                self.answers[3] += 1;
            }
        }
    }

    /// Runs a connected worker holds or has been sent, for policy choices.
    fn held(&self) -> Vec<(Run, Key)> {
        let mut rs: Vec<(Run, Key)> = (self.ran.iter())
            .filter(|(r, t)| self.wk[t.1].up && self.wk[t.1].runs.contains_key(r))
            .map(|(r, t)| (*r, t.0))
            .collect();
        for c in self.wk.iter().flat_map(|w| &w.inbox) {
            if let Command::Assign { run, key, .. } = c {
                rs.push((*run, *key));
            }
        }
        rs
    }

    fn deliver(&mut self, w: usize) {
        let wk = &mut self.wk[w];
        match (!wk.inbox.is_empty()).then(|| wk.inbox.remove(0)) {
            Some(Command::Assign { run, key, .. }) => {
                wk.runs.insert(run, (key, Phase::Assigned, false));
            }
            Some(Command::Drop { run, .. }) => {
                if let Some(r) = wk.runs.get_mut(&run) {
                    r.2 = true;
                }
            }
            _ => {}
        }
    }

    fn act(&mut self, a: Act) {
        match a {
            Act::Submit(key, client, can_local) => {
                let o = self.open.entry((key, client)).or_insert((can_local, 0));
                let can_local = o.0;
                o.1 = self.hubs;
                let sys = key % 3;
                self.send(Event::Submit {
                    key,
                    client,
                    sys,
                    can_local,
                });
            }
            Act::Leave(key, client) => {
                self.open.remove(&(key, client));
                self.send(Event::Leave { key, client });
            }
            Act::Connect(w) => {
                let wk = &mut self.wk[w];
                (wk.up, wk.inbox) = (true, Vec::new());
                let keep = wk.runs.iter().filter(|r| !r.1.2);
                let held = keep.map(|(r, (k, p, _))| (*r, *k, *p)).collect();
                self.known.insert(w, serves(w));
                let (worker, serves) = (w as Worker, serves(w));
                self.send(Event::Register {
                    worker,
                    serves,
                    held,
                });
            }
            Act::Down(w) | Act::Crash(w) => {
                let wk = &mut self.wk[w];
                if let Act::Crash(_) = a {
                    for (run, (key, phase, _)) in &wk.runs {
                        let done = matches!(phase, Phase::Finished(_)) && self.told.contains(run);
                        if *phase != Phase::Assigned && !done {
                            self.evidence.insert(*key);
                        }
                    }
                    wk.runs.clear();
                }
                (wk.up, wk.inbox) = (false, Vec::new());
            }
            Act::Deliver(w) => self.deliver(w),
            Act::Start(w, i) => {
                let wk = &mut self.wk[w];
                if let Some(run) = nth(wk, i, |p, _| p == Phase::Assigned) {
                    wk.runs.get_mut(&run).unwrap().1 = Phase::Started;
                    if wk.up {
                        self.send(Event::Ran {
                            run,
                            now: Some(Phase::Started),
                        });
                    }
                }
            }
            Act::Finish(w, i, ok) => {
                let wk = &mut self.wk[w];
                if let Some(run) = nth(wk, i, |p, _| p == Phase::Started) {
                    let o = if ok {
                        Outcome::Built
                    } else {
                        Outcome::Failed(1)
                    };
                    wk.runs.get_mut(&run).unwrap().1 = Phase::Finished(o);
                    self.ran.get_mut(&run).unwrap().2 = Some(o);
                    if wk.up {
                        let now = Some(Phase::Finished(o));
                        self.send(Event::Ran { run, now });
                    }
                }
            }
            Act::Reap(w, i) => {
                let wk = &mut self.wk[w];
                let gone = |p: Phase, d: bool| d || matches!(p, Phase::Finished(_));
                if let Some(run) = nth(wk, i, gone) {
                    let (key, _, dropped) = wk.runs.remove(&run).unwrap();
                    if !dropped && !self.told.contains(&run) {
                        self.evidence.insert(key); // an outcome the hub never saw
                    }
                    if self.wk[w].up {
                        self.send(Event::Ran { run, now: None });
                    }
                }
            }
            Act::Replay(i) => {
                if let Some(&(run, now)) = self.said.get(i % self.said.len().max(1)) {
                    self.send(Event::Ran { run, now });
                }
            }
            Act::Fetched(w) => {
                let (built, up) = (Phase::Finished(Outcome::Built), self.wk[w].up);
                let done: Vec<Run> = (self.wk[w].runs.iter())
                    .filter(|(r, (_, p, _))| self.fetch.contains(r) && *p == built)
                    .map(|(r, _)| *r)
                    .collect();
                for run in done.into_iter().filter(|_| up) {
                    self.fetched.insert(run);
                    self.send(Event::Fetched { run });
                }
            }
            Act::BogusFetched(w, i) => {
                if let Some(run) = nth(&self.wk[w], i, |_, _| true) {
                    self.fetched.insert(run);
                    self.send(Event::Fetched { run });
                }
            }
            Act::Contradict(i) => {
                let done = |s: &&(Run, Option<Phase>)| matches!(s.1, Some(Phase::Finished(_)));
                let said: Vec<_> = self.said.iter().filter(done).copied().collect();
                if let Some(&(run, Some(Phase::Finished(o)))) = said.get(i % said.len().max(1)) {
                    let o = match o {
                        Outcome::Built => Outcome::Failed(1),
                        Outcome::Failed(_) => Outcome::Built,
                    };
                    let now = Some(Phase::Finished(o));
                    self.send(Event::Ran { run, now });
                }
            }
            Act::HubRestart => {
                (self.hub, self.hubs) = (self.hub.restarted(), self.hubs + 1);
                for w in 0..3 {
                    if self.wk[w].up {
                        self.act(Act::Connect(w));
                    }
                }
                let open: Vec<(Key, Client)> = self.open.keys().copied().collect();
                for (key, client) in open {
                    self.act(Act::Submit(key, client, false));
                }
            }
            Act::Place(key, w) => {
                let worker = w as Worker;
                self.send(Event::Place { key, worker });
            }
            Act::Adopt(i, key) => {
                let held = self.held();
                if let Some(&(run, own)) = held.get(i % held.len().max(1)) {
                    let key = if key >= 4 { own } else { key };
                    self.send(Event::Adopt { key, run });
                }
            }
            Act::Abort(i) => {
                let held = self.held();
                if let Some(&(run, _)) = held.get(i % held.len().max(1)) {
                    self.send(Event::Abort { run });
                }
            }
            Act::Defer(key, client) => self.send(Event::Defer { key, client }),
            Act::Refuse(key) => self.send(Event::Refuse { key }),
            Act::Suspect(w) => {
                for (run, (key, _, o)) in self.ran.iter().filter(|t| t.1.1 == w) {
                    if o.is_none() || !self.told.contains(run) {
                        self.evidence.insert(*key);
                    }
                }
                let worker = w as Worker;
                self.send(Event::Suspect { worker });
                let kept = self.hub.runs.values().any(|r| r.worker == worker);
                assert!(!kept, "a suspected worker's runs are kept");
            }
            Act::Decommission(w) => {
                self.known.remove(&w);
                let worker = w as Worker;
                self.send(Event::Decommission { worker });
            }
        }
        for (k, b) in &self.hub.builds {
            let evidence = self.evidence.contains(k);
            assert!(
                !b.lost || evidence,
                "D4: isolation only after a loss of no known cause"
            );
            self.answers[4] = self.answers[4].max(usize::from(b.lost));
        }
        for (run, r) in &self.hub.runs {
            if let Phase::Finished(o) = r.phase {
                let first = *self.finals.entry(*run).or_insert(o);
                assert_eq!(first, o, "a finished run's outcome is final");
            }
        }
    }

    /// A fair end: workers 0 and 1 back, worker 2 (if down) suspected and
    /// gone for good, and a policy that refuses, adopts or places each
    /// waiting build while every run goes through (worker 1's fail).
    fn drain(&mut self) {
        (0..2).for_each(|w| self.act(Act::Connect(w)));
        if !self.wk[2].up {
            self.act(Act::Suspect(2));
        }
        for _ in 0..8 {
            let keys: BTreeSet<Key> = self.open.keys().map(|(k, _)| *k).collect();
            for key in keys {
                self.act(Act::Refuse(key));
                for run in self.held().into_iter().filter(|h| h.1 == key) {
                    let run = run.0;
                    self.send(Event::Adopt { key, run });
                }
                let up: Vec<usize> = (0..3).filter(|w| self.wk[*w].up).collect();
                up.into_iter().for_each(|w| self.act(Act::Place(key, w)));
            }
            for w in 0..3 {
                for _ in 0..8 {
                    self.act(Act::Deliver(w));
                    self.act(Act::Start(w, 0));
                    self.act(Act::Finish(w, 0, w != 1));
                }
                self.act(Act::Fetched(w));
            }
        }
    }

    fn run(acts: &[Act]) -> World {
        let mut world = World::default();
        (0..3).for_each(|w| world.act(Act::Connect(w)));
        acts.iter().for_each(|a| world.act(a.clone()));
        world.drain();
        world
    }
}

fn act() -> impl Strategy<Value = Act> {
    let w = 0..3usize;
    prop_oneof![
        3 => (0..4u32, 0..3u32, any::<bool>()).prop_map(|(k, c, l)| Act::Submit(k, c, l)),
        1 => (0..4u32, 0..3u32).prop_map(|(k, c)| Act::Leave(k, c)),
        2 => w.clone().prop_map(Act::Connect),
        1 => w.clone().prop_map(Act::Down),
        2 => w.clone().prop_map(Act::Crash),
        6 => w.clone().prop_map(Act::Deliver),
        5 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::Start(w, i)),
        4 => (w.clone(), 0..4usize, any::<bool>()).prop_map(|(w, i, ok)| Act::Finish(w, i, ok)),
        2 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::Reap(w, i)),
        3 => (0..64usize).prop_map(Act::Replay),
        2 => w.clone().prop_map(Act::Fetched),
        1 => (w.clone(), 0..4usize).prop_map(|(w, i)| Act::BogusFetched(w, i)),
        1 => (0..64usize).prop_map(Act::Contradict),
        1 => Just(Act::HubRestart),
        5 => (0..4u32, w.clone()).prop_map(|(k, w)| Act::Place(k, w)),
        2 => (0..8usize, 0..8u32).prop_map(|(i, k)| Act::Adopt(i, k)),
        2 => (0..8usize).prop_map(Act::Abort),
        1 => (0..4u32, 0..3u32).prop_map(|(k, c)| Act::Defer(k, c)),
        1 => (0..4u32).prop_map(Act::Refuse),
        1 => w.clone().prop_map(Act::Suspect),
        1 => w.prop_map(Act::Decommission),
    ]
}

/// Every law after every step of any history under any policy; progress (a
/// fair end answers every client still waiting); and the histories reach
/// every answer and an isolation, so no law holds vacuously.
#[test]
fn every_history_refines_local_building() {
    let seen = RefCell::new([0; 5]);
    let mut runner = TestRunner::new(Config::with_cases(4000));
    let result = runner.run(&vec(act(), 0..80), |acts| {
        let world = World::run(&acts);
        let mut seen = seen.borrow_mut();
        (0..5).for_each(|i| seen[i] += usize::from(world.answers[i] > 0));
        prop_assert!(
            world.open.is_empty(),
            "progress: unanswered {:?}",
            world.open
        );
        Ok(())
    });
    result.unwrap();
    let seen = seen.into_inner();
    eprintln!("histories with Give Failed, Give Built, Defer, Refuse, isolation: {seen:?}");
    assert!(seen.iter().all(|n| *n > 40), "a case no history reaches");
}
