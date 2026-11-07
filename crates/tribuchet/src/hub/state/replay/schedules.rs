//! Any delivery, with up to three clients attaching and reading at any
//! point, against a model of what each client must get. It kills every
//! mutant of replay.rs the nine decided rows killed (PLAN.md, step 12).

use proptest::strategy::{BoxedStrategy, Just, Strategy, Union};
use proptest::test_runner::{Config, RngSeed, TestRunner};
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};

use super::*;
use crate::hub::model::Paused;

/// Events a delivery publishes: OUT's three chunks and end, OUT2's two and end.
const DELIVERY: usize = 7;

/// A build as its delivery may go, and its clients: what the steps so far
/// leave, for which steps may come next and what each client must get.
#[derive(Clone, Debug, Default)]
struct Model {
    clients: usize,
    /// The clients that went away, one bit each.
    gone: u8,
    /// A delivery kept (none after a requeue), how many of its events went
    /// out; ever kept; ever kept with OUT's first chunk wrong in the cache.
    delivering: bool,
    sent: usize,
    kept: bool,
    corrupt: bool,
    /// The verdict (passed), once out; the build ended.
    exited: Option<bool>,
    finished: bool,
    pinned: bool,
    since_end: Duration,
}

impl ReferenceStateMachine for Model {
    type State = Self;
    type Transition = (Step, usize);

    fn init_state() -> BoxedStrategy<Self> {
        Just(Self::default()).boxed()
    }

    /// Only steps that may come next, weighted.
    fn transitions(m: &Self) -> BoxedStrategy<(Step, usize)> {
        let steps = [
            (3, Keep),
            (1, Corrupt),
            (10, Out),
            (4, Log),
            (1, Step::Big),
            (1, Fill),
            (1, Requeue),
            (1, Exit0),
            (1, Exit1),
            (1, Finish),
            (2, Attach),
            (3, Read1),
            (2, ReadAll),
            (1, Step::Drop),
            (1, Wait),
            (1, Tick),
        ];
        let next = steps
            .into_iter()
            .filter(|(_, s)| Self::preconditions(m, &(*s, 0)));
        let step = Union::new_weighted(next.map(|(w, s)| (w, Just(s))).collect());
        (step, 0..m.clients.max(1)).boxed()
    }

    fn preconditions(m: &Self, (step, k): &(Step, usize)) -> bool {
        let open = !m.finished && m.exited.is_none();
        match step {
            Keep | Corrupt => open && !m.delivering,
            Out => open && m.delivering && m.sent < DELIVERY,
            Log | Step::Big | Fill | Requeue => open,
            // As the relay: exit 0 only once the outputs went out whole.
            Exit0 => open && m.delivering && m.sent == DELIVERY,
            Exit1 => open && !(m.delivering && m.sent > 0 && m.sent < DELIVERY),
            Finish => !m.finished,
            Attach => m.clients < 3,
            Read1 | ReadAll | Step::Drop => *k < m.clients && m.gone & (1 << k) == 0,
            Wait | Tick => true,
        }
    }

    fn apply(mut m: Self, (step, k): &(Step, usize)) -> Self {
        match step {
            Keep | Corrupt => {
                (m.delivering, m.kept, m.sent, m.pinned) = (true, true, 0, true);
                m.corrupt |= *step == Corrupt;
            }
            Out => m.sent += 1,
            Requeue => {
                (m.delivering, m.sent, m.pinned) = (false, 0, false);
                // Requeued with no client still there, it is dropped.
                m.finished |= (0..m.clients).all(|k| m.gone & (1 << k) != 0);
            }
            Exit0 | Exit1 => m.exited = Some(*step == Exit0),
            Finish => m.finished = true,
            Attach => m.clients += 1,
            Step::Drop => m.gone |= 1 << k,
            Wait | Tick if m.finished => {
                m.since_end += Duration::from_secs(if *step == Wait { 599 } else { 2 });
                m.pinned &= m.since_end < CATCH_UP_PIN;
            }
            _ => {}
        }
        m
    }
}

struct Schedules;

impl StateMachineTest for Schedules {
    type SystemUnderTest = Paused<Run>;
    type Reference = Model;

    fn init_test(_: &Model) -> Paused<Run> {
        Paused::new(async { Run::new() })
    }

    /// Every step: publish never waits; the log holds at most the cap's
    /// worth of events, and counts what they cost; the outputs are pinned
    /// exactly while they can be caught up on; a requeue drops the build
    /// exactly when no client's stream is open (Run::step); the hub reads
    /// for each client whose stream is open, and for no other, and has a
    /// client exactly while one is.
    fn apply(mut p: Paused<Run>, m: &Model, (step, k): (Step, usize)) -> Paused<Run> {
        let Paused { rt, sut: run } = &mut p;
        rt.block_on(async {
            run.step(step, k).await.unwrap();
            let pinned = run.replay.outputs_pinned().await;
            assert_eq!(pinned, m.pinned, "pinned after {step:?}");
            let log = run.replay.inner.lock().await;
            let held = log.events.len() + log.went.len();
            assert!(held <= CAP / HELD, "{held} events held after {step:?}");
            let costs = log.events.iter().chain(&log.went).map(|h| cost(&h.event));
            assert_eq!(
                log.bytes,
                costs.sum::<usize>(),
                "bytes counted after {step:?}"
            );
            drop(log);
            let (open, readers) = run.open().await;
            assert_eq!(readers, open, "readers after {step:?}");
            let attached = run.replay.has_subscribers().await;
            assert_eq!(attached, open > 0, "attached after {step:?}");
        });
        p
    }

    /// End the build if it has not ended, then every client still there gets
    /// the verdict, the outputs whole with a passed one, or is told to
    /// resubmit: when the build ended without one (a client attached after
    /// the hub's drop), or where it needed OUT's first chunk of a corrupt
    /// delivery.
    fn teardown(mut p: Paused<Run>, m: Model) {
        let Paused { rt, sut: run } = &mut p;
        rt.block_on(async {
            let mut exited = m.exited;
            if !m.finished && exited.is_none() {
                if m.delivering && m.sent > 0 && m.sent < DELIVERY {
                    run.step(Requeue, 0).await.unwrap();
                }
                run.step(Exit1, 0).await.unwrap();
                exited = Some(false);
            }
            if !m.finished {
                run.step(Finish, 0).await.unwrap();
            }
            let want = exited.map_or(Got::Resubmit, |passed| {
                if passed { Got::Passed } else { Got::Failed }
            });
            for k in (0..run.clients.len()).filter(|k| m.gone & (1 << k) == 0) {
                let got = run.verdict(k).await;
                // Lost: OUT's first chunk was next, and a delivery's is wrong.
                // Resubmit: attached after the hub dropped the build.
                let dropped = run.dropped && k >= run.at_drop;
                let lost = got == Ok(Got::Lost) || dropped && got == Ok(Got::Resubmit);
                assert!(got == Ok(want) || lost, "client {k}: {got:?}, not {want:?}");
            }
        });
    }
}

/// Any delivery, with up to three clients attaching and reading at any
/// point: publish never waits for one, OUT is pinned while it can be caught
/// up on, and every client gets what `judged` asks; 128 cases under each
/// of eight seeds.
#[test]
fn every_client_of_any_delivery_gets_its_outputs_whole() {
    for seed in [1, 7, 12, 42, 99, 2026, 31337, 271_828] {
        let config = Config {
            cases: 128,
            failure_persistence: None,
            rng_seed: RngSeed::Fixed(seed),
            ..Config::default()
        };
        let schedules = Model::sequential_strategy(1..40);
        let ran = TestRunner::new(config.clone()).run(&schedules, |(model, steps, seen)| {
            Schedules::test_sequential(config.clone(), model, steps, seen);
            Ok(())
        });
        assert!(ran.is_ok(), "seed {seed}: {ran:?}");
    }
}

/// Client 0, its channel full, is behind a chunk whose bytes went: it gets
/// it from the cache, the delivery requeued after or mid-way (the review's
/// S1 and S1b, where a reader that ended on that chunk went unseen).
#[test]
fn a_client_behind_a_gone_chunk_reads_it_from_the_cache() {
    let behind = [
        (Keep, 0),
        (Attach, 0),
        (Fill, 0),
        (Fill, 0),
        (Fill, 0),
        (Fill, 0),
    ];
    let behind = behind
        .into_iter()
        .chain([(Out, 0), (Step::Big, 0), (ReadAll, 0)]);
    for requeue in [true, false] {
        let steps: Vec<_> = behind
            .clone()
            .chain(requeue.then_some((Requeue, 0)))
            .collect();
        let mut m = Model::default();
        for s in &steps {
            assert!(Model::preconditions(&m, s), "{s:?} may not come next");
            m = <Model as ReferenceStateMachine>::apply(m, s);
        }
        Schedules::test_sequential(Config::default(), Model::default(), steps, None);
    }
}
