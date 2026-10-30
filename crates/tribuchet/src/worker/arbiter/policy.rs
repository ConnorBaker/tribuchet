//! Inelastic-First, decided from demand samples alone: which builds to
//! freeze or thaw now. No I/O, so every rule is a unit test.
//!
//! A frozen build cannot be sampled (it has no runnable thread), so what
//! holds it frozen is what it showed before: a follower is frozen only
//! once its 1 s mean stayed parallel at every sample for SATURATED_FOR,
//! never for one burst.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Runnable threads (1 s mean) at or above which a build is parallel.
const PARALLEL_AT: f64 = 4.0;
/// At or below which it is serial; in between it keeps its phase.
const SERIAL_AT: f64 = 2.0;
/// How long the leader must keep every CPU busy before followers freeze.
const SATURATED_FOR: Duration = Duration::from_secs(5);
/// No freeze this soon after a thaw.
const REFREEZE_AFTER: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Parallel,
    Serial,
}

/// One build as the policy sees it at a sample.
pub(super) struct Build<'a> {
    pub(super) key: &'a str,
    /// Its start: the oldest parallel one leads, ties by key.
    pub(super) order: u64,
    /// May be frozen (the arbiter's `Tracked::eligible`): nothing the
    /// build declares, only its cgroup and how the arbiter has fared.
    pub(super) eligible: bool,
    /// Runnable threads at this sample.
    pub(super) runnable: u32,
    /// Mean runnable threads over the last second.
    pub(super) mean: f64,
    pub(super) frozen: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Action {
    Freeze(String),
    Thaw(String),
}

impl Action {
    pub(super) fn key(&self) -> &str {
        match self {
            Action::Freeze(k) | Action::Thaw(k) => k,
        }
    }
}

pub(super) struct Policy {
    ncpus: u32,
    phases: HashMap<String, Phase>,
    /// Since when each build's mean has been at least PARALLEL_AT at
    /// every sample it ran (unfrozen).
    sustained: HashMap<String, Instant>,
    /// The leader and when it began keeping every CPU busy.
    saturated: Option<(String, Instant)>,
    last_thaw: Option<Instant>,
}

impl Policy {
    pub(super) fn new(ncpus: u32) -> Self {
        Self {
            ncpus,
            phases: HashMap::new(),
            sustained: HashMap::new(),
            saturated: None,
            last_thaw: None,
        }
    }

    /// The actions for this sample. A frozen build reads 0 runnable
    /// threads, so it keeps the phase it had when frozen.
    pub(super) fn step(&mut self, now: Instant, builds: &[Build]) -> Vec<Action> {
        self.phases.retain(|k, _| builds.iter().any(|b| b.key == k));
        self.sustained
            .retain(|k, _| builds.iter().any(|b| b.key == k));
        for b in builds.iter().filter(|b| !b.frozen) {
            let phase = self.phases.entry(b.key.to_owned()).or_insert(Phase::Serial);
            if b.mean >= PARALLEL_AT {
                *phase = Phase::Parallel;
                self.sustained.entry(b.key.to_owned()).or_insert(now);
            } else {
                self.sustained.remove(b.key);
                if b.mean <= SERIAL_AT {
                    *phase = Phase::Serial;
                }
            }
        }
        let phases = &self.phases;
        let parallel = |b: &&Build| phases.get(b.key) == Some(&Phase::Parallel);
        let leader = builds
            .iter()
            .filter(parallel)
            .min_by_key(|b| (b.order, b.key));
        let busy = leader.filter(|l| !l.frozen && l.runnable >= self.ncpus);
        // Thaw what must not stay frozen: everything once the leader
        // leaves slack or ends (a frozen leader is a follower whose
        // leader ended), and a build no longer eligible at any time.
        let thaw: Vec<Action> = builds
            .iter()
            .filter(|b| b.frozen && (busy.is_none() || !b.eligible))
            .map(|b| Action::Thaw(b.key.to_owned()))
            .collect();
        let Some(leader) = busy else {
            self.saturated = None;
            if !thaw.is_empty() {
                self.last_thaw = Some(now);
            }
            return thaw;
        };
        if !thaw.is_empty() {
            self.last_thaw = Some(now);
            return thaw;
        }
        let held = self
            .saturated
            .as_ref()
            .filter(|(k, _)| k == leader.key)
            .map(|(_, t)| *t);
        let since = held.unwrap_or_else(|| {
            self.saturated = Some((leader.key.to_owned(), now));
            now
        });
        if now.duration_since(since) < SATURATED_FOR
            || self
                .last_thaw
                .is_some_and(|t| now.duration_since(t) < REFREEZE_AFTER)
        {
            return Vec::new();
        }
        // Parallel at every sample for SATURATED_FOR, so in a parallel phase.
        let sustained = |b: &&Build| {
            self.sustained
                .get(b.key)
                .is_some_and(|t| now.duration_since(*t) >= SATURATED_FOR)
        };
        builds
            .iter()
            .filter(|b| b.key != leader.key && b.eligible && !b.frozen)
            .filter(sustained)
            .map(|b| Action::Freeze(b.key.to_owned()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: u32 = 8;

    /// A change before a step: a build's runnable threads and 1 s mean, that
    /// it may no longer be frozen, or that it ended.
    #[derive(Clone, Copy)]
    enum Set {
        Load(&'static str, u32, f64),
        Ineligible(&'static str),
        Ends(&'static str),
    }
    use Set::{Ends, Ineligible, Load};

    /// Changes, then this many 200 ms samples, and the actions over them:
    /// "+k" freezes k, "-k" thaws it.
    type Step = (&'static [Set], u64, &'static str);
    const LF: &[(&str, u64, u32)] = &[("lead", 0, L), ("follow", 1, 6)];

    /// Builds (key, start, runnable threads, its mean alike) and steps.
    const TRACES: &[(&[(&str, u64, u32)], &[Step])] = &[
        // The follower freezes once the leader filled every CPU for 5 s.
        (LF, &[(&[], 20, ""), (&[], 10, "+follow")]),
        // A burst of 23 threads (a JVM's GC) raises the mean to 5.4 for a
        // second and freezes nothing; 5 s of parallel demand does.
        (
            &[("lead", 0, L), ("follow", 1, 1)],
            &[
                (&[], 30, ""),
                (&[Load("follow", 23, 5.4)], 1, ""),
                (&[Load("follow", 1, 5.4)], 4, ""),
                (&[Load("follow", 1, 1.0)], 150, ""),
                (&[Load("follow", 6, 6.0)], 25, ""),
                (&[], 1, "+follow"),
            ],
        ),
        // Serial and in-between builds are never frozen, nor ineligible ones.
        (
            &[("lead", 0, L), ("serial", 1, 1), ("mid", 2, 3)],
            &[(&[], 150, "")],
        ),
        (
            &[("lead", 0, L), ("other", 1, L)],
            &[(&[Ineligible("other")], 150, "")],
        ),
        // Followers thaw at the first sample the leader leaves a CPU spare,
        // its mean still full.
        (
            LF,
            &[
                (&[], 30, "+follow"),
                (&[Load("lead", L - 1, 8.0)], 1, "-follow"),
            ],
        ),
        // ... and when it ends; then the oldest leads.
        (
            &[("lead", 0, L), ("f1", 1, 6), ("f2", 2, 6)],
            &[
                (&[], 30, "+f1 +f2"),
                (&[Ends("lead")], 1, "-f1 -f2"),
                (&[Load("f1", L, 8.0), Load("f2", L, 8.0)], 30, "+f2"),
            ],
        ),
        // Of equal starts the first key leads.
        (&[("b", 7, L), ("a", 7, L)], &[(&[], 30, "+b")]),
        // No freeze within 5 s of a thaw.
        (
            LF,
            &[
                (&[], 30, "+follow"),
                (&[Load("lead", 1, 8.0)], 5, "-follow"),
                (&[Load("lead", L, 8.0)], 20, ""),
                (&[], 15, "+follow"),
            ],
        ),
        // Frozen, a build reads no runnable thread and stays frozen; one no
        // longer eligible is thawed.
        (
            LF,
            &[
                (&[], 30, "+follow"),
                (&[Load("follow", 0, 0.0)], 10, ""),
                (&[Ineligible("follow")], 1, "-follow"),
            ],
        ),
        // ... and keeps its phase: when the leader ends it is the oldest
        // parallel build, so it is thawed to lead, not left frozen behind a
        // younger one that may not be frozen.
        (
            &[("lead", 0, L), ("follow", 1, 6), ("other", 2, L)],
            &[
                (&[Ineligible("other")], 30, "+follow"),
                (&[Load("follow", 0, 0.0)], 10, ""),
                (&[Ends("lead")], 1, "-follow"),
            ],
        ),
    ];

    #[test]
    fn every_trace_acts_as_the_table_says() {
        for (row, (builds, steps)) in TRACES.iter().enumerate() {
            let (mut p, mut t) = (Policy::new(L), Instant::now());
            let mut b: Vec<Build> = (builds.iter())
                .map(|&(key, order, n)| Build {
                    key,
                    order,
                    eligible: true,
                    runnable: n,
                    mean: f64::from(n),
                    frozen: false,
                })
                .collect();
            for (step, &(sets, samples, want)) in steps.iter().enumerate() {
                for set in sets {
                    match *set {
                        Load(k, n, mean) => {
                            let x = b.iter_mut().find(|x| x.key == k).unwrap();
                            (x.runnable, x.mean) = (n, mean);
                        }
                        Ineligible(k) => {
                            b.iter_mut().find(|x| x.key == k).unwrap().eligible = false
                        }
                        Ends(k) => b.retain(|x| x.key != k),
                    }
                }
                let mut got = Vec::new();
                for _ in 0..samples {
                    t += Duration::from_millis(200);
                    for a in p.step(t, &b) {
                        let frozen = matches!(a, Action::Freeze(_));
                        b.iter_mut().find(|x| x.key == a.key()).unwrap().frozen = frozen;
                        got.push(format!("{}{}", if frozen { '+' } else { '-' }, a.key()));
                    }
                }
                assert_eq!(got.join(" "), want, "trace {row}, step {step}");
            }
        }
    }
}
