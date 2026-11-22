//! What builds used on this farm, by key, and what a build is therefore
//! expected to use: its memory, cores and running time (Expected).
//!
//! A key is a build's system and stem, its name less its versions and
//! hashes (stem); the builds of a stem that consumed the same
//! content-addressed inputs (hub/sources.rs) are a key within it. A build
//! takes the runs of its sources, else of its stem, else of the longest
//! dash-prefix of its stem that has runs (the runs of every stem that is
//! or starts with that prefix, pooled), then its class: its system and
//! whether it declares big-parallel, then a fixed prior.
//!
//! What a build costs is what it consumes, not its name: blob-deps runs
//! one build per program file, all named `program-observation.json`,
//! from 0.04 s to 3,000 s by the program. Replayed on the hub's record of
//! 2026-10-29..11-20 (1,559,881 results; each expected at its dispatch
//! from the results before it, then learned; sources as the daemon names
//! them, read from the derivations the hub's store still has, those of
//! 1,345,938 results; the others none), the hour-weighted mean |ln| of
//! expected over running seconds of the 1,024,931 observations that
//! succeeded went from 5.09, by system and Nix's version cut, to 0.64;
//! of those that ran 60 s or more, 2,623 of 3,312 were expected to,
//! from 257; and of the 320,781 other results, 0.72 to 0.61. Their
//! sources had runs for 94.1 % and 84.4 % of them. On the last week
//! alone (from 11-13T01:53Z), 5.17 to 1.33 and 0.80 to 0.64. Memory
//! peaking at 4 GiB or more was expected short for 277 of 1,511
//! observations, from 1,321, and for 1,551 of 5,703 others, from 1,486;
//! the bytes expected over those used, 1.34x and 1.32x from 4.72x and
//! 1.47x. That replay was of this file as it was before memory
//! admission, when a failed run taught nothing and an OOM-killed one
//! taught the class and prefixes; it was not run again under the rule
//! here.
//!
//! The window's figures below are from an earlier copy of the hub's
//! journal, 2026-10-28T22:55Z to 2026-11-06T01:19Z, 360,122 builds whose
//! system it names, each predicted from the builds that finished before
//! it was queued by a model of this file as it keyed by Nix's cut, before
//! memory admission; they were not measured again under the rule here.
//!
//! Each key keeps its last WINDOW runs:
//! - memory: the largest peak among them (a run that did not succeed
//!   enters as its own peak: Runs::learn, History::record). Of the 2,462
//!   builds that succeeded peaking at 8 GiB or more it covered 1,348,
//!   reserving 1.16x what those used (2.33x over all builds); the max of
//!   4 runs covered 1,067, the last run 620, the median 484, the max of
//!   16 1,583.
//! - cores (CPU time over running time: wall time less frozen time) and
//!   running seconds: medians over those that succeeded. Within 2x of
//!   the build's own for 97.4 % and 82.4 % of the builds.
//!
//! Placement ranks by memory and cores only; the running seconds, the
//! OOM kills and the level are logged at dispatch, and no replay has yet
//! shown a ranking that weighs them doing better.
//!
//! A build's running seconds on a worker of speed factor 1 (speed.rs)
//! are the median of each run's times its worker's factor; it is
//! expected to run them over a worker's factor there (History::secs_on),
//! both read from the history when asked, not kept with an expectation:
//! a refit changes them without changing anything an expectation read.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::iter;

use super::speed::Speed;
use crate::proto::BuildUsage;
#[cfg(test)]
use runs::quantile;
use runs::{Run, Runs};

#[path = "history/runs.rs"]
mod runs;

/// Runs kept per key and prefix (a class keeps CLASS_WINDOW).
pub(super) const WINDOW: usize = 8;
/// Runs kept per class, the guess for a key with no run and no prefix:
/// its peak is their 90th percentile, a wider sample's tail; with 10 runs
/// or fewer, their largest.
const CLASS_WINDOW: usize = 512;
const GIB: f64 = 1_073_741_824.0;

/// What a build is expected to use: cores and bytes of memory.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(in crate::hub) struct Demand {
    pub(in crate::hub) cores: f64,
    pub(in crate::hub) memory: f64,
}

impl Demand {
    pub(in crate::hub) fn plus(self, other: Self) -> Self {
        Self {
            cores: self.cores + other.cores,
            memory: self.memory + other.memory,
        }
    }
}

/// Where an expectation came from, most specific first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::hub) enum Level {
    Source,
    Key,
    Prefix,
    Class,
    Prior,
}

impl Level {
    pub(in crate::hub) fn name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Key => "key",
            Self::Prefix => "prefix",
            Self::Class => "class",
            Self::Prior => "prior",
        }
    }
}

/// A build's expected demand, its running seconds where a run of its
/// level succeeded (the median as they ran, which the dispatch row
/// records; those on a worker of speed factor 1 are read from the history
/// when asked, History::unit_secs), the level, the OOM kills among the
/// runs of the entry it came from (its sources', its key's, a prefix's or
/// its class's), and the running seconds learned to follow it
/// (placement/downstream.rs).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::hub) struct Expected {
    pub(in crate::hub) demand: Demand,
    pub(in crate::hub) secs: Option<f64>,
    pub(in crate::hub) level: Level,
    pub(in crate::hub) oom: u32,
    pub(in crate::hub) below: f64,
}

impl Expected {
    /// The guess with nothing learned: one core and 1 GiB, or for a
    /// build that declares big-parallel the median of the 33 such builds
    /// the hub ran on 2026-10-15..25, 17.8 cores and 38.5 GiB.
    pub(in crate::hub) fn prior(declared: bool) -> Self {
        let (cores, gib) = if declared { (17.8, 38.5) } else { (1.0, 1.0) };
        Self {
            demand: Demand {
                cores,
                memory: gib * GIB,
            },
            secs: None,
            level: Level::Prior,
            oom: 0,
            below: 0.0,
        }
    }
}

/// The stem: the name less every dash-part, but the first, that does not
/// start with a letter (where Nix's DrvName finds a version, names.cc) or
/// is a hash. Nix's cut stops at the version: it made `ghc-9.14.1` and
/// `ghc-9.14.1-with-packages` one key, and a 5,129 s compiler build took
/// its wrapper's 3.1 s (the hub's record, 2026-11-20).
pub(in crate::hub) fn stem(name: &str) -> String {
    let mut parts = name.split('-');
    let first = parts.next().unwrap_or_default();
    let kept =
        parts.filter(|part| part.starts_with(|c: char| c.is_ascii_alphabetic()) && !hashlike(part));
    iter::once(first).chain(kept).collect::<Vec<_>>().join("-")
}

/// A hash: eight or more hex digits, a digit among them. Not a word with
/// a digit in it, such as `cuda13`, `chacha20` or `secp256k1`. A git
/// short hash of 7 stays in the stem: its builds meet under the prefix
/// before it.
fn hashlike(part: &str) -> bool {
    let hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f');
    part.len() >= 8 && part.bytes().any(|c| c.is_ascii_digit()) && part.bytes().all(hex)
}

/// The dash-prefixes of a stem, longest first, the stem excluded.
fn prefixes(stem: &str) -> impl Iterator<Item = &str> {
    stem.rmatch_indices('-').map(move |(i, _)| &stem[..i])
}

/// What a build is, as history keys it.
pub(in crate::hub) struct Build<'a> {
    pub(in crate::hub) system: &'a str,
    /// Its derivation name, if one could be read.
    pub(in crate::hub) name: Option<&'a str>,
    pub(in crate::hub) declared: bool,
    /// The digest of the content-addressed inputs it consumes (hub/
    /// sources.rs), if it has any and the hub's daemon said which.
    pub(in crate::hub) sources: Option<&'a str>,
}

/// The key of a build named `name` for `system`: its system and stem.
pub(in crate::hub) fn key(system: &str, name: &str) -> String {
    format!("{system}/{}", stem(name))
}

/// A run as a comparison of two histories reads it: peak, CPU ms,
/// running ms, succeeded, its worker's name; or a speed factor, last.
#[cfg(test)]
pub(in crate::hub) type Entry = (u64, u64, u64, bool, Option<String>, f64);

/// Entries are never dropped: the fold of the build record (record.rs).
#[derive(Clone, Default)]
pub(in crate::hub) struct History {
    /// "system/stem/sources" to the runs of the builds of that stem that
    /// consumed those content-addressed inputs.
    sources: HashMap<String, Runs>,
    /// "system/stem" to its runs.
    keys: HashMap<String, Runs>,
    /// "system/prefix" to the runs of every stem that is or starts
    /// with it, at a dash.
    prefixes: HashMap<String, Runs>,
    /// "system/declared" (true or false) to the class's runs.
    classes: HashMap<String, Runs>,
    /// How many runs it has learned.
    learned: u64,
    /// Each entry's name (entry), to the count of runs learned when a run
    /// last changed it: what an expectation read of it (Read) still holds.
    touched: HashMap<u64, u64>,
    /// The workers' speed factors, from the keys' runs that teach it.
    speed: Speed,
}

/// What an expectation was read from: the runs learned then, and the
/// entries it read (entry). It holds while none of them has learned a
/// run since (History::fresh).
#[derive(Clone, Debug)]
pub(in crate::hub) struct Read {
    at: u64,
    entries: Vec<u64>,
}

/// An entry's name as `touched` keeps it: its kind (key, prefix, class,
/// sources) and name, hashed. Two names that hash alike only make a read stale
/// sooner.
fn entry(kind: u8, name: &str) -> u64 {
    let mut h = DefaultHasher::new();
    (kind, name).hash(&mut h);
    h.finish()
}

/// Where a run ran, as the record names it: its worker, and whether it
/// teaches speed (speed::taught).
#[derive(Clone, Copy)]
pub(in crate::hub) struct On<'a> {
    pub(in crate::hub) worker: &'a str,
    pub(in crate::hub) taught: bool,
}

impl History {
    /// Its sources' runs if one succeeded with running time, else its
    /// key's. A key, or sources, none of whose runs succeeded takes the
    /// guess of the levels below, no lower than its largest peak: a failed
    /// run bounds its own build's need from below and says nothing of how
    /// far above, so no failure stands in for a measured need.
    pub(in crate::hub) fn expect(&self, b: &Build) -> Expected {
        self.expect_from(b).0
    }

    /// `b`'s running seconds on worker `name`: on a worker of factor 1
    /// (unit_secs) over `name`'s factor, where a run of its level
    /// succeeded.
    pub(in crate::hub) fn secs_on(&self, b: &Build, name: &str) -> Option<f64> {
        Some(self.unit_secs(b)? / self.speed(name))
    }

    /// `b`'s running seconds on a worker of speed factor 1: the median of
    /// each run's seconds times its worker's factor, over the runs its
    /// expectation's seconds are the median of (expect), with the factors
    /// as they are now, so a refit since `b` was read counts.
    pub(in crate::hub) fn unit_secs(&self, b: &Build) -> Option<f64> {
        self.expect_from(b).1?.unit_secs(&self.speed)
    }

    /// `b`'s expectation (expect), and the runs its seconds came from.
    #[allow(clippy::cast_precision_loss)]
    fn expect_from<'a>(&'a self, b: &Build) -> (Expected, Option<&'a Runs>) {
        let class = self.classes.get(&format!("{}/{}", b.system, b.declared));
        let prior = Expected::prior(b.declared);
        let fallback = class
            .and_then(|c| c.timing())
            .map_or(prior.demand.cores, |t| t.0);
        let sources = Self::sources_of(b).and_then(|s| self.sources.get(&s));
        if let Some(e) = sources
            .filter(|r| r.timing().is_some())
            .and_then(|r| Some((r.expected(Level::Source, fallback)?, Some(r))))
        {
            return e;
        }
        let stem = b.name.map(stem);
        let stem = stem.as_deref();
        let key = stem.and_then(|p| self.keys.get(&format!("{}/{p}", b.system)));
        let some = |r: &&Runs| r.runs.iter().any(|r| r.3);
        let succeeded = key.filter(some);
        let failed = key.filter(|_| succeeded.is_none());
        let measured = |r: &&Runs| failed.is_none() || some(r);
        let by_prefix = || {
            prefixes(stem?).find_map(|pre| {
                (self.prefixes.get(&format!("{}/{pre}", b.system))).filter(measured)
            })
        };
        let at = |r: &'a Runs, level| Some((r.expected(level, fallback)?, Some(r)));
        let (e, from) = succeeded
            .and_then(|r| at(r, Level::Key))
            .or_else(|| at(by_prefix()?, Level::Prefix))
            .or_else(|| at(class.filter(measured)?, Level::Class))
            .unwrap_or((prior, None));
        let e = match failed.and_then(|r| Some((r.runs.iter().map(|r| r.0).max()?, r.oom))) {
            Some((peak, oom)) => Expected {
                demand: Demand {
                    memory: e.demand.memory.max(peak as f64),
                    ..e.demand
                },
                level: Level::Key,
                oom,
                ..e
            },
            None => e,
        };
        let e = match sources.and_then(|r| r.runs.iter().map(|r| r.0).max()) {
            Some(peak) => Expected {
                demand: Demand {
                    memory: e.demand.memory.max(peak as f64),
                    ..e.demand
                },
                ..e
            },
            None => e,
        };
        (e, from)
    }

    /// `b`'s expectation, and what it read: its sources, if it has them;
    /// they alone where they have a success with timing, else also its
    /// key; its key alone where the key has a success with timing, else
    /// also its prefixes and class.
    pub(in crate::hub) fn read(&self, b: &Build) -> (Expected, Read) {
        let e = self.expect(b);
        let at = self.learned;
        let sources = Self::sources_of(b);
        let mut entries: Vec<u64> = sources.iter().map(|s| entry(3, s)).collect();
        let runs = sources.as_ref().and_then(|s| self.sources.get(s));
        if runs.is_some_and(|r| r.timing().is_some()) {
            return (e, Read { at, entries });
        }
        let stem = b.name.map(stem);
        let key = Self::key_of(b);
        let runs = key.as_ref().and_then(|k| self.keys.get(k));
        let own =
            (runs.filter(|r| r.runs.iter().any(|r| r.3))).is_some_and(|r| r.timing().is_some());
        entries.extend(key.iter().map(|k| entry(0, k)));
        if !own {
            let pres = stem.as_deref().into_iter().flat_map(prefixes);
            entries.extend(pres.map(|p| entry(1, &format!("{}/{p}", b.system))));
            entries.push(entry(2, &format!("{}/{}", b.system, b.declared)));
        }
        (e, Read { at, entries })
    }

    /// Whether what `r` read is still the history's: none of its entries
    /// has learned a run since.
    pub(in crate::hub) fn fresh(&self, r: &Read) -> bool {
        (r.entries.iter()).all(|h| self.touched.get(h).is_none_or(|at| *at <= r.at))
    }

    /// Worker `name`'s speed factor (speed.rs): 1 for one never compared.
    /// Read by the dispatch log line only, so far.
    pub(in crate::hub) fn speed(&self, name: &str) -> f64 {
        self.speed.factor(name)
    }

    /// Every worker's name, speed factor and comparisons' decayed weight.
    pub(in crate::hub) fn speeds(&self) -> Vec<(String, f64, f64)> {
        self.speed.factors()
    }

    /// Learn from a build's run: its sources, its key, its stem's prefixes
    /// and its class. One that failed or was OOM-killed is right-censored at its
    /// own peak (Runs::expected, quantile): it teaches memory only, no
    /// timing. A run the worker measured nothing of teaches nothing. A run
    /// `on` a named worker keeps it; a succeeded one taught teaches speed,
    /// compared with the other workers' runs of its key (key_of).
    pub(in crate::hub) fn record(
        &mut self,
        b: &Build,
        exit_code: i32,
        u: &BuildUsage,
        on: Option<On>,
    ) {
        self.learned += 1;
        let (ok, oom) = (exit_code == 0 && !u.oom_killed, u.oom_killed);
        let worker = on.map(|on| self.speed.worker(on.worker));
        let run = Run::of(u, ok, worker);
        if let (Some(key), Some(r)) = (Self::key_of(b), run) {
            let taught = (on.zip(worker)).filter(|(on, _)| on.taught);
            if let (Some((_, w)), Some((_, secs))) = (taught, r.timed()) {
                self.speed.learn(&key, w, secs);
            }
            self.touched.insert(entry(0, &key), self.learned);
            self.keys.entry(key).or_default().learn(r, oom);
        }
        if let (Some(sources), Some(r)) = (Self::sources_of(b), run) {
            self.touched.insert(entry(3, &sources), self.learned);
            self.sources.entry(sources).or_default().learn(r, oom);
        }
        let Some(run) = run else {
            return;
        };
        let class = format!("{}/{}", b.system, b.declared);
        self.touched.insert(entry(2, &class), self.learned);
        (self.classes.entry(class).or_default()).push(run, oom, CLASS_WINDOW);
        let stem = b.name.map(stem);
        for pre in (stem.as_deref().into_iter()).flat_map(|p| iter::once(p).chain(prefixes(p))) {
            let name = format!("{}/{pre}", b.system);
            self.touched.insert(entry(1, &name), self.learned);
            (self.prefixes.entry(name).or_default()).learn(run, oom);
        }
    }

    /// Whether a run of `b`'s key has succeeded, its usage measured: until
    /// one has, its builds go one at a time (Placement::held_for_key), as
    /// what a failed or unmeasured run taught is no measure of them.
    pub(in crate::hub) fn ran(&self, b: &Build) -> bool {
        let key = Self::key_of(b).and_then(|k| self.keys.get(&k));
        key.is_some_and(|r| r.runs.iter().any(|r| r.3))
    }

    /// The key `b` is known by, "system/stem", if its name could be read.
    pub(in crate::hub) fn key_of(b: &Build) -> Option<String> {
        b.name.map(|n| key(b.system, n))
    }

    /// Its sources' entry, "system/stem/sources", if its name could be
    /// read and the hub's daemon said which sources it consumes.
    fn sources_of(b: &Build) -> Option<String> {
        Some(format!("{}/{}", Self::key_of(b)?, b.sources?))
    }

    /// Its keys, and its entries: sources, keys, prefixes and classes.
    pub(in crate::hub) fn len(&self) -> (usize, usize) {
        let entries =
            self.sources.len() + self.keys.len() + self.prefixes.len() + self.classes.len();
        (self.keys.len(), entries)
    }

    /// Every entry's runs, oldest first, as (peak, CPU ms, running ms,
    /// succeeded, worker), and its OOM kills, by "sources/", "keys/",
    /// "prefixes/" or "classes/" and its name, and every worker's speed
    /// factor and comparisons as "speed/" and its name: what a comparison
    /// of two histories reads.
    #[cfg(test)]
    pub(in crate::hub) fn entries(&self) -> HashMap<String, (Vec<Entry>, u32)> {
        let tables = [
            ("sources", &self.sources),
            ("keys", &self.keys),
            ("prefixes", &self.prefixes),
            ("classes", &self.classes),
        ];
        let name = |w: Option<u16>| w.map(|w| self.speed.name(w).to_owned());
        let runs = |r: &Runs| {
            r.runs
                .iter()
                .map(|&Run(p, c, t, ok, w)| (p, c, t, ok, name(w), 0.0))
                .collect()
        };
        let speeds = self.speed.factors().into_iter().map(|(n, f, c)| {
            let entry = (0, 0, 0, true, None, f);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            (format!("speed/{n}"), (vec![entry], c as u32))
        });
        (tables.into_iter())
            .flat_map(|(t, m)| {
                m.iter()
                    .map(move |(k, r)| (format!("{t}/{k}"), (runs(r), r.oom)))
            })
            .chain(speeds)
            .collect()
    }
}

#[cfg(test)]
#[path = "history/tests.rs"]
mod tests;
