//! What builds used on this farm, by key, and what a build is therefore
//! expected to use: its memory, cores and running time (Expected).
//!
//! A key is a build's system and pname. A build whose key has no run
//! yet takes the longest dash-prefix of its pname that has one (the runs
//! of every pname that is or starts with that prefix, pooled), then its
//! class: its system and whether it declares big-parallel, then a fixed
//! prior. The figures here are from a copy of the hub's journal from
//! 2026-10-28T22:55Z to 2026-11-06T01:19Z, 1,922,660 lines with 360,131
//! usage lines (the live journal has rotated since), 360,122 of them of
//! builds whose system it names, each predicted from the builds that
//! finished before it was queued by a model of this file. The key had
//! runs for 98.3 % of them, a prefix for 1.4 %, the class for 0.3 %, the
//! prior for 4 builds.
//!
//! Each key keeps its last WINDOW runs that succeeded or were OOM-killed:
//! - memory: the largest peak among them (an OOM-killed run's peak is a
//!   lower bound on what it needed, and enters as such). Of the 2,462
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

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io;
use std::iter;
use std::mem;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::proto::BuildUsage;

/// Runs kept per key and prefix (a class keeps CLASS_WINDOW).
const WINDOW: usize = 8;
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

/// Where an expectation came from, most specific first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::hub) enum Level {
    Key,
    Prefix,
    Class,
    Prior,
}

impl Level {
    pub(in crate::hub) fn name(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Prefix => "prefix",
            Self::Class => "class",
            Self::Prior => "prior",
        }
    }
}

/// A build's expected demand, its running seconds where a run of its
/// level succeeded, the level, and the OOM kills among its key's runs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::hub) struct Expected {
    pub(in crate::hub) demand: Demand,
    pub(in crate::hub) secs: Option<f64>,
    pub(in crate::hub) level: Level,
    pub(in crate::hub) oom: u32,
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
        }
    }
}

/// One run as the worker measured it: peak bytes, CPU and running
/// milliseconds, and whether it succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct Run(u64, u64, u64, bool);

impl Run {
    /// None where the worker measured nothing (it reports 0 then).
    fn of(u: &BuildUsage, ok: bool) -> Option<Self> {
        let running = u.wall_ms.saturating_sub(u.frozen_ms);
        (u.peak_memory_bytes > 0).then_some(Self(
            u.peak_memory_bytes,
            u.cpu_usec / 1000,
            running,
            ok,
        ))
    }

    /// Cores while it ran, and running seconds: succeeded runs with
    /// running time only. A worker can report no wall time (one failed run did on
    /// 2026-11-04); such a run would count as infinitely many cores, or
    /// with no CPU time as NaN, negative on x86 and least full in a rank.
    #[allow(clippy::cast_precision_loss)]
    fn timed(self) -> Option<(f64, f64)> {
        let Self(_, cpu, running, ok) = self;
        (ok && running > 0).then(|| (cpu as f64 / running as f64, running as f64 / 1000.0))
    }
}

/// The runs of a key, prefix or class, newest last.
#[derive(Clone, Default, Serialize, Deserialize)]
struct Runs {
    runs: VecDeque<Run>,
    /// OOM-killed runs ever.
    #[serde(default)]
    oom: u32,
}

#[allow(clippy::cast_precision_loss)]
fn median(mut xs: Vec<f64>) -> Option<f64> {
    xs.sort_by(f64::total_cmp);
    let n = xs.len();
    (n > 0).then(|| {
        if n % 2 == 1 {
            xs[n / 2]
        } else {
            f64::midpoint(xs[n / 2 - 1], xs[n / 2])
        }
    })
}

impl Runs {
    /// Add `run`, keeping the last `keep`: fewer than a loaded window
    /// held, if it was written with another `keep`.
    fn push(&mut self, run: Run, oom: bool, keep: usize) {
        while self.runs.len() >= keep {
            self.runs.pop_front();
        }
        self.runs.push_back(run);
        self.oom = self.oom.saturating_add(u32::from(oom));
    }

    /// Medians of cores and running seconds over the runs that
    /// succeeded, if one did.
    fn timing(&self) -> Option<(f64, f64)> {
        let timed: Vec<(f64, f64)> = self.runs.iter().filter_map(|r| r.timed()).collect();
        let cores = median(timed.iter().map(|t| t.0).collect())?;
        Some((cores, median(timed.iter().map(|t| t.1).collect())?))
    }

    /// Peaks as `stat` sees them, cores and seconds; a level without a
    /// succeeded run takes `fallback`'s cores and no seconds.
    #[allow(clippy::cast_precision_loss)]
    fn expected(&self, level: Level, peak: fn(&[u64]) -> u64, fallback: f64) -> Option<Expected> {
        let peaks: Vec<u64> = self.runs.iter().map(|r| r.0).collect();
        if peaks.is_empty() {
            return None;
        }
        let (cores, secs) = self
            .timing()
            .map_or((fallback, None), |(c, s)| (c, Some(s)));
        Some(Expected {
            demand: Demand {
                cores,
                memory: peak(&peaks) as f64,
            },
            secs,
            level,
            oom: self.oom,
        })
    }
}

fn largest(peaks: &[u64]) -> u64 {
    peaks.iter().copied().max().unwrap_or(0)
}

fn p90(peaks: &[u64]) -> u64 {
    let mut s = peaks.to_vec();
    s.sort_unstable();
    s[(s.len() * 9 / 10).min(s.len() - 1)]
}

/// The pname: the name up to the first dash-part that does not start
/// with a letter, where Nix's DrvName cuts off the version (names.cc),
/// or that is a hash (hashlike). On the journal above, as system and
/// pname: Nix's cut makes 9,705 keys, 4,096 one-off `tu-<hash>` names;
/// this makes 5,158, 19 pooling Nix's pnames: 18 by a hash (`tu-<hex>`
/// under `tu`), one by a trailing dash, which Nix keeps and this cuts.
pub(in crate::hub) fn pname(name: &str) -> &str {
    let mut end = name.find('-').unwrap_or(name.len());
    for part in name[end..].split('-').skip(1) {
        let letter = part.starts_with(|c: char| c.is_ascii_alphabetic());
        if !letter || hashlike(part) {
            break;
        }
        end += 1 + part.len();
    }
    &name[..end]
}

/// A hash: eight or more hex digits, a digit among them. Not a word with
/// a digit in it, such as `cuda13`, `chacha20` or `secp256k1`. A git
/// short hash of 7 stays in the pname: its builds meet under the prefix
/// before it.
fn hashlike(part: &str) -> bool {
    let hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f');
    part.len() >= 8 && part.bytes().any(|c| c.is_ascii_digit()) && part.bytes().all(hex)
}

/// The dash-prefixes of a pname, longest first, the pname excluded.
fn prefixes(pname: &str) -> impl Iterator<Item = &str> {
    pname.rmatch_indices('-').map(move |(i, _)| &pname[..i])
}

/// What a build is, as history keys it.
pub(in crate::hub) struct Build<'a> {
    pub(in crate::hub) system: &'a str,
    /// Its derivation name, if one could be read.
    pub(in crate::hub) name: Option<&'a str>,
    pub(in crate::hub) declared: bool,
}

/// Entries are never dropped (the previous format's file on the farm's
/// hub held 15,293 keys): seeded from the journal above, 5,082 keys and
/// 14,865 entries (prefixes and classes too), 1,901,745 bytes of JSON.
/// Its one class is x86_64-linux/false: none of the journal's dispatch
/// lines named features, so no build was seen to declare big-parallel.
/// Re-measure the file and its save once "placement history loaded"
/// counts ten times as many entries.
#[derive(Clone, Default, Serialize, Deserialize)]
pub(in crate::hub) struct History {
    /// "system/pname" to its runs.
    keys: HashMap<String, Runs>,
    /// "system/prefix" to the runs of every pname that is or starts
    /// with it, at a dash.
    prefixes: HashMap<String, Runs>,
    /// "system/declared" (true or false) to the class's runs.
    classes: HashMap<String, Runs>,
    #[serde(skip)]
    dirty: bool,
    /// How many copies `changed` has taken.
    #[serde(skip)]
    copies: u64,
}

impl History {
    pub(in crate::hub) fn expect(&self, b: &Build) -> Expected {
        let class = self.classes.get(&format!("{}/{}", b.system, b.declared));
        let prior = Expected::prior(b.declared);
        let fallback = class
            .and_then(Runs::timing)
            .map_or(prior.demand.cores, |t| t.0);
        let pname = b.name.map(pname);
        let key = pname.and_then(|p| self.keys.get(&format!("{}/{p}", b.system)));
        let by_prefix =
            || prefixes(pname?).find_map(|pre| self.prefixes.get(&format!("{}/{pre}", b.system)));
        key.and_then(|r| r.expected(Level::Key, largest, fallback))
            .or_else(|| by_prefix()?.expected(Level::Prefix, largest, fallback))
            .or_else(|| class?.expected(Level::Class, p90, fallback))
            .unwrap_or(prior)
    }

    /// Learn from a build's run: the worker's usage and its exit code. A
    /// failed run teaches nothing (in the replay, learning failed runs'
    /// peaks lengthened waits), an OOM-killed one its peak, as a lower
    /// bound. Each delivered result is a run: on the journal above, one
    /// build of 360,131 results was delivered twice, under two ids: the
    /// watchdog restarted the hub, the build was submitted anew, and its
    /// worker resumed it.
    pub(in crate::hub) fn record(&mut self, b: &Build, exit_code: i32, u: &BuildUsage) {
        let oom = u.oom_killed;
        if exit_code != 0 && !oom {
            return;
        }
        let Some(run) = Run::of(u, !oom) else {
            return;
        };
        let class = format!("{}/{}", b.system, b.declared);
        self.classes
            .entry(class)
            .or_default()
            .push(run, oom, CLASS_WINDOW);
        if let Some(p) = b.name.map(pname) {
            let key = format!("{}/{p}", b.system);
            self.keys.entry(key).or_default().push(run, oom, WINDOW);
            for pre in iter::once(p).chain(prefixes(p)) {
                self.prefixes
                    .entry(format!("{}/{pre}", b.system))
                    .or_default()
                    .push(run, oom, WINDOW);
            }
        }
        self.dirty = true;
    }

    /// The history at `path`, empty where there is no file; an error
    /// where it cannot be read or parsed.
    pub(in crate::hub) fn load(path: &Path) -> io::Result<Self> {
        let parsed = fs::read_to_string(path)
            .and_then(|text| serde_json::from_str(&text).map_err(io::Error::other));
        match parsed {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            parsed => parsed,
        }
    }

    /// A copy to save, if it changed since the last, numbered after the
    /// last: a later copy has the higher number, and write_newest keeps
    /// it. Numbered with the copy, under whatever lock guards the history
    /// (the placement lock), and not by a counter of its own beside it.
    pub(in crate::hub) fn changed(&mut self) -> Option<(u64, Self)> {
        mem::take(&mut self.dirty).then(|| {
            self.copies += 1;
            (self.copies, self.clone())
        })
    }

    /// Its keys, and its entries: keys, prefixes and classes.
    pub(in crate::hub) fn len(&self) -> (usize, usize) {
        let entries = self.keys.len() + self.prefixes.len() + self.classes.len();
        (self.keys.len(), entries)
    }
}

#[cfg(test)]
#[path = "history/tests.rs"]
mod tests;
