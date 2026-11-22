//! One run as the worker measured it, the runs a history entry keeps,
//! and what they expect of a build (History::expect).

use std::collections::VecDeque;

use super::super::speed::Speed;
use super::{Demand, Expected, Level, WINDOW};
use crate::proto::BuildUsage;

/// One run as the worker measured it: peak bytes, CPU and running
/// milliseconds, whether it succeeded, and its worker's index in Speed
/// where the record names it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Run(
    pub(super) u64,
    pub(super) u64,
    pub(super) u64,
    pub(super) bool,
    pub(super) Option<u16>,
);

impl Run {
    /// None where the worker measured nothing (it reports 0 then).
    pub(super) fn of(u: &BuildUsage, ok: bool, worker: Option<u16>) -> Option<Self> {
        let running = u.wall_ms.saturating_sub(u.frozen_ms);
        (u.peak_memory_bytes > 0).then_some(Self(
            u.peak_memory_bytes,
            u.cpu_usec / 1000,
            running,
            ok,
            worker,
        ))
    }

    /// Cores while it ran, and running seconds: succeeded runs with
    /// running time only. A worker can report no wall time (one failed run did on
    /// 2026-11-04); such a run would count as infinitely many cores, or
    /// with no CPU time as NaN, negative on x86 and least full in a rank.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn timed(self) -> Option<(f64, f64)> {
        let Self(_, cpu, running, ok, _) = self;
        (ok && running > 0).then(|| (cpu as f64 / running as f64, running as f64 / 1000.0))
    }
}

/// The runs of a key, prefix or class, newest last.
#[derive(Clone, Default)]
pub(super) struct Runs {
    pub(super) runs: VecDeque<Run>,
    /// OOM-killed runs ever recorded for the entry.
    pub(super) oom: u32,
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
    /// Add `run`, keeping the last `keep`.
    pub(super) fn push(&mut self, run: Run, oom: bool, keep: usize) {
        if self.runs.len() >= keep {
            self.runs.pop_front();
        }
        self.runs.push_back(run);
        self.oom = self.oom.saturating_add(u32::from(oom));
    }

    /// Add `run` to a key's or a prefix's last WINDOW, whose memory is a
    /// largest peak. A censored run peaking no higher than a success there
    /// is not added, which would push measured runs out; the smallest such
    /// success moves to the newest place instead, so a peak no lower than
    /// the censored one stays as long as that run would have.
    pub(super) fn learn(&mut self, run: Run, oom: bool) {
        let covers = |r: &&Run| r.3 && r.0 >= run.0;
        let cover = (self.runs.iter().enumerate())
            .filter(|(_, r)| !run.3 && covers(r))
            .min_by_key(|(_, r)| r.0)
            .map(|(i, _)| i);
        match cover.and_then(|i| self.runs.remove(i)) {
            Some(r) => {
                self.runs.push_back(r);
                self.oom = self.oom.saturating_add(u32::from(oom));
            }
            None => self.push(run, oom, WINDOW),
        }
    }

    /// Medians of cores and running seconds over the runs that
    /// succeeded, if one did.
    pub(super) fn timing(&self) -> Option<(f64, f64)> {
        let timed: Vec<(f64, f64)> = self.runs.iter().filter_map(|r| r.timed()).collect();
        let cores = median(timed.iter().map(|t| t.0).collect())?;
        let secs = median(timed.iter().map(|t| t.1).collect())?;
        Some((cores, secs))
    }

    /// The median over the runs that succeeded of each one's running
    /// seconds times its worker's factor in `speed`: the seconds on a
    /// worker of factor 1, read with the factors as they are now.
    pub(super) fn unit_secs(&self, speed: &Speed) -> Option<f64> {
        let unit = |r: &Run| r.timed().map(|(_, s)| s * speed.of(r.4));
        median(self.runs.iter().filter_map(unit).collect())
    }

    /// The peak `level` takes, cores and seconds; a level without a
    /// succeeded run takes `fallback`'s cores and no seconds. Sources and
    /// a key take their largest peak, censored or not (their own runs: a
    /// bound); a prefix, the largest of the peaks that succeeded, with
    /// none, its largest (a stem's peak censored above its siblings' is
    /// its own bound, no other stem's guess, as a runaway's); a class, its
    /// 90th percentile (quantile).
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn expected(&self, level: Level, fallback: f64) -> Option<Expected> {
        let largest = |succeeded: bool| {
            (self.runs.iter().filter(|r| r.3 || !succeeded))
                .map(|r| r.0)
                .max()
        };
        let peak = match level {
            Level::Source | Level::Key => largest(false),
            Level::Prefix => largest(true).or_else(|| largest(false)),
            Level::Class | Level::Prior => quantile(&self.runs, 0.9),
        }?;
        let (cores, secs) = self
            .timing()
            .map_or((fallback, None), |(c, s)| (c, Some(s)));
        Some(Expected {
            demand: Demand {
                cores,
                memory: peak as f64,
            },
            secs,
            level,
            oom: self.oom,
            below: 0.0,
        })
    }
}

/// The `q`-quantile of peaks by Kaplan-Meier (JASA 53, 1958), a run that
/// did not succeed right-censored at its peak, a success first at a tie.
/// Where censoring leaves it short of `q`, the `q`-quantile of all peaks,
/// a lower bound, as each build needs at least its peak, and no lower
/// than the last success: the quantile is above it. Uncensored, the
/// k-th smallest peak for the least k with k/n above `q`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(super) fn quantile(runs: &VecDeque<Run>, q: f64) -> Option<u64> {
    let mut obs: Vec<(u64, bool)> = runs.iter().map(|r| (r.0, r.3)).collect();
    obs.sort_by_key(|&(peak, ok)| (peak, !ok));
    let (n, mut survives) = (obs.len(), 1.0);
    for (i, &(peak, ok)) in obs.iter().enumerate() {
        if ok {
            survives *= 1.0 - 1.0 / (n - i) as f64;
            if survives <= 0.0 || 1.0 - survives > q + 1e-9 {
                return Some(peak);
            }
        }
    }
    let k = ((q + 1e-9) * n as f64).floor() as usize;
    let last = obs.iter().filter(|o| o.1).map(|o| o.0).max();
    obs.get(k.min(n.saturating_sub(1)))
        .map(|o| o.0.max(last.unwrap_or(0)))
}
