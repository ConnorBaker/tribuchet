//! How fast each worker runs a build, against the farm's typical worker:
//! its speed factor, learned from the build record.
//!
//! The model is that of uniformly related machines (Q||Cmax, Graham et
//! al. 1979): build b takes p_b / s_w on worker w, a time of its own over
//! a speed of the worker's, the same for every build. Machines whose
//! ratio differs by build (unrelated machines, R||) would need a speed
//! per key and worker; the record has runs of one key on two workers
//! for too few keys to learn that, and a per-worker factor is what an
//! earliest-finish-time rule (HEFT, Topcuoglu et al. 2002) takes.
//!
//! In logs, ln t = ln p_b - x_w + noise, with x_w = ln s_w. Two runs of
//! the same key on workers a and b compare them, p_b cancelling:
//! ln t_a - ln t_b = x_b - x_a. Each run that teaches speed (below) is
//! compared with each other worker's latest such run of its key that
//! came since this worker's previous one, so each pair of runs is
//! compared once, and the comparisons are summed per pair of workers.
//! The factors are then those minimising the squared error of every
//! comparison plus PRIOR times the sum of squared x: ridge regression
//! toward the prior x = 0, the factor 1 of a worker never compared,
//! shrinking a worker with few comparisons toward it. With every x
//! pulled toward 0, factor 1 is the mean, in ln, of each group of
//! workers connected by comparisons (directly or through others),
//! retired ones included: a factor is relative to its group, and a
//! worker never compared is a group of one.
//!
//! Two departures from plain least squares, each chosen by a replay of
//! the record (DESIGN.md, Worker speed). A comparison is clamped to
//! within BOUND of the fit when it is made, so that a run timed at 1 ms
//! or a pname pooling unlike inputs moves the fit by a bounded step (the
//! deviations' tails are heavy: a standard deviation of 2.0 in ln, a
//! median of 0.17). Unlike a Huber M-estimate, a comparison is not
//! revisited as the fit moves: the result depends on the runs' order,
//! which the fold keeps. And each pair's comparisons decay with a window
//! of WINDOW, counted in comparisons of either of its workers, so that a
//! worker that changes is relearned: a pair no longer compared, as of a
//! worker retired, fades as its other worker is compared with others,
//! and cannot hold that one at an old ratio through a cycle of pairs.
//! Runs that compare nothing age nothing. The tests pin what the clamp,
//! the window and the prior do; only the replay chose their values. The
//! fit is renewed once per
//! as many taught runs as there are workers, so its cost per run grows
//! with the square of the workers, not their cube.
//!
//! A run teaches speed only if its dispatch was quiet (`Placed`): it
//! succeeded with running time, and its dispatch was quiet and not
//! resumed, by the hub or by the worker's answer. The hub records the
//! decision beside the result (record::TAUGHT). Quiet is tested once, when the build is dispatched, on what
//! the hub knows then: the expected cores of the builds committed to the
//! worker, not what they used. Builds dispatched to the worker later,
//! while the run goes on, are not seen, so a busy worker's runs teach
//! under load the test missed, and it reads slower than it is. Taught
//! from every run, contention reads as speed: on the replay a worker
//! goes from the slowest to faster than typical (DESIGN.md). The rule
//! assumes a decider that adds a worker's load to its speed; none does
//! yet, and until one does, factors taught from every run predict
//! running times better on the replay.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

/// The other builds' expected cores, as a fraction of the worker's
/// cores, at most which a dispatch is quiet. The hub knows its builds'
/// expected cores, not what they use: the threshold is applied to those.
pub(in crate::hub) const QUIET: f64 = 0.25;

/// The weight of the prior, in comparisons: one comparison's variance
/// over the prior's, a prior of 0.25 covering the compared workers' ln
/// factors. Clamped at BOUND, one deviates from the fit with a standard
/// deviation of 0.281 in ln on the replay (DESIGN.md); 1.17 is
/// (0.27 / 0.25)^2 = 1.1664, rounded, from an earlier fit's 0.27,
/// truncated. 1.22 and 1.32 replay alike.
const PRIOR: f64 = 1.17;

/// How far from the fit a comparison counts, in ln: one beyond is
/// clamped to this far. 0.5 replays a little better than 1.
pub(in crate::hub) const BOUND: f64 = 0.5;

/// The comparisons a pair of workers is fitted to, by weight: each
/// comparison either worker takes part in weighs the pair's earlier ones
/// by (1 - 1/WINDOW)^(1/2), so that a pair compared only with itself
/// decays by 1 - 1/WINDOW a comparison, and its comparisons weigh e^-3
/// after 3 WINDOW more. The value is the replay's (DESIGN.md, Worker
/// speed).
const WINDOW: f64 = 1000.0;

/// Whether a dispatch is quiet: the other builds committed to the
/// worker, `load` less this build's `expect`, expect at most QUIET of
/// its `cores`: the values of the dispatch's record row, from which a
/// record's migration computes it alike (record::TAUGHT).
pub(in crate::hub) fn quiet(load: f64, expect: f64, cores: f64) -> bool {
    load - expect <= QUIET * cores
}

/// A build's dispatch as speed reads it: whether its run teaches speed,
/// the dispatch quiet and not resumed, by the hub or by the worker's
/// answer (a resumed dispatch may hand over a run that ended before it,
/// under a load it did not see). The result's record row keeps it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::hub) struct Placed {
    pub(in crate::hub) quiet: bool,
}

/// A worker's latest quiet run of a key: the worker, ln running
/// seconds, and which of the key's quiet runs it was.
type Latest = (u16, f64, u64);

/// A pair of workers' summed comparisons and their weight, as of
/// `clock`, the comparisons its two workers had taken part in then
/// (Speed::aged).
#[derive(Clone, Copy, Default)]
struct Pair {
    sum: f64,
    weight: f64,
    clock: u64,
}

/// The speed factors, learned from comparisons of quiet runs.
#[derive(Clone, Default)]
pub(in crate::hub) struct Speed {
    /// Each worker's name, by its index.
    names: Vec<String>,
    index: HashMap<String, u16>,
    /// Per key, its quiet runs counted, and each worker's latest.
    latest: HashMap<String, (u64, Vec<Latest>)>,
    /// Per pair of workers (a, b), a < b: the sum of ln t_a - ln t_b
    /// over their comparisons and their weight, decayed by WINDOW, as of
    /// the pair's clock then (Pair). Ordered, so that the solve sums
    /// alike in every hub, and a fold equals what was learned.
    pairs: BTreeMap<(u16, u16), Pair>,
    /// The comparisons each worker took part in, by index: a pair's
    /// clock is the sum of its two workers'.
    compared: Vec<u64>,
    /// x, by worker index, once solved since the fit was last renewed.
    solved: OnceLock<Vec<f64>>,
    /// Runs learned since the fit was last renewed.
    unfitted: usize,
}

impl Speed {
    /// The index of worker `name`, added if new.
    pub(super) fn worker(&mut self, name: &str) -> u16 {
        if let Some(&i) = self.index.get(name) {
            return i;
        }
        let i = u16::try_from(self.names.len()).unwrap_or(u16::MAX);
        self.names.push(name.to_owned());
        self.compared.push(0);
        self.index.insert(name.to_owned(), i);
        i
    }

    /// Learn from a quiet run of `key` on worker `w`, of `secs` running
    /// seconds: compare it with each other worker's latest run of the key
    /// that came since w's previous one, so that each pair of runs is
    /// compared once, each comparison clamped to within BOUND of the fit.
    /// The fit is renewed once per as many runs as there are workers, so
    /// that solving it costs O(workers^2) a run.
    pub(super) fn learn(&mut self, key: &str, w: u16, secs: f64) {
        let t = secs.ln();
        // The fit as of before this run, as the hub may read it between.
        let x = self.solve().to_vec();
        let x = |i: u16| x.get(usize::from(i)).copied().unwrap_or(0.0);
        if !self.latest.contains_key(key) {
            self.latest.insert(key.to_owned(), Default::default());
        }
        let (count, latest) = self.latest.get_mut(key).expect("inserted");
        let since = latest.iter().find(|l| l.0 == w).map(|l| l.2);
        // w's own latest is its previous run: not since it.
        let new = |l: &&Latest| since.is_none_or(|s| l.2 > s);
        let others: Vec<(u16, f64)> = latest.iter().filter(new).map(|l| (l.0, l.1)).collect();
        *count += 1;
        let count = *count;
        match latest.iter_mut().find(|l| l.0 == w) {
            Some(l) => (l.1, l.2) = (t, count),
            None => latest.push((w, t, count)),
        }
        for (v, tv) in others {
            let (pair, diff) = if w < v {
                ((w, v), t - tv)
            } else {
                ((v, w), tv - t)
            };
            let fit = x(pair.1) - x(pair.0);
            let diff = fit + (diff - fit).clamp(-BOUND, BOUND);
            let now = self.clock(pair);
            let (sum, weight) = self
                .pairs
                .get(&pair)
                .map_or((0.0, 0.0), |p| self.aged(pair, p));
            let p = Pair {
                sum: sum + diff,
                weight: weight + 1.0,
                clock: now,
            };
            self.pairs.insert(pair, p);
            for i in [w, v] {
                if let Some(n) = self.compared.get_mut(usize::from(i)) {
                    *n += 1;
                }
            }
        }
        self.unfitted += 1;
        if self.unfitted >= self.names.len() {
            (self.unfitted, self.solved) = (0, OnceLock::new());
        }
    }

    /// The comparisons `pair`'s two workers took part in: its clock now.
    fn clock(&self, (a, b): (u16, u16)) -> u64 {
        let n = |i: u16| self.compared.get(usize::from(i)).copied().unwrap_or(0);
        n(a) + n(b)
    }

    /// `pair`'s summed comparisons and weight now: as of its clock,
    /// weighed by (1 - 1/WINDOW)^(1/2) for each comparison either worker
    /// took part in since.
    fn aged(&self, pair: (u16, u16), p: &Pair) -> (f64, f64) {
        let since = self.clock(pair).saturating_sub(p.clock);
        let since = i32::try_from(since).unwrap_or(i32::MAX);
        let keep = (1.0 - 1.0 / WINDOW).sqrt().powi(since);
        (keep * p.sum, keep * p.weight)
    }

    /// The factor of the worker of index `w`; 1 for None, a run whose
    /// worker the record does not name.
    pub(super) fn of(&self, w: Option<u16>) -> f64 {
        let x = self.solve();
        w.and_then(|w| x.get(usize::from(w)))
            .map_or(1.0, |x| x.exp())
    }

    /// The name of the worker of index `w`.
    #[cfg(test)]
    pub(super) fn name(&self, w: u16) -> &str {
        &self.names[usize::from(w)]
    }

    /// Worker `name`'s speed factor: 1 for a worker never compared.
    pub(in crate::hub) fn factor(&self, name: &str) -> f64 {
        self.of(self.index.get(name).copied())
    }

    /// Each worker's name, factor and comparisons' decayed weight, by name.
    pub(in crate::hub) fn factors(&self) -> Vec<(String, f64, f64)> {
        let mut n = vec![0.0; self.names.len()];
        for (&pair, p) in &self.pairs {
            let (_, c) = self.aged(pair, p);
            n[usize::from(pair.0)] += c;
            n[usize::from(pair.1)] += c;
        }
        let mut all: Vec<_> = (self.names.iter().enumerate())
            .map(|(i, name)| (name.clone(), self.of(u16::try_from(i).ok()), n[i]))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }

    /// x: the solution of (PRIOR I + L) x = r, L the comparisons'
    /// weighted Laplacian, r each worker's summed differences. The
    /// matrix is symmetric positive definite, so elimination needs no
    /// pivoting.
    fn solve(&self) -> &[f64] {
        self.solved.get_or_init(|| {
            let size = self.names.len();
            let mut matrix = vec![vec![0.0; size]; size];
            let mut rhs = vec![0.0; size];
            for (i, row) in matrix.iter_mut().enumerate() {
                row[i] = PRIOR;
            }
            for (&pair, p) in &self.pairs {
                let (sum, count) = self.aged(pair, p);
                let (a, b) = (usize::from(pair.0), usize::from(pair.1));
                matrix[a][a] += count;
                matrix[b][b] += count;
                matrix[a][b] -= count;
                matrix[b][a] -= count;
                // sum is of x_b - x_a: a's x lower, b's higher.
                rhs[a] -= sum;
                rhs[b] += sum;
            }
            for k in 0..size {
                let pivot = matrix[k].clone();
                for i in k + 1..size {
                    let f = matrix[i][k] / pivot[k];
                    for (cell, p) in matrix[i][k..].iter_mut().zip(&pivot[k..]) {
                        *cell -= f * p;
                    }
                    rhs[i] -= f * rhs[k];
                }
            }
            let mut ln_speed = vec![0.0; size];
            for k in (0..size).rev() {
                let known: f64 = (k + 1..size).map(|j| matrix[k][j] * ln_speed[j]).sum();
                ln_speed[k] = (rhs[k] - known) / matrix[k][k];
            }
            ln_speed
        })
    }
}

#[cfg(test)]
#[path = "speed/tests.rs"]
mod tests;
