//! A tool, not a check (ignored, it asserts nothing): a replay of a build
//! record by the fold (each_run). Each succeeded run is predicted before
//! it is learned, with worker speed (Expected::secs_on) and without
//! (every run a factor-1 worker's), and the log errors are summed,
//! overall, for quiet runs and per worker; then one comparison's
//! deviation from the fit, by the runs' length. Its settings, from the
//! environment:
//! - TRIBUCHET_REPLAY_RECORD: the record's path;
//! - TRIBUCHET_REPLAY_SKIP: `from-to` in unix ms, a window passed over;
//! - TRIBUCHET_REPLAY_SYNTH: `worker=s,...`, those workers' running times
//!   divided by s (a synthetic mix of speeds); `worker=s@ms`, from unix
//!   ms on only (a worker that changes), the factors then traced;
//! - TRIBUCHET_REPLAY_QUIET=all: every run taught as quiet (an ablation).
//!
//! `cargo test --release -p tribuchet --bin tribuchet -- --ignored --exact
//! hub::record::replay::replay --nocapture`

use std::collections::HashMap;
use std::env;

use super::super::placement::{BOUND, pname};
use super::*;

/// Per worker, its latest quiet run's ln seconds and which of the
/// key's quiet runs it was.
type Latest = HashMap<String, (f64, u64)>;

/// Sums of log errors, with speed (0) and without (1).
#[derive(Default)]
struct Tally {
    n: f64,
    hours: f64,
    abs: [f64; 2],
    within2: [f64; 2],
    abs_hw: [f64; 2],
}

impl Tally {
    fn add(&mut self, secs: f64, predicted: [f64; 2]) {
        self.n += 1.0;
        self.hours += secs / 3600.0;
        for (i, p) in predicted.into_iter().enumerate() {
            let e = (p / secs).ln().abs();
            self.abs[i] += e;
            self.within2[i] += f64::from(u8::from(e <= 2f64.ln()));
            self.abs_hw[i] += e * secs / 3600.0;
        }
    }

    fn show(&self, name: &str) {
        let cut = |a: [f64; 2]| 100.0 * (a[1] - a[0]) / a[1];
        println!(
            "{name}: n={} hours={:.1} mean|ln err| {:.4} vs {:.4} (cut {:.2}%), \
             within 2x {:.2}% vs {:.2}%, hour-weighted {:.4} vs {:.4} (cut {:.2}%)",
            self.n,
            self.hours,
            self.abs[0] / self.n,
            self.abs[1] / self.n,
            cut(self.abs),
            100.0 * self.within2[0] / self.n,
            100.0 * self.within2[1] / self.n,
            self.abs_hw[0] / self.hours,
            self.abs_hw[1] / self.hours,
            cut(self.abs_hw),
        );
    }
}

#[test]
#[ignore = "replays the build record TRIBUCHET_REPLAY_RECORD names"]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn replay() {
    let path = env::var("TRIBUCHET_REPLAY_RECORD").expect("TRIBUCHET_REPLAY_RECORD");
    let skip: Option<(i64, i64)> = env::var("TRIBUCHET_REPLAY_SKIP").ok().map(|s| {
        let (a, b) = s.split_once('-').expect("from-to");
        (a.parse().unwrap(), b.parse().unwrap())
    });
    let synth: HashMap<String, (f64, i64)> = (env::var("TRIBUCHET_REPLAY_SYNTH"))
        .unwrap_or_default()
        .split(',')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| {
            let (s, from) = v.split_once('@').unwrap_or((v, "0"));
            (k.to_owned(), (s.parse().unwrap(), from.parse().unwrap()))
        })
        .collect();
    let change = synth.values().map(|s| s.1).filter(|&f| f > 0).min();
    let mut traced: Option<(HashMap<String, f64>, u64, u64)> = None;
    let all_quiet = env::var("TRIBUCHET_REPLAY_QUIET").is_ok_and(|q| q == "all");
    let (mut with, mut without) = (History::default(), History::default());
    // Taught from every run: against `with`, scored on the same runs.
    let mut every = History::default();
    let (mut q_vs_every_all, mut q_vs_every_quiet) = (Tally::default(), Tally::default());
    let (mut all, mut quiet) = (Tally::default(), Tally::default());
    let mut workers: HashMap<String, Tally> = HashMap::new();
    // Per key, its quiet runs counted and each worker's latest quiet ln t
    // and its count: one comparison's spread, each pair of runs once.
    let mut latest: HashMap<String, (u64, Latest)> = HashMap::new();
    let mut diffs: Vec<(String, String, f64, f64)> = Vec::new();
    let conn = reader(Path::new(&path)).unwrap();
    each_run(&conn, |mut run| {
        if skip.is_some_and(|(a, b)| (a..b).contains(&run.at_ms)) {
            return;
        }
        let u = &mut run.usage;
        if let Some(at) = change.filter(|&at| run.at_ms >= at) {
            // Each changed worker's factor over the geometric mean of the
            // compared workers that did not change, against that at the
            // change: it should reach s.
            let relative = || -> HashMap<String, f64> {
                let speeds = with.speeds();
                let others = speeds
                    .iter()
                    .filter(|s| s.2 > 0.0 && !synth.contains_key(&s.0));
                let (sum, n) = others.fold((0.0, 0.0), |(t, n), s| (t + s.1.ln(), n + 1.0));
                let mean = (sum / n).exp();
                let changed = speeds.iter().filter(|s| synth.contains_key(&s.0));
                changed.map(|s| (s.0.clone(), s.1 / mean)).collect()
            };
            let t = traced.get_or_insert_with(|| (relative(), 0, 0));
            t.1 += 1;
            let taught = run.on.as_ref().filter(|on| on.taught);
            t.2 += u64::from(taught.is_some_and(|on| synth.contains_key(on.worker)));
            if t.1.is_multiple_of(5000) {
                let hours = (run.at_ms - at) as f64 / 3.6e6;
                for (w, r) in relative() {
                    let r = r / t.0.get(&w).copied().unwrap_or(1.0);
                    println!("trace h={hours:.1} runs={} taught={} {w} {r:.4}", t.1, t.2);
                }
            }
        }
        let synth_on = (run.on.as_ref()).and_then(|on| synth.get(on.worker));
        if let Some(&(s, _)) = synth_on.filter(|(_, from)| run.at_ms >= *from) {
            let running = u.wall_ms.saturating_sub(u.frozen_ms) as f64 / s;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let running = running as u64;
            u.wall_ms = running + u.frozen_ms;
        }
        if let Some(on) = run.on.as_mut().filter(|_| all_quiet) {
            on.taught = true;
        }
        let u = run.usage;
        let secs = u.wall_ms.saturating_sub(u.frozen_ms) as f64 / 1000.0;
        let scored = run.exit_code == 0 && !u.oom_killed && secs > 0.0 && u.peak_memory_bytes > 0;
        if let (Some(on), Some(name), true) = (run.on, run.build.name, scored) {
            let ew = with.expect(&run.build).secs_on(with.speed(on.worker));
            let ea = every.expect(&run.build).secs_on(every.speed(on.worker));
            if let (Some(pw), Some(pa)) = (ew, ea) {
                q_vs_every_all.add(secs, [pw, pa]);
                if on.taught {
                    q_vs_every_quiet.add(secs, [pw, pa]);
                }
            }
            if let (Some(pw), Some(po)) = (ew, without.expect(&run.build).secs_on(1.0)) {
                all.add(secs, [pw, po]);
                workers
                    .entry(on.worker.to_owned())
                    .or_default()
                    .add(secs, [pw, po]);
                if on.taught {
                    quiet.add(secs, [pw, po]);
                }
            }
            if on.taught {
                let key = format!("{}/{}", run.build.system, pname(name));
                let (count, l) = latest.entry(key).or_default();
                let since = l.get(on.worker).map(|e| e.1);
                for (v, (tv, _)) in l.iter().filter(|(_, e)| since.is_none_or(|s| e.1 > s)) {
                    diffs.push((
                        on.worker.to_owned(),
                        v.clone(),
                        secs.ln() - tv,
                        secs.ln().min(*tv),
                    ));
                }
                *count += 1;
                l.insert(on.worker.to_owned(), (secs.ln(), *count));
            }
        }
        with.record(&run.build, run.exit_code, &u, run.on);
        without.record(&run.build, run.exit_code, &u, None);
        let on_every = run.on.map(|on| On { taught: true, ..on });
        every.record(&run.build, run.exit_code, &u, on_every);
    })
    .unwrap();
    println!("skip {skip:?} synthetic {synth:?} all quiet {all_quiet}");
    all.show("all runs");
    quiet.show("quiet runs");
    q_vs_every_all.show("quiet-taught vs every-taught, all runs");
    q_vs_every_quiet.show("quiet-taught vs every-taught, quiet runs");
    for (w, f, c) in every.speeds() {
        println!("every-taught speed {w} {f:.4} comparisons {c}");
    }
    let mut by: Vec<_> = workers.iter().collect();
    by.sort_by(|a, b| a.0.cmp(b.0));
    for (w, t) in by {
        t.show(&format!("worker {w}"));
    }
    let x: HashMap<String, f64> = (with.speeds().into_iter())
        .inspect(|(w, f, c)| println!("speed {w} {f:.4} comparisons {c}"))
        .map(|(w, f, _)| (w, f.ln()))
        .collect();
    // One comparison's deviation from the fit, by the shorter run's
    // seconds: from 0, 0.1, 1, 10 and 100 up.
    for floor in [0.0, 0.1, 1.0, 10.0, 100.0] {
        let mut res: Vec<f64> = (diffs.iter())
            .filter(|c| c.3 >= f64::ln(floor))
            .map(|(w, v, d, _)| d - (x[v] - x[w]))
            .collect();
        let sd = (res.iter().map(|e| e * e).sum::<f64>() / res.len() as f64).sqrt();
        let bound = |e: &f64| e.clamp(-BOUND, BOUND).powi(2);
        let huber = (res.iter().map(bound).sum::<f64>() / res.len() as f64).sqrt();
        res.sort_by(|a, b| a.abs().total_cmp(&b.abs()));
        let median = res.get(res.len() / 2).map_or(0.0, |e| e.abs());
        println!(
            "comparisons of runs >= {floor} s: {} sd of one {sd:.3}, clamped at BOUND \
             {huber:.3}, median |deviation| {median:.3}",
            res.len()
        );
    }
}
