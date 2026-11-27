//! A factor never compared is exactly 1, and a median of one run exact.
#![allow(clippy::float_cmp)]

use super::super::history::{Build, History, On};
use super::*;
use crate::proto::BuildUsage;

#[path = "tests/length.rs"]
mod length;

fn build(name: &str) -> Build<'_> {
    Build {
        system: "x86_64-linux",
        name: Some(name),
        declared: false,
        sources: None,
    }
}

/// A succeeded run of one core for `secs` seconds.
fn usage(secs: f64) -> BuildUsage {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let ms = (secs * 1000.0).round() as u64;
    BuildUsage {
        wall_ms: ms,
        cpu_usec: ms * 1000,
        peak_memory_bytes: 1 << 30,
        oom_killed: false,
        frozen_ms: 0,
        profile: None,
    }
}

/// Runs of `name` alternating between workers `a` and `b`, `rounds`
/// each, taking `secs` over each worker's true speed, with whether each
/// worker's dispatch was quiet.
fn alternate(
    h: &mut History,
    name: &str,
    secs: f64,
    a: (&str, f64, bool),
    b: (&str, f64, bool),
    rounds: u32,
) {
    for _ in 0..rounds {
        for (worker, speed, quiet) in [a, b] {
            let on = On {
                worker,
                taught: quiet,
            };
            h.record(&build(name), 0, &usage(secs / speed), Some(on));
        }
    }
}

fn ratio(h: &History, fast: &str, slow: &str) -> f64 {
    h.speed(fast) / h.speed(slow)
}

/// With nothing compared every worker has the prior's factor, 1; and a
/// worker never compared with another keeps it while others are.
#[test]
fn an_uncompared_worker_has_the_prior() {
    let mut h = History::default();
    assert_eq!(h.speed("new"), 1.0);
    alternate(
        &mut h,
        "k-1",
        100.0,
        ("a", 1.0, true),
        ("b", 2.0, true),
        1000,
    );
    h.record(
        &build("other-1"),
        0,
        &usage(5.0),
        Some(On {
            worker: "new",
            taught: true,
        }),
    );
    h.record(
        &build("other-1"),
        0,
        &usage(6.0),
        Some(On {
            worker: "new",
            taught: true,
        }),
    );
    assert_eq!(h.speed("new"), 1.0, "runs of a key no other worker ran");
    let new = h.speeds().into_iter().find(|s| s.0 == "new").unwrap();
    assert_eq!(new.2, 0.0, "a worker compared with itself");
    assert!(
        (ratio(&h, "b", "a") - 2.0).abs() < 0.1,
        "{}",
        ratio(&h, "b", "a")
    );
}

/// Few comparisons move a worker part way from the prior toward them,
/// more the more there are: b at 2x a reads 1x until the fit is renewed
/// (two taught runs after the last renewal, as many as there are
/// workers, the second of an unrelated key), then between 1x and the
/// clamped e^BOUND of one comparison, more after a second, and all but
/// 2x after many.
#[test]
fn few_comparisons_shrink_toward_the_prior() {
    let mut h = History::default();
    let (a, b) = (("a", 1.0, true), ("b", 2.0, true));
    alternate(&mut h, "k-1", 100.0, a, b, 1);
    assert_eq!(ratio(&h, "b", "a"), 1.0, "the fit not yet renewed");
    let on = Some(On {
        worker: "a",
        taught: true,
    });
    h.record(&build("j-1"), 0, &usage(100.0), on);
    let one = ratio(&h, "b", "a");
    assert!(one > 1.0 && one < BOUND.exp(), "one comparison: {one}");
    alternate(&mut h, "k-1", 100.0, a, b, 1);
    let two = ratio(&h, "b", "a");
    assert!(two > one && two < 2.0, "two comparisons: {two}, one {one}");
    alternate(&mut h, "k-1", 100.0, a, b, 2000);
    let many = ratio(&h, "b", "a");
    assert!(many > 1.95 && many < 2.0, "many: {many}");
}

/// Keys of very different lengths compare workers by their ratio alone:
/// a worker that ran only the long key is not slow for it.
#[test]
fn each_key_is_compared_with_itself() {
    let mut h = History::default();
    alternate(
        &mut h,
        "short-1",
        2.0,
        ("a", 1.0, true),
        ("b", 1.5, true),
        1000,
    );
    alternate(
        &mut h,
        "long-1",
        1000.0,
        ("b", 1.5, true),
        ("c", 3.0, true),
        1000,
    );
    alternate(
        &mut h,
        "mid-1",
        30.0,
        ("a", 1.0, true),
        ("c", 3.0, true),
        1000,
    );
    for (fast, slow, want) in [("b", "a", 1.5), ("c", "b", 2.0), ("c", "a", 3.0)] {
        let got = ratio(&h, fast, slow);
        assert!(
            (got / want - 1.0).abs() < 0.05,
            "{fast}/{slow}: {got}, not {want}"
        );
    }
}

/// A worker as fast as another whose runs were slowed by load is not
/// slow: its runs dispatched to a busy worker teach nothing of speed.
#[test]
fn a_loaded_worker_is_not_read_as_slow() {
    let mut h = History::default();
    // b's quiet runs: as fast as a.
    alternate(&mut h, "k-1", 60.0, ("a", 1.0, true), ("b", 1.0, true), 200);
    // b loaded: its runs take 3x, a's still quiet.
    alternate(
        &mut h,
        "k-1",
        60.0,
        ("a", 1.0, true),
        ("b", 1.0 / 3.0, false),
        2000,
    );
    let r = ratio(&h, "b", "a");
    assert!((r - 1.0).abs() < 0.01, "b read as {r}x a");
}

/// A dispatch is quiet with the other builds' expected cores at most a
/// quarter of the worker's.
#[test]
fn quiet_is_a_quarter_of_the_cores_at_most() {
    assert!(quiet(9.0, 1.0, 32.0), "8 others of 32");
    assert!(!quiet(9.5, 1.0, 32.0), "8.5 others of 32");
    assert!(quiet(17.8, 17.8, 32.0), "alone, however large");
}

/// A key's runs are compared with each worker's latest: a key whose
/// builds grew 10x after one run on a is not read as b being slow.
#[test]
fn comparisons_take_each_workers_latest_run() {
    let mut h = History::default();
    let on = Some(On {
        worker: "a",
        taught: true,
    });
    h.record(&build("k-1"), 0, &usage(10.0), on);
    alternate(
        &mut h,
        "k-1",
        100.0,
        ("b", 1.0, true),
        ("a", 1.0, true),
        1000,
    );
    let r = ratio(&h, "b", "a");
    assert!((r - 1.0).abs() < 0.01, "b read as {r}x a");
}

/// Histories fed the same runs of four workers have the same factors,
/// bit for bit, whatever their maps' order: the fold equals what a hub
/// learned.
#[test]
fn the_same_runs_give_the_same_factors_bit_for_bit() {
    let feed = |h: &mut History| {
        let workers = [("p", 1.0), ("q", 1.3), ("r", 0.7), ("s", 2.1)];
        for k in 0..400u32 {
            let secs = f64::from(1 + k % 17) * 7.3 + f64::from(k % 5) * 0.37;
            for i in 0..4u32 {
                let (worker, speed) = workers[usize::try_from((k * 3 + i) % 4).unwrap()];
                let jitter = 1.0 + f64::from((k * 7 + i) % 13) * 0.011;
                let on = On {
                    worker,
                    taught: true,
                };
                let run = usage(secs * jitter / speed);
                h.record(&build(&format!("k{}-1", k % 17)), 0, &run, Some(on));
            }
        }
    };
    let mut first = History::default();
    feed(&mut first);
    let bits = |h: &History| -> Vec<u64> { h.speeds().iter().map(|s| s.1.to_bits()).collect() };
    let want = bits(&first);
    for _ in 0..64 {
        let mut h = History::default();
        feed(&mut h);
        assert_eq!(bits(&h), want);
    }
}

/// The expected running time is a speed-1 worker's: each run's time
/// times its worker's factor; on a worker, that over its factor.
#[test]
fn a_build_runs_its_seconds_over_the_workers_factor() {
    let mut h = History::default();
    alternate(
        &mut h,
        "k-1",
        100.0,
        ("a", 1.0, true),
        ("b", 2.0, true),
        2000,
    );
    // Run on a only: 40 s there.
    for _ in 0..3 {
        h.record(
            &build("only-1"),
            0,
            &usage(40.0),
            Some(On {
                worker: "a",
                taught: true,
            }),
        );
    }
    let b = build("only-1");
    let (on_a, on_b) = (h.secs_on(&b, "a").unwrap(), h.secs_on(&b, "b").unwrap());
    assert!((on_a - 40.0).abs() < 0.5, "on a: {on_a}");
    assert!((on_b - 20.0).abs() < 0.5, "on b: {on_b}");
    // With no worker named, a run is a speed-1 worker's.
    h.record(&build("anon-1"), 0, &usage(40.0), None);
    assert_eq!(h.secs_on(&build("anon-1"), "new"), Some(40.0));
}

/// A build's expectation keeps its seconds as its runs ran, which the
/// dispatch row records, and on a factor-1 worker those times its
/// worker's factor.
#[test]
fn expected_secs_stay_as_the_runs_ran() {
    let mut h = History::default();
    alternate(
        &mut h,
        "k-1",
        100.0,
        ("a", 1.0, true),
        ("b", 2.0, true),
        2000,
    );
    for _ in 0..3 {
        let on = On {
            worker: "a",
            taught: true,
        };
        h.record(&build("only-1"), 0, &usage(40.0), Some(on));
    }
    let e = h.expect(&build("only-1"));
    assert_eq!(e.secs, Some(40.0));
    let unit = h.unit_secs(&build("only-1")).unwrap();
    assert!((unit - 40.0 * h.speed("a")).abs() < 1e-9, "{unit}");
}

/// The factors of uniformly related workers are recovered whatever the
/// order of their runs and however many workers compare each key.
#[test]
fn three_speeds_are_recovered() {
    let mut h = History::default();
    let workers = [("x1", 1.0), ("x1.5", 1.5), ("x2", 2.0)];
    for k in 0..900u32 {
        let secs = f64::from(1 + k % 11) * 13.0;
        for i in 0..3usize {
            let (worker, speed) = workers[(usize::try_from(k).unwrap() + i) % 3];
            let on = On {
                worker,
                taught: true,
            };
            h.record(
                &build(&format!("k{}-1", k % 11)),
                0,
                &usage(secs / speed),
                Some(on),
            );
        }
    }
    for (worker, want) in [("x1.5", 1.5), ("x2", 2.0)] {
        let got = ratio(&h, worker, "x1");
        assert!((got / want - 1.0).abs() < 0.05, "{worker}: {got}");
    }
}

/// A worker that changes is relearned: its earlier comparisons weigh
/// e^-3 after 3 WINDOW more. b as fast as a for 3,000 comparisons, then
/// twice as fast, reads within 10 % of 2x after 3,000 more. A window of
/// 3,000 fails it.
#[test]
fn a_worker_that_changes_is_relearned() {
    let mut h = History::default();
    let (a, b) = (("a", 1.0, true), ("b", 1.0, true));
    alternate(&mut h, "k-1", 100.0, a, b, 1_500);
    assert!((ratio(&h, "b", "a") - 1.0).abs() < 0.01);
    alternate(&mut h, "k-1", 100.0, a, ("b", 2.0, true), 1_500);
    let r = ratio(&h, "b", "a");
    assert!((r / 2.0 - 1.0).abs() < 0.1, "b read as {r}x a");
}

/// A worker that does not change reads steady through noisy runs: a
/// and b alike, each run's time off by a factor of e^-0.4 to e^0.4
/// (a fixed pseudo-random sequence), b reads within e^0.03 of a over
/// 6,000 comparisons after 6,000. A window of 300 strays to e^0.045.
#[test]
fn an_unchanged_worker_reads_steady() {
    let mut h = History::default();
    let mut s: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut noise = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        #[allow(clippy::cast_precision_loss)]
        let u = (s >> 11) as f64 / (1u64 << 53) as f64;
        (0.8 * u - 0.4).exp()
    };
    let mut worst = 0.0f64;
    for round in 0..6000 {
        for worker in ["a", "b"] {
            let on = Some(On {
                worker,
                taught: true,
            });
            h.record(&build("k-1"), 0, &usage(100.0 * noise()), on);
        }
        if round >= 3000 {
            worst = worst.max(ratio(&h, "b", "a").ln().abs());
        }
    }
    assert!(worst < 0.03, "b strayed to e^{worst} of a");
}

/// A comparison far from the fit is clamped to BOUND from it: b as fast as
/// a, one run in 50 of b's taking 1000x (a key pooling unlike inputs),
/// reads within 3 % of a, not 0.87x. Runs of 2 s, above SHORTEST.
#[test]
fn an_outlying_run_counts_as_a_bounded_deviation() {
    let mut h = History::default();
    for n in 0..2000u32 {
        let slow = if n % 50 == 49 { 1000.0 } else { 1.0 };
        for (worker, secs) in [("a", 2.0), ("b", 2.0 * slow)] {
            let on = On {
                worker,
                taught: true,
            };
            h.record(&build("k-1"), 0, &usage(secs), Some(on));
        }
    }
    let r = ratio(&h, "b", "a");
    assert!((r - 1.0).abs() < 0.03, "b read as {r}x a");
}

/// Each pair of runs is compared once: one 20x run of b's, kept as b's
/// latest of its key, is not compared again with each of a's later runs,
/// which would walk b's factor down, a clamp at a time.
#[test]
fn a_run_is_compared_once_with_each_other_run() {
    let mut h = History::default();
    alternate(
        &mut h,
        "k-1",
        10.0,
        ("a", 1.0, true),
        ("b", 1.0, true),
        1000,
    );
    alternate(&mut h, "j-1", 10.0, ("a", 1.0, true), ("b", 0.05, true), 1);
    let on = Some(On {
        worker: "a",
        taught: true,
    });
    for _ in 0..3000 {
        h.record(&build("j-1"), 0, &usage(10.0), on);
    }
    let r = ratio(&h, "b", "a");
    assert!((r - 1.0).abs() < 0.01, "b read as {r}x a");
}

/// A worker no longer compared does not hold the others at their old
/// ratio: r, compared with a and with b while all three ran alike,
/// retires; a doubles, and 30,000 comparisons of a with b read it 2x,
/// as without r. r's pairs fade with a's and b's runs: a pair that only
/// decayed when compared would keep r's weight and hold a near 1.6x.
#[test]
fn a_retired_worker_does_not_pin_a_changed_one() {
    for retired in [true, false] {
        let mut h = History::default();
        let (a, b, r) = (("a", 1.0, true), ("b", 1.0, true), ("r", 1.0, true));
        if retired {
            alternate(&mut h, "ar-1", 100.0, a, r, 1_500);
            alternate(&mut h, "br-1", 100.0, b, r, 1_500);
        }
        alternate(&mut h, "ab-1", 100.0, a, b, 1_500);
        alternate(&mut h, "ab-1", 100.0, ("a", 2.0, true), b, 15_000);
        let after = ratio(&h, "a", "b");
        assert!(
            (after / 2.0 - 1.0).abs() < 0.02,
            "a read as {after}x b, not 2x (r retired: {retired})"
        );
    }
}

/// A pair ages with its own workers' comparisons, not the farm's: c and
/// d, compared only with each other once per 1,000 comparisons of a with
/// b, keep their comparisons, and c reads 2x d after 30 such rounds. Aged
/// by every comparison, each of theirs would weigh e^-1 by the next, and
/// c would read 1.73x.
#[test]
fn a_pair_compared_rarely_keeps_its_comparisons() {
    let mut h = History::default();
    let (a, b) = (("a", 1.0, true), ("b", 1.0, true));
    for _ in 0..30 {
        alternate(&mut h, "ab-1", 100.0, a, b, 500);
        alternate(&mut h, "cd-1", 100.0, ("c", 2.0, true), ("d", 1.0, true), 1);
    }
    let r = ratio(&h, "c", "d");
    assert!((r / 2.0 - 1.0).abs() < 0.05, "c read as {r}x d, not 2x");
}

/// The prior holds back a comparison too small to be clamped: b at
/// 1.2x a, compared once, reads 1.12x, well below what it says. Without
/// a prior one comparison would read 1.2x.
#[test]
fn the_prior_holds_back_one_small_comparison() {
    let mut h = History::default();
    alternate(&mut h, "k-1", 100.0, ("a", 1.0, true), ("b", 1.2, true), 1);
    let on = Some(On {
        worker: "a",
        taught: true,
    });
    h.record(&build("j-1"), 0, &usage(100.0), on);
    let one = ratio(&h, "b", "a");
    assert!(one > 1.0 && one < 1.15, "one comparison of 1.2x: {one}");
}

/// A worker away loses its factor and relearns it on its return: a, at
/// 2x b and c, is away while b and c are compared 15,000 rounds; its
/// pairs age by their other worker's comparisons, and it reads within
/// 2 % of 1. Back, 20 rounds with b and with c read it within 5 % of 2x
/// again. A worker online whose runs are never quiet is not compared
/// either, and drifts alike.
#[test]
fn a_worker_away_drifts_to_1_and_is_relearned() {
    let mut h = History::default();
    let (a, b, c) = (("a", 2.0, true), ("b", 1.0, true), ("c", 1.0, true));
    for _ in 0..1500 {
        alternate(&mut h, "ab-1", 100.0, a, b, 1);
        alternate(&mut h, "ac-1", 100.0, a, c, 1);
        alternate(&mut h, "bc-1", 100.0, b, c, 1);
    }
    let before = ratio(&h, "a", "b");
    assert!((before / 2.0 - 1.0).abs() < 0.01, "before: {before}");
    alternate(&mut h, "bc-1", 100.0, b, c, 15_000);
    let away = ratio(&h, "a", "b");
    assert!((away - 1.0).abs() < 0.02, "away: {away}");
    alternate(&mut h, "ab-1", 100.0, a, b, 20);
    alternate(&mut h, "bc-1", 100.0, b, c, 20);
    let back = ratio(&h, "a", "b");
    assert!((back / 2.0 - 1.0).abs() < 0.05, "back: {back}");
}

/// A run that did not succeed, failed or OOM-killed, is censored: it
/// teaches memory (history.rs), and no speed comparison, though its
/// dispatch was quiet. a and b compared alike; then 200 runs of b's at
/// 4x, failed or killed, leave every factor as it was, where the same
/// runs succeeded read b slower (1.33x, each comparison clamped).
#[test]
fn a_censored_run_teaches_no_speed() {
    let mut base = History::default();
    alternate(
        &mut base,
        "k-1",
        100.0,
        ("a", 1.0, true),
        ("b", 1.0, true),
        500,
    );
    let before = base.speeds();
    let slow = |h: &mut History, exit_code: i32, oom: bool| {
        for _ in 0..200 {
            let mut u = usage(400.0);
            u.oom_killed = oom;
            for (worker, u) in [("a", usage(100.0)), ("b", u)] {
                let on = Some(On {
                    worker,
                    taught: true,
                });
                h.record(&build("k-1"), exit_code, &u, on);
            }
        }
    };
    for (exit_code, oom) in [(1, false), (0, true)] {
        let mut h = base.clone();
        slow(&mut h, exit_code, oom);
        let after: Vec<_> = (h.speeds().into_iter()).map(|(w, f, _)| (w, f)).collect();
        let was: Vec<_> = (before.iter()).map(|(w, f, _)| (w.clone(), *f)).collect();
        assert_eq!(after, was, "exit {exit_code}, killed {oom}");
    }
    let mut h = base.clone();
    slow(&mut h, 0, false);
    let r = ratio(&h, "a", "b");
    assert!(r > 1.2, "succeeded, b read {r}x slower");
}

/// Runs are compared under history's key (history::key, system and
/// stem): `tool-1.0` on a and `tool-2.0` on b, one stem, are compared,
/// and read b 2x a; `foo-1` on a and `bar-1` on b, two stems, are not,
/// and leave both at factor 1, though bar-1 takes half foo-1's time: a
/// coarser key (the system alone) would compare them and read b 2x.
#[test]
fn runs_are_compared_under_historys_key() {
    let mut h = History::default();
    for _ in 0..500 {
        for (worker, name, secs) in [("a", "tool-1.0", 100.0), ("b", "tool-2.0", 50.0)] {
            let on = Some(On {
                worker,
                taught: true,
            });
            h.record(&build(name), 0, &usage(secs), on);
        }
    }
    let r = ratio(&h, "b", "a");
    assert!((r / 2.0 - 1.0).abs() < 0.02, "one stem: b read {r}x a");
    let mut h = History::default();
    for _ in 0..500 {
        for (worker, name, secs) in [("a", "foo-1", 100.0), ("b", "bar-1", 50.0)] {
            let on = Some(On {
                worker,
                taught: true,
            });
            h.record(&build(name), 0, &usage(secs), on);
        }
    }
    assert_eq!((h.speed("a"), h.speed("b")), (1.0, 1.0), "two stems");
}
