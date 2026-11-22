//! Every value compared is exact: sums and medians of small integers.
#![allow(clippy::float_cmp)]

use std::collections::VecDeque;

use super::*;

fn build(name: &str, declared: bool) -> Build<'_> {
    Build {
        system: "x86_64-linux",
        name: Some(name),
        declared,
        sources: None,
    }
}

/// `cores` cores for `secs` seconds, peaking at `gib` GiB.
fn usage(cores: u64, secs: u64, gib: u64) -> BuildUsage {
    BuildUsage {
        wall_ms: secs * 1000,
        cpu_usec: cores * secs * 1_000_000,
        peak_memory_bytes: gib << 30,
        oom_killed: false,
        frozen_ms: 0,
        profile: None,
    }
}

fn gib(e: Expected) -> f64 {
    e.demand.memory / GIB
}

#[path = "tests/keys.rs"]
mod keys;

/// Memory is the largest peak of the key's last WINDOW runs; cores and
/// seconds the medians of those that succeeded. Another version of the
/// same stem shares them.
#[test]
fn a_key_expects_its_largest_recent_peak_and_median_timing() {
    let mut h = History::default();
    let (v1, v2) = (build("foo-1.0", false), build("foo-1.1", false));
    for (cores, secs, peak) in [(2, 10, 3), (4, 30, 9), (8, 20, 1)] {
        h.record(&v1, 0, &usage(cores, secs, peak), None);
    }
    let e = h.expect(&v2);
    assert_eq!(e.level, Level::Key);
    assert_eq!((gib(e), e.demand.cores, e.secs), (9.0, 4.0, Some(20.0)));
    // The window is 8 runs: the 9 GiB run, second, is in it after 6 more
    // and out of it after a seventh.
    for _ in 0..6 {
        h.record(&v1, 0, &usage(1, 1, 2), None);
    }
    assert_eq!(gib(h.expect(&v1)), 9.0, "the second of 8 runs");
    h.record(&v1, 0, &usage(1, 1, 2), None);
    assert_eq!(gib(h.expect(&v1)), 2.0, "after the window");
}

/// A key whose runs were all OOM-killed takes its class's guess, cores
/// and seconds, no lower than its peak.
#[test]
fn a_key_without_a_succeeded_run_takes_its_class_cores() {
    let mut h = History::default();
    h.record(&build("other-1", false), 0, &usage(4, 10, 1), None);
    let b = build("foo-1", false);
    let mut oom = usage(30, 1, 12);
    oom.oom_killed = true;
    h.record(&b, 0, &oom, None);
    let e = h.expect(&b);
    assert_eq!(
        (e.level, gib(e), e.demand.cores, e.secs),
        (Level::Key, 12.0, 4.0, Some(10.0))
    );
}

/// A run that did not succeed teaches memory only, its own peak,
/// censored, at every level, the kill counted. A key with no success takes
/// its succeeded kin's guess, no lower than its peaks: one that failed at
/// 2 GiB guesses its class's 20, its sibling the 2 it learned, not a
/// guess, and one failed unmeasured learns nothing
/// and has not run. A sibling under a prefix with no success guesses no
/// less, but a runaway above a success is no sibling's guess.
#[test]
fn a_run_that_did_not_succeed_is_censored_at_every_level() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(2, 10, 1), None);
    let rows: [(&str, &str, i32, u64, bool, f64); 5] = [
        ("failed, over", "foo-1", 1, 20, false, 20.0),
        ("failed, under", "foo-1", 1, 0, false, 20.0),
        ("killed, under", "foo-1", 0, 12, true, 20.0),
        (
            "unseen key, under its guess",
            "bar-baz-1",
            1,
            2,
            false,
            20.0,
        ),
        ("runaway", "tar-crash-1", 1, 96, false, 96.0),
    ];
    for (row, name, exit_code, peak, oom, want) in rows {
        let mut u = usage(30, 1, peak);
        u.oom_killed = oom;
        h.record(&build(name, false), exit_code, &u, None);
        let e = h.expect(&build(name, false));
        assert_eq!((e.level, gib(e)), (Level::Key, want), "{row}");
    }
    let e = h.expect(&build("bar-qux-1", false));
    assert_eq!((e.level, gib(e)), (Level::Prefix, 2.0), "its own peak");
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs, e.oom), (2.0, Some(10.0), 1));
    let empty = build("qux-1", false);
    h.record(&empty, 1, &BuildUsage::default(), None);
    let e = h.expect(&empty);
    assert!(
        !h.ran(&empty) && e.level != Level::Key,
        "an empty failure: {e:?}"
    );
    let e = h.expect(&build("tar-plugin-1", false));
    assert_eq!(
        (e.level, gib(e)),
        (Level::Prefix, 96.0),
        "no success under tar"
    );
    h.record(&build("tar-other-1", false), 0, &usage(1, 1, 2), None);
    let e = h.expect(&build("tar-plugin-1", false));
    assert_eq!(
        (e.level, gib(e)),
        (Level::Prefix, 2.0),
        "the runaway, a guess"
    );
    let e = h.expect(&build("tar-crash-demo-1", false));
    assert_eq!(
        (e.level, gib(e)),
        (Level::Prefix, 96.0),
        "none under tar-crash"
    );
    h.record(&b, 0, &BuildUsage::default(), None);
    assert_eq!(gib(h.expect(&b)), 20.0, "an unmeasured success taught");
    let blind = build("blind-1", false);
    h.record(&blind, 0, &BuildUsage::default(), None);
    assert!(!h.ran(&blind), "an unmeasured run counted as run");
    assert_eq!(h.expect(&blind).level, Level::Class, "it taught memory");
}

/// An unseen sibling of an OOM-killed build guesses at least the killed
/// peak; a sibling that succeeded above it leads; the cores are the
/// succeeded runs' alone. A killed peak at the top of a class whose runs
/// mostly succeeded leaves its 90th percentile to them.
#[test]
fn a_killed_peak_floors_its_siblings_and_successes_above_it_lead() {
    let mut h = History::default();
    let mut killed = usage(30, 1, 30);
    killed.oom_killed = true;
    h.record(&build("tar-crash-1", false), 0, &killed, None);
    let sibling = build("tar-plugin-1", false);
    assert_eq!(gib(h.expect(&sibling)), 30.0, "under the killed peak");
    h.record(&build("tar-other-1", false), 0, &usage(4, 10, 40), None);
    let e = h.expect(&sibling);
    assert_eq!((gib(e), e.demand.cores), (40.0, 4.0), "the success");
    for i in 0..20 {
        h.record(&build(&format!("x{i}-1"), false), 0, &usage(1, 10, 1), None);
    }
    assert_eq!(gib(h.expect(&build("zz-1", false))), 1.0, "the class");
}

/// Kaplan-Meier: a censored run moves its mass to the peaks above it (the
/// median of 1, 2+, 3+, 4, 5 is 4, not 3), a success first at a tie (the
/// 0.4 quantile of 2+, 2, 5 is 5). Short of `q`, the `q`-quantile of all
/// peaks (of eight 1s, 20+ and 96+, the 0.8 is 20, not 96), no less than
/// the last success (of ninety 1+, 10 and four 20+, 10, not 1).
#[test]
fn a_censored_peak_counts_as_at_least_itself() {
    let runs = |rs: &[(u64, bool)]| -> VecDeque<Run> {
        rs.iter().map(|&(p, ok)| Run(p, 0, 0, ok, None)).collect()
    };
    let km = runs(&[(1, true), (2, false), (3, false), (4, true), (5, true)]);
    assert_eq!(quantile(&km, 0.5), Some(4));
    let tie = runs(&[(2, false), (2, true), (5, true)]);
    assert_eq!(quantile(&tie, 0.4), Some(5), "a tie");
    assert_eq!(quantile(&runs(&[(7, false), (3, false)]), 0.9), Some(7));
    assert_eq!(quantile(&runs(&[(1, true), (9, true)]), 1.0), Some(9));
    let mut storm = vec![(1, true); 8];
    storm.extend([(96, false), (20, false)]);
    assert_eq!(quantile(&runs(&storm), 0.8), Some(20), "all peaks");
    let mut low = vec![(1, false); 90];
    low.extend([
        (10, true),
        (20, false),
        (20, false),
        (20, false),
        (20, false),
    ]);
    assert_eq!(quantile(&runs(&low), 0.9), Some(10), "the last success");
}

/// A key's own runaway leaves with its window: its later failed runs are
/// floored by no expectation of the key, which would keep the 96 GiB; its
/// class, of other builds' successes, guesses 1 GiB. Until then, with no
/// success, it guesses no less than its own peak, above its class's.
#[test]
fn a_runaways_bound_leaves_its_key_with_its_window() {
    let mut h = History::default();
    for n in 0..20 {
        h.record(&build(&format!("x{n}-1"), false), 0, &usage(1, 1, 1), None);
    }
    let b = build("tar-crash-1", false);
    h.record(&b, 1, &usage(30, 1, 96), None);
    assert_eq!(gib(h.expect(&b)), 96.0, "under its own peak");
    for _ in 0..WINDOW {
        h.record(&b, 1, &usage(30, 1, 1), None);
    }
    assert_eq!(gib(h.expect(&b)), 1.0);
}

/// Runs that fail below a key's measured peak push neither it nor its
/// timing out of the window; a kill among them is still counted.
#[test]
fn failures_below_a_measured_peak_keep_it() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(4, 100, 40), None);
    let mut killed = usage(1, 1, 1);
    killed.oom_killed = true;
    h.record(&b, 0, &killed, None);
    for _ in 0..WINDOW {
        h.record(&b, 1, &usage(1, 1, 1), None);
    }
    let e = h.expect(&b);
    assert_eq!((gib(e), e.secs, e.oom), (40.0, Some(100.0), 1));
    let e = h.expect(&build("foo-bar-1", false));
    assert_eq!((e.level, gib(e)), (Level::Prefix, 40.0), "its prefix");
}

/// A kill hidden by a larger success outlives it: the success moves to
/// the kill's place, so after six runs more of a window of 8 the key
/// still expects no less than the kill's 9 GiB (the success's 10 GiB),
/// where the success alone would have left the window.
#[test]
fn a_skipped_kill_outlives_the_success_that_hid_it() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(1, 1, 10), None);
    for _ in 0..6 {
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    let mut killed = usage(1, 1, 9);
    killed.oom_killed = true;
    h.record(&b, 0, &killed, None);
    for _ in 0..2 {
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    assert_eq!(gib(h.expect(&b)), 10.0, "under the kill's lower bound");
}

/// A kill at a success's own peak is covered by it too, so it pushes no
/// success out: the 9 GiB success stays its prefix's guess for a
/// sibling, where the kill, pushed in, would have evicted it.
#[test]
fn a_kill_at_a_successs_peak_is_covered_by_it() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(1, 100, 9), None);
    for _ in 0..7 {
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    let mut killed = usage(1, 1, 9);
    killed.oom_killed = true;
    h.record(&b, 0, &killed, None);
    let e = h.expect(&build("foo-bar-1", false));
    assert_eq!((e.level, gib(e)), (Level::Prefix, 9.0));
}

/// Of the successes covering a kill, the smallest moves to the newest
/// place: the bound it keeps is the tightest. A 20 GiB and a 10 GiB
/// success cover a kill at 9; two runs on, the 20 GiB one has left the
/// window and the key expects 10, not 20.
#[test]
fn the_smallest_success_covering_a_kill_stays() {
    let mut h = History::default();
    let b = build("foo-1", false);
    for gib in [20, 10] {
        h.record(&b, 0, &usage(1, 1, gib), None);
    }
    for _ in 0..5 {
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    let mut killed = usage(1, 1, 9);
    killed.oom_killed = true;
    h.record(&b, 0, &killed, None);
    for _ in 0..2 {
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    assert_eq!(gib(h.expect(&b)), 10.0);
}

/// However many runs the worker measured nothing of (it reports 0), they
/// push no measured run out of the window.
#[test]
fn unmeasured_runs_teach_nothing() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(2, 10, 9), None);
    let e = h.expect(&b);
    for _ in 0..WINDOW {
        h.record(&b, 0, &BuildUsage::default(), None);
    }
    assert_eq!(h.expect(&b), e);
}

/// A run that succeeded with no CPU time, a sleep, ran on no core.
#[test]
fn a_run_without_cpu_time_used_no_core() {
    let mut h = History::default();
    let b = build("nap-1", false);
    h.record(&b, 0, &usage(0, 10, 1), None);
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs), (0.0, Some(10.0)));
}

/// A run that succeeded with no running time teaches its peak but no
/// timing: neither 5 CPU-seconds in none (infinite) nor none in none
/// (NaN).
#[test]
fn a_run_without_running_time_teaches_no_timing() {
    let mut h = History::default();
    for (name, cpu_usec) in [("foo-1", 5_000_000), ("nap-1", 0)] {
        let b = build(name, false);
        let mut u = usage(0, 0, 2);
        u.cpu_usec = cpu_usec;
        h.record(&b, 0, &u, None);
        let e = h.expect(&b);
        assert_eq!((gib(e), e.demand.cores, e.secs), (2.0, 1.0, None), "{name}");
    }
}

/// 8 CPU-seconds over 3 s of wall time, 2 s of it frozen: 8 cores while
/// it ran, for 1 s.
#[test]
fn frozen_time_is_not_running_time() {
    let mut h = History::default();
    let b = build("foo-1", false);
    let mut u = usage(8, 1, 1);
    (u.wall_ms, u.frozen_ms) = (3000, 2000);
    h.record(&b, 0, &u, None);
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs), (8.0, Some(1.0)));
}

/// A key with no run takes the longest learned dash-prefix of its stem,
/// then its class (system and declaration), then the prior.
#[test]
fn an_unseen_key_falls_back_to_a_prefix_its_class_and_the_prior() {
    let mut h = History::default();
    let unseen = build("app-grpc-store-clang-tidy-0.2.3", false);
    assert_eq!(h.expect(&unseen), Expected::prior(false));
    assert_eq!(gib(h.expect(&build("torch-2", true))), 38.5);
    h.record(&build("app-util-2.36", false), 0, &usage(2, 10, 3), None);
    // "app" is a prefix of both; "app-grpc-store" a longer one.
    let e = h.expect(&unseen);
    assert_eq!((e.level, gib(e)), (Level::Prefix, 3.0));
    // A stem is pooled under itself too: "app-grpc-store" pools its own
    // run and its asan build's, and is the longest prefix learned.
    h.record(
        &build("app-grpc-store-0.2.3", false),
        0,
        &usage(6, 10, 16),
        None,
    );
    h.record(
        &build("app-grpc-store-asan-0.2.3", false),
        0,
        &usage(6, 10, 4),
        None,
    );
    h.record(&build("app-grpc-1", false), 0, &usage(6, 10, 32), None);
    let e = h.expect(&unseen);
    assert_eq!((e.level, gib(e)), (Level::Prefix, 16.0), "longest prefix");
    // No prefix: the class of builds that do not declare big-parallel.
    let e = h.expect(&build("other-1", false));
    assert_eq!((e.level, gib(e)), (Level::Class, 32.0));
    // The declaring class has no run yet.
    assert_eq!(h.expect(&build("torch-2", true)), Expected::prior(true));
    // A build whose name could not be read takes its class.
    let nameless = Build {
        name: None,
        sources: Some("s"),
        ..build("", false)
    };
    assert_eq!(h.expect(&nameless).level, Level::Class);
}

/// A prefix keeps WINDOW runs too: its peak forgets a run 8 newer ones
/// on, as an unseen key under it shows.
#[test]
fn a_prefix_keeps_the_last_window_of_runs() {
    let mut h = History::default();
    h.record(&build("foo-bar-1", false), 0, &usage(1, 1, 9), None);
    for _ in 0..WINDOW {
        h.record(&build("foo-bar-1", false), 0, &usage(1, 1, 1), None);
    }
    let e = h.expect(&build("foo-baz-1", false));
    assert_eq!((e.level, gib(e)), (Level::Prefix, 1.0));
}

/// The class's peak is the 90th percentile of its runs, not the largest.
#[test]
fn a_class_expects_its_ninetieth_percentile_peak() {
    let mut h = History::default();
    for n in 1..=10 {
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, n), None);
    }
    let e = h.expect(&build("unseen-1", false));
    assert_eq!((e.level, gib(e)), (Level::Class, 10.0));
    for n in 11..=20 {
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, n), None);
    }
    assert_eq!(gib(h.expect(&build("unseen-1", false))), 19.0);
    // CLASS_WINDOW is 512: of 52 runs of 100 GiB, then 460 of 1 GiB, the
    // 90th percentile of all 512 is 100 GiB; of any fewer, the last ones,
    // 1 GiB (at 511 runs, the 460th is the 90th percentile).
    let mut h = History::default();
    for n in 0..512 {
        let peak = if n < 52 { 100 } else { 1 };
        h.record(
            &build(&format!("k{n}-1"), false),
            0,
            &usage(1, 1, peak),
            None,
        );
    }
    assert_eq!(gib(h.expect(&build("unseen-1", false))), 100.0);
}

/// A class's 90th percentile is Kaplan-Meier's, as History::expect reads
/// it: of successes at 2 to 11 GiB and a failure at 1 GiB, the failure's
/// weight moves to the ten above it, so 11 GiB, where the 90th percentile
/// of all eleven peaks is 10.
#[test]
fn a_class_moves_a_censored_peaks_weight_above_it() {
    let mut h = History::default();
    h.record(&build("k0-1", false), 1, &usage(1, 1, 1), None);
    for n in 2..=11 {
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, n), None);
    }
    let e = h.expect(&build("unseen-1", false));
    assert_eq!((e.level, gib(e)), (Level::Class, 11.0));
}

/// An even count's median is the midpoint of its middle two.
#[test]
fn an_even_count_takes_the_midpoint() {
    let mut h = History::default();
    let b = build("foo-1", false);
    for (cores, secs) in [(2, 10), (4, 30)] {
        h.record(&b, 0, &usage(cores, secs, 1), None);
    }
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs), (3.0, Some(20.0)));
}

/// A read holds until an entry it read learns a run: a key with a timed
/// success reads only itself, so another key's run, even of its class,
/// leaves it; an unseen key reads its prefixes and class, so a sibling's
/// run under its prefix, or any run of its class, ends it.
#[test]
fn a_read_holds_until_what_it_read_learns() {
    let mut h = History::default();
    let own = build("own-1", false);
    h.record(&own, 0, &usage(1, 10, 1), None);
    let (_, read_own) = h.read(&own);
    let (_, read_unseen) = h.read(&build("py-foo-1", false));
    let (_, read_lone) = h.read(&build("lone-1", true));
    h.record(&build("other-1", false), 0, &usage(1, 10, 1), None);
    assert!(h.fresh(&read_own), "another key's run");
    assert!(!h.fresh(&read_unseen), "a run of its class");
    assert!(h.fresh(&read_lone), "a run of another class");
    let (_, read_unseen) = h.read(&build("py-foo-1", false));
    h.record(&build("py-bar-1", true), 0, &usage(1, 10, 1), None);
    assert!(!h.fresh(&read_unseen), "a sibling's run under its prefix");
    h.record(&own, 0, &usage(1, 10, 2), None);
    assert!(!h.fresh(&read_own), "its own key's run");
}

/// A read still fresh is the expectation itself: random runs over names
/// that share prefixes, both classes, sources or none, and every outcome
/// (success, failure, OOM kill, unmeasured, no running time), and after
/// each run every read
/// the history holds fresh equals a fresh expectation. A seeded generator:
/// the same runs each time.
#[test]
#[allow(clippy::cast_possible_truncation)]
fn a_fresh_read_is_never_stale() {
    let names = [
        "a-1",
        "a-b-1",
        "a-b-c-1",
        "a-bd-1",
        "a-b-c-d-1",
        "x-1",
        "x-y-1",
        "a",
        "q-r-s-1",
    ];
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    let sources = [None, Some("s"), Some("t")];
    let of = |i: usize, d: bool, s: usize| Build {
        sources: sources[s],
        ..build(names[i], d)
    };
    let (mut fresh, mut stale) = (0u64, Vec::new());
    for _ in 0..300 {
        let mut h = History::default();
        let mut reads: Vec<(usize, bool, usize, Expected, Read)> = Vec::new();
        for _ in 0..120 {
            let (i, d) = (next(names.len() as u64) as usize, next(2) == 1);
            let src = next(sources.len() as u64) as usize;
            if next(3) == 0 {
                let (e, r) = h.read(&of(i, d, src));
                reads.push((i, d, src, e, r));
                continue;
            }
            let (peak, secs, cores) = ((1 + next(40)) << 28, next(4), 1 + next(8));
            let u = BuildUsage {
                wall_ms: if next(10) == 0 { 0 } else { secs * 1000 },
                cpu_usec: cores * secs * 1_000_000,
                peak_memory_bytes: if next(10) == 0 { 0 } else { peak },
                oom_killed: next(6) == 0,
                ..BuildUsage::default()
            };
            h.record(&of(i, d, src), i32::from(next(4) == 0), &u, None);
            for (j, dj, sj, e, r) in &reads {
                if h.fresh(r) {
                    fresh += 1;
                    let now = h.expect(&of(*j, *dj, *sj));
                    if now != *e {
                        stale.push((names[*j], *dj, sources[*sj], *e, now));
                    }
                }
            }
        }
    }
    assert!(
        stale.is_empty(),
        "{} stale: {:?}",
        stale.len(),
        stale.first()
    );
    assert!(fresh > 1000, "too few fresh reads: {fresh}");
}
