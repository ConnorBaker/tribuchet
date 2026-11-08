//! Every value compared is exact: sums and medians of small integers.
#![allow(clippy::float_cmp)]

use super::*;

fn build(name: &str, declared: bool) -> Build<'_> {
    Build {
        system: "x86_64-linux",
        name: Some(name),
        declared,
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
    }
}

fn gib(e: Expected) -> f64 {
    e.demand.memory / GIB
}

#[test]
fn pname_cuts_where_the_version_starts() {
    assert_eq!(pname("nix-expr-2.36.0pre"), "nix-expr");
    assert_eq!(pname("python3.14-torch-2.9.1"), "python3.14-torch");
    assert_eq!(
        pname("content-observation.json"),
        "content-observation.json"
    );
    assert_eq!(pname("cuda13.4-libnvshmem-3.4"), "cuda13.4-libnvshmem");
    // A hash ends it too; a word with a digit not.
    assert_eq!(pname("tu-bbed23782ae68907"), "tu");
    assert_eq!(pname("jctrim-rel-f8bdb806db3a"), "jctrim-rel");
    assert_eq!(
        pname("cargo-package-chacha20-0.9"),
        "cargo-package-chacha20"
    );
    assert_eq!(pname("python3.13-secp256k1-0.14"), "python3.13-secp256k1");
    assert_eq!(pname("x-goldres2"), "x-goldres2");
    assert_eq!(
        pname("python3.14-jax-cuda13-plugin-0.7"),
        "python3.14-jax-cuda13-plugin"
    );
    assert_eq!(pname("jca-mut-s1"), "jca-mut-s1");
    assert_eq!(
        pname("server-aarch64-unknown-linux-gnu-0.1"),
        "server-aarch64-unknown-linux-gnu"
    );
    assert_eq!(pname("tu-Bbed23782ae68907"), "tu-Bbed23782ae68907");
    assert_eq!(pname("tu-bbedabcd"), "tu-bbedabcd");
    assert_eq!(pname("tu-bbed2378"), "tu");
    assert_eq!(pname("tu-bbed237"), "tu-bbed237");
}

/// Memory is the largest peak of the key's last WINDOW runs; cores and
/// seconds the medians of those that succeeded. Another version of the
/// same pname shares them.
#[test]
fn a_key_expects_its_largest_recent_peak_and_median_timing() {
    let mut h = History::default();
    let (v1, v2) = (build("foo-1.0", false), build("foo-1.1", false));
    for (cores, secs, peak) in [(2, 10, 3), (4, 30, 9), (8, 20, 1)] {
        h.record(&v1, 0, &usage(cores, secs, peak));
    }
    let e = h.expect(&v2);
    assert_eq!(e.level, Level::Key);
    assert_eq!((gib(e), e.demand.cores, e.secs), (9.0, 4.0, Some(20.0)));
    // The window is 8 runs: the 9 GiB run, second, is in it after 6 more
    // and out of it after a seventh.
    for _ in 0..6 {
        h.record(&v1, 0, &usage(1, 1, 2));
    }
    assert_eq!(gib(h.expect(&v1)), 9.0, "the second of 8 runs");
    h.record(&v1, 0, &usage(1, 1, 2));
    assert_eq!(gib(h.expect(&v1)), 2.0, "after the window");
}

/// A key whose runs were all OOM-killed takes its class's cores, where
/// a run of the class succeeded, and no seconds.
#[test]
fn a_key_without_a_succeeded_run_takes_its_class_cores() {
    let mut h = History::default();
    h.record(&build("other-1", false), 0, &usage(4, 10, 1));
    let b = build("foo-1", false);
    let mut oom = usage(30, 1, 12);
    oom.oom_killed = true;
    h.record(&b, 0, &oom);
    let e = h.expect(&b);
    assert_eq!(
        (e.level, gib(e), e.demand.cores, e.secs),
        (Level::Key, 12.0, 4.0, None)
    );
}

/// A failed run teaches nothing; an OOM-killed run its peak, as the
/// lower bound it is, and the kill is counted.
#[test]
fn failed_runs_teach_nothing_and_oom_killed_ones_their_peak() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(2, 10, 1));
    h.record(&b, 1, &usage(30, 1, 20));
    let mut oom = usage(30, 1, 12);
    oom.oom_killed = true;
    h.record(&b, 0, &oom);
    let e = h.expect(&b);
    assert_eq!((gib(e), e.demand.cores, e.secs), (12.0, 2.0, Some(10.0)));
    assert_eq!(e.oom, 1);
    // A run the worker could not measure teaches nothing.
    h.record(&b, 0, &BuildUsage::default());
    assert_eq!(h.expect(&b), e);
}

/// However many runs the worker measured nothing of (it reports 0), they
/// push no measured run out of the window.
#[test]
fn unmeasured_runs_teach_nothing() {
    let mut h = History::default();
    let b = build("foo-1", false);
    h.record(&b, 0, &usage(2, 10, 9));
    let e = h.expect(&b);
    for _ in 0..WINDOW {
        h.record(&b, 0, &BuildUsage::default());
    }
    assert_eq!(h.expect(&b), e);
}

/// A run that succeeded with no CPU time, a sleep, ran on no core.
#[test]
fn a_run_without_cpu_time_used_no_core() {
    let mut h = History::default();
    let b = build("nap-1", false);
    h.record(&b, 0, &usage(0, 10, 1));
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
        h.record(&b, 0, &u);
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
    h.record(&b, 0, &u);
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs), (8.0, Some(1.0)));
}

/// A key with no run takes the longest learned dash-prefix of its pname,
/// then its class (system and declaration), then the prior.
#[test]
fn an_unseen_key_falls_back_to_a_prefix_its_class_and_the_prior() {
    let mut h = History::default();
    let unseen = build("app-grpc-store-clang-tidy-0.2.3", false);
    assert_eq!(h.expect(&unseen), Expected::prior(false));
    assert_eq!(gib(h.expect(&build("torch-2", true))), 38.5);
    h.record(&build("app-util-2.36", false), 0, &usage(2, 10, 3));
    // "app" is a prefix of both; "app-grpc-store" a longer one.
    let e = h.expect(&unseen);
    assert_eq!((e.level, gib(e)), (Level::Prefix, 3.0));
    // A pname is pooled under itself too: "app-grpc-store" pools its own
    // run and its asan build's, and is the longest prefix learned.
    h.record(&build("app-grpc-store-0.2.3", false), 0, &usage(6, 10, 16));
    h.record(
        &build("app-grpc-store-asan-0.2.3", false),
        0,
        &usage(6, 10, 4),
    );
    h.record(&build("app-grpc-1", false), 0, &usage(6, 10, 32));
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
        ..build("", false)
    };
    assert_eq!(h.expect(&nameless).level, Level::Class);
}

/// A prefix keeps WINDOW runs too: its peak forgets a run 8 newer ones
/// on, as an unseen key under it shows.
#[test]
fn a_prefix_keeps_the_last_window_of_runs() {
    let mut h = History::default();
    h.record(&build("foo-bar-1", false), 0, &usage(1, 1, 9));
    for _ in 0..WINDOW {
        h.record(&build("foo-bar-1", false), 0, &usage(1, 1, 1));
    }
    let e = h.expect(&build("foo-baz-1", false));
    assert_eq!((e.level, gib(e)), (Level::Prefix, 1.0));
}

/// The class's peak is the 90th percentile of its runs, not the largest.
#[test]
fn a_class_expects_its_ninetieth_percentile_peak() {
    let mut h = History::default();
    for n in 1..=10 {
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, n));
    }
    let e = h.expect(&build("unseen-1", false));
    assert_eq!((e.level, gib(e)), (Level::Class, 10.0));
    for n in 11..=20 {
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, n));
    }
    assert_eq!(gib(h.expect(&build("unseen-1", false))), 19.0);
    // CLASS_WINDOW is 512: of 52 runs of 100 GiB, then 460 of 1 GiB, the
    // 90th percentile of all 512 is 100 GiB; of any fewer, the last ones,
    // 1 GiB (at 511 runs, the 460th is the 90th percentile).
    let mut h = History::default();
    for n in 0..512 {
        let peak = if n < 52 { 100 } else { 1 };
        h.record(&build(&format!("k{n}-1"), false), 0, &usage(1, 1, peak));
    }
    assert_eq!(gib(h.expect(&build("unseen-1", false))), 100.0);
}

/// An even count's median is the midpoint of its middle two.
#[test]
fn an_even_count_takes_the_midpoint() {
    let mut h = History::default();
    let b = build("foo-1", false);
    for (cores, secs) in [(2, 10), (4, 30)] {
        h.record(&b, 0, &usage(cores, secs, 1));
    }
    let e = h.expect(&b);
    assert_eq!((e.demand.cores, e.secs), (3.0, Some(20.0)));
}
