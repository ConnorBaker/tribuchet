//! A run's length decides whether it teaches speed (SHORTEST).

use super::*;

/// A worker's speed is its rate on a build's work, not its fixed cost per
/// build: e, with more of that cost (0.09 s where b has 0.04 s), runs a
/// 300 s build that b takes 420 s for. Without the short runs e reads 1.4x
/// b; with twenty to each long one, as on the farm, taught from every
/// length e read 0.46x b and the long build was expected sooner on b.
/// The short runs teach nothing: e reads 1.4x b, and the long one sooner.
#[test]
fn a_short_build_teaches_no_speed() {
    let on = |worker| {
        Some(On {
            worker,
            taught: true,
        })
    };
    let runs = |short: usize| {
        let mut h = History::default();
        for _ in 0..300 {
            for _ in 0..short {
                h.record(&build("obs-1"), 0, &usage(0.09), on("e"));
                h.record(&build("obs-1"), 0, &usage(0.04), on("b"));
            }
            h.record(&build("long-1"), 0, &usage(300.0), on("e"));
            h.record(&build("long-1"), 0, &usage(420.0), on("b"));
        }
        h
    };
    let long = build("long-1");
    for short in [0, 20] {
        let h = runs(short);
        let r = ratio(&h, "e", "b");
        assert!(
            (r / 1.4 - 1.0).abs() < 0.05,
            "{short}: e read {r}x b, not 1.4x"
        );
        let (e, b) = (
            h.secs_on(&long, "e").unwrap(),
            h.secs_on(&long, "b").unwrap(),
        );
        assert!(
            e < b,
            "{short}: the long build expected in {e} s on e, {b} s on b"
        );
    }
}

/// The cut is on a length two runs share, their mean, so noise selects
/// neither: keys near 1 s, a 1.4x b, each run off by e^-0.3, 1 or e^0.3
/// in all nine pairs, read a 1.4x b within 5 %. Cut by each run's own
/// length, only a's noisily slow runs would be kept: about 1.2x.
#[test]
fn the_cut_selects_no_run_by_its_noise() {
    let on = |worker| {
        Some(On {
            worker,
            taught: true,
        })
    };
    let noise = [(-0.3f64).exp(), 1.0, 0.3f64.exp()];
    let mut h = History::default();
    for round in 0..100 {
        for (i, na) in noise.iter().enumerate() {
            for (j, nb) in noise.iter().enumerate() {
                let name = format!("s{round}x{i}x{j}-1");
                h.record(&build(&name), 0, &usage(1.2 / 1.4 * na), on("a"));
                h.record(&build(&name), 0, &usage(1.2 * nb), on("b"));
            }
        }
    }
    let r = ratio(&h, "a", "b");
    assert!((r / 1.4 - 1.0).abs() < 0.05, "a read {r}x b, not 1.4x");
}

/// The cut is 1 s of the two runs' mean, inclusive: a at 4x b, runs of a
/// mean 0.5 s or 0.99 s are not compared, of exactly 1 s or 2 s are.
#[test]
fn the_cut_is_a_second_of_the_mean_inclusive() {
    for (a, b, compared) in [
        (0.25, 1.0, false),
        (0.7, 1.4, false),
        (0.5, 2.0, true),
        (1.0, 4.0, true),
    ] {
        let mut h = History::default();
        for _ in 0..50 {
            for (worker, secs) in [("a", a), ("b", b)] {
                let on = On {
                    worker,
                    taught: true,
                };
                h.record(&build("k-1"), 0, &usage(secs), Some(on));
            }
        }
        let r = ratio(&h, "a", "b");
        assert_eq!(r > 1.5, compared, "runs of {a} and {b} s: a read {r}x b");
        if !compared {
            assert_eq!(r, 1.0, "runs of {a} and {b} s compared");
        }
    }
}
