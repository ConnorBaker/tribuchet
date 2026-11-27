//! A short build (its seconds at factor 1 under SHORT, 10 s) is left to the
//! slowest workers' tier where it fits, the least full there, keeping the
//! faster workers' agents for longer builds (Rooms::passes).

use super::finish::two_speeds;
use super::*;

/// A room of 4 cores: its id, idle agents and cores held.
fn room(id: u64, idle: u32, cores: f64) -> Room {
    Room {
        id,
        idle,
        size: SMALL,
        load: Demand { cores, memory: 0.0 },
        clientless: Demand::default(),
        empty: false,
        only_clientless: false,
    }
}

const ONE: Demand = Demand {
    cores: 1.0,
    memory: 0.0,
};

/// Asker 1 and room 2, a build of 1 core: by its seconds at factor 1
/// (`unit`), each room's measured factor, idle agents and cores held.
#[test]
fn a_short_build_goes_to_the_slowest_tier() {
    let (fast, within) = (Some(1.4), Some(1.09));
    let slow = Some(1.0);
    // (row, unit, (my factor, idle, cores), (its factor, idle, cores), want)
    let rows = [
        (
            "short, asker fast",
            Some(5.0),
            (fast, 1, 0.0),
            (slow, 1, 2.0),
            Some(2),
        ),
        (
            "short, asker slow",
            Some(5.0),
            (slow, 1, 2.0),
            (fast, 1, 0.0),
            None,
        ),
        (
            "short, slow room no agent",
            Some(5.0),
            (fast, 1, 0.0),
            (slow, 0, 0.0),
            None,
        ),
        (
            "short, slow room too full",
            Some(5.0),
            (fast, 1, 0.0),
            (slow, 1, 3.5),
            None,
        ),
        (
            "short, slow room just full",
            Some(5.0),
            (fast, 1, 0.0),
            (slow, 1, 3.0),
            Some(2),
        ),
        (
            "short, 1.15x the slowest: its own tier",
            Some(5.0),
            (Some(1.15), 1, 0.0),
            (slow, 1, 2.0),
            Some(2),
        ),
        (
            "short, one tier: less full",
            Some(5.0),
            (slow, 1, 2.0),
            (within, 1, 0.0),
            Some(2),
        ),
        // Here the faster finishes first (5/1.09 x 1.3 < 5 x 1.25): fullness rules.
        (
            "short, one tier: fuller",
            Some(5.0),
            (slow, 1, 0.0),
            (within, 1, 0.2),
            None,
        ),
        (
            "at 10 s: not short",
            Some(10.0),
            (fast, 1, 0.0),
            (slow, 1, 2.0),
            None,
        ),
        (
            "just under 10 s",
            Some(9.99),
            (fast, 1, 0.0),
            (slow, 1, 2.0),
            Some(2),
        ),
        (
            "no seconds: not short",
            None,
            (fast, 1, 0.0),
            (slow, 1, 2.0),
            None,
        ),
        (
            "a factor unmeasured: not short",
            Some(5.0),
            (fast, 1, 0.0),
            (None, 1, 2.0),
            None,
        ),
    ];
    for (row, unit, (ms, mi, mc), (ts, ti, tc), want) in rows {
        let mut rooms = Rooms::new(1, vec![room(1, mi, mc), room(2, ti, tc)]);
        let speed = |id| if id == 1 { ms } else { ts };
        let got = rooms.passes(ONE, &mut None, |_| true, speed, unit, Instant::now());
        assert_eq!(got, want, "{row}");
    }
}

/// The slowest tier is of the rooms compared, those with an idle agent:
/// with the slowest worker (2) agentless, room 3 (1.3) is the slowest
/// and takes the build from the faster, emptier asker (1.5), which within
/// 1.1x of worker 2's factor it would not.
#[test]
fn the_tier_is_of_the_rooms_with_an_agent() {
    let rooms = vec![room(1, 1, 0.0), room(2, 0, 0.0), room(3, 1, 2.0)];
    let mut rooms = Rooms::new(1, rooms);
    let speed = |id| [None, Some(1.5), Some(1.0), Some(1.3)][usize::try_from(id).unwrap()];
    let got = rooms.passes(ONE, &mut None, |_| true, speed, Some(5.0), Instant::now());
    assert_eq!((got, rooms.why), (Some(3), "a slower worker's tier"));
}

/// The reason logged for a pass: an asker whose agents are all busy, its
/// lookahead asking, leaves a build to a room with an idle agent; a slow
/// asker too full for a short build leaves it to a faster room it fits.
#[test]
fn the_reason_is_logged() {
    let rows = [
        (
            "an idle agent",
            None,
            (Some(1.0), 0, 0.0),
            (Some(1.0), 1, 2.0),
        ),
        (
            "it fits",
            Some(5.0),
            (Some(1.0), 1, 3.5),
            (Some(1.4), 1, 0.0),
        ),
    ];
    for (why, unit, (ms, mi, mc), (ts, ti, tc)) in rows {
        let mut rooms = Rooms::new(1, vec![room(1, mi, mc), room(2, ti, tc)]);
        let speed = |id| if id == 1 { ms } else { ts };
        let got = rooms.passes(ONE, &mut None, |_| true, speed, unit, Instant::now());
        assert_eq!((got, rooms.why), (Some(2), why));
    }
}

/// A factor unmeasured makes no build short, though a slower worker too
/// full for it (3) would put asker 1 in a faster tier than room 2: they
/// rank by fullness, and the asker keeps it.
#[test]
fn an_unmeasured_factor_makes_no_tier() {
    let rooms = vec![room(1, 1, 0.0), room(2, 1, 1.0), room(3, 1, 3.5)];
    let mut rooms = Rooms::new(1, rooms);
    let speed = |id| [None, Some(1.4), None, Some(1.0)][usize::try_from(id).unwrap()];
    let got = rooms.passes(ONE, &mut None, |_| true, speed, Some(5.0), Instant::now());
    assert_eq!(got, None);
}

/// A take reads the build's seconds and the workers' factors from the
/// history: b, taught 2x a's speed, leaves a build of 1 s to a, and a
/// takes it when it asks.
#[tokio::test]
async fn a_take_leaves_a_short_build_to_the_slower_worker() {
    let (state, (a, a_caps), (b, b_caps)) = two_speeds();
    let mut e = expecting(1.0, 1.0);
    e.secs = Some(1.0);
    let _client = queue(&state, 0, "build-0", e).await;
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), None, "left to the slower worker");
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)));
}

/// A build the history has not seen is guessed short by its class (the
/// runs of `two_speeds` and of many builds of 50 ms); the take reads no
/// seconds from a guess, nor a factor from a worker never compared, and
/// b, asking, keeps it.
#[tokio::test]
async fn a_take_reads_no_guess() {
    let (state, _a, (b, b_caps)) = two_speeds();
    let mut j = job(0, Arc::new(Replay::default()));
    j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-llvm-19.1"))].into();
    {
        let mut p = state.placement.lock().unwrap();
        let usage = short_class(&mut p.history);
        p.stamp(&mut j);
        let build = placement::build_of(&j);
        let guess = p.history.unit_secs(&build);
        assert!(guess.is_some_and(|s| s < 10.0), "class guess {guess:?}");
        assert_eq!(p.history.own_secs(&build), None);
        // c ran a build, so the history names it, but was never compared.
        let lonely = placement::Build {
            system: "x86_64-linux",
            name: Some("lonely-1"),
            declared: false,
            sources: None,
        };
        let on = placement::On {
            worker: "c",
            taught: true,
        };
        p.history.record(&lonely, 0, &usage, Some(on));
        assert!(p.history.measured_speed("a").is_some());
        assert_eq!(p.history.measured_speed("c"), None);
    }
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)), "b keeps a guessed build");
}

/// Teach the x86 class 300 builds of 50 ms each: its guess is short.
fn short_class(h: &mut placement::History) -> BuildUsage {
    let usage = BuildUsage {
        wall_ms: 50,
        cpu_usec: 50_000,
        peak_memory_bytes: 1 << 20,
        ..Default::default()
    };
    for n in 0..300 {
        let name = format!("tiny-{n}");
        let tiny = placement::Build {
            system: "x86_64-linux",
            name: Some(&name),
            declared: false,
            sources: None,
        };
        h.record(&tiny, 0, &usage, None);
    }
    usage
}

/// A build's sources' runs are its own: their median seconds, not its
/// key's, are what the take reads.
#[test]
fn a_build_reads_its_sources_seconds() {
    let mut h = placement::History::default();
    let run = |ms: u64| BuildUsage {
        wall_ms: ms,
        cpu_usec: ms * 1000,
        peak_memory_bytes: 1 << 20,
        ..Default::default()
    };
    let obs = |sources| placement::Build {
        system: "x86_64-linux",
        name: Some("obs-1"),
        declared: false,
        sources: Some(sources),
    };
    for _ in 0..3 {
        h.record(&obs("a"), 0, &run(4_000), None);
        h.record(&obs("b"), 0, &run(40_000), None);
    }
    assert_eq!(h.expect(&obs("a")).level, Level::Source);
    assert_eq!(h.own_secs(&obs("a")), Some(4.0));
}

/// A key whose only run failed, after 3000 s, is expected at its key's
/// level, for its peak, but its seconds are its class's guess: the take
/// reads none, and the build is not short.
#[test]
fn a_failed_key_gives_no_own_seconds() {
    let mut h = placement::History::default();
    short_class(&mut h);
    let llvm = placement::Build {
        system: "x86_64-linux",
        name: Some("llvm-19.1"),
        declared: false,
        sources: None,
    };
    let failed = BuildUsage {
        wall_ms: 3_000_000,
        cpu_usec: 3_000_000_000,
        peak_memory_bytes: 1 << 30,
        ..Default::default()
    };
    h.record(&llvm, 1, &failed, None);
    assert_eq!(h.expect(&llvm).level, Level::Key);
    assert!(h.unit_secs(&llvm).is_some_and(|s| s < 10.0));
    assert_eq!(h.own_secs(&llvm), None);
}
