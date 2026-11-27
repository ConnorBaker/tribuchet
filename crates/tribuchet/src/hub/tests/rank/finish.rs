//! A build is left to the worker with an idle agent where it would
//! finish first: its seconds at factor 1 from its own runs over the
//! worker's measured factor, times one plus the worker's fullness with
//! it (Rooms::passes).

use super::*;

/// Rooms of 4 cores, a build of 200 s at factor 1 and 2 cores: rooms pass
/// by expected finish, the fullness weighing in, and by fullness alone
/// where a room's factor is unknown.
#[test]
fn a_build_goes_where_it_would_finish_first() {
    let room = |id, cores| Room {
        id,
        idle: 1,
        size: SMALL,
        load: Demand { cores, memory: 0.0 },
        clientless: Demand::default(),
        empty: false,
        only_clientless: false,
    };
    let build = Demand {
        cores: 2.0,
        memory: 0.0,
    };
    let rows = [
        (
            "faster elsewhere, as full",
            (0.0, Some(1.0)),
            (0.0, Some(2.0)),
            Some(2),
        ),
        (
            "slower elsewhere, as full",
            (0.0, Some(2.0)),
            (0.0, Some(1.0)),
            None,
        ),
        (
            "faster elsewhere, too full",
            (0.0, Some(1.0)),
            (8.0, Some(2.0)),
            None,
        ),
        (
            "faster elsewhere, a little fuller",
            (0.0, Some(1.0)),
            (4.0, Some(2.0)),
            Some(2),
        ),
        (
            "unknown on the asker: by fullness",
            (2.0, None),
            (0.0, Some(2.0)),
            Some(2),
        ),
    ];
    for (row, (mine, my_speed), (theirs, their_speed), want) in rows {
        let mut rooms = Rooms::new(1, vec![room(1, mine), room(2, theirs)]);
        let speed = |id| if id == 1 { my_speed } else { their_speed };
        let got = rooms.passes(
            build,
            &mut None,
            |_| true,
            speed,
            Some(200.0),
            Instant::now(),
        );
        assert_eq!(got, want, "{row}");
        let why = if want.is_some() {
            "it finishes first"
        } else {
            ""
        };
        assert_eq!(
            rooms.why,
            if row.contains("unknown") {
                "less full"
            } else {
                why
            },
            "{row}"
        );
    }
}

/// Finish is weighed among the rooms compared, those with an idle agent:
/// a serving room with none and no factor does not turn it to fullness.
/// Asker 1 (factor 1) and room 3 (factor 2) as full; room 2 agentless.
#[test]
fn an_agentless_room_unknown_leaves_finish() {
    let room = |id, idle| Room {
        id,
        idle,
        size: SMALL,
        load: Demand::default(),
        clientless: Demand::default(),
        empty: true,
        only_clientless: false,
    };
    let mut rooms = Rooms::new(1, vec![room(1, 1), room(2, 0), room(3, 1)]);
    let speed = |id| [None, Some(1.0), None, Some(2.0)][usize::try_from(id).unwrap()];
    let build = Demand {
        cores: 2.0,
        memory: 0.0,
    };
    let got = rooms.passes(
        build,
        &mut None,
        |_| true,
        speed,
        Some(200.0),
        Instant::now(),
    );
    assert_eq!(got, Some(3));
}

/// A take reads the build's seconds on each worker from the history: b,
/// taught 2x a's speed, as full and as idle as a, gets a build of 100 s
/// a asks for, and takes it when it asks.
#[tokio::test]
async fn a_take_leaves_a_build_to_the_faster_worker() {
    let (state, (a, a_caps), (b, b_caps)) = two_speeds();
    let mut e = expecting(1.0, 1.0);
    e.secs = Some(100.0);
    let _client = queue(&state, 0, "build-0", e).await;
    let job = state.take_job(&a_caps, Some(a.id)).await;
    assert_eq!(job.map(|j| j.id), None, "left to the faster worker");
    let job = state.take_job(&b_caps, Some(b.id)).await;
    assert_eq!(job.map(|j| j.id), Some(id(0)));
}

/// Workers a and b, as full and as idle, b taught 2x a's speed.
pub(super) fn two_speeds() -> (
    Arc<HubState>,
    (CapsGuard, WorkerCaps),
    (CapsGuard, WorkerCaps),
) {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let a = seated(&state, "a", (2, 1), SMALL, (0.0, 0.0));
    let b = seated(&state, "b", (2, 1), SMALL, (0.0, 0.0));
    {
        let mut p = state.placement.lock().unwrap();
        let build = placement::Build {
            system: "x86_64-linux",
            name: Some("speed-1"),
            declared: false,
            sources: None,
        };
        for _ in 0..50 {
            for (worker, ms) in [("a", 200_000), ("b", 100_000)] {
                let usage = BuildUsage {
                    wall_ms: ms,
                    cpu_usec: ms * 1000,
                    peak_memory_bytes: 1 << 30,
                    ..Default::default()
                };
                let on = placement::On {
                    worker,
                    taught: true,
                };
                p.history.record(&build, 0, &usage, Some(on));
            }
        }
        let (fa, fb) = (p.history.speed("a"), p.history.speed("b"));
        assert!(fb / fa > 1.9, "b taught {fb}, a {fa}");
    }
    (state, a, b)
}
