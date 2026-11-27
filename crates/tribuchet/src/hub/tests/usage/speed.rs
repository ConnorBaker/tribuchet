//! What the hub learns of worker speed, live and folded from its record.

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use super::super::rank::sized_worker;
use super::*;
use crate::proto::{BuildResult, BuildUsage, Resumed};

/// Rounds of one key over three workers' sessions (2x and 3x the
/// first's speed), each dispatched, run and learned as the hub does: a
/// round `loaded` is dispatched beside 2 cores in flight, and `hook`
/// runs between dispatch and learn (it may change the run or the
/// sessions). What the hub learned and what the next hub folds.
fn speed_round_trip(
    rounds: usize,
    loaded: impl Fn(usize) -> bool,
    mut hook: impl FnMut(&HubState, usize, (u64, &'static str), &mut Job, &mut BuildUsage),
) -> [HashMap<String, (Vec<placement::Entry>, u32)>; 2] {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open_record(&state, &path);
    let workers = [(1, "wa", 12_000), (2, "wb", 6_000), (3, "wc", 4_000)];
    let admission = Arc::new(Mutex::new(Admission::default()));
    for (id, _, _) in workers {
        (state.placement.lock().unwrap()).open(id, admission.clone(), SIZE);
    }
    for n in 0..rounds {
        let (session, worker, ms) = workers[n % 3];
        let (mut j, mut usage) = run_of("tt-speed-1.0", 1);
        j.id = format!("{n:032x}");
        usage.wall_ms = ms;
        if loaded(n) {
            let busy = placement::Demand {
                cores: 2.0,
                memory: 0.0,
            };
            let mut p = state.placement.lock().unwrap();
            let s = p.sessions.get_mut(&session).unwrap();
            s.committed.insert("in-flight".into(), in_flight(busy));
        }
        j.placed = placement::dispatch(&state, (session, worker), &j, false, &admission);
        hook(&state, n, (session, worker), &mut j, &mut usage);
        let rest = record::Rest::of(&j, worker);
        let mut p = state.placement.lock().unwrap();
        p.learn(&j, 0, &usage, rest);
        p.settled(session, &j.id);
        p.settled(session, "in-flight");
    }
    let learned = state.placement.lock().unwrap().history.entries();
    record::close(&state, writer);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let folded = restarted.placement.lock().unwrap().history.entries();
    [learned, folded]
}

/// Four cores, no memory reported.
const SIZE: placement::Size = placement::Size {
    cores: 4.0,
    memory: None,
};

fn speed_of(entries: &HashMap<String, (Vec<placement::Entry>, u32)>, worker: &str) -> f64 {
    entries[&format!("speed/{worker}")].0[0].5
}

/// Speed is the record's fold too, bit for bit, over three workers: a
/// run dispatched to a worker loaded by builds in flight is slow and
/// teaches none.
#[test]
fn speed_is_the_fold_of_the_record() {
    let loaded = |n| n % 5 == 1;
    let [learned, folded] = speed_round_trip(720, loaded, |_, n, _, _, usage| {
        if loaded(n) {
            usage.wall_ms *= 4;
        }
    });
    assert_eq!(folded, learned);
    let (b, c) = (speed_of(&learned, "wb"), speed_of(&learned, "wc"));
    let a = speed_of(&learned, "wa");
    assert!(
        (b / a - 2.0).abs() < 0.15 && (c / a - 3.0).abs() < 0.25,
        "{a} {b} {c}"
    );
}

/// A build dispatched again, loaded, after a quiet dispatch (requeued
/// before it ran) teaches as its last dispatch decided, live and folded.
#[test]
fn the_last_dispatch_decides() {
    let admission = Arc::new(Mutex::new(Admission::default()));
    let [learned, folded] = speed_round_trip(
        300,
        |_| false,
        |state, n, to, j, usage| {
            if n % 6 == 1 {
                {
                    let busy = placement::Demand {
                        cores: 2.0,
                        memory: 0.0,
                    };
                    let mut p = state.placement.lock().unwrap();
                    p.settled(to.0, &j.id);
                    (p.sessions.get_mut(&to.0).unwrap().committed)
                        .insert("in-flight".into(), in_flight(busy));
                }
                j.placed = placement::dispatch(state, to, j, false, &admission);
                state.placement.lock().unwrap().settled(to.0, "in-flight");
                usage.wall_ms *= 4;
            }
        },
    );
    assert_eq!(folded, learned);
    let (a, b) = (speed_of(&learned, "wa"), speed_of(&learned, "wb"));
    // 2x, short of it by the prior: the loaded runs, at 4x, taught none.
    assert!(b / a > 1.7 && b / a < 2.0, "wb {}x wa", b / a);
}

/// A result received again after a resumed dispatch (its worker held
/// the build as it registered again) teaches nothing, though it arrives
/// as if its run started after that dispatch, live and folded.
#[test]
fn a_redelivered_result_teaches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open_record(&state, &path);
    let admission = Arc::new(Mutex::new(Admission::default()));
    for id in [1, 2] {
        (state.placement.lock().unwrap()).open(id, admission.clone(), SIZE);
    }
    let learn = |j: &Job, worker, usage: &BuildUsage| {
        let rest = record::Rest::of(j, worker);
        state.placement.lock().unwrap().learn(j, 0, usage, rest);
    };
    for n in 0..240 {
        let (session, worker, ms) = [(1, "wa", 2), (2, "wb", 1)][n % 2];
        let (mut j, mut usage) = run_of("tt-redelivered-1.0", 1);
        j.id = format!("{n:032x}");
        // Reported in seconds, as a run shorter than SHORTEST teaches no
        // speed; the sleeps keep the record's order in milliseconds.
        usage.wall_ms = ms * 2000;
        j.placed = placement::dispatch(&state, (session, worker), &j, false, &admission);
        thread::sleep(Duration::from_millis(ms + 1));
        learn(&j, worker, &usage);
        state.placement.lock().unwrap().settled(session, &j.id);
        if n % 4 == 1 {
            // Dispatched again a few ms after its result, as delivery
            // failed (6 s apart at least on the farm's record).
            thread::sleep(Duration::from_millis(3));
            j.placed = placement::dispatch(&state, (session, worker), &j, true, &admission);
            usage.wall_ms = 4 * 2000;
            thread::sleep(Duration::from_millis(5));
            learn(&j, worker, &usage);
            state.placement.lock().unwrap().settled(session, &j.id);
        }
    }
    let learned = state.placement.lock().unwrap().history.entries();
    record::close(&state, writer);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let folded = restarted.placement.lock().unwrap().history.entries();
    assert_eq!(folded, learned);
    let (a, b) = (speed_of(&learned, "wa"), speed_of(&learned, "wb"));
    // 2x, short of it by the prior: the redelivered results, at 4x, none
    // (taught, they read about 1.68x).
    assert!(b / a > 1.9 && b / a < 2.0, "wb {}x wa", b / a);
}

/// A result learned after its worker's session closed and another
/// opened (it disconnected between its result and the relay learning
/// it) teaches as its dispatch decided, as the fold has it.
#[test]
fn a_result_after_its_session_closed_folds_alike() {
    let admission = Arc::new(Mutex::new(Admission::default()));
    let [learned, folded] = speed_round_trip(
        60,
        |_| false,
        |state, n, (session, _), _, _| {
            if n % 2 == 0 {
                let mut p = state.placement.lock().unwrap();
                p.close(session);
                p.open(session, admission.clone(), SIZE);
            }
        },
    );
    assert_eq!(folded, learned);
    assert!(learned["speed/wb"].1 > 0, "nothing compared");
}

/// Builds of one key run on two workers through the hub's worker loop,
/// as the relay learns each result (before its outputs are checked);
/// the worker answering each assignment that it already held the build
/// if `resumed`. What the hub learned and what the next hub folds.
async fn wired(resumed: bool) -> [HashMap<String, (Vec<placement::Entry>, u32)>; 2] {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let writer = open_record(&state, &path);
    let mut sessions = Vec::new();
    for n in 0..8 {
        // A session per build: the last still holds its build, whose
        // outputs never come.
        let (name, ms) = [("wa", 4), ("wb", 2)][n % 2];
        sessions.push(sized_worker(&state, name, 4, 8));
        let w = sessions.last_mut().unwrap();
        let replay = Arc::new(Replay::default());
        let client = replay.subscribe().await;
        let mut j = job(n, replay);
        let out = format!("/nix/store/{H}-tt-wired-1.0");
        j.req.outputs.insert("out".to_owned(), out);
        state.queue.lock().await.push_back(j);
        w.to_hub.send(request_job(1, Some(0))).await.unwrap();
        let build_id = w.assignment().await.expect("not assigned").build_id;
        if resumed {
            let build_id = build_id.clone();
            let resumed = Resumed {
                build_id,
                ..Resumed::default()
            };
            w.send(worker_message::Msg::Resumed(resumed)).await;
        }
        tokio::time::sleep(Duration::from_millis(ms + 1)).await;
        // Reported in seconds, as a run shorter than SHORTEST teaches no
        // speed; the sleep keeps the record's order in milliseconds.
        let usage = BuildUsage {
            wall_ms: ms * 1000,
            cpu_usec: ms * 1_000_000,
            peak_memory_bytes: 1 << 30,
            ..BuildUsage::default()
        };
        let runs = || {
            let entries = state.placement.lock().unwrap().history.entries();
            entries
                .get("classes/x86_64-linux/false")
                .map_or(0, |e| e.0.len())
        };
        let before = runs();
        let result = BuildResult {
            build_id,
            usage: Some(usage),
            ..Default::default()
        };
        w.send(worker_message::Msg::Result(result)).await;
        while runs() == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Its outputs never come: the build ends with its client gone.
        drop(client);
    }
    let learned = state.placement.lock().unwrap().history.entries();
    record::close(&state, writer);
    let (results, taught): (i64, i64) = (rusqlite::Connection::open(&path).unwrap())
        .query_row(
            "SELECT count(*), sum(quiet) FROM taught JOIN result ON result.rowid = taught.result",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((results, taught == 0), (8, resumed), "{taught} taught rows");
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let folded = restarted.placement.lock().unwrap().history.entries();
    [learned, folded]
}

/// A result and its `taught` row are handed to the writer as one unit,
/// so they are committed in one transaction: never one without the
/// other.
#[test]
fn a_result_and_its_taught_row_are_one_unit() {
    let (tx, rx) = mpsc::channel();
    let state = HubState::for_test(Duration::from_secs(30));
    state.placement.lock().unwrap().record = Some(record::Recorder::new(tx));
    let (j, usage) = run_of("tt-unit-1.0", 1);
    let rest = record::Rest::of(&j, "wa");
    state.placement.lock().unwrap().learn(&j, 0, &usage, rest);
    let units: Vec<Vec<&str>> = (rx.try_iter())
        .map(|unit| unit.iter().map(|r| r.table).collect())
        .collect();
    assert_eq!(units, [["result", "taught"]]);
}

/// Builds run through the hub's worker loop teach their workers'
/// factors, the dispatch the loop placed deciding, live and folded.
#[tokio::test]
async fn builds_run_through_the_hub_teach_speed() {
    let [learned, folded] = wired(false).await;
    assert_eq!(folded, learned);
    assert!(learned["speed/wa"].1 > 0 && learned["speed/wb"].1 > 0);
}

/// A build whose worker answers its dispatch (not flagged resumed) that
/// it already held it may have run before it: it teaches nothing, live
/// or folded.
#[tokio::test]
async fn a_build_its_worker_resumed_teaches_nothing() {
    let [learned, folded] = wired(true).await;
    assert_eq!(folded, learned);
    assert_eq!(learned["speed/wb"].1, 0, "a resumed build taught");
}

/// A result of a build never dispatched (the record has 11, from before
/// it recorded dispatches) teaches nothing, live or folded.
#[test]
fn an_undispatched_result_teaches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open_record(&state, &path);
    for n in 0..40 {
        // Seconds well above SHORTEST: undispatched, not short, teaches nothing.
        let (worker, ms) = [("wa", 8_000), ("wb", 4_000)][n % 2];
        let (mut j, mut usage) = run_of("tt-undispatched-1.0", 1);
        j.id = format!("{n:032x}");
        usage.wall_ms = ms;
        let rest = record::Rest::of(&j, worker);
        state.placement.lock().unwrap().learn(&j, 0, &usage, rest);
    }
    let learned = state.placement.lock().unwrap().history.entries();
    record::close(&state, writer);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let folded = restarted.placement.lock().unwrap().history.entries();
    assert_eq!(folded, learned);
    assert_eq!(folded["speed/wb"].1, 0, "an undispatched result taught");
}

/// A dispatch is quiet by the other builds' expected cores alone: a
/// build expecting more than a quarter of the worker's 4 cores, beside
/// another at exactly a quarter, is quiet; beside 1.5 cores, not; and a
/// resumed one never.
#[test]
fn quiet_counts_the_other_builds_alone() {
    let state = HubState::for_test(Duration::from_secs(30));
    let admission = Arc::new(Mutex::new(Admission::default()));
    (state.placement.lock().unwrap()).open(1, admission.clone(), SIZE);
    let (mut j, _) = run_of("tt-quiet-1.0", 1);
    j.expected.demand.cores = 2.0;
    for (others, want) in [(1.0, true), (1.5, false)] {
        {
            let mut p = state.placement.lock().unwrap();
            let s = p.sessions.get_mut(&1).unwrap();
            let busy = placement::Demand {
                cores: others,
                memory: 0.0,
            };
            s.committed.insert("in-flight".into(), in_flight(busy));
        }
        let placed = placement::dispatch(&state, (1, "wa"), &j, false, &admission);
        assert_eq!(placed.quiet, want, "beside {others} cores");
        state.placement.lock().unwrap().settled(1, &j.id);
    }
    // Resumed, it is not quiet however idle the worker.
    state.placement.lock().unwrap().settled(1, "in-flight");
    let placed = placement::dispatch(&state, (1, "wa"), &j, true, &admission);
    assert!(!placed.quiet, "a resumed dispatch quiet");
}

/// A build in flight on a session, of demand `demand`.
fn in_flight(demand: placement::Demand) -> placement::Placed {
    placement::Placed {
        demand,
        owners: Vec::new(),
        key: None,
        ends: None,
    }
}

/// A job's seconds on a worker are read from the history when asked
/// (History::secs_on), not kept with the expectation it read: wa and wb
/// run k alike; a job of k is read; then j's runs, wb 4x faster, refit
/// the factors without a run of k, and the read is still fresh. Its
/// seconds on wb change with the refit, to what the history says now.
#[test]
fn a_refit_counts_in_a_read_expectation() {
    let state = HubState::for_test(Duration::from_secs(30));
    let run = |name: &str, worker: &str, ms: u64| {
        let (mut j, mut usage) = run_of(name, 1);
        j.placed = placement::Teaching { quiet: true };
        usage.wall_ms = ms;
        let rest = record::Rest::of(&j, worker);
        state.placement.lock().unwrap().learn(&j, 0, &usage, rest);
    };
    for _ in 0..50 {
        run("k-1", "wa", 100_000);
        run("k-1", "wb", 100_000);
    }
    let (mut q, _) = run_of("k-1", 1);
    let p = state.placement.lock().unwrap();
    p.refresh(&mut q);
    let before = p.secs_on(&q, "wb").unwrap();
    drop(p);
    for _ in 0..500 {
        run("j-1", "wa", 100_000);
        run("j-1", "wb", 25_000);
    }
    let p = state.placement.lock().unwrap();
    let read = q.expected_at.as_ref().unwrap();
    assert!(p.history.fresh(read), "no run of k since it was read");
    let after = p.secs_on(&q, "wb").unwrap();
    let b = placement::build_of(&q);
    let now = p.history.unit_secs(&b).unwrap() / p.history.speed("wb");
    assert!((after - now).abs() < 1e-9, "{after} against {now}");
    assert!(after < 0.8 * before, "wb's seconds {before} then {after}");
}

/// What follows a build is learned in seconds on a worker of factor 1
/// (History::unit_secs), not as its dependant happened to run: wb runs
/// 2x wa (learned from k's runs); d, made from a's output, runs 50 s on
/// wb or 100 s on wa, and below(a) is d's seconds on a worker of factor
/// 1 (between the 50 and 100 it ran) and the same either way (within
/// 1 %, the fitted ratio's shortfall of 2x), where as d ran it was 50 or
/// 100.
#[test]
fn a_path_is_the_builds_not_its_workers() {
    let below = |worker: &str, ms: u64| {
        let state = HubState::for_test(Duration::from_secs(30));
        let run = |name: &str, inputs: &[String], worker: &str, ms: u64| {
            let (mut j, mut usage) = run_of(name, 1);
            j.req.input_paths = inputs.to_vec();
            j.placed = placement::Teaching { quiet: true };
            usage.wall_ms = ms;
            let rest = record::Rest::of(&j, worker);
            state.placement.lock().unwrap().learn(&j, 0, &usage, rest);
        };
        for _ in 0..500 {
            run("k-1", &[], "wa", 100_000);
            run("k-1", &[], "wb", 50_000);
        }
        run("a-1", &[], "wa", 10_000);
        let made = run_of("a-1", 1).0.req.outputs["out"].clone();
        run("d-1", &[made], worker, ms);
        let p = state.placement.lock().unwrap();
        let unit = (p.history).unit_secs(&placement::build_of(&run_of("d-1", 1).0));
        let below = p.downstream.below("x86_64-linux/a");
        assert!(
            (below - unit.unwrap()).abs() < 1e-9,
            "{below} against {unit:?}"
        );
        below
    };
    let (fast, slow) = (below("wb", 50_000), below("wa", 100_000));
    assert!(fast > 50.0 && fast < 100.0, "below a: {fast}");
    assert!(
        (fast / slow - 1.0).abs() < 0.01,
        "{fast} on wb, {slow} on wa"
    );
}
