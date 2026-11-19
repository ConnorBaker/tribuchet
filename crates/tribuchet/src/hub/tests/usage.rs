//! The hub's usage line for a build.

use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::types::Value;

use super::rank::{declare, sized_worker};
use super::*;
use crate::proto::{BuildResult, BuildUsage};

/// A build's usage line names the worker whose session ran it, its
/// system, and the scratch path of the output it is known by (see
/// `named_output`).
#[tokio::test]
async fn the_usage_line_names_the_worker_and_the_output_path() {
    const OUT: &str = "/nix/store/00000000000000000000000000000000-tt-usage";
    let log = Captured::default();
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let mut client = replay.subscribe().await;
    let mut j = job(0, replay);
    j.req.outputs.insert("out".to_owned(), OUT.to_owned());
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w1", 1, &[]);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let usage = Some(BuildUsage::default());
    let result = BuildResult {
        build_id,
        exit_code: 1,
        usage,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    let line = log.line("build usage");
    assert!(line.contains(r#"worker="w1""#), "{line}");
    assert!(line.contains(r#"system="x86_64-linux""#), "{line}");
    assert!(line.contains(&format!(r#"out_path="{OUT}""#)), "{line}");
}

/// What a build's run used is learned, here an OOM-killed run's peak: the
/// next build of its pname, submitted as the attach program does, is
/// dispatched expecting it, where the first had only the prior. A failed
/// run that was not OOM-killed teaches nothing: the next build of its
/// pname takes its prefix's, `tt`.
#[tokio::test]
async fn a_run_teaches_the_next_dispatch_of_its_pname() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = connect(&state, "w1", 1, &[]);
    for n in 0..4u64 {
        let (pname, oom_killed) = if n < 2 {
            ("learn", true)
        } else {
            ("fail", false)
        };
        let req = BuildRequest {
            system: "x86_64-linux".into(),
            builder: format!("/nix/store/{H}-bash/bin/bash"),
            outputs: [("out".to_owned(), format!("/nix/store/{H}-tt-{pname}-{n}.0"))].into(),
            tmp_dir_in_sandbox: "/build".into(),
            store_dir: "/nix/store".into(),
            ..Default::default()
        };
        let mut client = submit(&state, req).await;
        w.to_hub.send(request_job(1, Some(n))).await.unwrap();
        let build_id = w.assignment().await.expect("not assigned").build_id;
        let usage = BuildUsage {
            wall_ms: 2000,
            cpu_usec: 6_000_000,
            peak_memory_bytes: 5 << 30,
            oom_killed,
            ..BuildUsage::default()
        };
        let result = BuildResult {
            build_id,
            exit_code: 1,
            usage: Some(usage),
            ..Default::default()
        };
        w.send(worker_message::Msg::Result(result)).await;
        while client.message().await.is_ok_and(|m| m.is_some()) {}
    }
    let first = log.line("dispatching build");
    assert!(first.contains(r#"drv="tt-learn-0.0""#), "{first}");
    assert!(first.contains(r#"learned="prior""#), "{first}");
    let second = log.line(r#"drv="tt-learn-1.0" resumed"#);
    for field in [r#"learned="key""#, "expect_mem=5368709120"] {
        assert!(second.contains(field), "{field} missing from {second}");
    }
    let failed = log.line(r#"drv="tt-fail-3.0" resumed"#);
    assert!(failed.contains(r#"learned="prefix""#), "{failed}");
}

/// A build that ran and the usage it reported, to learn from.
fn learned_build() -> (Job, BuildUsage) {
    let mut j = job(0, Arc::new(Replay::default()));
    let out = format!("/nix/store/{H}-tt-saved-1.0");
    j.req.outputs.insert("out".to_owned(), out);
    let usage = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 3_000_000,
        peak_memory_bytes: 5 << 30,
        ..BuildUsage::default()
    };
    (j, usage)
}

/// What `state` expects of `j`: its level, and memory in bytes.
fn expected(state: &HubState, j: &Job) -> (&'static str, u64) {
    let e = state.placement.lock().unwrap().expect(j);
    (e.level.name(), placement::bytes(e.demand.memory))
}

/// A build of `name`, from a worker's run of it that used `gib` GiB.
fn run_of(name: &str, gib: u64) -> (Job, BuildUsage) {
    let (mut j, mut usage) = learned_build();
    let out = format!("/nix/store/{H}-{name}");
    j.req.outputs.insert("out".to_owned(), out);
    usage.peak_memory_bytes = gib << 30;
    (j, usage)
}

/// The record at `path` opened into `state`, as the hub opens it.
fn open_record(state: &HubState, path: &Path) -> Option<thread::JoinHandle<()>> {
    record::open(path).install(state)
}

/// Learn `j`'s run as the relay does, from worker w1.
fn learn(state: &HubState, j: &Job, exit_code: i32, usage: &BuildUsage) {
    let rest = record::Rest::of(j, "w1");
    state
        .placement
        .lock()
        .unwrap()
        .learn(j, exit_code, usage, rest);
}

/// What a hub learned, the next hub folds from the record, at a
/// path whose directories the first created.
#[test]
fn learning_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state/tribuchet/record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open_record(&state, &path);
    let (j, usage) = learned_build();
    learn(&state, &j, 0, &usage);
    record::close(&state, writer);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let after = expected(&restarted, &j);
    assert_eq!(after, ("key", 5 << 30), "after a restart");
}

/// The history a hub learned is the fold of the record it wrote, in the
/// order learned, over runs that teach (successes, an OOM kill), one
/// that does not (a failure), pnames sharing a prefix, and more runs of
/// a key than its window.
#[test]
fn the_history_is_the_fold_of_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open_record(&state, &path);
    for n in 0..30u64 {
        let name = ["tt-a", "tt-b-c", "tt-b-d"][usize::try_from(n % 3).unwrap()];
        let (j, mut usage) = run_of(&format!("{name}-{n}.0"), n + 1);
        usage.oom_killed = n == 4;
        // Received in an order its times do not follow: the fold's is
        // the rows' (rowid), the order learned.
        let mut rest = record::Rest::of(&j, "w1");
        rest.at_ms = 1_000_000 - i64::try_from(n).unwrap();
        let exit_code = i32::from(n == 4 || n == 5);
        state
            .placement
            .lock()
            .unwrap()
            .learn(&j, exit_code, &usage, rest);
    }
    let learned = state.placement.lock().unwrap().history.entries();
    record::close(&state, writer);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _writer = open_record(&restarted, &path);
    let folded = restarted.placement.lock().unwrap().history.entries();
    assert_eq!(folded, learned);
}

/// A build run through the hub appends one dispatch row and one result
/// row, the result row with its log line's values.
#[tokio::test]
async fn a_build_appends_a_dispatch_and_a_result_row() {
    const OUT: &str = "/nix/store/00000000000000000000000000000000-tt-rows-1.0";
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let writer = open_record(&state, &path);
    let before = unix_ms();
    let replay = Arc::new(Replay::default());
    let mut client = replay.subscribe().await;
    let mut j = job(0, replay);
    j.req.outputs.insert("out".to_owned(), OUT.to_owned());
    j.features = vec!["big-parallel".to_owned()];
    state.queue.lock().await.push_back(j);
    // A worker that serves big-parallel (connect's serves no feature).
    let mut w = sized_worker(&state, "w1", 4, 8);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let usage = BuildUsage {
        wall_ms: 2000,
        cpu_usec: 3_000_000,
        peak_memory_bytes: 7,
        oom_killed: true,
        frozen_ms: 500,
    };
    let result = BuildResult {
        build_id: build_id.clone(),
        exit_code: 137,
        usage: Some(usage),
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    let after = unix_ms();
    let held = state.clone();
    let closed = tokio::task::spawn_blocking(move || record::close(&held, writer));
    closed.await.unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    let text = |sql: &str| -> Vec<String> {
        let row = db.query_row(sql, [], |r| {
            (0..r.as_ref().column_count())
                .map(|i| r.get::<_, Value>(i).map(|v| format!("{v:?}")))
                .collect::<rusqlite::Result<Vec<_>>>()
        });
        row.unwrap()
    };
    let result = text(
        "SELECT id, key, system, drv, declared, out_path, worker, exit_code, wall_ms, \
         cpu_usec, peak_memory_bytes, oom_killed, frozen_ms FROM result",
    );
    let want = [
        format!("Text({build_id:?})"),
        r#"Text("k0")"#.to_owned(),
        r#"Text("x86_64-linux")"#.to_owned(),
        r#"Text("tt-rows-1.0")"#.to_owned(),
        "Integer(1)".to_owned(),
        format!("Text({OUT:?})"),
        r#"Text("w1")"#.to_owned(),
        "Integer(137)".to_owned(),
        "Integer(2000)".to_owned(),
        "Integer(3000000)".to_owned(),
        "Integer(7)".to_owned(),
        "Integer(1)".to_owned(),
        "Integer(500)".to_owned(),
    ];
    assert_eq!(result, want);
    // The other columns are the line's (the_dispatch_row_holds_the_dispatch_lines_values).
    let dispatch = text("SELECT id, key FROM dispatch");
    assert_eq!(
        dispatch,
        [format!("Text({build_id:?})"), r#"Text("k0")"#.to_owned()]
    );
    let rows = text("SELECT (SELECT count(*) FROM result), (SELECT count(*) FROM dispatch)");
    assert_eq!(rows, ["Integer(1)", "Integer(1)"], "one row each");
    for table in ["result", "dispatch"] {
        let at = text(&format!("SELECT at_ms FROM {table}"));
        let at: i64 = at[0]
            .trim_start_matches("Integer(")
            .trim_end_matches(')')
            .parse()
            .unwrap();
        assert!(
            (before..=after).contains(&at),
            "{table} at {at}, not {before}..={after}"
        );
    }
}

/// Now in unix milliseconds, by the test's own reading of the clock.
fn unix_ms() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    i64::try_from(since.as_millis()).unwrap()
}

/// A requeued build keeps the expectation it was first queued with, not
/// one taken again from what the history learned since.
#[tokio::test]
async fn a_requeued_build_keeps_its_expectation() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (mut j, _) = learned_build();
    j.expected = placement::Expected::prior(true);
    let _client = j.replay.subscribe().await;
    state.requeue(j).await;
    let kept = state.queue.lock().await.front().map(|j| j.expected);
    assert_eq!(kept, Some(placement::Expected::prior(true)));
}

/// A build that declares big-parallel, its key unseen, takes the prior
/// of the builds that do.
#[test]
fn a_declaring_build_takes_their_prior() {
    let state = HubState::for_test(Duration::from_secs(30));
    let (mut j, _) = learned_build();
    j.features = vec!["big-parallel".to_owned()];
    let e = state.placement.lock().unwrap().expect(&j);
    assert_eq!(e, placement::Expected::prior(true));
}

/// The dispatch row holds the dispatch line's values: every column but
/// the hub's time and the build's key, which the line does not carry.
/// Of the other workers that serve the build, two have an idle agent and
/// one has none.
#[tokio::test]
async fn the_dispatch_row_holds_the_dispatch_lines_values() {
    let log = Captured;
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let writer = open_record(&state, &path);
    let mut workers = Vec::new();
    for (name, capacity, free) in [("w1", 3, 2), ("w2", 1, 0), ("w3", 1, 1), ("w4", 2, 2)] {
        let w = sized_worker(&state, name, 4, 8);
        w.to_hub.send(declare(capacity, free, 0)).await.unwrap();
        workers.push(w);
    }
    tokio::time::sleep(QUIET).await;
    let mut j = job(0, Arc::new(Replay::default()));
    let out = format!("/nix/store/{H}-tt-row-1.0");
    j.req.outputs = [("out".to_owned(), out)].into();
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    state.notify.notify_waiters();
    for _ in 0..500 {
        if log.has(&["dispatching build"]) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let line = log.line("dispatching build");
    assert!(line.contains("idle_elsewhere=2"), "{line}");
    let held = state.clone();
    let closed = tokio::task::spawn_blocking(move || record::close(&held, writer));
    closed.await.unwrap();
    let (_, logged) = line.split_once("dispatching build ").unwrap();
    let logged: HashMap<&str, &str> = (logged.split(' '))
        .filter_map(|w| w.split_once('='))
        .map(|(k, v)| (k, v.trim_matches('"')))
        .collect();
    let db = rusqlite::Connection::open(&path).unwrap();
    let mut stmt = db.prepare("SELECT * FROM dispatch").unwrap();
    let names: Vec<String> = stmt.column_names().into_iter().map(str::to_owned).collect();
    let values: Vec<Value> = stmt
        .query_row([], |r| (0..names.len()).map(|i| r.get(i)).collect())
        .unwrap();
    let mut compared = 0;
    for (name, value) in names.iter().zip(&values) {
        if name == "at_ms" || name == "key" {
            continue;
        }
        let line = logged.get(name.as_str()).copied();
        let same = match (value, line) {
            (Value::Null, None) => true,
            (Value::Integer(i), Some("true")) => *i == 1,
            (Value::Integer(i), Some("false")) => *i == 0,
            (Value::Integer(i), Some(l)) => l == i.to_string(),
            (Value::Real(x), Some(l)) => l.parse::<f64>().ok() == Some(placement::round3(*x)),
            (Value::Text(t), Some(l)) => l == t,
            _ => false,
        };
        assert!(same, "{name}: row {value:?}, line {line:?}");
        compared += 1;
    }
    assert_eq!(compared, 19, "columns compared");
    drop(workers);
}
