//! The record's open, writer and close.

use std::fs;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use rusqlite::config::DbConfig;

use super::*;
use crate::hub::metrics;
use crate::hub::testlog::Captured;

/// A reader of the record at `path`, as another process would open it,
/// that never waits for a lock.
fn reader(path: &Path) -> Connection {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    db.busy_timeout(Duration::ZERO).unwrap();
    db
}

fn rows(path: &Path, table: &str) -> i64 {
    let sql = format!("SELECT count(*) FROM {table}");
    reader(path).query_row(&sql, [], |r| r.get(0)).unwrap()
}

/// Close an opened record as the hub does at SIGTERM.
fn shut(opened: Opened) {
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = opened.install(&state);
    close(&state, writer);
}

/// A run of foo-1 received at `at_ms`, as build `id`.
fn result(id: &str, at_ms: i64) -> Row {
    let build = Build {
        system: "x86_64-linux",
        name: Some("foo-1"),
        declared: false,
    };
    let usage = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 1 << 30,
        ..BuildUsage::default()
    };
    let rest = Rest {
        at_ms,
        id: id.into(),
        key: None,
        out_path: None,
        worker: None,
    };
    result_row(&build, 0, &usage, rest)
}

/// Wait until `record`'s writer has committed what it was handed.
fn committed(record: &Recorder) {
    for _ in 0..1000 {
        if record.tally.uncommitted.load(Ordering::Relaxed) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("rows not committed in 10 s");
}

/// Another process writing `path` past the hub's lock, here holding the
/// write lock until COMMIT.
fn hold_write_lock(path: &Path) -> Connection {
    let db = Connection::open(path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    db
}

/// The rows queued to the writer as the hub stops are committed before
/// close returns: here the writer waits for another process's write lock
/// until after close is called.
#[test]
fn rows_queued_at_close_are_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open(&path).install(&state);
    let blocker = hold_write_lock(&path);
    for n in 0..1000 {
        state
            .placement
            .lock()
            .unwrap()
            .append(result(&n.to_string(), n));
    }
    let release = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        blocker.execute_batch("COMMIT").unwrap();
    });
    close(&state, writer);
    assert_eq!(
        rows(&path, "result"),
        1000,
        "close returned before the commit"
    );
    release.join().unwrap();
    assert_eq!(fold(&reader(&path)).unwrap().1, 1000, "rows the fold read");
}

/// A batch whose commit fails, here by another process's trigger, is
/// kept and committed with the next rows, and the failure counted: the
/// record stays the history's fold.
#[test]
fn a_failed_commit_is_kept_for_the_next() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let state = HubState::for_test(Duration::from_secs(30));
    let writer = open(&path).install(&state);
    let record = state.placement.lock().unwrap().record.clone().unwrap();
    let other = Connection::open(&path).unwrap();
    other
        .execute_batch(
            "CREATE TABLE ctl (fail INTEGER); INSERT INTO ctl VALUES (1);
             CREATE TRIGGER t BEFORE INSERT ON result WHEN (SELECT fail FROM ctl)
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    for n in 0..100 {
        record.send(result(&n.to_string(), n));
    }
    for _ in 0..1000 {
        if record.tally.failed.load(Ordering::Relaxed) > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        record.tally.failed.load(Ordering::Relaxed) > 0,
        "no commit failed"
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let shown = rt.block_on(metrics::render(&state));
    let value = |name: &str| -> u64 {
        let line = shown.lines().find(|l| l.starts_with(&format!("{name} ")));
        line.and_then(|l| l.split(' ').nth(1)?.parse().ok())
            .unwrap()
    };
    assert_eq!(value("tribuchet_hub_record_writing"), 1);
    assert_eq!(value("tribuchet_hub_record_uncommitted_rows"), 100);
    assert!(value("tribuchet_hub_record_failed_commits_total") > 0);
    other.execute_batch("UPDATE ctl SET fail = 0").unwrap();
    for n in 100..200 {
        record.send(result(&n.to_string(), n));
    }
    committed(&record);
    drop(record);
    close(&state, writer);
    let db = reader(&path);
    let mut ids = db.prepare("SELECT id FROM result ORDER BY rowid").unwrap();
    let ids: Vec<String> = ids
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let sent: Vec<String> = (0..200).map(|n: i32| n.to_string()).collect();
    assert_eq!(ids, sent, "the failed batch dropped or reordered");
}

/// The hub's record is in WAL mode (in its header), with every commit
/// synced.
#[test]
fn the_record_is_wal_and_synchronous() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let (conn, _lock, _) = take(&path).unwrap().unwrap();
    let synchronous: i64 = conn
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .unwrap();
    assert_eq!(synchronous, 2, "not FULL");
    drop(conn);
    assert_eq!(fs::read(&path).unwrap()[18..20], [2, 2], "not WAL");
}

/// A second hub opening the record folds what it reads, records nothing
/// and says so, and the first records on; another process reads each
/// commit while the hub writes.
#[test]
fn a_record_another_hub_writes_is_read_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let first = open(&path);
    let record = first.record.clone().unwrap();
    record.send(result("a", 1));
    committed(&record);
    let log = Captured::default();
    let _guard = log.install();
    let second = open(&path);
    assert!(
        second.record.is_none() && second.writer.is_none(),
        "a second writer"
    );
    assert_eq!(second.history.len(), (1, 3), "not folded");
    assert!(log.has(&["not written by this hub", "rows=1"]));
    record.send(result("b", 2));
    committed(&record);
    assert_eq!(rows(&path, "result"), 2, "not read while the hub writes");
    drop(record);
    shut(first);
}

/// A result row with no system (an imported build queued before the
/// journal began) is read and teaches nothing.
#[test]
fn a_result_with_no_system_teaches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let opened = open(&path);
    let record = opened.record.clone().unwrap();
    let mut unknown = result("a", 1);
    unknown.values[3] = Value::Null; // the system column
    record.send(unknown);
    record.send(result("b", 2));
    drop(record);
    shut(opened);
    let (history, read) = fold(&reader(&path)).unwrap();
    assert_eq!((read, history.len()), (2, (1, 3)));
}

/// A file that is not a SQLite database, a database of another
/// user_version, one of other tables, one of user_version 1 without the
/// record's tables or with other columns in them, or a WAL database with
/// frames not yet checkpointed, is left byte for byte as it is (its WAL
/// too), with an empty history and nothing recorded.
#[test]
fn a_record_not_ours_is_never_written() {
    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage");
    fs::write(&garbage, [b'x'; 4096]).unwrap();
    let mut files = vec![garbage];
    for (name, sql) in [
        (
            "future",
            "PRAGMA user_version = 2; CREATE TABLE result (a);",
        ),
        (
            "other",
            "CREATE TABLE other (a); INSERT INTO other VALUES (1);",
        ),
        ("v1", "PRAGMA user_version = 1; CREATE TABLE other (a);"),
        (
            "columns",
            "PRAGMA user_version = 1; CREATE TABLE result (a); CREATE TABLE dispatch (a);",
        ),
        (
            "wal",
            "PRAGMA journal_mode = WAL; PRAGMA user_version = 7; CREATE TABLE other (a);",
        ),
    ] {
        let path = dir.path().join(name);
        let db = Connection::open(&path).unwrap();
        db.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
            .unwrap();
        db.execute_batch(sql).unwrap();
        files.push(path);
    }
    let wal = dir.path().join("wal-wal");
    assert!(fs::metadata(&wal).unwrap().len() > 0, "no WAL frames left");
    let wal_before = fs::read(&wal).unwrap();
    for path in files {
        let before = fs::read(&path).unwrap();
        let opened = open(&path);
        assert!(opened.record.is_none(), "{}: recording", path.display());
        assert_eq!(opened.history.len(), (0, 0));
        shut(opened);
        let after = fs::read(&path).unwrap();
        assert!(after == before, "{} written", path.display());
    }
    assert!(fs::read(&wal).unwrap() == wal_before, "the WAL written");
}
