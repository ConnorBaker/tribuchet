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
        placed: Placed::default(),
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
            .append(vec![result(&n.to_string(), n)]);
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

/// A record from before the `taught` table is read with no run taught
/// (keys learned, no speed), and gains it as a hub takes it: each result
/// taught if the dispatch it answered, its id's last by at_ms at or
/// before it, was quiet and not resumed.
#[test]
fn a_record_from_before_taught_gains_it_by_the_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    // A dispatch's at_ms, resumed and load cores; each result at 20.
    type Dispatch = (i64, i64, Option<f64>);
    let builds: [(&str, &str, &[Dispatch]); 6] = [
        ("quiet", "wa", &[(10, 0, Some(2.0))]),
        ("loud", "wa", &[(10, 0, Some(3.5))]),
        ("resumed", "wa", &[(10, 1, Some(1.0))]),
        ("none", "wa", &[]),
        ("held", "wb", &[(10, 0, Some(1.0)), (30, 0, Some(4.0))]),
        ("journal", "wb", &[(10, 0, None)]),
    ];
    for (id, worker, dispatches) in builds {
        for &(d_at, resumed, load) in dispatches {
            db.execute(
                "INSERT INTO dispatch (at_ms, id, worker, resumed, capacity, expect_cores, \
                 load_cores, cores) VALUES (?1, ?2, ?3, ?4, 1, 1.0, ?5, 4.0)",
                rusqlite::params![d_at, id, worker, resumed, load],
            )
            .unwrap();
        }
        let mut row = result(id, 20);
        row.values[7] = worker.to_owned().into();
        insert(&db, &row).unwrap();
    }
    drop(db);
    let (old, _) = read(&path).unwrap();
    assert!(old.len().0 > 0 && old.speeds().iter().all(|s| s.2 == 0.0));
    let opened = open(&path);
    shut(opened);
    assert_eq!(taught(&path), ["held", "quiet"]);
    assert_eq!(rows(&path, "taught"), 6);
    let (new, _) = read(&path).unwrap();
    assert!(new.speeds().iter().any(|s| s.2 > 0.0), "nothing compared");
}

/// The ids of the results `path`'s record says teach speed.
fn taught(path: &Path) -> Vec<String> {
    reader(path)
        .prepare("SELECT r.id FROM taught t JOIN result r ON r.rowid = t.result WHERE t.quiet ORDER BY 1")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// A quiet dispatch of build `id` on worker `worker` at `at_ms`, and its
/// result at `at_ms` + 10, inserted as a hub before the `taught` table
/// would.
fn quiet_run(db: &Connection, id: &str, worker: &str, at_ms: i64) {
    db.execute(
        "INSERT INTO dispatch (at_ms, id, worker, resumed, capacity, expect_cores, load_cores, \
         cores) VALUES (?1, ?2, ?3, 0, 1, 1.0, 1.0, 4.0)",
        rusqlite::params![at_ms, id, worker],
    )
    .unwrap();
    let mut row = result(id, at_ms + 10);
    row.values[7] = worker.to_owned().into();
    insert(db, &row).unwrap();
}

/// Results a hub before the `taught` table writes after another took the
/// record (a rollback) gain their rows as a hub takes it again, the rows
/// already there kept: no result is left untaught.
#[test]
fn results_written_by_an_older_hub_are_taught_at_the_next_take() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    quiet_run(&db, "first", "wa", 10);
    drop(db);
    shut(open(&path));
    let db = Connection::open(&path).unwrap();
    db.execute("UPDATE taught SET quiet = 0", []).unwrap();
    quiet_run(&db, "older", "wa", 30);
    drop(db);
    shut(open(&path));
    assert_eq!(taught(&path), ["older"]);
    assert_eq!(rows(&path, "taught"), rows(&path, "result"));
}

/// The record refuses a delete or an update of a result, so a VACUUM,
/// which renumbers rowids only past a deleted row, keeps each `taught`
/// row naming its result.
#[test]
fn a_vacuum_keeps_the_taught_rows_on_their_results() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    quiet_run(&db, "a", "wa", 10);
    db.execute("INSERT INTO dispatch (at_ms, id) VALUES (20, 'b')", [])
        .unwrap();
    insert(&db, &result("b", 30)).unwrap();
    quiet_run(&db, "c", "wa", 40);
    drop(db);
    shut(open(&path));
    let db = Connection::open(&path).unwrap();
    let deleted = db.execute("DELETE FROM result WHERE id = 'b'", []);
    let updated = db.execute("UPDATE result SET id = 'd' WHERE id = 'b'", []);
    assert!(
        deleted.is_err() && updated.is_err(),
        "{deleted:?} {updated:?}"
    );
    db.execute_batch("VACUUM").unwrap();
    drop(db);
    assert_eq!(taught(&path), ["a", "c"]);
}

/// A record whose missing `taught` rows cannot be given (here a trigger
/// refuses them) is still folded and written: those results untaught
/// until a hub takes it again, nothing else lost.
#[test]
fn a_failed_migration_still_folds_and_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(TAUGHT).unwrap();
    quiet_run(&db, "a", "wa", 10);
    db.execute_batch(
        "CREATE TRIGGER refuse BEFORE INSERT ON taught WHEN NEW.result = 1 \
         BEGIN SELECT RAISE(ABORT, 'refused'); END",
    )
    .unwrap();
    drop(db);
    let opened = open(&path);
    assert!(opened.history.len().0 > 0, "nothing folded");
    let record = opened.record.clone().expect("written by this hub");
    record.send_all(vec![result("b", 30), taught_row(true)]);
    drop(record);
    shut(opened);
    assert_eq!(
        (rows(&path, "result"), taught(&path)),
        (2, vec!["b".to_owned()])
    );
}
