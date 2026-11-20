//! The hub's build record: a SQLite database of every build result
//! with usage and every dispatch, appended to and never changed, which
//! other processes may query while the hub runs. The placement history
//! is its fold: built from the `result` rows in rowid order, each with
//! its build's last `dispatch` row (whether it teaches speed), as the
//! hub starts, and kept equal to it after, each result recorded into the
//! history and handed to the record in one step under the placement
//! lock (Placement::learn). One writer thread commits the rows; a send
//! to it never waits, so no runtime thread does the record's file I/O.
//! A batch whose commit fails is kept and committed with the next; a
//! crash loses only the rows handed over and not yet committed.

use std::fs::{self, File, TryLockError};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params_from_iter};

use super::naming::named_output;
use super::placement::{Build, History, On, Placed, QUIET};
use super::state::{HubState, Job};
use crate::errors::{Result, err_msg};
use crate::proto::BuildUsage;

#[cfg(test)]
#[path = "record/replay.rs"]
mod replay;
#[cfg(test)]
#[path = "record/tests.rs"]
mod tests;

/// How far the record's open (take or read) has got, by SQLite's
/// progress handler on its connections: the fold's own reading of the
/// rows advances it too. What the hub's start reads to tell a long open
/// from a stalled one (sd::extending_start).
pub(in crate::hub) static PROGRESS: AtomicU64 = AtomicU64::new(0);

/// Count SQLite's work on `conn` in PROGRESS, every 1,000 VM steps.
fn counted(conn: Connection) -> rusqlite::Result<Connection> {
    conn.progress_handler(
        1000,
        Some(|| {
            PROGRESS.fetch_add(1, Ordering::Relaxed);
            false
        }),
    )?;
    Ok(conn)
}

/// The schema `user_version` 1 names. A column takes NULL where its value
/// is not known: a field a journal line predates, or a result's system
/// where its build was queued before the imported journal begins.
pub(in crate::hub) const SCHEMA: &str = "
CREATE TABLE result (at_ms INTEGER NOT NULL, id TEXT NOT NULL, key TEXT, system TEXT,
  drv TEXT, declared INTEGER NOT NULL, out_path TEXT, worker TEXT, exit_code INTEGER NOT NULL,
  wall_ms INTEGER NOT NULL, cpu_usec INTEGER NOT NULL, peak_memory_bytes INTEGER NOT NULL,
  oom_killed INTEGER NOT NULL, frozen_ms INTEGER NOT NULL);
CREATE TABLE dispatch (at_ms INTEGER NOT NULL, id TEXT NOT NULL, key TEXT, worker TEXT, drv TEXT,
  resumed INTEGER, capacity INTEGER, assignments INTEGER, free INTEGER, sent INTEGER,
  features TEXT, idle_elsewhere INTEGER, learned TEXT, expect_mem INTEGER, expect_cores REAL,
  expect_secs REAL, oom_runs INTEGER, load_mem INTEGER, load_cores REAL, memory INTEGER,
  cores REAL);
PRAGMA user_version = 1;
";

/// Whether each `result` row's run teaches speed (speed::Placed), by
/// rowid, committed with it; a hub that predates the table ignores it,
/// and a result it wrote gains its row at the next take (migration).
/// VACUUM renumbers rowids, `result` having no INTEGER PRIMARY KEY (one
/// would change the column count every hub inserts by), only past a
/// deleted row (bundled SQLite's behaviour, pinned by a test): the
/// triggers refuse deletes and updates.
pub(in crate::hub) const TAUGHT: &str = "
CREATE TABLE IF NOT EXISTS taught (result INTEGER PRIMARY KEY, quiet INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS result_kept BEFORE DELETE ON result
  BEGIN SELECT RAISE(ABORT, 'the build record is appended to, never changed'); END;
CREATE TRIGGER IF NOT EXISTS result_unchanged BEFORE UPDATE ON result
  BEGIN SELECT RAISE(ABORT, 'the build record is appended to, never changed'); END;
";

/// A row for table `table`: its values, in the schema's column order.
pub(in crate::hub) struct Row {
    pub(in crate::hub) table: &'static str,
    pub(in crate::hub) values: Vec<Value>,
}

/// Now on the hub's wall clock, in unix milliseconds: a row's `at_ms`.
pub(in crate::hub) fn now_ms() -> i64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

/// A value as SQLite stores it: NULL for None.
pub(in crate::hub) fn val<T: Into<Value>>(v: Option<T>) -> Value {
    v.map_or(Value::Null, Into::into)
}

/// An unsigned count as SQLite's signed integer, saturating.
pub(in crate::hub) fn int(x: u64) -> Value {
    Value::Integer(i64::try_from(x).unwrap_or(i64::MAX))
}

/// What a `result` row holds beyond what history learns from, and
/// whether its run teaches speed (a `taught` row).
pub(in crate::hub) struct Rest {
    pub(in crate::hub) placed: Placed,
    pub(in crate::hub) at_ms: i64,
    pub(in crate::hub) id: String,
    pub(in crate::hub) key: Option<String>,
    pub(in crate::hub) out_path: Option<String>,
    pub(in crate::hub) worker: Option<String>,
}

impl Rest {
    /// What `job`'s result row holds beside its usage, received now from
    /// `worker`: the build's id, dedupe key and named output path.
    pub(in crate::hub) fn of(job: &Job, worker: &str) -> Self {
        Self {
            placed: job.placed,
            at_ms: now_ms(),
            id: job.id.clone(),
            key: Some(job.key.clone()),
            out_path: named_output(&job.req).map(|(_, p)| p.clone()),
            worker: Some(worker.to_owned()),
        }
    }
}

/// The `result` row of a build's result: what History::record learns
/// from, and the rest of its usage line.
pub(in crate::hub) fn result_row(b: &Build, exit_code: i32, u: &BuildUsage, rest: Rest) -> Row {
    let values = vec![
        Value::Integer(rest.at_ms),
        rest.id.into(),
        val(rest.key),
        b.system.to_owned().into(),
        val(b.name.map(str::to_owned)),
        b.declared.into(),
        val(rest.out_path),
        val(rest.worker),
        exit_code.into(),
        int(u.wall_ms),
        int(u.cpu_usec),
        int(u.peak_memory_bytes),
        u.oom_killed.into(),
        int(u.frozen_ms),
    ];
    Row {
        table: "result",
        values,
    }
}

/// The `taught` row of the `result` row inserted just before it, in the
/// same unit (Recorder::send_all): whether its run teaches speed.
pub(in crate::hub) fn taught_row(quiet: bool) -> Row {
    Row {
        table: "taught",
        values: vec![quiet.into()],
    }
}

fn insert(conn: &Connection, row: &Row) -> rusqlite::Result<usize> {
    if row.table == "taught" {
        let sql = "INSERT INTO taught VALUES (last_insert_rowid(), ?)";
        return conn
            .prepare_cached(sql)?
            .execute(params_from_iter(&row.values));
    }
    let marks = vec!["?"; row.values.len()].join(",");
    let sql = format!("INSERT INTO {} VALUES ({marks})", row.table);
    conn.prepare_cached(&sql)?
        .execute(params_from_iter(&row.values))
}

/// What the writer has not done: rows handed to it and not yet
/// committed, and commits that failed (the hub's metrics show both).
#[derive(Default)]
pub(in crate::hub) struct Tally {
    pub(in crate::hub) uncommitted: AtomicU64,
    pub(in crate::hub) failed: AtomicU64,
}

/// Where rows go: the writer's channel, and its tally. A row the writer
/// never takes, it being gone, stays uncommitted.
#[derive(Clone)]
pub(in crate::hub) struct Recorder {
    rows: mpsc::Sender<Vec<Row>>,
    pub(in crate::hub) tally: Arc<Tally>,
}

impl Recorder {
    /// A recorder whose rows go to `rows`.
    pub(in crate::hub) fn new(rows: mpsc::Sender<Vec<Row>>) -> Self {
        Self {
            rows,
            tally: Arc::default(),
        }
    }

    /// Hand `row` to the writer. Never waits: the channel is unbounded.
    pub(in crate::hub) fn send(&self, row: Row) {
        self.send_all(vec![row]);
    }

    /// Hand `rows` to the writer as one: they are committed in the same
    /// transaction. Never waits.
    pub(in crate::hub) fn send_all(&self, rows: Vec<Row>) {
        let n = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        self.tally.uncommitted.fetch_add(n, Ordering::Relaxed);
        if self.rows.send(rows).is_err() {
            tracing::error!("build record's writer is gone: rows not recorded");
        }
    }
}

/// The record opened: the history folded from it and, where this hub
/// writes it, where rows go and the writer's thread.
pub(in crate::hub) struct Opened {
    pub(in crate::hub) history: History,
    pub(in crate::hub) record: Option<Recorder>,
    pub(in crate::hub) writer: Option<JoinHandle<()>>,
}

/// Open the record at `path`, creating it (and its directory) if missing,
/// and fold its history. Blocking: run off the runtime. Where another
/// process writes it (it holds `path`.lock), the hub folds what it reads
/// and records nothing; a file that is not a record (not SQLite, another
/// user_version, a database without the record's tables) leaves an empty
/// history and nothing recorded, and is not written to.
pub(in crate::hub) fn open(path: &Path) -> Opened {
    let shown = path.display();
    let taken = take(path).and_then(|taken| match taken {
        Some((conn, lock, folded)) => Ok((folded, Some((conn, lock)))),
        None => Ok((read(path)?, None)),
    });
    let ((history, rows), writing) = match taken {
        Ok(taken) => taken,
        Err(e) => {
            tracing::error!(path = %shown, "build record not readable, so nothing recorded: {e}");
            (Default::default(), None)
        }
    };
    let (keys, entries) = history.len();
    let Some((conn, lock)) = writing else {
        tracing::error!(path = %shown, rows, keys, "build record not written by this hub: folded, nothing recorded");
        return Opened {
            history,
            record: None,
            writer: None,
        };
    };
    let speeds: Vec<String> = (history.speeds().into_iter())
        .map(|(w, f, n)| format!("{w}={f:.3}/{n:.0}"))
        .collect();
    let speeds = speeds.join(",");
    tracing::info!(path = %shown, rows, keys, entries, speeds, "build record folded");
    let (tx, rx) = mpsc::channel();
    let record = Recorder::new(tx);
    let (tally, at) = (record.tally.clone(), shown.to_string());
    let writer = thread::Builder::new()
        .name("build-record".into())
        .spawn(move || {
            let _lock = lock;
            write(&conn, &rx, &tally, &at);
        })
        .expect("spawning the build record's writer");
    Opened {
        history,
        record: Some(record),
        writer: Some(writer),
    }
}

impl Opened {
    /// Make the fold `state`'s history and the record where its rows go,
    /// before the hub serves. The writer, for close.
    pub(in crate::hub) fn install(self, state: &HubState) -> Option<JoinHandle<()>> {
        let mut placement = state.placement.lock().unwrap();
        placement.history = self.history;
        placement.record = self.record;
        self.writer
    }
}

/// The lock on writing the record at `path`, `path` and ".lock": None
/// while another holds it, a hub or the journal import. The file is
/// opened only to read where it exists: a lock left by a run as another
/// user locks too.
fn lock(path: &Path) -> io::Result<Option<File>> {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    let file = (File::open(&name))
        .or_else(|_| File::create_new(&name))
        .or_else(|_| File::open(&name))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// Whether `conn` holds the record's schema (true) or nothing (false);
/// an error for anything else. Reads only: a file refused is unchanged.
fn known(conn: &Connection) -> Result<bool> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let (entries, tables): (i64, i64) = conn.query_row(
        "SELECT count(*), count(*) FILTER (WHERE type = 'table' AND name IN ('result', 'dispatch')) \
         FROM sqlite_schema",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    match (version, entries, tables) {
        (1, _, 2) => Ok(true),
        (0, 0, _) => Ok(false),
        _ => Err(err_msg(format!(
            "user_version {version} with {entries} schema entries is not a build record this hub knows"
        ))),
    }
}

/// The record at `path`, its writer's lock held, and its fold; None
/// where another holds the lock. A missing or empty file gets the schema.
/// The file is checked read only, and switched to WAL (readers do not
/// wait for the writer, nor it for them) with every commit synced only
/// once it folds: one not a record is not written, nor checkpointed.
fn take(path: &Path) -> Result<Option<(Connection, File, (History, u64))>> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let Some(lock) = lock(path)? else {
        return Ok(None);
    };
    let fresh = match fs::metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => true,
        _ => !known(&reader(path)?)?,
    };
    let mut conn = counted(Connection::open(path)?)?;
    // The `taught` table, where the record has the columns the migration
    // reads (it compiles); the migration after, which may fail, as space
    // for its sort runs out, with the record still written (migration).
    let migration = migration();
    let tx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;
    tx.execute_batch(if fresh { SCHEMA } else { "" })?;
    tx.execute_batch(TAUGHT)?;
    tx.prepare(&migration)?;
    tx.commit()?;
    match conn.execute(&migration, []) {
        Ok(0) => {}
        Ok(rows) => {
            tracing::info!(path = %path.display(), rows, "build record: results given taught rows");
        }
        Err(e) => tracing::error!(
            path = %path.display(),
            "build record: results not given taught rows, so folded untaught until a hub takes it again: {e}"
        ),
    }
    let folded = fold(&conn)?;
    let mode: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(err_msg(format!("journal_mode {mode}, not WAL")));
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(Some((conn, lock, folded)))
}

/// The statement giving each `result` row with no `taught` row one, as the hub would
/// have: quiet if the dispatch it answered, its id's last by at_ms at or
/// before it, was quiet and not resumed (speed::quiet).
/// One statement: all or none. A hub that predates the table wrote
/// these, or a hub before it; each take gives only the rows missing.
fn migration() -> String {
    let rule = format!("d.resumed = 0 AND d.load_cores - d.expect_cores <= {QUIET:?} * d.cores");
    format!(
        "WITH m AS (SELECT rowid AS rrow, id, at_ms FROM result r \
             WHERE NOT EXISTS (SELECT 1 FROM taught t WHERE t.result = r.rowid)) \
             INSERT INTO taught SELECT h.rrow, coalesce({rule}, 0) FROM (SELECT rrow, max(drow) \
             OVER (PARTITION BY id ORDER BY at_ms, kind ROWS UNBOUNDED PRECEDING) AS drow FROM \
             (SELECT id, at_ms, 0 AS kind, rowid AS drow, NULL AS rrow FROM dispatch \
             WHERE id IN (SELECT id FROM m) UNION ALL SELECT id, at_ms, 1, NULL, rrow FROM m)) h \
         LEFT JOIN dispatch d ON d.rowid = h.drow WHERE h.rrow IS NOT NULL ORDER BY h.rrow"
    )
}

/// A read-only connection to `path`.
fn reader(path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).and_then(counted)
}

/// The fold of the record at `path` as another process writes it.
fn read(path: &Path) -> Result<(History, u64)> {
    let conn = reader(path)?;
    if !known(&conn)? {
        return Err(err_msg("an empty database"));
    }
    Ok(fold(&conn)?)
}

/// The history the `result` rows teach, in rowid order, and how many
/// rows it read (each_run).
pub(in crate::hub) fn fold(conn: &Connection) -> rusqlite::Result<(History, u64)> {
    let mut history = History::default();
    let read = each_run(conn, |run| {
        history.record(&run.build, run.exit_code, &run.usage, run.on);
    })?;
    Ok((history, read))
}

/// A `result` row's run, as the fold learns it.
pub(in crate::hub) struct Folded<'a> {
    /// When the result was recorded, for the replay tool's windows.
    #[cfg(test)]
    pub(in crate::hub) at_ms: i64,
    pub(in crate::hub) build: Build<'a>,
    pub(in crate::hub) exit_code: i32,
    pub(in crate::hub) usage: BuildUsage,
    pub(in crate::hub) on: Option<On<'a>>,
}

/// Whether `conn`'s record has the `taught` table.
fn has_taught(conn: &Connection) -> rusqlite::Result<bool> {
    let sql = "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'taught'";
    conn.query_row(sql, [], |r| r.get::<_, i64>(0).map(|n| n > 0))
}

/// Each `result` row's run, in rowid order, to `f`; the rows read (one
/// with no system passed over). A run is on the worker its row names,
/// and teaches speed as its `taught` row says: none does that has no
/// row, as in a record only a hub before that table took (migration).
pub(in crate::hub) fn each_run(
    conn: &Connection,
    mut f: impl FnMut(Folded<'_>),
) -> rusqlite::Result<u64> {
    let taught = if has_taught(conn)? {
        "coalesce(t.quiet, 0) FROM result r LEFT JOIN taught t ON t.result = r.rowid"
    } else {
        "0 FROM result r"
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT r.system, r.drv, r.declared, r.exit_code, r.wall_ms, r.cpu_usec, \
         r.peak_memory_bytes, r.oom_killed, r.frozen_ms, r.worker, r.at_ms, {taught} ORDER BY r.rowid"
    ))?;
    let mut rows = stmt.query([])?;
    let mut read = 0;
    while let Some(r) = rows.next()? {
        read += 1;
        let (system, drv): (Option<String>, Option<String>) = (r.get(0)?, r.get(1)?);
        let Some(system) = system else { continue };
        let n = |i| r.get::<_, i64>(i).map(|v| u64::try_from(v).unwrap_or(0));
        let usage = BuildUsage {
            wall_ms: n(4)?,
            cpu_usec: n(5)?,
            peak_memory_bytes: n(6)?,
            oom_killed: r.get(7)?,
            frozen_ms: n(8)?,
        };
        let worker = r.get_ref(9)?.as_str_or_null()?;
        let taught: bool = r.get(11)?;
        f(Folded {
            #[cfg(test)]
            at_ms: r.get(10)?,
            build: Build {
                system: &system,
                name: drv.as_deref(),
                declared: r.get(2)?,
            },
            exit_code: r.get(3)?,
            usage,
            on: worker.map(|worker| On { worker, taught }),
        });
    }
    Ok(read)
}

/// Commit the rows `rx` brings, all those waiting in one transaction,
/// until every sender is gone (the hub stopping): then the last are in.
/// A batch whose commit fails is kept, and tried again with the next
/// rows, as the history learned it; what is still not committed as the
/// hub stops is lost, and said so.
fn write(conn: &Connection, rx: &mpsc::Receiver<Vec<Row>>, tally: &Tally, at: &str) {
    let mut pending = Vec::new();
    loop {
        let stopping = match rx.recv() {
            Ok(rows) => {
                pending.extend(rows);
                false
            }
            Err(_) => true,
        };
        pending.extend(rx.try_iter().flatten());
        if pending.is_empty() {
            return;
        }
        let done = conn.unchecked_transaction().and_then(|tx| {
            for row in &pending {
                insert(&tx, row)?;
            }
            tx.commit()
        });
        match done {
            Ok(()) => {
                let n = u64::try_from(pending.len()).unwrap_or(u64::MAX);
                tally.uncommitted.fetch_sub(n, Ordering::Relaxed);
                pending.clear();
            }
            Err(e) => {
                tally.failed.fetch_add(1, Ordering::Relaxed);
                let (rows, then) = (
                    pending.len(),
                    if stopping {
                        "lost as the hub stops"
                    } else {
                        "kept for the next commit"
                    },
                );
                tracing::error!(
                    path = at,
                    rows,
                    "build record: rows not committed, {then}: {e}"
                );
            }
        }
        if stopping {
            return;
        }
    }
}

/// Stop recording: take the recorder from `state`'s placement, so the
/// writer commits what is queued and ends once the dispatches holding a
/// clone of it are done, and wait for it. Blocking: run off the runtime.
pub(in crate::hub) fn close(state: &HubState, writer: Option<JoinHandle<()>>) {
    drop(state.placement.lock().unwrap().record.take());
    if let Some(writer) = writer
        && writer.join().is_err()
    {
        tracing::error!("build record's writer panicked: rows queued to it lost");
    }
}
