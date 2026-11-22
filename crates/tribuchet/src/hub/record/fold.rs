//! The fold of the record: its `result` rows' runs, in rowid order, each
//! with its sources and whether it teaches speed, learned into a history.

use rusqlite::Connection;

use super::super::placement::{Build, History, On};
use crate::proto::BuildUsage;

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

/// Whether `conn`'s record has the table `name`.
fn has_table(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    let sql = "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1";
    conn.query_row(sql, [name], |r| r.get::<_, i64>(0).map(|n| n > 0))
}

/// Each `result` row's run, in rowid order, to `f`; the rows read. A row
/// with no system (its build queued before the journal it was imported
/// from begins) is passed over: history keys every run by its system. A
/// run has the sources its `source` row gives (none in a record without
/// the table), is on the worker its row names, and teaches speed as its
/// `taught` row says: none does that has no row, as in a record only a
/// hub before that table took (migration).
pub(in crate::hub) fn each_run(
    conn: &Connection,
    mut f: impl FnMut(Folded<'_>),
) -> rusqlite::Result<u64> {
    let (taught, by_taught) = if has_table(conn, "taught")? {
        (
            "coalesce(t.quiet, 0)",
            "LEFT JOIN taught t ON t.result = r.rowid",
        )
    } else {
        ("0", "")
    };
    let (sources, by_source) = if has_table(conn, "source")? {
        (
            "s.sources",
            "LEFT JOIN source s ON s.at_ms = r.at_ms AND s.id = r.id",
        )
    } else {
        ("NULL", "")
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT r.system, r.drv, r.declared, r.exit_code, r.wall_ms, r.cpu_usec, \
         r.peak_memory_bytes, r.oom_killed, r.frozen_ms, r.worker, r.at_ms, {taught}, \
         {sources} FROM result r {by_taught} {by_source} ORDER BY r.rowid"
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
            profile: None,
        };
        let worker = r.get_ref(9)?.as_str_or_null()?;
        let taught: bool = r.get(11)?;
        let sources = r.get_ref(12)?.as_str_or_null()?;
        f(Folded {
            #[cfg(test)]
            at_ms: r.get(10)?,
            build: Build {
                system: &system,
                name: drv.as_deref(),
                declared: r.get(2)?,
                sources,
            },
            exit_code: r.get(3)?,
            usage,
            on: worker.map(|worker| On { worker, taught }),
        });
    }
    Ok(read)
}
