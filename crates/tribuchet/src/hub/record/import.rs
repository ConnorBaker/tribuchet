//! The one-time import of the hub's journal into the build record: a
//! migration, run once on the farm's hub before the first hub that keeps
//! a record starts, and deleted in the commit after. Each `build usage` line
//! becomes the `result` row and each `dispatching build` line the
//! `dispatch` row the record would hold had it existed then. Nothing the
//! hub runs reads the journal.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::Path;

use rusqlite::Connection;
use rusqlite::types::Value;

use super::{Rest, Row, insert, result_row, take};
use crate::errors::{Result, err_msg};
use crate::hub::placement::{Build, declares};
use crate::proto::BuildUsage;

const QUEUE: &str = "queueing build ";
const DISPATCH: &str = "dispatching build ";
const USAGE: &str = "build usage ";

/// `line` without ANSI escape sequences (ESC '[' ... a final byte from
/// '@' to '~'): the hub's formatter colours field names and `=` where its
/// output is a terminal, and was run so on the farm.
fn plain(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            let mut rest = chars.clone();
            if rest.next() == Some('[') {
                chars = rest;
                chars.by_ref().find(|c| ('@'..='~').contains(c));
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// The `key=value` fields after `message` in a log line, values
/// unquoted. Nix's name charset has no space or quote, nor do the
/// numbers and lists logged with these lines.
fn fields<'a>(line: &'a str, message: &str) -> Option<HashMap<&'a str, &'a str>> {
    let (_, rest) = line.split_once(message)?;
    let mut out = HashMap::new();
    for word in rest.split(' ') {
        if let Some((k, v)) = word.split_once('=') {
            out.insert(k, v.trim_matches('"'));
        }
    }
    Some(out)
}

/// The line's own time, its first word (`2026-10-29T11:05:49.094255Z`),
/// in unix milliseconds.
pub(super) fn at_ms(line: &str) -> Option<i64> {
    let stamp = line.split(' ').next()?.strip_suffix('Z')?;
    let (date, time) = stamp.split_once('T')?;
    let mut d = date.splitn(3, '-').map(str::parse::<i64>);
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut t = hms.splitn(3, ':').map(str::parse::<i64>);
    let (h, min, s) = (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?);
    let ms: i64 = format!("{frac:0<3}").get(..3)?.parse().ok()?;
    // Days from 1970-01-01 to y-m-day (Howard Hinnant's days_from_civil).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    Some(((days * 24 + h) * 60 + min) * 60_000 + s * 1000 + ms)
}

/// A dispatch line's columns as the record types them; Err names the
/// field whose value does not parse.
fn dispatch_row(at: i64, f: &HashMap<&str, &str>) -> Result<Row, &'static str> {
    // The columns after at_ms in the schema's order, by how each is
    // written: text, a bool, an integer, a real.
    const COLS: [(&str, u8); 20] = [
        ("id", b't'),
        ("key", b't'),
        ("worker", b't'),
        ("drv", b't'),
        ("resumed", b'b'),
        ("capacity", b'i'),
        ("assignments", b'i'),
        ("free", b'i'),
        ("sent", b'i'),
        ("features", b't'),
        ("idle_elsewhere", b'i'),
        ("learned", b't'),
        ("expect_mem", b'i'),
        ("expect_cores", b'r'),
        ("expect_secs", b'r'),
        ("oom_runs", b'i'),
        ("load_mem", b'i'),
        ("load_cores", b'r'),
        ("memory", b'i'),
        ("cores", b'r'),
    ];
    let mut values = vec![Value::Integer(at)];
    for (c, kind) in COLS {
        values.push(match (f.get(c), kind) {
            (None, _) => Value::Null,
            (Some(v), b't') => Value::Text((*v).to_owned()),
            (Some(v), b'b') => Value::Integer(i64::from(v.parse::<bool>().map_err(|_| c)?)),
            (Some(v), b'i') => Value::Integer(v.parse().map_err(|_| c)?),
            (Some(v), _) => Value::Real(v.parse().map_err(|_| c)?),
        });
    }
    Ok(Row {
        table: "i_dispatch",
        values,
    })
}

/// What a usage line says; Err names what is missing or does not parse.
/// A usage figure an older line lacks is 0, the worker's "unknown"; a
/// system neither the line nor its build's queueing line names, NULL.
fn result_from(at: i64, f: &HashMap<&str, &str>, seen: &Seen) -> Result<Row, &'static str> {
    let id = f.get("id").ok_or("id")?;
    let num = |k: &'static str| f.get(k).map_or(Ok(0), |v| v.parse::<u64>().map_err(|_| k));
    let usage = BuildUsage {
        wall_ms: num("wall_ms")?,
        cpu_usec: num("cpu_usec")?,
        peak_memory_bytes: num("peak_memory_bytes")?,
        oom_killed: f
            .get("oom_killed")
            .map_or(Ok(false), |v| v.parse().map_err(|_| "oom_killed"))?,
        frozen_ms: num("frozen_ms")?,
    };
    let exit_code: i32 = f
        .get("exit_code")
        .ok_or("exit_code")?
        .parse()
        .map_err(|_| "exit_code")?;
    let system = f
        .get("system")
        .copied()
        .or(seen.systems.get(*id).map(String::as_str));
    let build = Build {
        system: system.unwrap_or_default(),
        name: f.get("drv").copied().filter(|d| !d.is_empty()),
        declared: seen.declared.get(*id).copied().unwrap_or(false),
    };
    let text = |k| f.get(k).map(|v| (*v).to_owned());
    let rest = Rest {
        at_ms: at,
        id: (*id).to_owned(),
        key: text("key"),
        out_path: text("out_path"),
        worker: text("worker"),
    };
    let mut row = result_row(&build, exit_code, &usage, rest);
    row.table = "i_result";
    if system.is_none() {
        row.values[3] = Value::Null; // the system column
    }
    Ok(row)
}

/// What the journal said of each build so far, by build id.
#[derive(Default)]
struct Seen {
    systems: HashMap<String, String>,
    declared: HashMap<String, bool>,
}

/// Lines of each message read, and those that did not parse: how many,
/// and the first few, by line number and why.
#[derive(Default)]
struct Report {
    read: [usize; 3],
    /// Usage lines whose build's system no line names.
    no_system: usize,
    failed: usize,
    first: Vec<String>,
}

/// Read `journal` into the temporary tables, in its order.
fn stage(conn: &Connection, journal: impl BufRead) -> Result<Report> {
    let (mut seen, mut report) = (Seen::default(), Report::default());
    for (n, line) in journal.split(b'\n').enumerate() {
        let line = line?;
        let line = plain(&String::from_utf8_lossy(
            line.strip_suffix(b"\r").unwrap_or(&line),
        ));
        let Some((kind, f)) = [QUEUE, DISPATCH, USAGE]
            .iter()
            .enumerate()
            .find_map(|(k, m)| Some((k, fields(&line, m)?)))
        else {
            continue;
        };
        report.read[kind] += 1;
        let at = at_ms(&line).ok_or("its time");
        let row = at.and_then(|at| match kind {
            0 => {
                let (id, system) = (f.get("id").ok_or("id")?, f.get("system").ok_or("system")?);
                seen.systems.insert((*id).to_owned(), (*system).to_owned());
                Ok(None)
            }
            1 => {
                if let (Some(id), Some(features)) = (f.get("id"), f.get("features")) {
                    let features: Vec<String> = features.split(',').map(str::to_owned).collect();
                    seen.declared.insert((*id).to_owned(), declares(&features));
                }
                dispatch_row(at, &f).map(Some)
            }
            _ => {
                let row = result_from(at, &f, &seen)?;
                report.no_system += usize::from(row.values[3] == Value::Null);
                Ok(Some(row))
            }
        });
        match row {
            Ok(Some(row)) => _ = insert(conn, &row)?,
            Ok(None) => {}
            Err(why) => {
                report.failed += 1;
                if report.first.len() < 10 {
                    report.first.push(format!("line {}: {why}", n + 1));
                }
            }
        }
    }
    Ok(report)
}

/// Import the hub's `journal` (`journalctl -u tribuchet-hub -o cat`) into
/// the record at `path`, no hub writing it: every line counted, none
/// that does not parse dropped (then nothing is written), and the rows
/// placed before every row the record holds, which must all be later
/// than the journal's last line: a second run adds nothing. The lines
/// read: queueing, dispatching and usage lines, and the usage lines that
/// name no system.
pub fn import_journal(path: &Path, journal: impl BufRead) -> Result<[usize; 4]> {
    let Some((conn, _lock, _)) = take(path)? else {
        return Err(err_msg(format!(
            "another process (a hub?) writes {}: stop it first",
            path.display()
        )));
    };
    conn.execute_batch(
        "CREATE TEMP TABLE i_result AS SELECT * FROM result WHERE 0; \
         CREATE TEMP TABLE i_dispatch AS SELECT * FROM dispatch WHERE 0;",
    )?;
    let tx = conn.unchecked_transaction()?;
    let report = stage(&tx, journal)?;
    let [queued, dispatched, used] = report.read;
    eprintln!(
        "read {queued} queueing, {dispatched} dispatching and {used} usage lines; {} did not \
         parse; {} usage lines name no system, nor does a queueing line before them: recorded \
         with none, and not learned from",
        report.failed, report.no_system
    );
    if report.failed > 0 {
        return Err(err_msg(format!(
            "{} lines do not parse, nothing imported; the first: {}",
            report.failed,
            report.first.join("; ")
        )));
    }
    let last: Option<i64> = tx.query_row(
        "SELECT max(m) FROM (SELECT max(at_ms) m FROM i_result UNION ALL SELECT max(at_ms) FROM i_dispatch)",
        [],
        |r| r.get(0),
    )?;
    let first_live: Option<i64> = tx.query_row(
        "SELECT min(m) FROM (SELECT min(at_ms) m FROM result UNION ALL SELECT min(at_ms) FROM dispatch)",
        [],
        |r| r.get(0),
    )?;
    if let (Some(last), Some(live)) = (last, first_live)
        && last >= live
    {
        return Err(err_msg(format!(
            "the record holds a row at {live} ms, not after the journal's last line at {last} ms: \
             nothing imported (import only lines before the record's first, journalctl --until)"
        )));
    }
    for t in ["result", "dispatch"] {
        let cols: Vec<String> = tx
            .prepare(&format!("SELECT name FROM pragma_table_info('{t}')"))?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let cols = cols.join(",");
        // Below every existing row: the fold, in rowid order, sees these first.
        let sql = format!(
            "INSERT INTO main.{t} (rowid, {cols}) SELECT rowid + \
             (SELECT coalesce(min(rowid), (SELECT count(*) FROM i_{t}) + 1) FROM main.{t}) \
             - 1 - (SELECT count(*) FROM i_{t}), {cols} FROM i_{t} ORDER BY rowid"
        );
        tx.execute(&sql, [])?;
    }
    tx.commit()?;
    eprintln!("imported: {used} results, {dispatched} dispatches");
    Ok([queued, dispatched, used, report.no_system])
}
