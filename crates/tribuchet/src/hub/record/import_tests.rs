//! The journal import, removed with it.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, BufReader};
use std::mem;
use std::sync::{Arc, Mutex};

use super::tests::{row, rows, shut};
use super::*;

/// A usage line whose build's system its own line names.
const USAGE: &str = r#"2026-10-29T11:05:49.094255Z  INFO tribuchet::hub::relay: build usage id="b" key="kb" system="x86_64-linux" drv="foo-1" out_path="/nix/store/h-foo-1" worker="w" exit_code=0 wall_ms=1000 cpu_usec=1000000 peak_memory_bytes=1073741824 oom_killed=false frozen_ms=0"#;

#[test]
fn a_journal_time_is_unix_milliseconds() {
    for (stamp, ms) in [
        ("2026-10-29T11:05:49.094255Z", 1_793_271_949_094),
        ("2000-02-29T23:59:59.999Z", 951_868_799_999),
        ("1970-01-01T00:00:00Z", 0),
        ("2026-03-01T00:00:00.5Z", 1_772_323_200_500),
    ] {
        assert_eq!(
            import::at_ms(&format!("{stamp}  INFO x")),
            Some(ms),
            "{stamp}"
        );
    }
    assert_eq!(import::at_ms("... INFO x"), None);
}

/// The import of `journal` into a new record at `path`.
fn import(path: &Path, journal: &str) -> Result<[usize; 4]> {
    import_journal(path, journal.as_bytes())
}

/// Every field of a dispatch line and of a usage line reaches its row:
/// the usage line's system from its build's queueing line, its
/// declaration from its dispatch line's features; a field an older
/// line lacks (expect_secs, frozen_ms here) is NULL or 0.
#[test]
fn the_import_reads_every_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let journal = [
        r#"2026-10-29T11:00:00.000001Z  INFO tribuchet::hub::submit: queueing build id="a" system="aarch64-linux" drv="foo-1""#,
        r#"2026-10-29T11:00:00.002Z  INFO tribuchet::hub::placement: dispatching build id="a" worker="w" drv="foo-1" resumed=true capacity=4 assignments=7 free=2 sent=8 arm="fcfs" features="big-parallel,kvm" idle_elsewhere=3 learned="key" expect_mem=1024 expect_cores=2.5 oom_runs=1 load_mem=4096 load_cores=6.25 memory=8192 cores=16.0"#,
        r#"2026-10-29T11:00:01.5Z  INFO tribuchet::hub::relay: build usage id="a" key="ka" drv="foo-1" worker="w" exit_code=0 wall_ms=10000 cpu_usec=40000000 peak_memory_bytes=4294967296 oom_killed=false"#,
    ]
    .join("\n");
    assert_eq!(import(&path, &journal).unwrap(), [1, 1, 1, 0], "lines read");
    let dispatch = row(&path, "SELECT * FROM dispatch");
    let want = [
        "Integer(1793271600002)",
        r#"Text("a")"#,
        "Null",
        r#"Text("w")"#,
        r#"Text("foo-1")"#,
        "Integer(1)",
        "Integer(4)",
        "Integer(7)",
        "Integer(2)",
        "Integer(8)",
        r#"Text("big-parallel,kvm")"#,
        "Integer(3)",
        r#"Text("key")"#,
        "Integer(1024)",
        "Real(2.5)",
        "Null",
        "Integer(1)",
        "Integer(4096)",
        "Real(6.25)",
        "Integer(8192)",
        "Real(16.0)",
    ];
    assert_eq!(dispatch, want);
    let result = row(&path, "SELECT * FROM result");
    let want = [
        "Integer(1793271601500)",
        r#"Text("a")"#,
        r#"Text("ka")"#,
        r#"Text("aarch64-linux")"#,
        r#"Text("foo-1")"#,
        "Integer(1)",
        "Null",
        r#"Text("w")"#,
        "Integer(0)",
        "Integer(10000)",
        "Integer(40000000)",
        "Integer(4294967296)",
        "Integer(0)",
        "Integer(0)",
    ];
    assert_eq!(result, want);
    let opened = open(&path);
    let foo = Build {
        system: "aarch64-linux",
        name: Some("foo-2"),
        declared: true,
    };
    let e = opened.history.expect(&foo);
    assert_eq!((e.demand.cores, e.demand.memory), (4.0, 4_294_967_296.0));
    shut(opened);
}

/// A usage line whose build's system no line names (queued before the
/// journal begins) is recorded with none, and teaches nothing.
#[test]
fn a_result_with_no_system_known_is_recorded_not_learned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let read = import(&path, &USAGE.replace(r#" system="x86_64-linux""#, "")).unwrap();
    assert_eq!(read, [0, 0, 1, 1], "lines read");
    assert_eq!(row(&path, "SELECT system FROM result"), ["Null"]);
    let opened = open(&path);
    assert_eq!(opened.history.len(), (0, 0));
    shut(opened);
}

/// A line of the three messages that does not parse is counted and
/// named by its line number, and nothing is imported.
#[test]
fn a_line_that_does_not_parse_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    for (n, bad) in [
        USAGE.replace("exit_code=0", "exit_code=x"),
        USAGE.replace("2026-10-29T11:05:49.094255Z", "yesterday"),
        USAGE.replace("wall_ms=1000", "wall_ms=-1"),
        r#"2026-10-29T11:00:00Z  INFO x: dispatching build id="a" capacity=many"#.to_owned(),
        r#"2026-10-29T11:00:00Z  INFO x: queueing build drv="foo-1""#.to_owned(),
    ]
    .iter()
    .enumerate()
    {
        let path = dir.path().join(format!("record{n}.sqlite"));
        let journal = [USAGE, bad.as_str()].join("\n");
        let err = import(&path, &journal).unwrap_err().to_string();
        assert!(
            err.contains("1 lines do not parse") && err.contains("line 2: "),
            "{bad}: {err}"
        );
        assert_eq!(rows(&path, "result") + rows(&path, "dispatch"), 0, "{bad}");
    }
    let path = dir.path().join("record-many.sqlite");
    let bad = USAGE.replace("exit_code=0", "exit_code=x");
    let journal = vec![bad.as_str(); 11].join("\n");
    let err = import(&path, &journal).unwrap_err().to_string();
    let named = err.matches("line ").count();
    assert!(
        err.contains("11 lines do not parse") && named == 10,
        "{err}"
    );
    assert!(
        err.contains("line 10: exit_code") && !err.contains("line 11"),
        "{err}"
    );
}

/// A second import of a journal adds nothing: its lines do not
/// all precede the record's.
#[test]
fn a_second_import_adds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    import(&path, USAGE).unwrap();
    let err = import(&path, USAGE).unwrap_err().to_string();
    assert!(err.contains("nothing imported"), "{err}");
    assert_eq!(rows(&path, "result"), 1);
}

/// An import goes before the record's rows, which must all be later
/// than its lines: the fold sees the journal's runs first. One
/// whose lines meet a live row is refused.
#[test]
fn an_import_goes_before_the_records_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    let opened = open(&path);
    let build = Build {
        system: "x86_64-linux",
        name: Some("foo-1"),
        declared: false,
    };
    for (at, id) in [(1_793_271_949_100, "live1"), (1_793_271_949_200, "live2")] {
        let rest = Rest {
            at_ms: at,
            id: id.into(),
            key: None,
            out_path: None,
            worker: None,
        };
        let row = result_row(&build, 0, &BuildUsage::default(), rest);
        opened.record.as_ref().unwrap().send(row);
    }
    shut(opened);
    let later = USAGE.replace("49.094255Z", "49.100Z");
    let err = import(&path, &later).unwrap_err().to_string();
    assert!(err.contains("nothing imported"), "{err}");
    let earlier = [
        USAGE.replace(r#"id="b""#, r#"id="j1""#),
        USAGE.replace(r#"id="b""#, r#"id="j2""#),
    ];
    import(&path, &earlier.join("\n")).unwrap();
    let db = Connection::open(&path).unwrap();
    let mut stmt = db.prepare("SELECT id FROM result ORDER BY rowid").unwrap();
    let ids: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(ids, ["j1", "j2", "live1", "live2"]);
}

/// A usage line as the hub's formatter writes it to a terminal, its
/// field names and `=` coloured, imports as a plain one does.
#[test]
fn a_coloured_journal_line_imports() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let sink = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(true)
        .with_writer(move || SinkWriter(sink.clone()))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            id = "a",
            system = "x86_64-linux",
            drv = "foo-1",
            exit_code = 0,
            wall_ms = 10_000_u64,
            cpu_usec = 40_000_000_u64,
            peak_memory_bytes = 4_294_967_296_u64,
            oom_killed = false,
            frozen_ms = 0_u64,
            "build usage"
        );
    });
    let journal = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(journal.contains("\u{1b}["), "uncoloured: {journal}");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    import(&path, &journal).unwrap();
    let got = row(&path, "SELECT system, drv, peak_memory_bytes FROM result");
    assert_eq!(
        got,
        [
            r#"Text("x86_64-linux")"#,
            r#"Text("foo-1")"#,
            "Integer(4294967296)"
        ]
    );
}

struct SinkWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for SinkWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A line that is not UTF-8 is read lossily, neither skipped nor the
/// end of the import, and one ending CRLF as one ending LF: here its
/// last field, worker, would keep the CR.
#[test]
fn a_line_not_utf8_or_crlf_reads_as_text() {
    let usage = USAGE.replace(r#"worker="w""#, "");
    let line = [usage.as_bytes(), b" worker=\"w\xff\"\r\n"].concat();
    let journal = [&b"\xff\xfe not text\n"[..], &line].concat();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    import_journal(&path, &journal[..]).unwrap();
    assert_eq!(
        row(&path, "SELECT worker FROM result"),
        ["Text(\"w\u{fffd}\")"]
    );
}

/// The farm's journal imported folds to the history the farm's hub
/// saved: for every key, prefix and class in both, the hub's runs end
/// with the fold's (the hub's may start before the journal does), and it
/// counts as many OOM kills or more. TRIBUCHET_JOURNAL is the journal
/// (`journalctl -u tribuchet-hub -o cat`), TRIBUCHET_HISTORY the hub's
/// placement-history.json, saved after the journal's last line.
#[test]
#[ignore = "reads the farm's journal and history file, named by TRIBUCHET_JOURNAL and TRIBUCHET_HISTORY"]
fn the_farm_journal_folds_to_its_history_file() {
    let var = |v| env::var_os(v).unwrap_or_else(|| panic!("{v} unset"));
    let journal = fs::File::open(var("TRIBUCHET_JOURNAL")).unwrap();
    let farm: History =
        serde_json::from_str(&fs::read_to_string(var("TRIBUCHET_HISTORY")).unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record.sqlite");
    import_journal(&path, BufReader::new(journal)).unwrap();
    // The record's size, and the CPU time of the fold the hub runs at
    // start, on this thread (not wall time).
    let size: u64 = ["", "-wal"]
        .iter()
        .filter_map(|s| fs::metadata(format!("{}{s}", path.display())).ok())
        .map(|m| m.len())
        .sum();
    let (cpu_before, (folded, read), cpu_after) =
        (cpu_us(), take(&path).unwrap().unwrap().2, cpu_us());
    let (keys, entries) = folded.len();
    println!(
        "record {size} bytes; fold of {read} result rows into {keys} keys, {entries} entries: {} ms of CPU",
        (cpu_after - cpu_before) / 1000
    );
    let (farm, folded) = (farm.entries(), folded.entries());
    let mut tally: HashMap<String, usize> = HashMap::new();
    let mut shown = 0;
    for (name, (runs, oom)) in &folded {
        let kind = name.split('/').next().unwrap();
        let verdict = match farm.get(name) {
            None => "only in the fold",
            Some((hub, hub_oom)) if hub == runs && hub_oom == oom => "identical",
            Some((hub, hub_oom)) if hub.ends_with(runs) && hub_oom >= oom => {
                "equal from the journal on"
            }
            Some((hub, hub_oom)) => {
                if shown < 20 {
                    shown += 1;
                    println!(
                        "MISMATCH {name}\n  hub  {hub:?} oom {hub_oom}\n  fold {runs:?} oom {oom}"
                    );
                }
                "differ"
            }
        };
        *tally.entry(format!("{kind} {verdict}")).or_default() += 1;
    }
    for name in farm.keys().filter(|n| !folded.contains_key(*n)) {
        let kind = name.split('/').next().unwrap();
        *tally
            .entry(format!("{kind} only in the hub's"))
            .or_default() += 1;
    }
    let mut tally: Vec<_> = tally.into_iter().collect();
    tally.sort_unstable();
    println!("result rows folded: {read}; entries: {tally:?}");
    assert!(
        tally.iter().all(|(k, _)| !k.ends_with("differ")),
        "{tally:?}"
    );
}

/// This thread's user and system CPU time so far, in microseconds.
fn cpu_us() -> i64 {
    // SAFETY: an all-zero rusage is a valid value, which getrusage fills.
    let mut u: libc::rusage = unsafe { mem::zeroed() };
    // SAFETY: `u` is a valid rusage to write.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_THREAD, &raw mut u) },
        0
    );
    let us = |t: libc::timeval| t.tv_sec * 1_000_000 + t.tv_usec;
    us(u.ru_utime) + us(u.ru_stime)
}
