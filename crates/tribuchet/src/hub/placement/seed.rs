//! Seeding the placement history from the hub's journal: every build's
//! usage line, its system from that line or its queueing line, and
//! whether it declares big-parallel from its dispatch line's features
//! (logged since "hub: name a build's features and the room for it on
//! its dispatch line"; before that, none is taken to).

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead};
use std::path::Path;

use super::history::{Build, History};
use super::{declares, lock_history, reserved_name};
use crate::errors::{Result, err_ctx, err_msg};
use crate::fsutil::io_ctx;
use crate::hub::state::write_atomic;
use crate::proto::BuildUsage;

/// `line` without ANSI escape sequences (ESC '[' ... a final byte from
/// '@' to '~'): tracing's formatter colours a line's field names and
/// `=` where its output is a terminal, and was run so in the journal.
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

/// What the journal says of the builds seen so far, by build id.
#[derive(Default)]
struct Seen {
    systems: HashMap<String, String>,
    declared: HashMap<String, bool>,
}

/// Record every build usage line of `journal` in `history`, in order.
/// Returns the usage lines read.
pub(super) fn seed(history: &mut History, journal: impl BufRead) -> io::Result<usize> {
    let (mut seen, mut runs) = (Seen::default(), 0);
    for line in journal.split(b'\n') {
        let line = line?;
        let line = plain(&String::from_utf8_lossy(
            line.strip_suffix(b"\r").unwrap_or(&line),
        ));
        if let Some(f) = fields(&line, "queueing build ") {
            if let (Some(id), Some(system)) = (f.get("id"), f.get("system")) {
                seen.systems.insert((*id).to_owned(), (*system).to_owned());
            }
        } else if let Some(f) = fields(&line, "dispatching build ") {
            if let (Some(id), Some(features)) = (f.get("id"), f.get("features")) {
                let features: Vec<String> = features.split(',').map(str::to_owned).collect();
                seen.declared.insert((*id).to_owned(), declares(&features));
            }
        } else if let Some(f) = fields(&line, "build usage ") {
            let num = |k| f.get(k).and_then(|v| v.parse::<u64>().ok());
            let id = f.get("id").copied().unwrap_or_default();
            let system = f
                .get("system")
                .copied()
                .or(seen.systems.get(id).map(String::as_str));
            let (Some(system), Some(exit_code)) =
                (system, f.get("exit_code").and_then(|v| v.parse().ok()))
            else {
                continue;
            };
            let usage = BuildUsage {
                wall_ms: num("wall_ms").unwrap_or(0),
                cpu_usec: num("cpu_usec").unwrap_or(0),
                peak_memory_bytes: num("peak_memory_bytes").unwrap_or(0),
                oom_killed: f.get("oom_killed") == Some(&"true"),
                frozen_ms: num("frozen_ms").unwrap_or(0),
            };
            let build = Build {
                system,
                name: f.get("drv").copied().filter(|d| !d.is_empty()),
                declared: seen.declared.get(id).copied().unwrap_or(false),
            };
            history.record(&build, exit_code, &usage);
            runs += 1;
        }
    }
    Ok(runs)
}

/// Seed a new history file at `path` from the hub's `journal`. Refused
/// at a name ending .tmp or .lock (a hub's), while a hub holds its lock
/// (it saves its own over it), where `path` exists (a history the
/// journal may repeat), and where it seeds nothing (an empty stdin,
/// failed builds only), an empty history a swap would put in. Lines not
/// UTF-8 are read with their bad bytes replaced.
pub fn seed_history(path: &Path, journal: impl BufRead) -> Result<()> {
    if reserved_name(path) {
        return Err(err_msg(format!(
            "{}: a hub's temporary file or lock is named so",
            path.display()
        )));
    }
    let Some(_lock) = lock_history(path).map_err(io_ctx("locking", path))? else {
        return Err(err_msg(format!(
            "a hub or seed-history holds {}: a hub saves its own history over it",
            path.display()
        )));
    };
    if fs::symlink_metadata(path).is_ok() {
        return Err(err_msg(format!(
            "{} exists: seed only a new file",
            path.display()
        )));
    }
    let mut history = History::default();
    let lines = seed(&mut history, journal).map_err(err_ctx("reading the journal"))?;
    if history.len().1 == 0 {
        return Err(err_msg(format!(
            "nothing to seed (usage lines read: {lines}): is stdin the hub's journal?"
        )));
    }
    let json = serde_json::to_string(&history).map_err(err_ctx("serialising the history"))?;
    write_atomic(path, &json)?;
    let (keys, entries) = history.len();
    tracing::info!(path = %path.display(), lines, keys, entries, "placement history seeded");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)] // exact byte counts

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::hub::placement::{Expected, open_history};
    use crate::hub::state::HubState;

    fn build<'a>(system: &'a str, name: &'a str, declared: bool) -> Build<'a> {
        Build {
            system,
            name: Some(name),
            declared,
        }
    }

    /// A usage line counts with the system of its queueing line, or its
    /// own, and the declaration of its dispatch line; lines of other
    /// messages, and a usage line with no system known, count nothing.
    #[test]
    fn the_journal_seeds_usage_by_system_and_declaration() {
        let journal = [
            r#"... INFO tribuchet::hub::submit: queueing build id="a" system="x86_64-linux" drv="foo-1""#,
            r#"... INFO tribuchet::hub::placement: dispatching build id="a" worker="w" drv="foo-1" features="big-parallel,kvm""#,
            r#"... INFO tribuchet::hub::relay: build usage id="a" key="ka" drv="foo-1" worker="w" exit_code=0 wall_ms=10000 cpu_usec=40000000 peak_memory_bytes=4294967296 oom_killed=false frozen_ms=0"#,
            r#"... INFO tribuchet::hub::relay: build usage id="b" key="kb" system="aarch64-linux" drv="bar-2" worker="w" exit_code=0 wall_ms=1000 cpu_usec=1000000 peak_memory_bytes=1073741824 oom_killed=false frozen_ms=0"#,
            r#"... INFO tribuchet::hub::relay: build usage id="c" key="kc" drv="baz-1" worker="w" exit_code=0 wall_ms=1000 cpu_usec=1000000 peak_memory_bytes=1 oom_killed=false frozen_ms=0"#,
            "... INFO tribuchet::hub: worker registered worker=\"w\"",
        ]
        .join("\n");
        let mut h = History::default();
        assert_eq!(seed(&mut h, journal.as_bytes()).unwrap(), 2);
        let foo = h.expect(&build("x86_64-linux", "foo-2", true));
        assert_eq!(
            (foo.demand.cores, foo.demand.memory),
            (4.0, 4_294_967_296.0)
        );
        // Learned in the declaring class: an unseen declaring build takes it.
        let unseen = h.expect(&build("x86_64-linux", "qux-1", true));
        assert_eq!(unseen.demand.memory, 4_294_967_296.0);
        let bar = h.expect(&build("aarch64-linux", "bar-1", false));
        assert_eq!(bar.demand.memory, 1_073_741_824.0);
        assert_eq!(h.len().0, 2);
        // bar-2 had no dispatch line: it was learned as not declaring.
        let declaring = h.expect(&build("aarch64-linux", "qux-1", true));
        assert_eq!(
            declaring,
            Expected::prior(true),
            "undeclared seeded as declaring"
        );
    }

    /// A usage line as the hub's formatter writes it to a terminal, its
    /// field names and `=` coloured, seeds as a plain one does.
    #[test]
    fn a_coloured_journal_line_seeds() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(true)
            .with_writer(move || SinkWriter(sink.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                id = "a",
                key = "ka",
                system = "x86_64-linux",
                drv = "foo-1",
                worker = "w",
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
        let mut h = History::default();
        assert_eq!(seed(&mut h, journal.as_bytes()).unwrap(), 1, "{journal}");
        let foo = h.expect(&build("x86_64-linux", "foo-2", false));
        assert_eq!(foo.demand.memory, 4_294_967_296.0);
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

    /// No seeding over a file that exists, a history or an empty one,
    /// nor while a hub holds its lock.
    #[tokio::test]
    async fn seeding_is_refused_over_a_file_or_beside_a_running_hub() {
        let dir = tempfile::tempdir().unwrap();
        let (path, new) = (dir.path().join("h.json"), dir.path().join("new.json"));
        let empty = dir.path().join("h.empty");
        fs::write(&path, "{}").unwrap();
        fs::write(&empty, "").unwrap();
        for file in [&path, &empty] {
            let err = seed_history(file, USAGE.as_bytes())
                .unwrap_err()
                .to_string();
            assert!(err.contains("exists"), "{err}");
        }
        assert_eq!(fs::read(&empty).unwrap(), b"", "seeded over the empty file");
        let state = HubState::for_test(Duration::from_secs(30));
        let _hub = open_history(&state, new.clone()).await;
        let err = seed_history(&new, &b""[..]).unwrap_err().to_string();
        assert!(err.contains("a hub or seed-history holds"), "{err}");
    }

    /// No seeding that learns nothing, from a journal without a usage
    /// line or with failed builds' only: nothing written.
    #[test]
    fn seeding_that_learns_nothing_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let failed = r#"... build usage id="a" system="x86_64-linux" drv="foo-1" exit_code=1 wall_ms=1000 cpu_usec=1000000 peak_memory_bytes=1073741824 oom_killed=false frozen_ms=0"#;
        // A path each: a child another test forks may hold a lock a while.
        for (n, journal) in ["... INFO tribuchet::hub: worker registered", failed]
            .iter()
            .enumerate()
        {
            let path = dir.path().join(format!("h{n}.json"));
            let err = seed_history(&path, journal.as_bytes()).unwrap_err();
            assert!(err.to_string().contains("nothing to seed"), "{err}");
            assert!(!path.exists(), "a history was written");
        }
    }

    /// No seeding into a name a hub writes through or locks with.
    #[test]
    fn seeding_is_refused_into_a_temporary_or_lock_name() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["h.json.tmp", "h.json.lock"] {
            let path = dir.path().join(name);
            let err = seed_history(&path, USAGE.as_bytes()).unwrap_err();
            assert!(err.to_string().contains("temporary file or lock"), "{err}");
            assert!(!path.exists(), "seeded into {name}");
        }
    }

    /// seed-history holds SIDE's lock while it reads the journal: a hub
    /// started then finds it held. A stale SIDE.tmp is removed first.
    #[test]
    fn seeding_holds_the_lock_while_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        let (path, tmp) = (dir.path().join("h.json"), dir.path().join("h.json.tmp"));
        fs::write(&tmp, "stale").unwrap();
        let held = Arc::new(Mutex::new(None));
        let journal = Held(path.clone(), held.clone(), USAGE.as_bytes());
        seed_history(&path, io::BufReader::new(journal)).unwrap();
        assert_eq!(*held.lock().unwrap(), Some(true), "not held while reading");
        assert!(!tmp.exists(), "a stale SIDE.tmp left");
    }

    /// seed-history writes SIDE through SIDE.tmp only: with a directory
    /// in the way there, it fails, and no SIDE appears. (A writer that
    /// also wrote SIDE in place would get past this.)
    #[test]
    fn seeding_writes_only_through_side_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.json");
        fs::create_dir_all(dir.path().join("h.json.tmp/in-the-way")).unwrap();
        assert!(seed_history(&path, USAGE.as_bytes()).is_err());
        assert!(!path.exists(), "SIDE written in place");
    }

    /// A journal that, as it is first read, records whether `0`'s lock
    /// is held.
    struct Held(PathBuf, Arc<Mutex<Option<bool>>>, &'static [u8]);

    impl io::Read for Held {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut held = self.1.lock().unwrap();
            held.get_or_insert_with(|| lock_history(&self.0).unwrap().is_none());
            self.2.read(buf)
        }
    }

    /// A seedable usage line.
    const USAGE: &str = r#"... build usage id="a" system="x86_64-linux" drv="foo-1" exit_code=0 wall_ms=1000 cpu_usec=1000000 peak_memory_bytes=1073741824 oom_killed=false frozen_ms=0"#;

    /// A line that is not UTF-8 is read lossily, neither skipped nor the
    /// end of the seed, and one ending CRLF as one ending LF: here its
    /// last field, exit_code, would not parse with the CR.
    #[test]
    fn a_line_not_utf8_or_crlf_reads_as_text() {
        let usage = USAGE.replace(" exit_code=0", "");
        let line = [usage.as_bytes(), b" worker=\"w\xff\" exit_code=0\r\n"].concat();
        let journal = [&b"\xff\xfe not text\n"[..], &line].concat();
        assert_eq!(seed(&mut History::default(), &journal[..]).unwrap(), 1);
    }
}
