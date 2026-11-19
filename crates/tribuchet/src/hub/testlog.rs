//! Log capture for tests that check what a log line carries. One
//! global subscriber writes each event to a buffer of the emitting
//! thread (a current-thread test runtime runs its tasks there). A
//! scoped subscriber per test lost events: as the only dispatcher, it
//! let tracing-core cache a callsite's interest from the default of the
//! thread that hit it first (callsite.rs, Rebuilder::JustOne).

use std::cell::RefCell;
use std::io;
use std::sync::Once;

thread_local! {
    static SINK: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

struct Writer;

impl io::Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        SINK.with(|s| s.borrow_mut().extend_from_slice(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct Captured;

/// Clears the thread's captured lines when the capture ends.
pub(super) struct Guard;

impl Drop for Guard {
    fn drop(&mut self) {
        SINK.with(|s| s.borrow_mut().clear());
    }
}

impl Captured {
    /// Capture this thread's events from now until the guard drops.
    pub(super) fn install(&self) -> Guard {
        static GLOBAL: Once = Once::new();
        GLOBAL.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(|| Writer)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("tests install no other global subscriber");
        });
        SINK.with(|s| s.borrow_mut().clear());
        Guard
    }

    /// Whether a captured line contains each of `needles`.
    pub(super) fn has(&self, needles: &[&str]) -> bool {
        let log = SINK.with(|s| String::from_utf8(s.borrow().clone()).unwrap());
        log.lines().any(|l| needles.iter().all(|n| l.contains(n)))
    }

    /// The first captured line containing `needle`.
    pub(super) fn line(&self, needle: &str) -> String {
        let log = SINK.with(|s| String::from_utf8(s.borrow().clone()).unwrap());
        log.lines()
            .find(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("no {needle:?} line in {log}"))
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::process::Command;
    use std::thread;

    use super::*;

    #[test]
    #[ignore = "run alone, in its own process, by the test below"]
    fn probe_alone() {
        let log = Captured::default();
        let _guard = log.install();
        let probe = || tracing::info!(probe = 1, "testlog probe");
        thread::spawn(probe).join().unwrap();
        probe();
        assert!(log.line("testlog probe").contains("probe=1"));
    }

    /// Alone in a process the capture is the only dispatcher, the case
    /// where tracing-core takes a new callsite's interest from the
    /// thread that reaches it first; in the full suite other captures
    /// may be live and hide it.
    #[test]
    fn a_callsite_another_thread_reaches_first_is_still_captured() {
        let out = Command::new(env::current_exe().unwrap())
            .args(["--exact", "hub::testlog::tests::probe_alone"])
            .args(["--ignored", "--test-threads=1"])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains("1 passed"), "{text}");
    }
}
