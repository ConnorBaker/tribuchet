//! Log capture for tests that check what a log line carries. One
//! global subscriber writes each event to a buffer of the emitting
//! thread (a current-thread test runtime runs its tasks there). A
//! scoped subscriber per test lost events: as the only dispatcher, it
//! let tracing-core cache a callsite's interest from the default of the
//! thread that hit it first (callsite.rs, Rebuilder::JustOne).
//!
//! The global one has a window of its own: set_global_default registers
//! the subscriber (Dispatch::new, which rebuilds every callsite's
//! interest) before it is the global default, and a callsite another
//! thread reaches first in between asks get_default, finds none, and
//! keeps "never" (tracing-core 0.1.36). `go_global` rebuilds after the
//! set. That narrows the race, not closes it: a registration that began
//! before the set (callsite.rs:318-320) can store its "never" after that
//! rebuild (:490-506), so every install rebuilds again, repairing it
//! before that capture reads.

use std::cell::RefCell;
use std::io;
use std::sync::Once;

use tracing::Dispatch;

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

/// The subscriber that writes to each thread's SINK, registered.
fn capture() -> Dispatch {
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(|| Writer)
        .finish();
    Dispatch::new(subscriber)
}

/// Make `d`, registered already, the global default, then rebuild what
/// callsites reached in between cached (the module doc).
fn go_global(d: Dispatch) {
    tracing::dispatcher::set_global_default(d).expect("tests install no other global subscriber");
    tracing::callsite::rebuild_interest_cache();
}

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
        GLOBAL.call_once(|| go_global(capture()));
        // See the module doc: a "never" stored after go_global's rebuild.
        tracing::callsite::rebuild_interest_cache();
        SINK.with(|s| s.borrow_mut().clear());
        Guard
    }

    /// Whether a captured line contains each of `needles`.
    pub(super) fn has(&self, needles: &[&str]) -> bool {
        let log = SINK.with(|s| String::from_utf8(s.borrow().clone()).unwrap());
        log.lines().any(|l| needles.iter().all(|n| l.contains(n)))
    }

    /// How many captured lines contain `needle`.
    pub(super) fn count(&self, needle: &str) -> usize {
        let log = SINK.with(|s| String::from_utf8(s.borrow().clone()).unwrap());
        log.lines().filter(|l| l.contains(needle)).count()
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
    #[ignore = "run alone, in its own process, by a test below"]
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
        passes_alone("probe_alone");
    }

    #[test]
    #[ignore = "run alone, in its own process, by a test below"]
    fn window_alone() {
        let d = capture();
        let probe = || tracing::info!(window = 1, "testlog window");
        thread::spawn(probe).join().unwrap();
        go_global(d);
        probe();
        assert!(Captured.line("testlog window").contains("window=1"));
    }

    /// A callsite another thread reaches first between the capture's
    /// registration and its set as the global default is captured after.
    #[test]
    fn a_callsite_reached_before_the_capture_is_global_is_still_captured() {
        passes_alone("window_alone");
    }

    /// Run the ignored test `name` alone in a process of its own: it passes.
    fn passes_alone(name: &str) {
        let out = Command::new(env::current_exe().unwrap())
            .args(["--exact", &format!("hub::testlog::tests::{name}")])
            .args(["--ignored", "--test-threads=1"])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.contains("1 passed"), "{text}");
    }
}
