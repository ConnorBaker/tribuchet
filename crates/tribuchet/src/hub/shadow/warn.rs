//! The shadow's warnings: found under its lock, logged after it, and as
//! long as the build they are about, whatever its worker holds.

use super::Mirror;
use super::sem::{self, Answer, Command, Event};

/// A divergence, to log once the lock is let go.
pub(super) type Warning = Box<dyn FnOnce() + Send>;

/// `ev` for a build's events: a Register's lists by their length only,
/// for they are as long as its worker's runs and each of its keys has it.
pub(super) fn brief(ev: &Event) -> String {
    match ev {
        Event::Register {
            worker,
            serves,
            held,
        } => {
            let (serves, held) = (serves.len(), held.len());
            format!("Register {{ worker: {worker}, serves: {serves}, held: {held} }}")
        }
        ev => format!("{ev:?}"),
    }
}

impl Mirror {
    /// Warn of what diverged at `point` about each of `keys`.
    pub(super) fn diverged(
        &mut self,
        point: &str,
        keys: &[sem::Key],
        what: &'static str,
        live: &[(u32, Answer)],
        core: &[Command],
    ) {
        let mut keys = keys.to_vec();
        keys.dedup();
        for k in keys {
            let build = self.hub.builds.get(&k).cloned();
            let run = build.as_ref().and_then(|b| b.live);
            let hub_run = run.and_then(|r| self.hub_runs.get(&r).cloned());
            let seen = self.seen.get(&k).cloned();
            let (point, live, core) = (point.to_owned(), live.to_vec(), core.to_vec());
            self.warnings.push(Box::new(move || {
                let key = format!("{k:08x}");
                tracing::warn!(target: "tribuchet::shadow", point, key, what, ?live, ?core,
                    ?build, ?hub_run, ?seen, "shadow divergence");
            }));
        }
    }
}
