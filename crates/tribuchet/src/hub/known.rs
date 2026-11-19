//! Known workers (SEMANTICS-7, L11): those that registered and were not
//! decommissioned since (U4, `tribuchet decommission`). Kept across hub
//! restarts beside the run epoch, so that the hub refuses a build only if
//! none of them serves it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::state::{WorkerCaps, unserved};
use crate::fsutil::write_atomic;

/// System -> the features a worker honors for it.
type Systems = BTreeMap<String, BTreeSet<String>>;

/// What a client whose Nix cannot build the request is told when no known
/// worker builds `system` with `features` (L11).
pub(super) fn refusal(system: &str, features: &[String]) -> String {
    format!(
        "no worker this hub knows builds {system} [{}], and the client's Nix cannot",
        features.join(",")
    )
}

/// Why a worker was not decommissioned.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Forget {
    Unknown,
    Connected,
}

#[derive(Default)]
pub(super) struct Known {
    /// Where they are stored; none in tests.
    path: Option<PathBuf>,
    /// Worker name -> what it served when it last registered.
    workers: Mutex<BTreeMap<String, Systems>>,
}

impl Known {
    /// The workers stored at `path`: none if it is absent, or unreadable,
    /// which is said.
    pub(super) fn open(path: &Path) -> Self {
        let read = fs::read_to_string(path).map_err(|e| (e.kind(), e.to_string()));
        let read = read.and_then(|s| {
            serde_json::from_str(&s).map_err(|e| (ErrorKind::InvalidData, e.to_string()))
        });
        let workers = read.unwrap_or_else(|(kind, e)| {
            if kind != ErrorKind::NotFound {
                tracing::error!(path = %path.display(), "reading the known workers: {e}");
            }
            BTreeMap::new()
        });
        tracing::info!(workers = workers.len(), "known workers");
        Self {
            path: Some(path.to_owned()),
            workers: Mutex::new(workers),
        }
    }

    /// `caps`'s worker registered: it is known, serving what it said.
    #[cfg(test)]
    pub(super) fn learn(&self, caps: &WorkerCaps) {
        self.join(caps, || {});
    }

    /// `caps`'s worker registers: it is known, serving what it said, before
    /// `connect` makes it connected, both under this set's lock, so that a
    /// connected worker is always known (SEMANTICS-7 cut-over 7).
    pub(super) fn join(&self, caps: &WorkerCaps, connect: impl FnOnce()) {
        let systems: Systems = (caps.systems.iter())
            .map(|(s, f)| (s.clone(), f.iter().cloned().collect()))
            .collect();
        let mut workers = self.workers.lock().unwrap();
        if workers.get(&caps.name) != Some(&systems) {
            workers.insert(caps.name.clone(), systems);
            self.store(&workers);
        }
        connect();
    }

    /// Decommission the worker `name`, unless it is unknown, or `connected`
    /// says it is connected, which is asked under this set's lock.
    pub(super) fn forget(
        &self,
        name: &str,
        connected: impl FnOnce() -> bool,
    ) -> Result<(), Forget> {
        let mut workers = self.workers.lock().unwrap();
        if !workers.contains_key(name) {
            return Err(Forget::Unknown);
        }
        if connected() {
            return Err(Forget::Connected);
        }
        workers.remove(name);
        self.store(&workers);
        Ok(())
    }

    fn store(&self, workers: &BTreeMap<String, Systems>) {
        let (Some(path), Ok(json)) = (&self.path, serde_json::to_string_pretty(workers)) else {
            return;
        };
        if let Err(e) = write_atomic(path, &json) {
            tracing::error!(path = %path.display(), "storing the known workers: {e}");
        }
    }

    /// `f` of the systems the known workers serve, under this set's lock,
    /// which a registration takes before the connected set's.
    pub(super) fn with_systems<T>(&self, f: impl FnOnce(&BTreeSet<&str>) -> T) -> T {
        let workers = self.workers.lock().unwrap();
        f(&workers
            .values()
            .flat_map(BTreeMap::keys)
            .map(String::as_str)
            .collect())
    }

    /// Whether a known worker serves `system` with `features` (WorkerCaps::serves).
    pub(super) fn serves(&self, system: &str, features: &[String]) -> bool {
        let has = |have: &BTreeSet<String>| features.iter().all(|f| have.contains(f));
        !unserved(features)
            && (self.workers.lock().unwrap().values()).any(|w| w.get(system).is_some_and(has))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Instant;

    use super::*;
    use crate::hub::tests::caps;

    /// The known workers' systems are those of every one of them.
    #[test]
    fn the_known_systems_are_every_known_workers() {
        let known = Known::default();
        let mut arm = caps("a");
        arm.systems = [("aarch64-linux".to_owned(), HashSet::new())].into();
        known.learn(&caps("x"));
        known.learn(&arm);
        let both = BTreeSet::from(["aarch64-linux", "x86_64-linux"]);
        assert!(known.with_systems(|s| *s == both));
    }

    /// A worker that registered is known to a hub started after, with
    /// what it served then, and only that; once decommissioned, to none.
    #[test]
    fn a_registered_worker_is_known_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("known-workers");
        let caps = WorkerCaps {
            name: "w".into(),
            connected_at: Instant::now(),
            systems: [("x86_64-linux".to_owned(), ["kvm".to_owned()].into())].into(),
            max_jobs: 1,
        };
        Known::open(&path).learn(&caps);
        let known = Known::open(&path);
        assert!(known.serves("x86_64-linux", &["kvm".to_owned()]));
        assert!(!known.serves("x86_64-linux", &["big-parallel".to_owned()]));
        assert!(!known.serves("aarch64-linux", &[]));
        assert_eq!(known.forget("w", || true), Err(Forget::Connected));
        assert_eq!(known.forget("w", || false), Ok(()));
        assert_eq!(known.forget("w", || false), Err(Forget::Unknown));
        let known = Known::open(&path);
        assert!(!known.serves("x86_64-linux", &["kvm".to_owned()]));
    }
}
