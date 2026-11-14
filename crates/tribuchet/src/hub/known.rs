//! Known workers (SEMANTICS-7, L11): those that registered and were not
//! decommissioned since. Kept across hub restarts beside the run epoch, so
//! that the hub refuses a build only if none of them serves it. To
//! decommission one, stop the hub and delete its entry.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::state::{WorkerCaps, unserved};
use crate::fsutil::write_atomic;

/// System -> the features a worker honors for it.
type Systems = BTreeMap<String, BTreeSet<String>>;

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
    pub(super) fn learn(&self, caps: &WorkerCaps) {
        let systems: Systems = (caps.systems.iter())
            .map(|(s, f)| (s.clone(), f.iter().cloned().collect()))
            .collect();
        let mut workers = self.workers.lock().unwrap();
        if workers.get(&caps.name) == Some(&systems) {
            return;
        }
        workers.insert(caps.name.clone(), systems);
        let (Some(path), Ok(json)) = (&self.path, serde_json::to_string_pretty(&*workers)) else {
            return;
        };
        if let Err(e) = write_atomic(path, &json) {
            tracing::error!(path = %path.display(), "storing the known workers: {e}");
        }
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
    use std::time::Instant;

    use super::*;

    /// A worker that registered is known to a hub started after, with
    /// what it served then, and only that.
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
    }
}
