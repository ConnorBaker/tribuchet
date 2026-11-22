//! Lost keys (SEMANTICS-7 D4, L10): those a run of which ended with no
//! known cause and that have had no outcome since. Such a key runs alone
//! (state/take.rs). Kept across hub restarts beside the known workers, so a
//! restart does not run a possibly poisonous key beside others again.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::fsutil::write_atomic;

#[derive(Default)]
pub(super) struct Lost {
    /// Where they are stored; none in tests.
    path: Option<PathBuf>,
    /// Full dedupe keys.
    keys: Mutex<BTreeSet<String>>,
}

impl Lost {
    /// The keys stored at `path`: none if it is absent, or unreadable,
    /// which is said.
    pub(super) fn open(path: &Path) -> Self {
        let read = fs::read_to_string(path).map_err(|e| (e.kind(), e.to_string()));
        let read = read.and_then(|s| {
            serde_json::from_str(&s).map_err(|e| (ErrorKind::InvalidData, e.to_string()))
        });
        let keys: BTreeSet<String> = read.unwrap_or_else(|(kind, e)| {
            if kind != ErrorKind::NotFound {
                tracing::error!(path = %path.display(), "reading the lost keys: {e}");
            }
            BTreeSet::new()
        });
        tracing::info!(keys = keys.len(), "lost keys");
        Self {
            path: Some(path.to_owned()),
            keys: Mutex::new(keys),
        }
    }

    /// A run of `key` ended with no known cause (D4).
    pub(super) fn mark(&self, key: &str) {
        let mut keys = self.keys.lock().unwrap();
        if keys.insert(key.to_owned()) {
            tracing::info!(key, "lost: runs alone until it has an outcome");
            self.store(&keys);
        }
    }

    /// `key` has an outcome the farm took (L10).
    pub(super) fn clear(&self, key: &str) {
        let mut keys = self.keys.lock().unwrap();
        if keys.remove(key) {
            self.store(&keys);
        }
    }

    pub(super) fn has(&self, key: &str) -> bool {
        self.keys.lock().unwrap().contains(key)
    }

    fn store(&self, keys: &BTreeSet<String>) {
        let (Some(path), Ok(json)) = (&self.path, serde_json::to_string_pretty(keys)) else {
            return;
        };
        if let Err(e) = write_atomic(path, &json) {
            tracing::error!(path = %path.display(), "storing the lost keys: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mark outlives a restart until its key's outcome lifts it (L10).
    #[test]
    fn a_lost_key_stays_lost_across_a_restart_until_its_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lost-keys");
        Lost::open(&path).mark("k");
        let lost = Lost::open(&path);
        assert!(lost.has("k") && !lost.has("j"));
        lost.clear("k");
        assert!(!Lost::open(&path).has("k"));
    }
}
