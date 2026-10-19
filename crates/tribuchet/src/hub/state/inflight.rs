//! Registry of in-flight builds for submission dedupe.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::Replay;

#[derive(Default)]
pub(in crate::hub) struct Inflight {
    /// Dedupe key (hash of the full request) -> replay buffer.
    pub(in crate::hub) by_key: HashMap<String, Arc<Replay>>,
    /// Scratch output paths of in-flight jobs. Different requests naming
    /// the same scratch path would unpack into the same destination.
    pub(in crate::hub) by_path: HashSet<String>,
    /// Woken when a listing drops, freeing its paths.
    pub(in crate::hub) released: Arc<Notify>,
}

/// A job's entries in `Inflight`, removed when this drops.
pub(in crate::hub) struct Listing {
    inflight: Arc<Mutex<Inflight>>,
    key: String,
    paths: Vec<String>,
}

impl Inflight {
    /// None if another in-flight job claims one of `paths`.
    pub(in crate::hub) fn list(
        this: &Arc<Mutex<Self>>,
        key: &str,
        paths: Vec<String>,
        replay: &Arc<Replay>,
    ) -> Option<Listing> {
        let mut inner = this.lock().unwrap();
        if paths.iter().any(|p| inner.by_path.contains(p)) {
            return None;
        }
        inner.by_key.insert(key.to_owned(), replay.clone());
        inner.by_path.extend(paths.iter().cloned());
        Some(Listing {
            inflight: this.clone(),
            key: key.to_owned(),
            paths,
        })
    }
}

impl Drop for Listing {
    fn drop(&mut self) {
        let mut inner = self.inflight.lock().unwrap();
        inner.by_key.remove(&self.key);
        for p in &self.paths {
            inner.by_path.remove(p);
        }
        inner.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_listing_frees_paths_and_wakes_waiters() {
        let inflight = Arc::new(Mutex::new(Inflight::default()));
        let paths = vec!["/nix/store/aaaa-out".to_owned()];
        let replay = Arc::new(Replay::default());
        let first = Inflight::list(&inflight, "a", paths.clone(), &replay).unwrap();
        assert!(Inflight::list(&inflight, "b", paths.clone(), &replay).is_none());

        let released = inflight.lock().unwrap().released.clone();
        let notified = released.notified();
        drop(first);
        notified.await;
        assert!(Inflight::list(&inflight, "b", paths, &replay).is_some());
    }
}
