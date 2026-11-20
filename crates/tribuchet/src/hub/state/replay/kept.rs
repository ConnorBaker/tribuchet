//! The outputs a delivery keeps for its build's clients.

use std::sync::Mutex as StdMutex;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::chunker::Recipe;
use crate::chunkstore::PinGuard;
use crate::hub::chunkcache::ChunkCache;

/// How long after its build ends a client catching up can count on the
/// outputs staying in the hub's cache. UNMEASURED: no client stalled in
/// the deployed hub journal it could be sized from. Past it the
/// catch-up goes on while the chunks last, else the client resubmits.
pub(super) const CATCH_UP_PIN: Duration = Duration::from_mins(10);

/// A delivery's outputs, pinned in the hub's chunk cache so a
/// subscriber that missed them can be sent them again.
pub(in crate::hub) struct KeptOutputs {
    pub(super) cache: Arc<ChunkCache>,
    pub(super) outputs: Vec<(String, Arc<Recipe>)>,
    /// Released at a requeue, or CATCH_UP_PIN after the build ends.
    pub(super) pin: StdMutex<Option<PinGuard>>,
}

impl KeptOutputs {
    pub(in crate::hub) fn new(
        cache: Arc<ChunkCache>,
        outputs: Vec<(String, Arc<Recipe>)>,
        pin: PinGuard,
    ) -> Self {
        Self {
            cache,
            outputs,
            pin: StdMutex::new(Some(pin)),
        }
    }

    /// Leave the outputs' chunks to the cache.
    pub(super) fn unpin(&self) {
        drop(self.pin.lock().unwrap().take());
    }
}

/// Release a finished build's outputs from the cache's pin once
/// CATCH_UP_PIN has passed, whoever still holds them.
pub(super) async fn unpin_after(kept: Weak<KeptOutputs>) {
    tokio::time::sleep(CATCH_UP_PIN).await;
    if let Some(kept) = kept.upgrade() {
        kept.unpin();
    }
}
