//! A connected worker's capabilities, held for its session.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::state::{HubState, WorkerCaps};

/// Registers the worker's capabilities while alive; removes them on
/// drop so admission control tracks actual capacity.
pub(super) struct CapsGuard {
    state: Arc<HubState>,
    pub(super) id: u64,
    pub(super) caps: WorkerCaps,
}

impl CapsGuard {
    pub(super) fn new(state: Arc<HubState>, caps: WorkerCaps) -> Self {
        let id = state.next_worker_id.fetch_add(1, Ordering::Relaxed);
        state.worker_caps.lock().unwrap().insert(id, caps.clone());
        state.known.learn(&caps);
        // Wake submissions waiting for a capable worker to appear.
        state.caps_changed.notify_waiters();
        state.regen_nix_config();
        Self { state, id, caps }
    }
}

impl Drop for CapsGuard {
    fn drop(&mut self) {
        self.state.worker_caps.lock().unwrap().remove(&self.id);
        self.state.holding.lock().unwrap().remove(&self.id);
        self.state.regen_nix_config();
        // Remember the platform so a build for it briefly waits for the
        // worker to reconnect instead of declining immediately.
        self.state.record_departed(self.caps.clone());
    }
}
