//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, and the dispatch log line. A queued build
//! goes to the first worker with room that asks (HubState::take_job).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::Admission;
use super::naming::drv_name;
use super::state::Job;

#[derive(Default)]
pub(super) struct Placement {
    /// Each worker session's admission, by session id.
    pub(super) sessions: HashMap<u64, Arc<Mutex<Admission>>>,
}

impl Placement {
    pub(super) fn open(&mut self, id: u64, admission: Arc<Mutex<Admission>>) {
        self.sessions.insert(id, admission);
    }

    pub(super) fn close(&mut self, id: u64) {
        self.sessions.remove(&id);
    }
}

/// Log the dispatch of `job` to `worker` with the declaration that
/// admitted it.
pub(super) fn dispatch(worker: &str, job: &Job, resumed: bool, admission: &Mutex<Admission>) {
    let (capacity, assignments, free, sent) = {
        let a = admission.lock().unwrap();
        (a.capacity, a.assignments, a.free, a.sent)
    };
    tracing::info!(
        id = job.id,
        worker,
        drv = drv_name(&job.req).unwrap_or_default(),
        resumed,
        capacity,
        assignments,
        free,
        sent,
        "dispatching build"
    );
}
