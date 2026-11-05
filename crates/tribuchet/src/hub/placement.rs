//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, and the dispatch log line. A queued build
//! goes to the first worker with room that asks (HubState::take_job).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::Admission;
use super::naming::drv_name;
use super::state::{HubState, Job};

/// Whether a build is heavy: it declares big-parallel, as builds that
/// use every core do (nixpkgs' requiredSystemFeatures).
pub(super) fn heavy(features: &[String]) -> bool {
    features.iter().any(|f| f == "big-parallel")
}

#[derive(Default)]
pub(super) struct Placement {
    /// Each worker session's admission, by session id.
    pub(super) sessions: HashMap<u64, Arc<Mutex<Admission>>>,
}

/// A worker session's room, as its admission stands.
pub(super) struct Room {
    pub(super) id: u64,
    /// Agents with no build, apart from the lookahead.
    pub(super) idle: u32,
    /// Heavy builds in flight there.
    pub(super) heavy: u32,
}

impl Placement {
    pub(super) fn open(&mut self, id: u64, admission: Arc<Mutex<Admission>>) {
        self.sessions.insert(id, admission);
    }

    pub(super) fn close(&mut self, id: u64) {
        self.sessions.remove(&id);
    }

    pub(super) fn rooms(&self) -> Vec<Room> {
        let room = |(id, a): (&u64, &Arc<Mutex<Admission>>)| {
            let a = a.lock().unwrap();
            Room {
                id: *id,
                idle: a.idle(),
                heavy: a.heavy,
            }
        };
        self.sessions.iter().map(room).collect()
    }
}

/// Log the dispatch of `job` to worker session `to` (id and name) with
/// the declaration that admitted it, the features it declares, how many
/// other workers that serve it have an idle agent, and each worker's
/// heavy builds in flight, this one counted.
pub(super) fn dispatch(
    state: &HubState,
    to: (u64, &str),
    job: &Job,
    resumed: bool,
    admission: &Mutex<Admission>,
) {
    let (capacity, assignments, free, sent) = {
        let a = admission.lock().unwrap();
        (a.capacity, a.assignments, a.free, a.sent)
    };
    let (idle_elsewhere, mut heavy_builds) = {
        let workers = state.worker_caps.lock().unwrap();
        let rooms = state.placement.lock().unwrap().rooms();
        let serves = |r: &&Room| {
            (workers.get(&r.id)).is_some_and(|c| c.serves(&job.req.system, &job.features))
        };
        let idle = rooms.iter().filter(|r| r.id != to.0 && r.idle > 0);
        let name = |r: &Room| Some(format!("{}:{}", workers.get(&r.id)?.name, r.heavy));
        (
            idle.filter(serves).count(),
            rooms.iter().filter_map(name).collect::<Vec<_>>(),
        )
    };
    heavy_builds.sort();
    tracing::info!(
        id = job.id,
        worker = to.1,
        drv = drv_name(&job.req).unwrap_or_default(),
        resumed,
        capacity,
        assignments,
        free,
        sent,
        features = job.features.join(","),
        heavy = heavy(&job.features),
        idle_elsewhere,
        heavy_builds = heavy_builds.join(","),
        "dispatching build"
    );
}
