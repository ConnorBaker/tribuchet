//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, its size, and what the builds handed to it
//! are expected to use; what builds used, learned from their runs
//! (history.rs) and kept across restarts in a file; and the dispatch log
//! line. A queued build goes to the worker that asks for it unless
//! another with an idle agent would be less full with it (rank.rs).

use std::collections::HashMap;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::Admission;
use super::naming::drv_name;
use super::state::{HubState, Job, write_newest};
use crate::proto::{BuildUsage, Register};

#[path = "placement/history.rs"]
mod history;
#[path = "placement/rank.rs"]
mod rank;
#[path = "placement/seed.rs"]
mod seed;
#[cfg(test)]
#[path = "placement/tests.rs"]
mod tests;
#[cfg(test)]
pub(super) use history::Level;
pub(super) use history::{Build, Demand, Expected, History};
#[cfg(test)]
pub(super) use rank::GRACE;
pub(super) use rank::{Room, Rooms, Size};
pub use seed::seed_history;

/// How often a changed history is written. It is a cache: a crash
/// loses at most this much learning.
pub(super) const SAVE_EVERY: Duration = Duration::from_mins(5);

/// What history knows `job` by.
pub(super) fn build_of(job: &Job) -> Build<'_> {
    Build {
        system: &job.req.system,
        name: drv_name(&job.req),
        declared: declares(&job.features),
    }
}

/// Whether a build declares big-parallel, as nixpkgs' builds that use
/// every core do: the class whose runs, or prior, an unseen key takes.
pub(super) fn declares(features: &[String]) -> bool {
    features.iter().any(|f| f == "big-parallel")
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(v: u64) -> f64 {
    v as f64
}

/// A worker's size as it registered: its cores, else its agents (a
/// worker predating the field), at least one, and the memory its builds
/// may use. A worker can register 0 (no agents yet, an old one) and have
/// idle agents later: as 0 cores it would be infinitely full, or NaN.
pub(super) fn size_of(register: &Register) -> Size {
    Size {
        cores: f64::from(register.cores.unwrap_or(register.max_jobs).max(1)),
        memory: register.build_memory.map(as_f64),
    }
}

/// A worker session as placement sees it.
pub(super) struct Session {
    pub(super) admission: Arc<Mutex<Admission>>,
    pub(super) size: Size,
    /// What the builds handed to it and not yet settled are expected to
    /// use, by build id.
    pub(super) committed: HashMap<String, Demand>,
}

impl Session {
    pub(super) fn load(&self) -> Demand {
        (self.committed.values()).fold(Demand::default(), |sum, d| sum.plus(*d))
    }
}

#[derive(Default)]
pub(super) struct Placement {
    /// Each worker session, by session id.
    pub(super) sessions: HashMap<u64, Session>,
    /// What builds used, by key.
    pub(super) history: History,
}

impl Placement {
    pub(super) fn open(&mut self, id: u64, admission: Arc<Mutex<Admission>>, size: Size) {
        let committed = HashMap::new();
        let session = Session {
            admission,
            size,
            committed,
        };
        self.sessions.insert(id, session);
    }

    pub(super) fn close(&mut self, id: u64) {
        self.sessions.remove(&id);
    }

    pub(super) fn expect(&self, job: &Job) -> Expected {
        self.history.expect(&build_of(job))
    }

    /// Learn from a finished run of `job`.
    pub(super) fn learn(&mut self, job: &Job, exit_code: i32, u: &BuildUsage) {
        self.history.record(&build_of(job), exit_code, u);
    }

    /// `job` went to `session`: its expected demand counts there until
    /// it settles. The session's room with it.
    pub(super) fn dispatched(&mut self, session: u64, job: &Job) -> Option<Room> {
        let s = self.sessions.get_mut(&session)?;
        s.committed.insert(job.id.clone(), job.expected.demand);
        Some(room(session, s))
    }

    /// The build left its worker: finished, failed, requeued or given up.
    pub(super) fn settled(&mut self, session: u64, build_id: &str) {
        if let Some(s) = self.sessions.get_mut(&session) {
            s.committed.remove(build_id);
        }
    }

    pub(super) fn rooms(&self) -> Vec<Room> {
        (self.sessions.iter()).map(|(id, s)| room(*id, s)).collect()
    }

    /// Every session's room as session `asker` asks for work.
    pub(super) fn rooms_for(&self, asker: u64) -> Rooms {
        Rooms {
            asker,
            rooms: self.rooms(),
        }
    }
}

fn room(id: u64, s: &Session) -> Room {
    Room {
        id,
        idle: s.admission.lock().unwrap().idle(),
        size: s.size,
        load: s.load(),
    }
}

/// Count `job`'s expected demand against worker session `to` (id and
/// name), and log the dispatch with the declaration that admitted it,
/// the features it declares, how many other workers that serve it have
/// an idle agent, what it is expected to use, and the worker's size and
/// expected load with it. Where that load is more memory than the worker
/// has, say so: nothing holds a build back for memory (DESIGN.md,
/// Placement).
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
    let e = job.expected;
    let (idle_elsewhere, room) = {
        let workers = state.worker_caps.lock().unwrap();
        let mut placement = state.placement.lock().unwrap();
        let room = placement.dispatched(to.0, job);
        let serves = |r: &&Room| {
            (workers.get(&r.id)).is_some_and(|c| c.serves(&job.req.system, &job.features))
        };
        let rooms = placement.rooms();
        let idle = rooms.iter().filter(|r| r.id != to.0 && r.idle > 0);
        (idle.filter(serves).count(), room)
    };
    let load = room.as_ref().map(|r| r.load);
    let memory = room.as_ref().and_then(|r| r.size.memory);
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
        idle_elsewhere,
        learned = e.level.name(),
        expect_mem = bytes(e.demand.memory),
        expect_cores = round3(e.demand.cores),
        expect_secs = e.secs.map(round3),
        oom_runs = e.oom,
        load_mem = load.map(|l| bytes(l.memory)),
        load_cores = load.map(|l| round3(l.cores)),
        memory = memory.map(bytes),
        cores = room.as_ref().map(|r| r.size.cores),
        "dispatching build"
    );
    if let (Some(load), Some(memory)) = (load, memory)
        && load.memory > memory
    {
        tracing::info!(
            id = job.id,
            worker = to.1,
            load_mem = bytes(load.memory),
            memory = bytes(memory),
            "placed over predicted memory"
        );
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(super) fn bytes(memory: f64) -> u64 {
    memory.max(0.0) as u64
}

/// `x` to three decimals, for a log line.
pub(super) fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// The history's file, and the number of the newest copy written
/// (write_newest): the saves every SAVE_EVERY and at SIGTERM may
/// overlap, and leave the newest.
pub(super) struct HistoryFile {
    pub(super) path: PathBuf,
    written: Arc<Mutex<u64>>,
    /// The file's lock, held while the hub runs: seed-history refuses to
    /// seed beside it. None where it could not be taken or the file could
    /// not be loaded, and then the history is never saved.
    lock: Option<File>,
}

impl HistoryFile {
    /// Write copy number `generation`, if the hub holds the lock.
    fn write(&self, generation: u64, history: &History) {
        if self.lock.is_none() {
            return;
        }
        let json = match serde_json::to_string(history) {
            Ok(json) => json,
            Err(e) => return tracing::warn!("failed to serialise placement history: {e}"),
        };
        write_newest(
            &self.written,
            generation,
            &self.path,
            &json,
            "placement history",
        );
    }
}

/// The lock on the history file at `path`, `path` and ".lock", creating
/// its directory: None while another holds it, a hub or seed-history. The
/// file is opened only to read where it exists: a lock left by a run as
/// another user locks too.
pub(super) fn lock_history(path: &Path) -> io::Result<Option<File>> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    let file = match File::open(&name) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            File::create_new(&name).or_else(|_| File::open(&name))?
        }
        file => file?,
    };
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// Save the history if it changed: a copy taken, and numbered, under the
/// placement lock (History::changed), serialised and written on a
/// blocking thread, outside it: no file I/O on the runtime or under a
/// lock. A save that fails is warned of, and not tried again until the
/// history changes.
pub(super) async fn save_history(state: &HubState, file: &Arc<HistoryFile>) {
    let copy = state.placement.lock().unwrap().history.changed();
    let Some((generation, history)) = copy else {
        return;
    };
    let file = file.clone();
    let _ = tokio::task::spawn_blocking(move || file.write(generation, &history)).await;
}

/// Whether `path` is named as a history file's own temporary file or
/// lock is (it ends .tmp or .lock): seeding beside such a history would
/// remove or lock it, so neither a hub nor seed-history takes one.
pub(super) fn reserved_name(path: &Path) -> bool {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    name.ends_with(".tmp") || name.ends_with(".lock")
}

/// Lock the history file at `path` and load it into `state`, on a
/// blocking thread, before the hub serves anything. A hub that cannot take
/// the lock, which another hub or seed-history holds, loads the file but
/// never saves it; one that cannot read or parse it (an old format, its
/// permissions) starts empty and never saves over it, the only copy; and
/// one at a reserved_name neither loads nor saves.
pub(super) async fn open_history(state: &HubState, path: PathBuf) -> Arc<HistoryFile> {
    let owned = path.clone();
    let opened = tokio::task::spawn_blocking(move || {
        if reserved_name(&owned) {
            let e = io::Error::other("its name ends .tmp or .lock, a history file's own");
            return (Err(e), Ok(History::default()));
        }
        let lock = lock_history(&owned).and_then(|l| l.ok_or(io::ErrorKind::WouldBlock.into()));
        (lock, History::load(&owned))
    });
    let (lock, loaded) = opened
        .await
        .unwrap_or_else(|e| (Err(io::Error::other(e)), Ok(History::default())));
    let (lock, history) = match loaded {
        Ok(history) => {
            if let Err(e) = &lock {
                tracing::warn!(path = %path.display(), "placement history not locked, so never saved: {e}");
            }
            (lock.ok(), history)
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), "placement history unreadable, so never saved over: {e}");
            (None, History::default())
        }
    };
    let (keys, entries) = history.len();
    tracing::info!(path = %path.display(), keys, entries, "placement history loaded");
    state.placement.lock().unwrap().history = history;
    Arc::new(HistoryFile {
        path,
        written: Arc::default(),
        lock,
    })
}

/// Save the history every SAVE_EVERY while it changes.
pub(super) async fn keep_history(state: Arc<HubState>, file: Arc<HistoryFile>) {
    let mut tick = tokio::time::interval(SAVE_EVERY);
    tick.tick().await;
    loop {
        tick.tick().await;
        save_history(&state, &file).await;
    }
}
