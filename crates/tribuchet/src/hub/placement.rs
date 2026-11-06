//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, and the dispatch log line; and what builds
//! used, learned from their runs (history.rs), kept across restarts in a
//! file. A queued build goes to the first worker with room that asks
//! (HubState::take_job), but a heavy one is left a while for a worker
//! with more room (Spread).

use std::collections::HashMap;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::Admission;
use super::naming::drv_name;
use super::state::{HubState, Job, write_newest};
use crate::proto::BuildUsage;

#[path = "placement/history.rs"]
mod history;
#[path = "placement/seed.rs"]
mod seed;
#[cfg(test)]
#[path = "placement/tests.rs"]
mod tests;
pub(super) use history::{Build, Expected, History};
pub use seed::seed_history;

/// How often a changed history is written. It is a cache: a crash
/// loses at most this much learning.
pub(super) const SAVE_EVERY: Duration = Duration::from_mins(5);

/// What history knows `job` by.
pub(super) fn build_of(job: &Job) -> Build<'_> {
    Build {
        system: &job.req.system,
        name: drv_name(&job.req),
        declared: heavy(&job.features),
    }
}

/// Whether a build is heavy: it declares big-parallel, as builds that
/// use every core do (nixpkgs' requiredSystemFeatures).
pub(super) fn heavy(features: &[String]) -> bool {
    features.iter().any(|f| f == "big-parallel")
}

#[derive(Default)]
pub(super) struct Placement {
    /// Each worker session's admission, by session id.
    pub(super) sessions: HashMap<u64, Arc<Mutex<Admission>>>,
    /// What builds used, by key.
    pub(super) history: History,
}

/// How long after it was first passed over a heavy build may be passed
/// over again: the woken worker's time to ask, not a wait for room.
pub(super) const GRACE: Duration = Duration::from_secs(2);

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

    pub(super) fn expect(&self, job: &Job) -> Expected {
        self.history.expect(&build_of(job))
    }

    /// Learn from a finished run of `job`.
    pub(super) fn learn(&mut self, job: &Job, exit_code: i32, u: &BuildUsage) {
        self.history.record(&build_of(job), exit_code, u);
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

    /// What session `asker` sees of the others' room; None while it has
    /// no heavy build in flight, when it passes nothing over.
    pub(super) fn spread(&self, asker: u64) -> Option<Spread> {
        let mine = self.sessions.get(&asker)?.lock().unwrap().heavy;
        let others = || self.rooms().into_iter().filter(|r| r.id != asker);
        (mine > 0).then(|| Spread {
            mine,
            rooms: others().collect(),
        })
    }
}

/// The other sessions' room as a session with `mine` heavy builds in
/// flight asked for work, less what its scan of the queue left them.
pub(super) struct Spread {
    pub(super) mine: u32,
    pub(super) rooms: Vec<Room>,
}

impl Spread {
    /// The session to leave a heavy build to, at `now`: one that `serves`
    /// it with an idle agent and fewer heavy builds than the asker, the
    /// fewest first, while `since`, when it was first passed over, is
    /// less than GRACE ago. That agent is then spoken for, and `since`
    /// set if it was not.
    pub(super) fn passes(
        &mut self,
        since: &mut Option<Instant>,
        serves: impl Fn(u64) -> bool,
        now: Instant,
    ) -> Option<u64> {
        if since.is_some_and(|s| now.duration_since(s) >= GRACE) {
            return None;
        }
        let mine = self.mine;
        let room = (self.rooms.iter_mut())
            .filter(|r| r.idle > 0 && r.heavy < mine && serves(r.id))
            .min_by_key(|r| (r.heavy, r.id))?;
        room.idle -= 1;
        room.heavy += 1;
        since.get_or_insert(now);
        Some(room.id)
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
    let expected = state.placement.lock().unwrap().expect(job);
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
        learned = expected.level.name(),
        expect_mem = bytes(expected.demand.memory),
        expect_cores = round3(expected.demand.cores),
        expect_secs = expected.secs.map(round3),
        oom_runs = expected.oom,
        "dispatching build"
    );
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
