//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, its size, and what the builds handed to it
//! are expected to use; what builds used, learned from their runs
//! (history.rs), the fold of the build record (record.rs); and the
//! dispatch's log line and record row. A queued build goes to the worker that asks for it unless
//! another with an idle agent would be less full with it (rank.rs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::types::Value;

use super::Admission;
use super::naming::drv_name;
use super::record::{Recorder, Rest, Row, int, now_ms, result_row, val};
use super::state::{HubState, Job};
use crate::proto::{BuildUsage, Register};

#[path = "placement/history.rs"]
mod history;
#[path = "placement/rank.rs"]
mod rank;
#[cfg(test)]
pub(super) use history::Level;
pub(super) use history::{Build, Demand, Expected, History};
#[cfg(test)]
pub(super) use rank::GRACE;
pub(super) use rank::{Room, Rooms, Size};

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
    /// What builds used, by key: the record's fold.
    pub(super) history: History,
    /// Where the record's rows go; None where the hub holds no record.
    pub(super) record: Option<Recorder>,
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

    /// Learn from a finished run of `job`, and append it to the record,
    /// in one step: the history stays the record's fold. The send never
    /// waits.
    pub(super) fn learn(&mut self, job: &Job, exit_code: i32, u: &BuildUsage, rest: Rest) {
        let build = build_of(job);
        self.history.record(&build, exit_code, u);
        self.append(result_row(&build, exit_code, u, rest));
    }

    /// Hand `row` to the record's writer, if there is one.
    pub(super) fn append(&self, row: Row) {
        if let Some(record) = &self.record {
            record.send(row);
        }
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
/// name); log the dispatch, and append its record row, with the
/// declaration that admitted it, its features, how many other workers
/// that serve it have an idle agent, what it is expected to use, and the
/// worker's size and expected load with it. Over-memory is only logged:
/// nothing holds a build back for memory (DESIGN.md, Placement).
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
    let (idle_elsewhere, room, record) = {
        let workers = state.worker_caps.lock().unwrap();
        let mut placement = state.placement.lock().unwrap();
        let room = placement.dispatched(to.0, job);
        let serves = |r: &&Room| {
            (workers.get(&r.id)).is_some_and(|c| c.serves(&job.req.system, &job.features))
        };
        let rooms = placement.rooms();
        let idle = rooms.iter().filter(|r| r.id != to.0 && r.idle > 0);
        (idle.filter(serves).count(), room, placement.record.clone())
    };
    let load = room.as_ref().map(|r| r.load);
    let memory = room.as_ref().and_then(|r| r.size.memory);
    if let Some(record) = record {
        let values = vec![
            Value::Integer(now_ms()),
            job.id.clone().into(),
            job.key.clone().into(),
            to.1.to_owned().into(),
            val(drv_name(&job.req).map(str::to_owned)),
            resumed.into(),
            capacity.into(),
            assignments.map_or(Value::Null, int),
            free.into(),
            int(sent),
            job.features.join(",").into(),
            int(u64::try_from(idle_elsewhere).unwrap_or(u64::MAX)),
            e.level.name().to_owned().into(),
            int(bytes(e.demand.memory)),
            e.demand.cores.into(),
            val(e.secs),
            e.oom.into(),
            load.map_or(Value::Null, |l| int(bytes(l.memory))),
            val(load.map(|l| l.cores)),
            memory.map_or(Value::Null, |m| int(bytes(m))),
            val(room.as_ref().map(|r| r.size.cores)),
        ];
        record.send(Row {
            table: "dispatch",
            values,
        });
    }
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
