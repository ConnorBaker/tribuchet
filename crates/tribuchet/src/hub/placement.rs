//! What placement knows of each worker session: its admission, which the
//! clientless decider reads, its size, and what the builds handed to it
//! are expected to use; what builds used, learned from their runs
//! (history.rs), the fold of the build record (record.rs); and the
//! dispatch's log line and record row. A queued build goes to the worker that asks for it unless
//! another with an idle agent would be less full with it (rank.rs).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::types::Value;

use super::Admission;
use super::naming::drv_name;
use super::record::{
    Recorder, Rest, Row, int, now_ms, profile_row, result_row, source_row, taught_row, val,
};
use super::state::{HubState, Job, Replay};
use crate::proto::{BuildUsage, Register};

#[path = "placement/downstream.rs"]
mod downstream;
#[path = "placement/history.rs"]
mod history;
#[path = "placement/objective.rs"]
pub(super) mod objective;
#[path = "placement/rank.rs"]
mod rank;
#[path = "placement/reserve.rs"]
mod reserve;
#[path = "placement/speed.rs"]
mod speed;
pub(super) use downstream::Downstream;
#[cfg(test)]
pub(super) use history::Level;
pub(super) use history::{Build, Demand, Expected, History, On, Read};
#[cfg(test)]
pub(super) use history::{Entry, key};
use objective::{Charges, Priority};
#[cfg(test)]
pub(super) use rank::GRACE;
pub(super) use rank::{Room, Rooms, Size};
#[cfg(test)]
pub(super) use reserve::AGE;
#[cfg(test)]
pub(super) use speed::BOUND;
pub(super) use speed::{QUIET, Teaching, quiet};

/// The history key of `job`, if its name could be read.
fn key_of(job: &Job) -> Option<String> {
    drv_name(&job.req).map(|n| history::key(&job.req.system, n))
}

/// What history knows `job` by.
pub(super) fn build_of(job: &Job) -> Build<'_> {
    Build {
        system: &job.req.system,
        name: drv_name(&job.req),
        declared: declares(&job.features),
        sources: job.sources.as_deref(),
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

/// A build handed to a session and not yet settled: what it is expected
/// to use, the clients it holds a share for, those waiting for it as it
/// was placed (objective.rs), and its history key, which holds the key's
/// other builds while the key has no run of its own
/// (Placement::held_for_key).
#[derive(Clone, Debug)]
pub(super) struct Placed {
    pub(super) demand: Demand,
    pub(super) owners: Vec<Option<Arc<str>>>,
    pub(super) key: Option<String>,
    /// When it is expected to end: its dispatch and its expected seconds.
    pub(super) ends: Option<Instant>,
}

/// A worker session as placement sees it.
pub(super) struct Session {
    pub(super) admission: Arc<Mutex<Admission>>,
    pub(super) size: Size,
    /// The builds handed to it and not yet settled, by build id.
    pub(super) committed: HashMap<String, Placed>,
    /// The builds, by id, that placement left elsewhere or to the asker
    /// only for the load of this session's clientless builds (Rooms's
    /// wants) since the clientless decider last read them.
    pub(super) wants: HashMap<String, Arc<Replay>>,
    /// Its CPU arbiter's leader and the builds it keeps frozen, by key,
    /// as its latest heartbeat said (hub/state/clientless.rs).
    pub(super) frozen_by: Option<(String, Vec<String>)>,
    /// The build of a lost key it runs, alone (D4, state/take.rs).
    pub(super) alone: Option<String>,
}

impl Session {
    pub(super) fn load(&self) -> Demand {
        (self.committed.values()).fold(Demand::default(), |sum, c| sum.plus(c.demand))
    }
}

#[derive(Default)]
pub(super) struct Placement {
    /// Each worker session, by session id.
    pub(super) sessions: HashMap<u64, Session>,
    /// What builds used, by key: the record's fold.
    pub(super) history: History,
    /// What follows builds of each key, learned from their dependants.
    pub(super) downstream: Downstream,
    /// Where the record's rows go; None where the hub holds no record.
    pub(super) record: Option<Recorder>,
    /// The session taking no new build until it is empty, for a lost key
    /// waiting to run alone (D4, state/take.rs).
    pub(super) drain: Option<u64>,
    /// What the placed builds hold for each client.
    pub(super) charges: Charges,
}

/// What a build placed on `s` holds of the farm: a worker that reports no
/// memory counts none, in the total or a share.
fn counted(s: &Session, d: Demand) -> Demand {
    Demand {
        memory: s.size.memory.map_or(0.0, |_| d.memory),
        ..d
    }
}

impl Placement {
    pub(super) fn open(&mut self, id: u64, admission: Arc<Mutex<Admission>>, size: Size) {
        let (committed, wants) = (HashMap::new(), HashMap::new());
        let session = Session {
            admission,
            size,
            committed,
            wants,
            frozen_by: None,
            alone: None,
        };
        self.sessions.insert(id, session);
    }

    pub(super) fn close(&mut self, id: u64) {
        if let Some(s) = self.sessions.remove(&id) {
            for p in s.committed.values() {
                self.charges.release(&p.owners, counted(&s, p.demand));
            }
        }
        if self.drain == Some(id) {
            self.drain = None;
        }
    }

    pub(super) fn expect(&self, job: &Job) -> Expected {
        self.with_below(job, self.history.expect(&build_of(job)))
    }

    /// `e`, the history's expectation of `job`, with the running seconds
    /// learned to follow `job` (downstream.rs), which the queue's order
    /// reads (objective.rs).
    fn with_below(&self, job: &Job, e: Expected) -> Expected {
        let below = key_of(job).map_or(0.0, |k| self.downstream.below(&k));
        Expected { below, ..e }
    }

    /// The least dominant share of the farm among the clients waiting for
    /// `job` (objective.rs), as they held it while `job` waited: the charge
    /// `job` holds from its take (state/take.rs) is not in it, as a build
    /// resumed by key (take_job_by_key) holds none before its dispatch.
    pub(super) fn share(&self, job: &Job) -> f64 {
        let total = (self.sessions.values()).fold(Demand::default(), |t, s| {
            t.plus(Demand {
                cores: s.size.cores,
                memory: s.size.memory.unwrap_or(0.0),
            })
        });
        let own = (self.sessions.values()).find_map(|s| {
            let p = s.committed.get(&job.id)?;
            Some((p.owners.as_slice(), counted(s, p.demand)))
        });
        self.charges.least(job, total, own)
    }

    /// Whether `job` waits for its key: no run of the key has succeeded
    /// with its usage measured, so `job`'s expectation is a guess (prefix,
    /// class, prior, or no lower than a failed run's peak), and another
    /// build of the key is taken and not settled. The end of that run ends
    /// the wait, and a measured success ends the hold (History::ran). On
    /// 2026-11-18 a batch guessed at 2-9 GiB used 29-44 GiB each, together.
    pub(super) fn held_for_key(&self, job: &Job) -> bool {
        let build = build_of(job);
        let key = History::key_of(&build).filter(|_| !self.history.ran(&build));
        let of_key = |c: &Placed| c.key.is_some() && c.key == key;
        key.is_some() && (self.sessions.values()).any(|s| s.committed.values().any(of_key))
    }

    /// What `job` is expected to use: as a scan last read it, before it
    /// asks (Placement::refresh), or afresh if it was never read.
    pub(super) fn expected(&self, job: &Job) -> Expected {
        match &job.expected_at {
            Some(_) => job.expected,
            None => self.expect(job),
        }
    }

    /// Mark `job`'s expectation as read from the history now, as a
    /// submit does, keeping it.
    #[cfg(test)]
    pub(super) fn stamp(&self, job: &mut Job) {
        job.expected_at = Some(self.history.read(&build_of(job)).1);
    }

    /// Read `job`'s expectation afresh where what it read learned since.
    /// Its path (`below`) is the one it was first read with, as it was
    /// queued: the queue is kept in order by it (objective.rs, enqueue),
    /// and a build is not moved in it.
    pub(super) fn refresh(&self, job: &mut Job) {
        if !job
            .expected_at
            .as_ref()
            .is_some_and(|r| self.history.fresh(r))
        {
            let (e, r) = self.history.read(&build_of(job));
            let e = match job.expected_at {
                Some(_) => Expected {
                    below: job.expected.below,
                    ..e
                },
                None => self.with_below(job, e),
            };
            (job.expected, job.expected_at) = (e, Some(r));
        }
    }

    /// `job`'s expected running seconds on worker `worker` (by name),
    /// read from the history now (History::secs_on), not from the
    /// expectation the job keeps: a refit since that was read counts. For
    /// a decider weighing finish times; none reads it yet, but the
    /// dispatch log line.
    pub(super) fn secs_on(&self, job: &Job, worker: &str) -> Option<f64> {
        self.history.secs_on(&build_of(job), worker)
    }

    /// Learn from a finished run of `job`, and append it to the record,
    /// in one step: the history stays the record's fold. The send never
    /// waits. The run teaches speed as its dispatch decided (Teaching),
    /// which the record keeps beside its row; its result, taught and
    /// profile rows are committed together (record::taught_row).
    pub(super) fn learn(&mut self, job: &Job, exit_code: i32, u: &BuildUsage, rest: Rest) {
        let build = build_of(job);
        let taught = rest.placed.quiet;
        let on = (rest.worker.as_deref()).map(|worker| On { worker, taught });
        self.history.record(&build, exit_code, u, on);
        if let Some(key) = key_of(job).filter(|_| exit_code == 0 && !u.oom_killed) {
            // A path's seconds are the build's, on a worker of factor 1,
            // not those of the workers its runs happened to take.
            let secs = self.history.unit_secs(&build).unwrap_or(0.0);
            let outputs = job.req.outputs.values().cloned();
            (self.downstream).learn(&key, secs, outputs, &job.req.input_paths);
        }
        let profile = (u.profile.as_ref()).map(|p| profile_row(rest.at_ms, &rest.id, p));
        let source = (build.sources).map(|s| source_row(rest.at_ms, &rest.id, s));
        let mut rows = vec![result_row(&build, exit_code, u, rest), taught_row(taught)];
        rows.extend(profile.into_iter().chain(source));
        self.append(rows);
    }

    /// Hand `rows` to the record's writer as one, if there is one.
    pub(super) fn append(&self, rows: Vec<Row>) {
        if let Some(record) = &self.record {
            record.send_all(rows);
        }
    }

    /// `job` went to `session`, `alone` if its key is lost: its expected
    /// demand counts there until it settles. The session's room with it.
    pub(super) fn dispatched(&mut self, session: u64, job: &Job, alone: bool) -> Option<Room> {
        let key = History::key_of(&build_of(job));
        let s = self.sessions.get_mut(&session)?;
        // A take commits a build to its session (state/take.rs) and its
        // dispatch (dispatch, below) again: the second replaces the first,
        // whose charge goes, so a build holds one charge until it settles.
        if let Some(old) = s.committed.remove(&job.id) {
            self.charges.release(&old.owners, counted(s, old.demand));
        }
        let (demand, owners) = (job.expected.demand, objective::owners(job));
        self.charges.charge(&owners, counted(s, demand));
        let ends = (job.expected.secs).map(|s| Instant::now() + Duration::from_secs_f64(s));
        let placed = Placed {
            demand,
            owners,
            key,
            ends,
        };
        s.committed.insert(job.id.clone(), placed);
        if alone {
            s.alone = Some(job.id.clone());
        }
        Some(room(session, s))
    }

    /// The build left its worker: finished, failed, requeued or given up.
    pub(super) fn settled(&mut self, session: u64, build_id: &str) {
        if let Some(s) = self.sessions.get_mut(&session) {
            if let Some(p) = s.committed.remove(build_id) {
                self.charges.release(&p.owners, counted(s, p.demand));
            }
            if s.alone.as_deref() == Some(build_id) {
                s.alone = None;
            }
        }
    }

    /// Session `id` asked for work: a lost key it serves waits for room
    /// (`wants`), or none does. With no session draining, it drains: it
    /// takes no new build until it is empty, and then the lost key's. It
    /// drains no longer once no lost key it serves waits.
    pub(super) fn drain_for(&mut self, id: u64, wants: bool) {
        match self.drain {
            None if wants => self.drain = Some(id),
            Some(d) if d == id && !wants => self.drain = None,
            _ => {}
        }
    }

    /// Of session `id`: whether it holds a build, and one alone (D4).
    pub(super) fn residence(&self, id: u64) -> (bool, bool) {
        let s = self.sessions.get(&id);
        let busy = s.is_some_and(|s| !s.committed.is_empty());
        (busy, s.is_some_and(|s| s.alone.is_some()))
    }

    pub(super) fn rooms(&self) -> Vec<Room> {
        (self.sessions.iter()).map(|(id, s)| room(*id, s)).collect()
    }

    /// Every session's room as session `asker` asks for work, with what
    /// its builds of `clientless`, those the clientless rule may give
    /// up, hold of its load.
    pub(super) fn rooms_for(&self, asker: u64, clientless: &HashSet<String>) -> Rooms {
        let rooms = self.sessions.iter().map(|(id, s)| {
            let theirs = s.committed.iter().filter(|(b, _)| clientless.contains(*b));
            let only_clientless =
                !s.committed.is_empty() && theirs.clone().count() == s.committed.len();
            Room {
                clientless: theirs.fold(Demand::default(), |sum, (_, c)| sum.plus(c.demand)),
                only_clientless,
                ..room(*id, s)
            }
        });
        Rooms::new(asker, rooms.collect())
    }
}

fn room(id: u64, s: &Session) -> Room {
    Room {
        id,
        idle: s.admission.lock().unwrap().idle(),
        size: s.size,
        load: s.load(),
        clientless: Demand::default(),
        empty: s.committed.is_empty(),
        only_clientless: false,
    }
}

/// Count `job`'s expected demand against worker session `to` (id and
/// name); log the dispatch and append its record row: the declaration,
/// features, other serving workers with an idle agent, expected use, and
/// the worker's size and load with it (the line adds the worker's speed
/// and the build's seconds there). Over-memory is logged: a build larger
/// than every worker serving it goes to the largest once it is empty
/// (Room::admits). Returns whether its run teaches speed (Teaching).
pub(super) fn dispatch(
    state: &HubState,
    to: (u64, &str),
    job: &Job,
    resumed: bool,
    admission: &Mutex<Admission>,
) -> Teaching {
    let (capacity, assignments, free, sent) = {
        let a = admission.lock().unwrap();
        (a.capacity, a.assignments, a.free, a.sent)
    };
    let e = job.expected;
    let (idle_elsewhere, room, record, p, secs_on, speed) = {
        let workers = state.worker_caps.lock().unwrap();
        let mut placement = state.placement.lock().unwrap();
        let p = Priority::of(job, placement.share(job), Instant::now());
        let room = placement.dispatched(to.0, job, state.lost.has(&job.key));
        let serves = |r: &&Room| {
            (workers.get(&r.id)).is_some_and(|c| c.serves(&job.req.system, &job.features))
        };
        let rooms = placement.rooms();
        let idle = rooms.iter().filter(|r| r.id != to.0 && r.idle > 0);
        let idle = idle.filter(serves).count();
        let (secs_on, speed) = (placement.secs_on(job, to.1), placement.history.speed(to.1));
        (idle, room, placement.record.clone(), p, secs_on, speed)
    };
    let load = room.as_ref().map(|r| r.load);
    let memory = room.as_ref().and_then(|r| r.size.memory);
    let placed = Teaching {
        quiet: !resumed
            && (room.as_ref()).is_some_and(|r| quiet(r.load.cores, e.demand.cores, r.size.cores)),
    };
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
        expect_below = round3(e.below),
        share = round3(p.share),
        path = round3(p.path),
        waited = round3(p.wait),
        speed = round3(speed),
        expect_secs_on = secs_on.map(round3),
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
    placed
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(super) fn bytes(memory: f64) -> u64 {
    memory.max(0.0) as u64
}

/// `x` to three decimals, for a log line.
pub(super) fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}
