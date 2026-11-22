//! Waiting for memory: a build short of memory at every worker that
//! serves it waits, and past AGE keeps the worker with room for it
//! soonest, which takes later builds only as EASY's backfill
//! (Placement::waits; DESIGN.md, Placement).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::{Demand, Expected, Placement, Room};
use crate::hub::state::Job;

/// How long a build waits for memory before a worker is kept for it
/// (Placement::waits). DESIGN.md's replays are of this value alone: with
/// it, the batch's 95th-percentile KVM wait was 192 s, as with no worker
/// kept.
pub(in crate::hub) const AGE: Duration = Duration::from_mins(50);

/// What a scan of the queue reads of the sessions once, its time, and
/// the sessions kept for a build it passed.
pub(in crate::hub) struct Scan {
    rooms: Vec<Room>,
    now: Instant,
    kept: HashMap<u64, Kept>,
}

/// A session kept for a build: its id, when the session is expected to
/// have room for it (its shadow start; None, unknown), and the memory
/// left beside it then.
struct Kept {
    build: String,
    shadow: Option<Instant>,
    beside: f64,
}

impl Kept {
    /// Whether a later build expected to use `e` may go there first
    /// (EASY's backfill): only before the shadow start, and only if it is
    /// expected to end by then or fits beside the kept build.
    fn backfills(&self, e: &Expected, now: Instant) -> bool {
        let ends = |s: Instant| {
            e.secs
                .is_some_and(|x| now + Duration::from_secs_f64(x) <= s)
        };
        (self.shadow.filter(|s| *s > now))
            .is_some_and(|s| ends(s) || e.demand.memory <= self.beside)
    }
}

impl Placement {
    /// The sessions' rooms, read once for a scan of the queue (each
    /// locks its session's admission), and none kept yet.
    pub(in crate::hub) fn scan(&self) -> Scan {
        Scan {
            rooms: self.rooms(),
            now: Instant::now(),
            kept: HashMap::new(),
        }
    }

    /// Whether `job` waits rather than go to session `at` (None: none
    /// asks), of those `serves` serves: held for its key, `at` kept for
    /// another build and not a backfill there (Kept::backfills), or short
    /// of memory (Room::admits). Past AGE a build keeps one worker, with
    /// room for it soonest (frees; a tie, the lowest id), one build to a
    /// worker, until it fits: EASY (Mu'alem and Feitelson, 2001). One test
    /// for a take and a wait for a slot, which ask of each worker in turn.
    pub(in crate::hub) fn waits(
        &self,
        job: &Job,
        at: Option<u64>,
        serves: impl Fn(u64) -> bool,
        scan: &mut Scan,
    ) -> bool {
        let Some(at) = at else {
            return self.held_for_key(job);
        };
        let e = self.expected(job);
        let other = scan.kept.get(&at).filter(|k| k.build != job.id);
        if other.is_some_and(|k| !k.backfills(&e, scan.now)) {
            return true;
        }
        let serving = || (scan.rooms.iter()).filter(|r| r.id == at || serves(r.id));
        let largest = serving().filter_map(|r| r.size.memory).reduce(f64::max);
        let Some(mine) = scan.rooms.iter().find(|r| r.id == at) else {
            return true;
        };
        if mine.admits(e.demand, largest) {
            return self.held_for_key(job);
        }
        let keeps = scan.kept.values().any(|k| k.build == job.id);
        let aged = scan.now.duration_since(job.queued_at) >= AGE;
        if !keeps && aged && !self.held_for_key(job) {
            let fits = |r: &&Room| {
                let m = r.size.memory;
                m.is_some_and(|m| e.demand.memory <= m) || (m.is_some() && m == largest)
            };
            let soonest = (serving().filter(fits))
                .filter(|r| !scan.kept.contains_key(&r.id))
                .map(|r| (self.frees(r.id, e.demand, scan.now), r.id))
                .min_by_key(|(start, id)| (start.is_none(), start.map(|s| s.0), *id));
            if let Some((start, id)) = soonest {
                let (shadow, beside) = start.map_or((None, 0.0), |(s, b)| (Some(s), b));
                let build = job.id.clone();
                scan.kept.insert(
                    id,
                    Kept {
                        build,
                        shadow,
                        beside,
                    },
                );
            }
        }
        true
    }

    /// Whether `job`, short of memory at session `at`, would fit there
    /// were its clientless builds given up (`rooms`, from rooms_for): it
    /// wants them, as a build Rooms::passes ranks away for their load
    /// does. Not one held for its key, which no room lets in.
    pub(in crate::hub) fn wants_memory(
        &self,
        job: &Job,
        at: u64,
        serves: impl Fn(u64) -> bool,
        rooms: &[Room],
    ) -> bool {
        let d = self.expected(job).demand;
        let serving = (rooms.iter()).filter(|r| r.id == at || serves(r.id));
        let largest = serving.filter_map(|r| r.size.memory).reduce(f64::max);
        (rooms.iter().find(|r| r.id == at))
            .is_some_and(|r| !r.admits(d, largest) && r.freed().admits(d, largest))
            && !self.held_for_key(job)
    }

    /// When session `id` is expected to have room for a build expected to
    /// use `d`, from now: as its builds end, by their expected ends, enough
    /// of them for `d` (all of them for a build larger than it, as `d`
    /// over its memory leaves too little however many end), and the memory
    /// left beside `d` then. None where a build that must end has no
    /// expected end.
    fn frees(&self, id: u64, d: Demand, now: Instant) -> Option<(Instant, f64)> {
        let s = self.sessions.get(&id)?;
        let m = s.size.memory?;
        let mut need = s.load().memory + d.memory - m;
        let mut ends: Vec<_> = s
            .committed
            .values()
            .map(|c| (c.ends, c.demand.memory))
            .collect();
        ends.sort_by_key(|(end, _)| (end.is_none(), *end));
        let mut at = now;
        for (end, memory) in ends {
            if need <= 0.0 {
                break;
            }
            at = at.max(end?);
            need -= memory;
        }
        Some((at, -need))
    }
}
