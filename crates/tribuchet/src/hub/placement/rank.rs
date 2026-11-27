//! Which worker a queued build is left to: of those with an idle agent
//! that admit it for its memory (Room::admits), where it would finish
//! first, its seconds at factor 1 from its own runs (History::own_secs)
//! over the worker's measured factor, times one plus the worker's
//! fullness, or the least full where any of those is unknown; a short build to the least full of the slowest workers
//! where it fits their cores and memory (SHORT, TIER). Fullness, by what its builds
//! are expected to use (history.rs) against its size, is the
//! larger of committed cores over the worker's
//! cores and committed memory over the memory its builds may use, each
//! with the build added. Least full, on the copy of the hub's journal of history.rs,
//! replayed by a model with 32 cores and a slot per core on every worker,
//! and three seeds of which worker asks, cut against the spread rule
//! (a build declaring big-parallel left to the worker with fewer such
//! builds): the excess running time of builds using 4 cores or more for a
//! minute from 4.2-5.2 % to 2.4-2.6 % (their 99th percentile from
//! queueing to finish from 1.84-2.03x to 1.45-1.52x their running time),
//! worker time with cores oversubscribed from 1.56-1.74 % to 1.25-1.37 %,
//! and with the builds' summed peaks over the worker's memory from
//! 2.08-2.17 % to 1.64-1.67 %. Workers of different sizes were not
//! replayed.

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use super::history::Demand;

/// How long after it was first passed over a build may be passed over
/// again: the woken worker's time to ask, not a wait for room.
pub(in crate::hub) const GRACE: Duration = Duration::from_secs(2);

/// The seconds, on a worker of speed factor 1, under which a build is
/// short: it is left to the slowest workers, keeping the faster ones'
/// agents for longer builds. The same sources ran on a cloud worker in
/// 0.67x-1.01x local worker L1's time up to 10 s, 1.12x to 100 s, 1.39x
/// from 100 s, but 1.08x-1.44x local worker L2's up to 10 s (DESIGN.md,
/// Worker speed): one moved off L2 runs up to 1.44x as long.
pub(in crate::hub) const SHORT: f64 = 10.0;

/// How much faster than the slowest worker a worker must be to be kept
/// for longer builds: factors within it are the same tier. Measured
/// factors of one kind of machine differ by about 1 %, the fastest kind
/// by 20 % or more; any value between gives the same tiers.
pub(in crate::hub) const TIER: f64 = 1.1;

/// What a worker's builds share: its cores and, where it reported it,
/// the memory its builds may use.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::hub) struct Size {
    pub(in crate::hub) cores: f64,
    pub(in crate::hub) memory: Option<f64>,
}

/// A worker session's room: its idle agents, size, and what its
/// unsettled builds are expected to use.
#[derive(Clone, Debug)]
pub(in crate::hub) struct Room {
    pub(in crate::hub) id: u64,
    /// Agents with no build, apart from the lookahead.
    pub(in crate::hub) idle: u32,
    pub(in crate::hub) size: Size,
    pub(in crate::hub) load: Demand,
    /// Of `load`, what its builds the clientless rule may give up hold.
    pub(in crate::hub) clientless: Demand,
    /// It holds no build.
    pub(in crate::hub) empty: bool,
    /// It holds builds, each one the clientless rule may give up.
    pub(in crate::hub) only_clientless: bool,
}

/// The most memory any of `rooms` reported for its builds; None where none
/// reported any.
pub(in crate::hub) fn largest(rooms: &[Room]) -> Option<f64> {
    rooms.iter().filter_map(|r| r.size.memory).reduce(f64::max)
}

impl Room {
    /// Whether the worker admits a build expected to use `d`, for its
    /// memory: what it holds and the build fit its builds' memory; or it
    /// holds nothing and is the largest serving the build (`largest`), so
    /// a build larger than every worker waits for that, and is not failed
    /// for its size. No memory reported admits any. Cores are not tested:
    /// the CPU arbiter shares them, and a test of them starved builds of
    /// many cores in an offline replay of the hub's recorded placements.
    pub(in crate::hub) fn admits(&self, d: Demand, largest: Option<f64>) -> bool {
        let Some(m) = self.size.memory else {
            return true;
        };
        self.load.memory + d.memory <= m || (self.empty && largest.is_none_or(|l| m >= l))
    }

    /// The room with its clientless builds given up: empty if they are
    /// all it holds.
    pub(in crate::hub) fn freed(&self) -> Self {
        let (l, c) = (self.load, self.clientless);
        let load = Demand {
            cores: l.cores - c.cores,
            memory: l.memory - c.memory,
        };
        let empty = self.empty || self.only_clientless;
        Self {
            load,
            empty,
            only_clientless: false,
            ..*self
        }
    }

    /// How full the worker would be with `d` added: its committed cores
    /// over its cores, or, with `memory`, its committed memory over its
    /// memory if fuller.
    pub(in crate::hub) fn fullness(&self, d: Demand, memory: bool) -> f64 {
        let cpu = (self.load.cores + d.cores) / self.size.cores;
        match self.size.memory.filter(|_| memory) {
            Some(m) => cpu.max((self.load.memory + d.memory) / m),
            None => cpu,
        }
    }
}

/// The rooms as the asking session saw them before its scan of the
/// queue, less what the scan left to others.
pub(in crate::hub) struct Rooms {
    pub(in crate::hub) asker: u64,
    pub(in crate::hub) rooms: Vec<Room>,
    /// A room per build passes ranked that lost it only for the load of
    /// its clientless builds (passes): they are wanted.
    pub(in crate::hub) wants: Vec<u64>,
    /// Why passes last left a build to another room, for the log.
    pub(in crate::hub) why: &'static str,
}

impl Rooms {
    pub(in crate::hub) fn new(asker: u64, rooms: Vec<Room>) -> Self {
        Self {
            asker,
            rooms,
            wants: Vec::new(),
            why: "",
        }
    }

    /// The session that `serves` a build expected to use `d` with an idle
    /// agent and ranks better for it than the asker; ties stay with the
    /// asker. None once `since`, its first pass, is GRACE ago before `now`.
    /// That room is then spoken for, an agent and `d`, and `since` set if
    /// it was not. By `speed`, each room's measured speed factor; by
    /// `unit`, the build's seconds at factor 1 from its own runs (rank).
    pub(in crate::hub) fn passes(
        &mut self,
        d: Demand,
        since: &mut Option<Instant>,
        serves: impl Fn(u64) -> bool,
        speed: impl Fn(u64) -> Option<f64>,
        unit: Option<f64>,
        now: Instant,
    ) -> Option<u64> {
        if since.is_some_and(|s| now.duration_since(s) >= GRACE) {
            return None;
        }
        let mine = self.rooms.iter().find(|r| r.id == self.asker)?;
        let serving: Vec<Room> = (self.rooms.iter())
            .filter(|r| r.id == self.asker || serves(r.id))
            .cloned()
            .collect();
        let largest = largest(&serving);
        let others: Vec<usize> = (0..self.rooms.len())
            .filter(|&i| {
                let r = &self.rooms[i];
                r.id != self.asker && r.idle > 0 && serves(r.id) && r.admits(d, largest)
            })
            .collect();
        let memory = mine.size.memory.is_some()
            && others.iter().all(|&i| self.rooms[i].size.memory.is_some());
        let secs = |id| Some(unit? / speed(id)?);
        let compared = || (others.iter().map(|&i| self.rooms[i].id)).chain([mine.id]);
        let known = compared().all(|id| secs(id).is_some());
        let short = known && unit.is_some_and(|u| u < SHORT);
        let slowest = (compared().filter_map(&speed)).fold(f64::INFINITY, f64::min);
        // An idle agent first; then a short build where it fits, in the
        // slowest tier, the least full; any other the earliest finish, its
        // seconds times one plus fullness; fullness alone where any room
        // compared has no seconds (memory only where each has a size).
        let rank = |r: &Room| {
            let full = r.fullness(d, memory);
            if short {
                let tier = speed(r.id).is_some_and(|f| f > slowest * TIER);
                return (r.idle == 0, full > 1.0, tier, full);
            }
            let finish = secs(r.id)
                .filter(|_| known)
                .map_or(full, |s| s * (1.0 + full));
            (r.idle == 0, false, false, finish)
        };
        let order = |a: (bool, bool, bool, f64), b: (bool, bool, bool, f64)| {
            (a.0, a.1, a.2)
                .cmp(&(b.0, b.1, b.2))
                .then(a.3.total_cmp(&b.3))
        };
        let (asker, mine) = (mine.id, rank(mine));
        let best = others.iter().copied().min_by(|&i, &j| {
            let (a, b) = (&self.rooms[i], &self.rooms[j]);
            order(rank(a), rank(b)).then(a.id.cmp(&b.id))
        })?;
        let passed = order(rank(&self.rooms[best]), mine) == Ordering::Less;
        // A room with an idle agent that would win the build were its
        // clientless builds given up, and does not, wants them given up.
        let (won, win) = if passed {
            (self.rooms[best].id, rank(&self.rooms[best]))
        } else {
            (asker, mine)
        };
        let lost_only_to_them = |r: &Room| {
            r.id != won
                && r.idle > 0
                && order(rank(&r.freed()), win) == Ordering::Less
                && order(rank(r), win) != Ordering::Less
        };
        let asker = self.rooms.iter().position(|r| r.id == asker);
        let wants = (others.into_iter().chain(asker)).map(|i| &self.rooms[i]);
        self.wants
            .extend(wants.filter(|r| lost_only_to_them(r)).map(|r| r.id));
        if !passed {
            return None;
        }
        let (b, m) = (rank(&self.rooms[best]), mine);
        self.why = match (short, known) {
            _ if b.0 != m.0 => "an idle agent",
            (true, _) if b.1 != m.1 => "it fits",
            (true, _) if b.2 != m.2 => "a slower worker's tier",
            (false, true) => "it finishes first",
            _ => "less full",
        };
        let room = &mut self.rooms[best];
        room.idle -= 1;
        room.load = room.load.plus(d);
        (room.empty, room.only_clientless) = (false, false);
        since.get_or_insert(now);
        Some(room.id)
    }
}
