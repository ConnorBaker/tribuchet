//! Which worker a queued build is left to: the least full with an idle
//! agent, by what its builds are expected to use (history.rs) against its
//! size. Fullness is the larger of committed cores over the worker's
//! cores and committed memory over the memory its builds may use, each
//! with the build added. On the copy of the hub's journal of history.rs,
//! replayed by a model with 32 cores and a slot per core on every worker,
//! and three seeds of which worker asks, it cut against the spread rule
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
}

impl Room {
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
}

impl Rooms {
    /// The session that `serves` a build expected to use `d` with an
    /// idle agent and ranks better for it than the asker (an idle agent,
    /// then less full; memory only where every room compared has a
    /// size for it); ties stay with the asker. None once `since`, its
    /// first pass, is GRACE ago before `now`. That room is then spoken
    /// for, an agent and `d`, and `since` set if it was not.
    pub(in crate::hub) fn passes(
        &mut self,
        d: Demand,
        since: &mut Option<Instant>,
        serves: impl Fn(u64) -> bool,
        now: Instant,
    ) -> Option<u64> {
        if since.is_some_and(|s| now.duration_since(s) >= GRACE) {
            return None;
        }
        let mine = self.rooms.iter().find(|r| r.id == self.asker)?;
        let others: Vec<usize> = (0..self.rooms.len())
            .filter(|&i| {
                let r = &self.rooms[i];
                r.id != self.asker && r.idle > 0 && serves(r.id)
            })
            .collect();
        let memory = mine.size.memory.is_some()
            && others.iter().all(|&i| self.rooms[i].size.memory.is_some());
        let rank = |r: &Room| (r.idle == 0, r.fullness(d, memory));
        let order = |a: (bool, f64), b: (bool, f64)| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1));
        let mine = rank(mine);
        let best = others.into_iter().min_by(|&i, &j| {
            let (a, b) = (&self.rooms[i], &self.rooms[j]);
            order(rank(a), rank(b)).then(a.id.cmp(&b.id))
        })?;
        if order(rank(&self.rooms[best]), mine) != Ordering::Less {
            return None;
        }
        let room = &mut self.rooms[best];
        room.idle -= 1;
        room.load = room.load.plus(d);
        since.get_or_insert(now);
        Some(room.id)
    }
}
