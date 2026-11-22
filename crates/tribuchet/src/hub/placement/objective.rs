//! The order a worker takes queued builds in, and each client's share.
//!
//! A waiting build's urgency is its wait plus its path:
//!
//! - **path**: the expected running seconds learned to follow it
//!   (downstream.rs): the longest chain of builds that wait for it, its
//!   upward rank less its own seconds. 0 where nothing was learned. Its
//!   own seconds are not in it: among builds nothing follows, they would
//!   take the longest first, and in a class few workers serve (KVM) the
//!   shorter ones then wait out the longer that came after them.
//! - **wait**: seconds since it last entered the queue (a requeue enters
//!   it again).
//!
//! The most urgent goes first; equal ones in the order they came. As
//! wait(b) = now - queued(b), urgency(b) = now - due(b), where
//! due(b) = queued(b) - path(b): the order is by due, earliest first, the
//! same at every instant. So the queue is kept in it (`enqueue`), and a
//! worker takes the first build in it it can (state/take.rs). With every
//! path equal it is the order builds came in, FIFO.
//!
//! Starvation bound: a build c queued after b goes before it only if it
//! came less than path(c) - path(b) seconds after b. No stream of later
//! builds keeps b waiting: one arriving more than the longest path learned
//! after b goes behind it. Memory admission skipping b is outside this.
//!
//! - **share**: the least, among the clients still waiting for a build, of
//!   its dominant share (Dominant Resource Fairness, Ghodsi et al., NSDI
//!   2011): the larger of the expected cores and the expected memory that
//!   the builds placed and not settled hold for the client, over the
//!   connected workers' cores and memory (a worker that reports none
//!   counts none). A placed build holds for the clients waiting for it as
//!   it was placed, each once, until it settles (`Charges`). The clients
//!   no request names are one client, unknown, charged as one. Share is
//!   logged at each dispatch and is not in the order: behind
//!   nix-grpc-store, which opens a nix-daemon connection per build, each
//!   build is its own client and holds nothing while it waits, so a client
//!   the hub can name would wait behind every one of them, without bound.
//!   It enters the order once nix-grpc-store forwards each build's
//!   originating client.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use super::{Demand, Expected};
use crate::hub::state::Job;

/// The expected seconds learned to follow it.
fn path(e: &Expected) -> f64 {
    e.below
}

/// Seconds from `b` to `a`, negative if `a` is earlier.
fn since(a: Instant, b: Instant) -> f64 {
    match a.checked_duration_since(b) {
        Some(d) => d.as_secs_f64(),
        None => -b.duration_since(a).as_secs_f64(),
    }
}

/// Whether `a` is due before `b`: queued less its path, earlier.
fn before(a: &Job, b: &Job) -> bool {
    since(a.queued_at, b.queued_at) < path(&a.expected) - path(&b.expected)
}

/// Put `job` in `queue`, which is in due order, behind every build due no
/// later than it.
pub(in crate::hub) fn enqueue(queue: &mut VecDeque<Job>, job: Job) {
    let at = queue.partition_point(|q| !before(&job, q));
    queue.insert(at, job);
}

/// The clients a build holds for: each waiting for it, once.
pub(in crate::hub) fn owners(job: &Job) -> Vec<Option<Arc<str>>> {
    let all = job.replay.owners();
    let one: BTreeSet<_> = all.into_iter().collect();
    one.into_iter().collect()
}

/// What the builds placed and not settled hold for each client, and how
/// many builds; None, the clients no request named, pooled.
#[derive(Default)]
pub(in crate::hub) struct Charges {
    of: HashMap<Option<Arc<str>>, (Demand, usize)>,
}

impl Charges {
    /// A build placed for `owners`, holding `d`.
    pub(in crate::hub) fn charge(&mut self, owners: &[Option<Arc<str>>], d: Demand) {
        for o in owners {
            let (sum, n) = self.of.entry(o.clone()).or_default();
            (*sum, *n) = (sum.plus(d), *n + 1);
        }
    }

    /// That build settled.
    pub(in crate::hub) fn release(&mut self, owners: &[Option<Arc<str>>], d: Demand) {
        for o in owners {
            if let Some((sum, n)) = self.of.get_mut(o) {
                *n -= 1;
                *sum = Demand {
                    cores: sum.cores - d.cores,
                    memory: sum.memory - d.memory,
                };
                if *n == 0 {
                    self.of.remove(o);
                }
            }
        }
    }

    /// How many clients the placed builds hold for: each with one left.
    #[cfg(test)]
    pub(in crate::hub) fn clients(&self) -> usize {
        self.of.len()
    }

    /// The least dominant share, of the farm's `total`, among the clients
    /// waiting for `job` now; 0 for none.
    pub(in crate::hub) fn least(&self, job: &Job, total: Demand) -> f64 {
        let frac = |x: f64, of: f64| if of > 0.0 { x / of } else { 0.0 };
        let share = |o: &Option<Arc<str>>| {
            self.of.get(o).map_or(0.0, |(d, _)| {
                frac(d.cores, total.cores).max(frac(d.memory, total.memory))
            })
        };
        (job.replay.owners().iter())
            .map(share)
            .reduce(f64::min)
            .unwrap_or(0.0)
    }
}

/// The terms of a waiting build, as a dispatch logs them (module doc).
#[derive(Clone, Copy)]
pub(in crate::hub) struct Priority {
    pub(in crate::hub) share: f64,
    pub(in crate::hub) path: f64,
    pub(in crate::hub) wait: f64,
}

impl Priority {
    pub(in crate::hub) fn of(job: &Job, share: f64, now: Instant) -> Self {
        Self {
            share,
            path: path(&job.expected),
            wait: now.saturating_duration_since(job.queued_at).as_secs_f64(),
        }
    }
}
