//! The order a worker takes queued builds in.
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

use std::collections::VecDeque;
use std::time::Instant;

use super::Expected;
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

/// The terms of a waiting build, as a dispatch logs them (module doc).
#[derive(Clone, Copy)]
pub(in crate::hub) struct Priority {
    pub(in crate::hub) path: f64,
    pub(in crate::hub) wait: f64,
}

impl Priority {
    pub(in crate::hub) fn of(job: &Job, now: Instant) -> Self {
        Self {
            path: path(&job.expected),
            wait: now.saturating_duration_since(job.queued_at).as_secs_f64(),
        }
    }
}
