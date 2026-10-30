//! CPU arbitration among a worker's builds: Inelastic-First by freezing.
//! The oldest parallel build (by its persisted start; ties by key) leads;
//! while it keeps every CPU busy, the other parallel builds are frozen,
//! thawing at the first 200 ms sample it leaves slack or ends. Always on,
//! with no way to turn it off, wherever the kernel counts each cgroup's
//! frozen time (cgroup.stat.local frozen_usec, Linux 6.18), which its
//! bound and the builds' running time need: on a host
//! without that count it is off, and the worker says so at start. Every
//! build with a cgroup of its own takes part, whatever it declares.
//! That count also bounds it ([`bound`]): a build whose cgroup has been
//! frozen for 10 s of the last 30 s is thawed, and not frozen again until
//! that falls, which bounds what freezing takes from any span of its wall
//! time. Nor are builds held frozen that together pin more than the
//! memory caps above them, or the host, leave room for ([`memory`]).
//! Frozen time counts toward neither a build's timeout nor its
//! max-silent-time ([`Tracking::frozen`]), its client's or the worker's
//! own. The worker freezes and thaws a build only through the
//! write-only handle on its cgroup.freeze that the build's agent sends
//! with the lease, and opens none itself (the module's worker, not root,
//! cannot). A build whose agent sends none is not frozen. The agent thaws
//! the build when the lease ends, so no freeze outlives the worker that
//! made it, and the worker unit's ExecStopPost thaws every build as root
//! once the worker has exited, should an agent not.

mod bound;
mod memory;
mod policy;
mod sample;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::errors::chain;
use crate::worker::resume::ResumeState;
use bound::{FROZEN_PER_WINDOW, FROZEN_WINDOW, FrozenTime};
use policy::{Action, Policy};

pub(crate) use sample::{cgroup_path, frozen_usec, subgroups};

const SAMPLE_EVERY: Duration = Duration::from_millis(200);
/// Samples in the demand mean: one second.
const WINDOW: usize = 5;
/// A throttled build took 1.05 s to reach frozen in one measurement;
/// uninterruptible sleep can take longer.
const FREEZE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Arbiter {
    enabled: bool,
    /// A build with a cgroup came without a handle, which is said once.
    unhandled: AtomicBool,
    state: Mutex<State>,
    /// Where the memory test reads (a test's own tree in tests).
    memory: memory::Roots,
}

struct State {
    builds: HashMap<String, Tracked>,
    policy: Policy,
}

/// A running build, keyed by dedupe key.
struct Tracked {
    build_id: String,
    cgroup: Option<PathBuf>,
    /// Its cgroup.freeze, write-only, from its agent; none where it has
    /// no cgroup, and it is then never frozen.
    freeze: Option<File>,
    order: u64,
    /// May be frozen: it has a handle, and no freeze of it went unreached.
    eligible: bool,
    /// Its cgroup's frozen time over the last FROZEN_WINDOW: within the
    /// bound, it may be frozen.
    frozen_time: FrozenTime,
    samples: VecDeque<u32>,
    /// Asked to be frozen, and not thawed since.
    frozen: bool,
    /// When a freeze not yet reached was asked for.
    unreached: Option<Instant>,
}

impl Tracked {
    /// Write `state` ("1" freezes, "0" thaws) to its cgroup.freeze; false,
    /// said, if it cannot be.
    fn ask(&self, state: &str) -> bool {
        let Some(mut freeze) = self.freeze.as_ref() else {
            return false;
        };
        let written = freeze.write_all(state.as_bytes());
        if let Err(e) = &written {
            tracing::warn!(id = self.build_id, "writing {state} to cgroup.freeze: {e}");
        }
        written.is_ok()
    }

    /// Thaw it.
    fn thaw(&mut self) {
        self.ask("0");
        self.frozen = false;
        self.unreached = None;
        tracing::info!(id = self.build_id, frozen = false, "arbiter applied");
    }
}

/// Untracks its build when its supervision ends.
pub(super) struct Tracking {
    arbiter: Arc<Arbiter>,
    key: String,
    /// The most frozen time read so far, in µs.
    most_usec: AtomicU64,
}

impl Tracking {
    /// The build's frozen time so far, by its cgroup's own count (zero
    /// where none is kept): not running time, which alone counts toward
    /// its build timeout and max-silent-time. The count never falls: one
    /// that cannot be read now is what it was.
    pub(super) fn frozen(&self) -> Duration {
        let cgroup = self
            .arbiter
            .lock()
            .builds
            .get(&self.key)
            .and_then(|t| t.cgroup.clone());
        let us = cgroup.as_deref().and_then(frozen_usec).unwrap_or(0);
        Duration::from_micros(self.most_usec.fetch_max(us, Ordering::Relaxed).max(us))
    }
}

impl Drop for Tracking {
    fn drop(&mut self) {
        self.arbiter.lock().builds.remove(&self.key);
    }
}

impl Arbiter {
    /// This host's arbiter: a thread samples the tracked builds and applies
    /// the policy every 200 ms, unless the kernel counts no frozen time
    /// (sample::require_frozen_time), a capability of the host, not a
    /// choice: then it is off, and a warning says why.
    pub(super) fn new() -> Arc<Self> {
        let cgroup = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
        let release = rustix::system::uname()
            .release()
            .to_string_lossy()
            .into_owned();
        if let Err(e) = sample::require_frozen_time(&cgroup, &release) {
            tracing::warn!("CPU arbiter off on this host: {}", chain(&e));
            return Self::off();
        }
        let ncpus = thread::available_parallelism()
            .ok()
            .and_then(|n| u32::try_from(n.get()).ok())
            .unwrap_or(1);
        let arbiter = Self::with_cpus(true, ncpus);
        tracing::info!(
            ncpus,
            frozen_per_window_secs = FROZEN_PER_WINDOW.as_secs(),
            frozen_window_secs = FROZEN_WINDOW.as_secs(),
            "CPU arbiter on"
        );
        let a = arbiter.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(SAMPLE_EVERY);
                a.tick(Instant::now());
            }
        });
        arbiter
    }

    /// An arbiter that freezes nothing: on a host that cannot count frozen
    /// time, and in tests.
    pub(super) fn off() -> Arc<Self> {
        Self::with_cpus(false, 1)
    }

    fn with_cpus(enabled: bool, ncpus: u32) -> Arc<Self> {
        Arc::new(Self {
            enabled,
            unhandled: AtomicBool::new(false),
            state: Mutex::new(State {
                builds: HashMap::new(),
                policy: Policy::new(ncpus),
            }),
            memory: memory::Roots::system(),
        })
    }

    /// The state, also after a panic elsewhere left the lock poisoned:
    /// the arbiter is the only thawer and must keep going.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Track a build with `freeze`, its cgroup.freeze as its agent sent it.
    pub(super) fn track(self: &Arc<Self>, st: &ResumeState, freeze: Option<File>) -> Tracking {
        let cgroup = sample::build_cgroup(st.pid, &st.build_id);
        let id = &st.build_id;
        self.insert(&st.dedupe_key, id, st.started_unix, cgroup, freeze);
        Tracking {
            arbiter: self.clone(),
            key: st.dedupe_key.clone(),
            most_usec: AtomicU64::new(0),
        }
    }

    /// An adopted build an earlier worker froze was thawed by its agent as
    /// that worker's lease ended; one still frozen (as root may leave it) is
    /// thawed here.
    fn insert(
        &self,
        key: &str,
        id: &str,
        order: u64,
        cgroup: Option<PathBuf>,
        freeze: Option<File>,
    ) {
        let t = Tracked {
            build_id: id.to_owned(),
            eligible: self.enabled && cgroup.is_some() && freeze.is_some(),
            cgroup,
            freeze,
            order,
            frozen_time: FrozenTime::default(),
            samples: VecDeque::with_capacity(WINDOW),
            frozen: false,
            unreached: None,
        };
        let cg = t.cgroup.as_deref();
        if self.enabled
            && cg.is_some()
            && t.freeze.is_none()
            && !self.unhandled.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                id,
                "a build came with no handle on its cgroup.freeze, as from an agent older than this worker: such builds are not frozen"
            );
        }
        if cg.and_then(sample::freeze_requested) == Some(true) {
            t.ask("0");
        }
        self.lock().builds.insert(key.to_owned(), t);
    }

    /// One sample: read every build's demand without the lock, then decide
    /// and write each build's cgroup.freeze under it.
    fn tick(&self, now: Instant) {
        let dirs: Vec<(String, PathBuf)> = self
            .lock()
            .builds
            .iter()
            .filter_map(|(key, t)| Some((key.clone(), t.cgroup.clone()?)))
            .collect();
        // Demand, the time frozen so far by the cgroup's own count, the
        // memory it would pin frozen (memory.rs), and whether a freeze
        // asked for is reached.
        let available = memory::available(&self.memory);
        let counts: Vec<_> = dirs
            .into_iter()
            .map(|(key, cg)| {
                let held = memory::held(&cg, &self.memory);
                let reached = sample::frozen_reached(&cg);
                (
                    key,
                    sample::runnable_threads(&cg),
                    frozen_usec(&cg),
                    held,
                    reached,
                )
            })
            .collect();
        let mut state = self.lock();
        let State { builds, policy, .. } = &mut *state;
        let mut held = HashMap::new();
        for (key, runnable, frozen_us, h, reached) in counts {
            let Some(t) = builds.get_mut(&key) else {
                continue;
            };
            if t.samples.len() == WINDOW {
                t.samples.pop_front();
            }
            t.samples.push_back(runnable);
            if let Some(h) = h {
                held.insert(key, h);
            }
            // Frozen for FROZEN_PER_WINDOW of the last FROZEN_WINDOW, or
            // not counted: the policy thaws it now, and freezes it again
            // only once it is within the bound. By the cgroup's own count,
            // so that a worker restart grants no build more (bound.rs).
            if t.frozen_time.record(now, frozen_us) {
                tracing::info!(
                    id = t.build_id,
                    frozen_in_window_ms = t.frozen_time.in_window(now).as_millis(),
                    bound_secs = FROZEN_PER_WINDOW.as_secs(),
                    window_secs = FROZEN_WINDOW.as_secs(),
                    "frozen for its bound in the window; thawed, not frozen again until under it"
                );
            }
            // A freeze not reached in FREEZE_TIMEOUT is undone and not
            // asked again: tasks that do not reach the freezer would keep
            // the build half-stopped while its clock counted it frozen.
            if let Some(asked) = t.unreached {
                if reached {
                    t.unreached = None;
                    tracing::info!(id = t.build_id, frozen = true, "arbiter applied");
                } else if now.duration_since(asked) >= FREEZE_TIMEOUT {
                    t.ask("0");
                    (t.frozen, t.unreached, t.eligible) = (false, None, false);
                    tracing::warn!(
                        id = t.build_id,
                        "freezer state not reached; not frozen again"
                    );
                }
            }
        }
        // The frozen builds' pins, then the freezes asked for, each in
        // order, against what memory leaves (memory.rs): a frozen build
        // that does not fit is thawed, a freeze that does not is not made.
        let mut budget = memory::Budget::new(available);
        let mut unfit: Vec<(&String, &Tracked)> = builds.iter().filter(|(_, t)| t.frozen).collect();
        unfit.sort_by_key(|(key, t)| (t.order, *key));
        unfit.retain(|(key, _)| !budget.take(held.get(*key)));
        let view: Vec<policy::Build> = builds
            .iter()
            .map(|(key, t)| policy::Build {
                key,
                order: t.order,
                eligible: t.eligible
                    && t.frozen_time.within()
                    && !unfit.iter().any(|(k, _)| *k == key),
                runnable: t.samples.back().copied().unwrap_or(0),
                mean: sample::mean(&t.samples),
                frozen: t.frozen,
            })
            .collect();
        let mut actions = policy.step(now, &view);
        actions.sort_by_key(|a| builds.get_key_value(a.key()).map(|(k, t)| (t.order, k)));
        for action in actions {
            let Some(t) = builds.get_mut(action.key()) else {
                continue;
            };
            match action {
                Action::Thaw(_) => t.thaw(),
                Action::Freeze(_) if budget.take(held.get(action.key())) => {
                    if t.ask("1") {
                        (t.frozen, t.unreached) = (true, Some(now));
                    } else {
                        t.eligible = false;
                    }
                }
                Action::Freeze(_) => {}
            }
        }
    }
}
