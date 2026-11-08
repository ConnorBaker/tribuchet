//! Build leases: a slot and the agent its build runs on, taken and given
//! back together. Every lease counts against the slots, so a slot is free
//! only while an agent is: a new build never holds a slot without one.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use sandbox_proto::agent::StartRequest;
use tokio::sync::Notify;

use super::agents::{AgentBuild, unreachable};
use super::resume::CANCELLED;
use crate::errors::{Result, chain, err_msg};
use crate::proto::RequestJob;

/// The agent sockets and the leases on them.
pub(super) struct Pool {
    agents: Mutex<Agents>,
    max_jobs: usize,
    /// A lease ended, an agent came up or a build was cancelled: a lease
    /// waiting for an agent looks again.
    looked_for: Condvar,
    /// A lease ended or an agent came up: the session loop hands out the
    /// free slots and declares them.
    pub(super) changed: Notify,
}

/// An agent in `all` that is neither free nor down is leased.
struct Agents {
    all: Vec<PathBuf>,
    free: Vec<PathBuf>,
    /// Agents nothing answered on, until a probe reaches them again.
    down: Vec<PathBuf>,
    /// Every lease, also one on an agent outside `all` (an adopted build's,
    /// no longer configured) and one waiting for an agent.
    leases: usize,
    /// The agents of builds that ran already (`lease_this`): discover
    /// leaves one outside `all` out until its lease ends (a sweep of it,
    /// down, would kill its build).
    running: Vec<PathBuf>,
}

/// The slots, read together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Room {
    /// One per agent not down, at most max-jobs.
    pub(super) capacity: usize,
    pub(super) free: usize,
    pub(super) leases: usize,
}

impl Room {
    /// What to ask the hub for: the free slots, and one build staged ahead
    /// when `lookahead` (every build holds a lease), not with no agent up.
    pub(super) fn request(self, lookahead: bool, assignments: u64) -> RequestJob {
        let asked = self.free + usize::from(lookahead && self.capacity > 0);
        RequestJob {
            capacity: u32::try_from(asked).unwrap_or(u32::MAX),
            assignments: Some(assignments),
            free_slots: u32::try_from(self.free).ok(),
        }
    }
}

impl Agents {
    fn room(&self, max_jobs: usize) -> Room {
        let capacity = max_jobs.min(self.all.len() - self.down.len());
        Room {
            capacity,
            free: capacity.saturating_sub(self.leases),
            leases: self.leases,
        }
    }
}

/// Move `socket` from `from` to `to`; false if it is not in `from`.
fn shift(from: &mut Vec<PathBuf>, to: &mut Vec<PathBuf>, socket: &Path) -> bool {
    let Some(i) = from.iter().position(|s| s == socket) else {
        return false;
    };
    to.push(from.remove(i));
    true
}

impl Pool {
    pub(super) fn new(sockets: Vec<PathBuf>, max_jobs: usize) -> Arc<Self> {
        let agents = Agents {
            free: sockets.clone(),
            all: sockets,
            down: Vec::new(),
            leases: 0,
            running: Vec::new(),
        };
        Arc::new(Self {
            agents: Mutex::new(agents),
            max_jobs,
            looked_for: Condvar::new(),
            changed: Notify::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Agents> {
        self.agents.lock().unwrap()
    }

    pub(super) fn room(&self) -> Room {
        self.lock().room(self.max_jobs)
    }

    pub(super) fn idle_sockets(&self) -> Vec<PathBuf> {
        self.lock().free.clone()
    }

    pub(super) fn down_sockets(&self) -> Vec<PathBuf> {
        self.lock().down.clone()
    }

    /// A free slot and a free agent, in one step; None with no slot free.
    pub(super) fn lease(self: &Arc<Self>) -> Option<Lease> {
        let mut agents = self.lock();
        if agents.room(self.max_jobs).free == 0 {
            return None;
        }
        let socket = Some(agents.free.pop()?);
        agents.leases += 1;
        let pool = self.clone();
        Some(Lease { pool, socket })
    }

    /// The lease of a build that runs on `socket` already: counted, a slot
    /// free or not.
    pub(super) fn lease_this(self: &Arc<Self>, socket: &Path) -> Lease {
        let mut agents = self.lock();
        agents.free.retain(|s| s != socket);
        agents.down.retain(|s| s != socket);
        agents.running.push(socket.to_path_buf());
        agents.leases += 1;
        let (pool, socket) = (self.clone(), Some(socket.to_path_buf()));
        Lease { pool, socket }
    }

    /// Set a free agent aside until a probe reaches it again. False if it
    /// is not free.
    pub(super) fn mark_down(&self, socket: &Path) -> bool {
        let agents = &mut *self.lock();
        shift(&mut agents.free, &mut agents.down, socket)
    }

    /// A probe reached `socket`: free again if it was down.
    pub(super) fn probe_up(&self, socket: &Path) {
        let up = {
            let agents = &mut *self.lock();
            shift(&mut agents.down, &mut agents.free, socket)
        };
        if up {
            tracing::info!(agent = %socket.display(), "build agents joined");
            self.wake();
        }
    }

    /// Add sockets not seen before, down until a probe reaches them.
    pub(super) fn discover(&self, sockets: Vec<PathBuf>) {
        let mut agents = self.lock();
        for socket in sockets {
            if !agents.all.contains(&socket) && !agents.running.contains(&socket) {
                agents.all.push(socket.clone());
                agents.down.push(socket);
            }
        }
    }

    /// A lease ended, an agent came up or a build was cancelled: the leases
    /// waiting for an agent look again, and the session loop. The lock is
    /// taken first, so a lease between its look and its wait is not missed.
    pub(super) fn wake(&self) {
        drop(self.lock());
        self.looked_for.notify_all();
        self.changed.notify_one();
    }
}

/// A slot and the agent its build runs on; dropping it gives both back.
pub(super) struct Lease {
    pool: Arc<Pool>,
    /// None while it waits for an agent, its own having not answered.
    socket: Option<PathBuf>,
}

impl Lease {
    /// Start the build on the lease's agent, or on the next one free if
    /// nothing answers there (`next_agent`).
    pub(super) fn start(
        &mut self,
        req: &StartRequest,
        tmp_pack: &fs::File,
        log: &fs::File,
        cancelled: impl Fn() -> bool,
        waiting: impl FnOnce(),
    ) -> Result<(PathBuf, AgentBuild)> {
        let mut waiting = Some(waiting);
        loop {
            if let Some(socket) = self.socket.clone() {
                match AgentBuild::start(&socket, req, tmp_pack, log) {
                    Err(e) if unreachable(&e) => {
                        tracing::warn!(id = req.build_id, "{}; trying another agent", chain(&e));
                    }
                    started => return started.map(|build| (socket, build)),
                }
            }
            self.next_agent(&cancelled, &mut waiting)?;
        }
    }

    /// Its agent did not answer: set it down and take a free one, in one
    /// step. With none free the lease, still counted, waits for one,
    /// calling `waiting` once, until `cancelled`, which runs under the
    /// pool's lock (a panic in it would poison the pool).
    /// Only a lease from `lease()` starts, so its agent is in the pool.
    fn next_agent(
        &mut self,
        cancelled: &impl Fn() -> bool,
        waiting: &mut Option<impl FnOnce()>,
    ) -> Result<()> {
        let mut agents = self.pool.lock();
        agents.down.extend(self.socket.take());
        loop {
            self.socket = agents.free.pop();
            if self.socket.is_some() {
                return Ok(());
            }
            if cancelled() {
                return Err(err_msg(CANCELLED));
            }
            if let Some(waiting) = waiting.take() {
                drop(agents);
                waiting();
                agents = self.pool.lock();
            } else {
                agents = self.pool.looked_for.wait(agents).unwrap();
            }
        }
    }
}

impl Drop for Lease {
    /// Give the slot and the agent back, then wake: a loop woken before
    /// would find neither free and wait for a wake that does not come. An
    /// agent another running build still leases stays leased.
    fn drop(&mut self) {
        let mut agents = self.pool.lock();
        agents.leases -= 1;
        if let Some(socket) = self.socket.take() {
            if let Some(i) = agents.running.iter().position(|s| *s == socket) {
                agents.running.swap_remove(i);
            }
            if agents.all.contains(&socket) && !agents.running.contains(&socket) {
                agents.free.push(socket);
            }
        }
        drop(agents);
        self.pool.wake();
    }
}

#[cfg(test)]
mod tests;
