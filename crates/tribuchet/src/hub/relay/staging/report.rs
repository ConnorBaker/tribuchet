//! A build not started yet says where its staging stands, once per
//! crate::report::REPORT: what the worker has asked, and where the Need
//! being served waits (served's permit_us and send_us; the rest of its
//! serve_us is the hub's work and its wait for frames), so that a Need
//! never answered shows apart from a slow link. Counts, not a line per
//! Need: those flooded journald (serving_tests). "Not started": the hub
//! does not know whether the worker staged it and waits for a slot.

use std::mem;

use super::Staging;
use crate::proto::RunId;

impl Staging<'_> {
    /// Log where the staging stands, once per REPORT, and what worker
    /// session `session` has in build agents (HubState::agent_room): of one
    /// build's wait only its worker can tell (agent_wait= in its line).
    pub(in crate::hub::relay) fn report_if_due(&mut self, session: u64) {
        let Some(staging_s) = self.report.due() else {
            return;
        };
        let room = self.state.agent_room(session);
        tracing::info!(
            id = self.job.id,
            worker = self.sess.worker,
            staging_s,
            computing = self.tasks.len(),
            needs_received = self.needs,
            serving = self.serving.is_some(),
            served = ?self.served,
            slots = ?room.map(|r| r.0),
            idle = ?room.map(|r| r.1),
            worker_agents = room.map_or("unknown", worker_agents),
            "build not started"
        );
    }

    /// Log what its Needs were served, once: when the worker starts or
    /// resumes the build, with the run it names, or as the staging ends.
    /// Staging is over then, but the worker may still wait for a build
    /// agent, and a reuse, a refused network or a setup error starts too.
    pub(in crate::hub::relay) fn summarize(&mut self, run: Option<&RunId>) {
        if mem::replace(&mut self.summarized, true) {
            return;
        }
        let (id, worker) = (&self.job.id, &self.sess.worker);
        let run = run.map(ToString::to_string);
        tracing::info!(id, worker, run, served = ?self.served, "needs served");
    }
}

/// What a worker of `slots` with `idle` free has in build agents:
/// "none_reachable" with no slot, "none_free" with every one leased or
/// staged for, else "some_free".
fn worker_agents((slots, idle): (usize, usize)) -> &'static str {
    match (slots, idle) {
        (0, _) => "none_reachable",
        (_, 0) => "none_free",
        _ => "some_free",
    }
}
