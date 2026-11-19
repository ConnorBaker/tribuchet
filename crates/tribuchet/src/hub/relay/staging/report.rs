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
    /// Log where the staging stands, once per REPORT.
    pub(in crate::hub::relay) fn report_if_due(&mut self) {
        let Some(staging_s) = self.report.due() else {
            return;
        };
        tracing::info!(
            id = self.job.id,
            worker = self.sess.worker,
            staging_s,
            computing = self.tasks.len(),
            needs_received = self.needs,
            serving = self.serving.is_some(),
            served = ?self.served,
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
