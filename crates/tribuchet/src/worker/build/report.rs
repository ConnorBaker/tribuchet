//! A build not started yet says where it stands, once per
//! crate::report::REPORT, as the hub's does (hub/relay/staging/report.rs):
//! what the worker still waits on. Staged, it waits for a slot.

use super::ActiveBuild;

impl ActiveBuild {
    /// Log where the build stands, once per REPORT until it starts.
    pub(in crate::worker) fn report_if_due(&mut self) {
        let Some(staging_s) = self.report.due() else {
            return;
        };
        tracing::info!(
            id = self.assignment.build_id,
            staging_s,
            negotiated = self.daemon.is_some(),
            tmp_dir_done = self.tmp_dir_done,
            pending = self.pending.len(),
            parked = self.parked.len(),
            importing = self.imports.len(),
            waits = self.waits.len(),
            needs_outstanding = self.needs_outstanding,
            "build not started"
        );
    }
}
