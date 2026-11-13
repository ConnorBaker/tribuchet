//! Run ids: each dispatch of a build is a run of its own, numbered so
//! that no two runs share an id, across hub restarts too: a report about
//! a run an earlier hub process assigned must never be read as one about
//! a run of this one (SEMANTICS-7, corrections: mutant f16).

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::fsutil::write_atomic;
use crate::proto::RunId;
use crate::worker::unix_now;

#[derive(Default)]
pub(super) struct Runs {
    epoch: u64,
    seq: AtomicU64,
}

impl Runs {
    /// A hub process's runs: its epoch is past the one stored at `path`
    /// by the hub before, and past the clock's seconds, should that file
    /// be lost; it is stored there in turn.
    pub(super) fn open(path: &Path) -> Self {
        let stored = fs::read_to_string(path).ok();
        let stored = stored.and_then(|s| s.trim().parse::<u64>().ok());
        let epoch = stored.unwrap_or(0).saturating_add(1).max(unix_now());
        // A failure here fails the write, which says so.
        let _ = path.parent().map(fs::create_dir_all);
        if let Err(e) = write_atomic(path, &epoch.to_string()) {
            tracing::error!(path = %path.display(), "storing the run epoch: {e}");
        }
        tracing::info!(epoch, "numbering runs");
        Self {
            epoch,
            seq: AtomicU64::default(),
        }
    }

    /// A run id none had before.
    pub(super) fn next(&self) -> RunId {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        RunId {
            epoch: self.epoch,
            seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hub numbers its runs after every run a hub before it numbered,
    /// whatever the clock says, and each of its runs anew.
    #[test]
    fn a_restarted_hub_numbers_runs_after_every_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run-epoch");
        let ahead = unix_now() + 1_000_000;
        fs::write(&path, ahead.to_string()).unwrap();
        let first = Runs::open(&path);
        let (a, b) = (first.next(), first.next());
        assert_eq!((a.epoch, a.seq, b.seq), (ahead + 1, 1, 2));
        let again = Runs::open(&path).next();
        assert_eq!((again.epoch, again.seq), (ahead + 2, 1));
    }

    /// A hub whose epoch file is lost numbers its runs past the clock's
    /// seconds, as every hub before it did at its start: not from 1 again.
    #[test]
    fn a_hub_whose_epoch_file_is_lost_numbers_runs_past_the_clock() {
        let dir = tempfile::tempdir().unwrap();
        let before = unix_now();
        let run = Runs::open(&dir.path().join("run-epoch")).next();
        assert!(run.epoch >= before, "epoch {} before {before}", run.epoch);
    }
}
