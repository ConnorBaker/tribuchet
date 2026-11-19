//! The period at which a build's staging, not yet started, says where it
//! stands: on the hub (hub/relay/staging/report.rs) and on the worker
//! (worker/build/report.rs) alike, so that their lines pair up.

use std::time::Duration;

use tokio::time::Instant;

/// A constant, not a setting: over the hub's journal of 2026-11-17 00:00
/// to 11-18 03:30 its lines go as one over the period, with no knee (1 min
/// 36,205, 10 min 2,226, 30 min 546), and any period to 30 min reports a
/// staging silent for hours within half an hour. At 10 min, 542 of the
/// 120,154 runs delivered and 21 of the 52 still open get a line.
pub(crate) const REPORT: Duration = Duration::from_mins(10);

/// When a staging began, and when it reports next.
pub(crate) struct Ticker {
    begun: Instant,
    next: Instant,
}

impl Default for Ticker {
    fn default() -> Self {
        let begun = Instant::now();
        Self {
            begun,
            next: begun + REPORT,
        }
    }
}

impl Ticker {
    /// The seconds since it began, once per REPORT; None in between.
    pub(crate) fn due(&mut self) -> Option<u64> {
        let now = Instant::now();
        if now < self.next {
            return None;
        }
        self.next = now + REPORT;
        Some(now.duration_since(self.begun).as_secs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing a second short of a REPORT, the seconds since it began once
    /// it is up, and nothing again a second short of the next.
    #[tokio::test(start_paused = true)]
    async fn a_ticker_is_due_once_per_report() {
        let mut t = Ticker::default();
        let second = Duration::from_secs(1);
        for k in 1..=2 {
            tokio::time::advance(REPORT - second).await;
            assert_eq!(t.due(), None, "report {k}");
            tokio::time::advance(second).await;
            assert_eq!((t.due(), t.due()), (Some(k * REPORT.as_secs()), None));
        }
    }
}
