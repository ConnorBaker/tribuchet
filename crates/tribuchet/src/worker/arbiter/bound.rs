//! How long the arbiter may keep a build frozen: at most
//! FROZEN_PER_WINDOW of any FROZEN_WINDOW, by its cgroup's own count
//! (cgroup.stat.local frozen_usec), so that a timer in the build that
//! counts wall time loses at most that much of any span to freezes: a
//! test of timeout τ that needs d of running time fails only if freezes
//! take more than τ - d, and B per window W lets them take at most
//! ceil(τ / W) B, or min(τ, B) for τ < W. A build that reaches the bound
//! is thawed, and may be frozen again once its frozen time over the last
//! FROZEN_WINDOW is under FROZEN_PER_WINDOW: so one freeze lasts at most
//! FROZEN_PER_WINDOW (and the sample and thaw after it), and a build
//! behind a leader that keeps every CPU busy runs at least two thirds of
//! the time.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// The span over which a build's frozen time is bounded.
pub(super) const FROZEN_WINDOW: Duration = Duration::from_secs(30);
/// The most frozen time a build may have in any FROZEN_WINDOW.
pub(super) const FROZEN_PER_WINDOW: Duration = Duration::from_secs(10);

/// A build's frozen count at each sample of the last FROZEN_WINDOW, and
/// the latest one at or before its start.
#[derive(Default)]
pub(super) struct FrozenTime {
    counts: VecDeque<(Instant, u64)>,
    /// Under FROZEN_PER_WINDOW at the last sample, by a count read then.
    within: bool,
}

impl FrozenTime {
    /// Record the cgroup's count at `now`, `None` if it could not be read
    /// (the build may then not be frozen). True if this sample put the
    /// build past the bound.
    pub(super) fn record(&mut self, now: Instant, us: Option<u64>) -> bool {
        let was = self.within;
        self.within = us.is_some_and(|us| {
            self.counts.push_back((now, us));
            if let Some(start) = now.checked_sub(FROZEN_WINDOW) {
                while self.counts.get(1).is_some_and(|&(t, _)| t <= start) {
                    self.counts.pop_front();
                }
            }
            self.in_window(now) < FROZEN_PER_WINDOW
        });
        was && !self.within && us.is_some()
    }

    /// Whether the build may be frozen: its frozen time over the last
    /// FROZEN_WINDOW was under FROZEN_PER_WINDOW at the last sample.
    pub(super) fn within(&self) -> bool {
        self.within
    }

    /// The build's frozen time over the last FROZEN_WINDOW, at most: what
    /// its count gained since the latest count at or before the window's
    /// start. Failing one (it was tracked less than FROZEN_WINDOW ago, as
    /// by a worker that adopted it), the window's span before the first
    /// count is taken as frozen, as far as that count allows.
    pub(super) fn in_window(&self, now: Instant) -> Duration {
        let (Some(&(t0, first)), Some(&(_, last))) = (self.counts.front(), self.counts.back())
        else {
            return Duration::ZERO;
        };
        let unseen = FROZEN_WINDOW.saturating_sub(now.saturating_duration_since(t0));
        Duration::from_micros(last.saturating_sub(first)) + unseen.min(Duration::from_micros(first))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000;

    /// Feed `counts` (µs) at 200 ms samples from `t`; whether the build
    /// is then within the bound.
    fn feed(f: &mut FrozenTime, t: &mut Instant, counts: impl IntoIterator<Item = u64>) -> bool {
        for us in counts {
            *t += Duration::from_millis(200);
            f.record(*t, Some(us));
        }
        f.within()
    }

    /// Tracked at dispatch: a freeze is thawed at 10 s, and the build may
    /// be frozen again once the freeze's start leaves the window, 30 s on.
    #[test]
    fn a_freeze_reaches_the_bound_at_10_s_and_another_waits_for_the_window() {
        let (mut f, mut t) = (FrozenTime::default(), Instant::now());
        assert!(feed(&mut f, &mut t, (0..50).map(|_| 0)));
        // Frozen from the 50th sample on: 0.2 s more at every sample.
        assert!(feed(&mut f, &mut t, (1..50).map(|i| i * S / 5)));
        assert!(!feed(&mut f, &mut t, [10 * S]));
        // Thawed at 10 s: until the 50th sample, at which the freeze
        // began, is the window's start, the window holds all 10 s.
        assert!(!feed(&mut f, &mut t, (0..100).map(|_| 10 * S)));
        assert_eq!(f.in_window(t), Duration::from_secs(10));
        assert!(feed(&mut f, &mut t, [10 * S]), "{:?}", f.in_window(t));
    }

    /// Adopted with 6 s frozen already, at a count first read now: those
    /// 6 s may lie within the window, as far as its span before the first
    /// count allows, so 4 s more reach the bound.
    #[test]
    fn a_count_from_before_tracking_is_taken_as_within_the_window() {
        let (mut f, mut t) = (FrozenTime::default(), Instant::now());
        assert!(feed(&mut f, &mut t, [6 * S]));
        assert_eq!(f.in_window(t), Duration::from_secs(6));
        assert!(feed(&mut f, &mut t, (1..20).map(|i| 6 * S + i * S / 5)));
        assert!(!feed(&mut f, &mut t, [10 * S]));
        // 20 s on, 4 s seen and 6 s of the window unseen: still 10 s.
        assert!(!feed(&mut f, &mut t, (0..100).map(|_| 10 * S)));
        assert!(feed(&mut f, &mut t, [10 * S]), "{:?}", f.in_window(t));
        // 30 s from the first count, the window is all seen: 3.8 s.
        feed(&mut f, &mut t, (0..30).map(|_| 10 * S));
        assert_eq!(f.in_window(t), Duration::from_millis(3800));
    }

    /// A count that cannot be read leaves the build outside the bound,
    /// and that is not reported as reaching it.
    #[test]
    fn an_unread_count_may_not_be_frozen() {
        let (mut f, mut t) = (FrozenTime::default(), Instant::now());
        assert!(feed(&mut f, &mut t, [0]));
        t += Duration::from_millis(200);
        assert!(!f.record(t, None));
        assert!(!f.within());
        t += Duration::from_millis(200);
        assert!(!f.record(t, Some(0)) && f.within());
        t += Duration::from_millis(200);
        assert!(f.record(t, Some(10_000_000)) && !f.within());
    }
}
