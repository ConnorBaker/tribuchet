//! Peak of a build's summed memory, sampled while it runs.
//!
//! macOS has no cgroup, and `ru_maxrss` from `wait4` is the largest single
//! process. A build of many parallel compilers needs the sum over its
//! processes, so a thread samples the sum and keeps the maximum. The
//! result is a lower bound: a spike between two samples is missed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(super) struct PeakSampler {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<u64>,
}

impl PeakSampler {
    /// Call `sum` now and every `every` until `finish`. It returns the
    /// current total in bytes.
    pub(super) fn start(every: Duration, sum: impl Fn() -> u64 + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::spawn(move || {
            let mut peak = sum();
            while !flag.load(Ordering::Acquire) {
                thread::sleep(every);
                peak = peak.max(sum());
            }
            peak
        });
        Self { stop, thread }
    }

    /// Stop sampling and return the largest total seen, or 0 if the thread
    /// panicked.
    pub(super) fn finish(self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.thread.join().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn keeps_the_largest_sum_not_the_last() {
        let n = Arc::new(AtomicU64::new(0));
        let seen = n.clone();
        // 10, 50, 30, 30, ...: the peak is the middle sample.
        let s = PeakSampler::start(Duration::from_millis(5), move || {
            [10, 50, 30][seen.fetch_add(1, Ordering::Relaxed).min(2) as usize]
        });
        while n.load(Ordering::Relaxed) < 4 {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(s.finish(), 50);
    }

    #[test]
    fn samples_once_even_when_stopped_at_once() {
        let s = PeakSampler::start(Duration::from_mins(1), || 7);
        assert_eq!(s.finish(), 7);
    }
}
