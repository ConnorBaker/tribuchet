//! How a build's parallelism and memory changed over its run, as the
//! arbiter samples them: the record keeps only totals (CPU time over
//! running time, memory.peak), and a build serial for 40 minutes that
//! then runs 64 threads for 5 has the totals of a 6-core build.
//!
//! Each sample is the time since the build's previous sample, measured:
//! its running time (the clock's advance less the cgroup's own frozen
//! time over it), the CPU time the cgroup used over it (cpu.stat), and,
//! where the build was not frozen, its runnable threads (demand) and
//! memory.current. So the points' running times sum to the time the
//! build ran while tracked and their CPU times to its cgroup's count,
//! however late a sample comes or however long a freeze took to reach.
//!
//! At most POINTS points, each of the same number of consecutive
//! samples, oldest first, the last possibly short: when a sample would
//! make one too many, neighbours are merged in pairs and each point
//! covers twice as many. A point sums its running and CPU time, keeps
//! the mean of its runnable counts and the largest memory.current. So a
//! profile is a few hundred bytes whatever the run's length.

use crate::proto::BuildProfile;

/// Points kept: a run's 32 equal parts and more.
const POINTS: usize = 64;

/// One sample, or several merged.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) struct Sample {
    /// Running time covered, µs.
    pub(super) running: u64,
    /// CPU time used over it, µs.
    pub(super) cpu: u64,
    /// Runnable threads summed over the samples taken unfrozen, and their
    /// number.
    pub(super) runnable: u64,
    pub(super) unfrozen: u32,
    /// memory.current, largest as sampled.
    pub(super) memory: u64,
    samples: u32,
}

impl Sample {
    /// A sample: `runnable` None where the build was frozen.
    pub(super) fn new(running: u64, cpu: u64, runnable: Option<u32>, memory: u64) -> Self {
        Self {
            running,
            cpu,
            runnable: u64::from(runnable.unwrap_or(0)),
            unfrozen: u32::from(runnable.is_some()),
            memory,
            samples: 1,
        }
    }

    fn merge(self, b: Self) -> Self {
        Self {
            running: self.running + b.running,
            cpu: self.cpu + b.cpu,
            runnable: self.runnable + b.runnable,
            unfrozen: self.unfrozen + b.unfrozen,
            memory: self.memory.max(b.memory),
            samples: self.samples + b.samples,
        }
    }
}

#[derive(Debug)]
pub(super) struct Profile {
    points: Vec<Sample>,
    /// Samples a point covers.
    per: u32,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            points: Vec::new(),
            per: 1,
        }
    }
}

impl Profile {
    pub(super) fn push(&mut self, s: Sample) {
        if let Some(last) = self.points.last_mut()
            && last.samples < self.per
        {
            *last = last.merge(s);
            return;
        }
        if self.points.len() == POINTS {
            self.points = (self.points.chunks(2))
                .map(|p| p.iter().copied().fold(Sample::default(), Sample::merge))
                .collect();
            self.per *= 2;
            return self.push(s);
        }
        self.points.push(s);
    }

    /// As the usage report carries it; None with no sample.
    pub(super) fn report(&self) -> Option<BuildProfile> {
        if self.points.is_empty() {
            return None;
        }
        let milli = |p: &Sample| {
            u32::try_from(p.runnable * 1000 / u64::from(p.unfrozen.max(1))).unwrap_or(u32::MAX)
        };
        let ps = &self.points;
        Some(BuildProfile {
            running_usec: ps.iter().map(|p| p.running).collect(),
            cpu_usec: ps.iter().map(|p| p.cpu).collect(),
            runnable_milli: ps.iter().map(milli).collect(),
            memory_bytes: ps.iter().map(|p| p.memory).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(running: u64, runnable: u32, memory: u64) -> Sample {
        Sample::new(
            running,
            running * u64::from(runnable),
            Some(runnable),
            memory,
        )
    }

    /// A run of 64 samples is 64 points; the 65th halves them: 33 points
    /// of 2 samples, the last of 1, each the mean of its runnable counts,
    /// the largest memory and the sums of its running and CPU time.
    #[test]
    fn a_long_run_is_halved_keeping_sums_means_and_peaks() {
        let mut p = Profile::default();
        for i in 0..64u32 {
            p.push(sample(200_000 + u64::from(i), i, u64::from(100 - i)));
        }
        let r = p.report().unwrap();
        assert_eq!(r.runnable_milli.len(), 64);
        assert_eq!(r.runnable_milli[63], 63_000);
        p.push(sample(1, 64, 1));
        let r = p.report().unwrap();
        assert_eq!(r.runnable_milli.len(), 33);
        assert_eq!(&r.runnable_milli[..2], &[500, 2500]);
        assert_eq!(&r.memory_bytes[..2], &[100, 98]);
        assert_eq!(&r.running_usec[..2], &[400_001, 400_005]);
        assert_eq!(r.cpu_usec[1], 200_002 * 2 + 200_003 * 3);
        assert_eq!((r.runnable_milli[32], r.memory_bytes[32]), (64_000, 1));
        let total: u64 = (0..64).map(|i| 200_000 + i).sum::<u64>() + 1;
        assert_eq!(r.running_usec.iter().sum::<u64>(), total);
    }

    /// The serial-then-burst build of the module doc: 40 minutes at 1
    /// thread, then 5 at 64, in 200 ms samples. Its mean is about 8, its
    /// last points 64: the profile tells them apart, the totals do not.
    /// A frozen sample counts its time and CPU but no demand.
    #[test]
    fn a_late_burst_survives_any_length() {
        let mut p = Profile::default();
        for _ in 0..(40 * 300) {
            p.push(sample(200_000, 1, 1 << 30));
        }
        for _ in 0..(5 * 300) {
            p.push(sample(200_000, 64, 8 << 30));
        }
        p.push(Sample::new(7, 3, None, 0));
        let r = p.report().unwrap();
        assert!((32..=POINTS).contains(&r.runnable_milli.len()), "{r:?}");
        assert_eq!(r.runnable_milli.first(), Some(&1000));
        assert_eq!(r.runnable_milli.last(), Some(&64_000));
        assert_eq!(r.memory_bytes.last(), Some(&(8 << 30)));
        let running: u64 = r.running_usec.iter().sum();
        assert_eq!(running, 45 * 60 * 1_000_000 + 7);
        let cpu: u64 = r.cpu_usec.iter().sum();
        assert_eq!(cpu, (40 * 300 + 5 * 300 * 64) * 200_000 + 3);
        assert_eq!(Profile::default().report(), None);
    }
}
