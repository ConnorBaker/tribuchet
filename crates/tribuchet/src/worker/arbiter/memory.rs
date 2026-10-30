//! A frozen build frees no memory and holds what it has for longer: if
//! the builds still running grow into the room its memory cap leaves
//! (the agents' slice), the kernel OOM-kills one of them, or a frozen
//! build, which it would not have had to with the builds running on and
//! letting go. So the frozen builds together may pin no more than every
//! capped cgroup above them has headroom for, nor more than the host has
//! available:
//!
//! - headroom = memory.max - memory.current + inactive_file + active_file:
//!   page cache is reclaimed before any OOM kill, frozen or not.
//! - what a build pins = its memory.current less its inactive_file and
//!   active_file: anon, shmem, swap cache and the kernel memory charged to
//!   it. Under zswap with writeback off, pages zswap refuses (those that
//!   do not compress) leave anon for the swap cache and stay in RAM, which
//!   anon alone misses.
//! - the host: MemAvailable, whether or not a cgroup above is capped, so
//!   that pressure outside the slice counts too.
//!
//! Swap is no room: a page zswap keeps stays charged to the build,
//! compressed (mm/memcontrol.c, obj_cgroup_charge_zswap), and the
//! module's slice keeps pages from the swap device (nix/agent-memory.nix).
//!
//! The pins are summed (`Budget`): the builds frozen already, oldest
//! first, then the freezes a sample asks for, likewise; one that does
//! not fit is thawed, or not frozen: two followers that each fit alone
//! can make the kernel OOM-kill the leader when frozen together.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::fsutil::stat_field;

/// Where the test reads: the cgroup tree and the host's meminfo; a tree
/// of a test's own in tests.
pub(super) struct Roots {
    pub(super) cgroup: PathBuf,
    pub(super) meminfo: PathBuf,
}

impl Roots {
    pub(super) fn system() -> Self {
        Self {
            cgroup: "/sys/fs/cgroup".into(),
            meminfo: "/proc/meminfo".into(),
        }
    }

    /// The same files under `dir`, as a test writes them.
    #[cfg(test)]
    pub(super) fn under(dir: &Path) -> Self {
        Self {
            cgroup: dir.to_path_buf(),
            meminfo: dir.join("meminfo"),
        }
    }
}

/// The host's MemAvailable, in bytes, read once per sample.
pub(super) fn available(roots: &Roots) -> Option<u64> {
    let kib = stat_field(&fs::read_to_string(&roots.meminfo).ok()?, "MemAvailable:")?;
    Some(kib.saturating_mul(1024))
}

/// What a build pins, and the headroom of each capped cgroup above it.
pub(super) struct Held {
    pins: u64,
    caps: Vec<(PathBuf, u64)>,
}

/// What the build in cgroup `dir` pins; None when its cgroup has gone
/// (with its builder: it holds nothing). One whose memory.current cannot
/// be read, as with no memory controller, or that of a capped cgroup
/// above it, fits no budget: it may not be frozen, since what it would
/// pin, or the room for it, is unknown.
pub(super) fn held(dir: &Path, roots: &Roots) -> Option<Held> {
    let unknown = Held {
        pins: u64::MAX,
        caps: Vec::new(),
    };
    let build = Cgroup::read(dir);
    let Some(current) = build.current else {
        return dir.exists().then_some(unknown);
    };
    let above: Vec<Cgroup> = dir
        .ancestors()
        .skip(1)
        .take_while(|d| *d != roots.cgroup && d.starts_with(&roots.cgroup))
        .map(Cgroup::read)
        .collect();
    if above.iter().any(|c| c.max.is_some() && c.current.is_none()) {
        return Some(unknown);
    }
    Some(Held {
        pins: current.saturating_sub(build.file),
        caps: above
            .iter()
            .filter_map(|c| Some((c.dir.clone(), c.headroom()?)))
            .collect(),
    })
}

/// What the frozen builds may still pin: each capped cgroup's headroom
/// and the host's MemAvailable, less the pins counted against them.
pub(super) struct Budget {
    host: Option<u64>,
    caps: HashMap<PathBuf, u64>,
}

impl Budget {
    pub(super) fn new(available: Option<u64>) -> Self {
        Self {
            host: available,
            caps: HashMap::new(),
        }
    }

    /// Count a build's pins against every budget it falls under, if they
    /// fit them all; whether they did. A build whose memory cannot be
    /// read (gone) fits and counts nothing.
    pub(super) fn take(&mut self, held: Option<&Held>) -> bool {
        let Some(h) = held else {
            return true;
        };
        let left = |(dir, room): &(PathBuf, u64)| self.caps.get(dir).copied().unwrap_or(*room);
        let fits =
            self.host.is_some_and(|a| a >= h.pins) && h.caps.iter().all(|c| left(c) >= h.pins);
        if fits {
            self.host = self.host.map(|a| a - h.pins);
            for (dir, headroom) in &h.caps {
                *self.caps.entry(dir.clone()).or_insert(*headroom) -= h.pins;
            }
        }
        fits
    }
}

/// What the test reads of a cgroup.
struct Cgroup {
    dir: PathBuf,
    /// memory.max, None if it reads `max`.
    max: Option<u64>,
    current: Option<u64>,
    /// inactive_file + active_file: page cache on the reclaim lists.
    file: u64,
}

impl Cgroup {
    fn read(dir: &Path) -> Self {
        let num =
            |f: &str| -> Option<u64> { fs::read_to_string(dir.join(f)).ok()?.trim().parse().ok() };
        let stat = fs::read_to_string(dir.join("memory.stat")).unwrap_or_default();
        let field = |key| stat_field(&stat, key).unwrap_or(0);
        Self {
            dir: dir.to_path_buf(),
            max: num("memory.max"),
            current: num("memory.current"),
            file: field("inactive_file").saturating_add(field("active_file")),
        }
    }

    /// What it can still take before an OOM kill, if it is capped.
    fn headroom(&self) -> Option<u64> {
        Some(
            self.max?
                .saturating_sub(self.current?)
                .saturating_add(self.file),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    /// A file absent.
    const NO: u64 = u64::MAX;
    /// memory.max reading `max`.
    const MAX: u64 = u64::MAX - 1;
    /// More than any build here pins: 100 GiB.
    const ALL: u64 = 102_400;

    /// In MiB: a slice's memory.max, memory.current and page cache; its
    /// builds (under an uncapped cgroup), each memory.current and page
    /// cache (NO, NO for one gone); MemAvailable; whether each build's
    /// pins fit in turn, against one budget.
    type Row = (u64, u64, u64, &'static [(u64, u64)], u64, &'static [bool]);
    const ROWS: &[Row] = &[
        // Page cache is headroom: 1 MiB free, 1.55 GiB of it, not none.
        (2132, 2131, 1587, &[(520, 0)], ALL, &[true]),
        (2132, 2131, 0, &[(520, 0)], ALL, &[false]),
        // A build pins its memory.current less its page cache, swap cache
        // included (0.653 GiB, 0.277 of it anon), against 0.4 GiB of
        // headroom.
        (2048, 1638, 0, &[(653, 0)], ALL, &[false]),
        (2048, 1638, 0, &[(653, 500)], ALL, &[true]),
        // Pins are summed: two of 0.568 GiB fit 0.621 GiB alone, not
        // together; one gone counts 0.
        (2048, 1412, 0, &[(582, 0), (582, 0)], ALL, &[true, false]),
        (2048, 1412, 0, &[(100, 0), (500, 0)], ALL, &[true, true]),
        (2048, 1412, 0, &[(NO, NO)], ALL, &[true]),
        // A capped cgroup above whose memory.current cannot be read leaves
        // no room; one uncapped and unread (no memory controller) no cap;
        // a build there but unread fits nothing.
        (8192, NO, 0, &[(1, 0)], ALL, &[false]),
        (NO, NO, 0, &[(1, 0)], ALL, &[true]),
        (NO, NO, 0, &[(NO, 0)], ALL, &[false]),
        // MemAvailable bounds the pins, capped or not; unread, none fit.
        (8192, 1024, 0, &[(1024, 0)], 2048, &[true]),
        (8192, 1024, 0, &[(1024, 0)], 512, &[false]),
        (MAX, 51_200, 0, &[(1536, 0)], 2048, &[true]),
        (MAX, 51_200, 0, &[(1536, 0)], 1024, &[false]),
        (MAX, 51_200, 0, &[(1536, 0)], NO, &[false]),
        (
            MAX,
            51_200,
            0,
            &[(1536, 0), (1536, 0)],
            2048,
            &[true, false],
        ),
    ];

    /// The files of a cgroup in `dir`, its page cache over both lists, the
    /// rest anon.
    fn cgroup(dir: &Path, max: u64, current: u64, file: u64) {
        fs::create_dir_all(dir).unwrap();
        for (f, n) in [("memory.max", max), ("memory.current", current)] {
            let text = match n {
                NO => continue,
                MAX => "max".into(),
                n => (n * MIB).to_string(),
            };
            fs::write(dir.join(f), text + "\n").unwrap();
        }
        let (inactive, active) = ((file - file / 2) * MIB, file / 2 * MIB);
        let anon = (current - file).checked_mul(MIB).unwrap_or(0);
        let stat = format!("anon {anon}\ninactive_file {inactive}\nactive_file {active}\n");
        fs::write(dir.join("memory.stat"), stat).unwrap();
    }

    /// Each row with 4 GiB of swap free: swap is no room (a rule that
    /// credits it to anon fails rows).
    #[test]
    fn every_tree_fits_as_the_table_says() {
        for (row, &(max, current, file, builds, available_mib, want)) in ROWS.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let roots = Roots::under(dir.path());
            let agent = dir.path().join("slice/agent");
            cgroup(&dir.path().join("slice"), max, current, file);
            cgroup(&agent, MAX, NO, 0);
            if available_mib != NO {
                let kib = available_mib << 10;
                let meminfo = format!("MemAvailable: {kib} kB\nSwapFree: 4194304 kB\n");
                fs::write(&roots.meminfo, meminfo).unwrap();
            }
            let mut budget = Budget::new(available(&roots));
            let got: Vec<bool> = (builds.iter().enumerate())
                .map(|(i, &(current, file))| {
                    let d = agent.join(format!("build-{i}"));
                    if file != NO {
                        cgroup(&d, NO, current, file);
                    }
                    budget.take(held(&d, &roots).as_ref())
                })
                .collect();
            assert_eq!(got, want, "row {row}");
        }
    }

    /// What the rows' files leave out, as the kernel writes them: page
    /// cache on either list is headroom, a cgroup past its memory.max has
    /// no other, and a build pins the rest of memory.current, its swap
    /// cache too (anon alone missed it: the module's doc).
    #[test]
    fn the_cgroup_files_are_read_where_they_are() {
        let dir = tempfile::tempdir().unwrap();
        let b = dir.path().join("build");
        fs::create_dir(&b).unwrap();
        let stat = "anon 42\nshmem 5\nswapcached 2\ninactive_file 3\nactive_file 4\n";
        fs::write(b.join("memory.stat"), stat).unwrap();
        fs::write(b.join("memory.max"), "100\n").unwrap();
        fs::write(b.join("memory.current"), "130\n").unwrap();
        let c = Cgroup::read(&b);
        assert_eq!((c.file, c.headroom()), (7, Some(7)));
        let pins = held(&b, &Roots::under(dir.path())).map(|h| h.pins);
        assert_eq!(pins, Some(123), "memory.current less page cache");
    }
}
