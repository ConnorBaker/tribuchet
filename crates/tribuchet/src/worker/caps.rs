//! Advertised system capabilities and feature probing.

use std::env::consts;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::thread;

use super::WorkerCtx;
use crate::config::WorkerConfig;
use crate::fsutil::stat_field;
use crate::proto::{BuildAssignment, NETWORK, SystemCaps};

/// Nix's `uid-range` system feature: a full 65536-uid range with the
/// builder as in-namespace root (containers, systemd-nspawn).
pub(super) fn requires_uid_range(a: &BuildAssignment) -> bool {
    a.required_features.iter().any(|f| f == "uid-range")
}

/// System features this worker can honor, advertised to the hub for
/// scheduling. Mirrors Nix's defaults. Emulated systems get only the
/// baseline: kvm is an x86 device to an emulated guest, and uid-range
/// under emulation is untested. uid-range needs the delegated cgroup
/// that spawned agents lack. The builder opens /dev/kvm as a groupless
/// block uid, so the node must be 0666. NETWORK where a build with
/// network is not refused (`fod_isolation`), whatever its system.
fn local_features(native: bool, opts: &WorkerConfig, ctx: &WorkerCtx) -> Vec<String> {
    let mut features = vec![
        "nixos-test".to_owned(),
        "benchmark".to_owned(),
        "big-parallel".to_owned(),
    ];
    if ctx.fod_isolation {
        features.push(NETWORK.to_owned());
    }
    if cfg!(target_os = "linux") && native {
        if kvm_usable(Path::new("/dev/kvm")) {
            features.push("kvm".to_owned());
        }
        if opts.spawn_agents == 0 {
            features.push("uid-range".to_owned());
        }
    }
    features
}

fn kvm_usable(dev: &Path) -> bool {
    dev.metadata()
        .is_ok_and(|m| m.permissions().mode() & 0o006 == 0o006)
}

/// Per-system capability list for Register; native systems get the
/// full feature set, emulated ones only the baseline.
pub(super) fn system_caps(opts: &WorkerConfig, ctx: &WorkerCtx) -> Vec<SystemCaps> {
    let native = local_features(true, opts, ctx);
    let emulated = local_features(false, opts, ctx);
    opts.systems
        .iter()
        .map(|s| SystemCaps {
            system: s.clone(),
            features: if ctx.emulators.contains_key(s) {
                emulated.clone()
            } else {
                native.clone()
            },
        })
        .collect()
}

/// The agents' slice under the cgroup root, where every agent and its
/// builds run (a host may cap it).
const AGENT_SLICE: &str = "system.slice/system-tribuchet\\x2dagent.slice";

/// The CPUs this worker's builds share, for the hub's placement: the
/// worker's own, as its affinity and CPU quota allow. The agents' are
/// taken to be the same; a host's cap on the slice limits only memory.
pub(super) fn cores() -> Option<u32> {
    let n = thread::available_parallelism().ok()?;
    u32::try_from(n.get()).ok()
}

/// Bytes of memory this worker's builds may use, for the hub's
/// placement: MemTotal from `meminfo`, or less where a cgroup under
/// `cgroup` caps it: the agents' slice, or one at or above `own`, the
/// worker's cgroup (a container's limit, which its spawned agents run
/// under). None where meminfo does not read, as off Linux.
pub(super) fn build_memory(cgroup: &Path, meminfo: &Path, own: &Path) -> Option<u64> {
    let total = stat_field(&fs::read_to_string(meminfo).ok()?, "MemTotal:")?.saturating_mul(1024);
    let max = |dir: &Path| {
        let text = fs::read_to_string(dir.join("memory.max")).ok()?;
        text.trim().parse::<u64>().ok()
    };
    let own = cgroup.join(own.strip_prefix("/").unwrap_or(own));
    let above = own.ancestors().take_while(|d| d.starts_with(cgroup));
    let caps = above.filter_map(max).chain(max(&cgroup.join(AGENT_SLICE)));
    Some(caps.fold(total, u64::min))
}

/// The worker's cgroup, as /proc/self/cgroup names it (cgroup v2), else
/// the root.
pub(super) fn own_cgroup() -> PathBuf {
    let text = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let path = text.lines().find_map(|l| l.strip_prefix("0::"));
    PathBuf::from(path.unwrap_or("/"))
}

pub fn host_system() -> String {
    let arch = consts::ARCH;
    let os = match consts::OS {
        "macos" => "darwin",
        os => os,
    };
    format!("{arch}-{os}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// The slice's cap where it has one and it is below MemTotal; MemTotal
    /// where the slice is uncapped (`max`) or absent; a cap above the
    /// worker's own cgroup where it is lower still.
    #[test]
    fn build_memory_is_the_lowest_cap_or_mem_total() {
        let dir = tempfile::tempdir().unwrap();
        let meminfo = dir.path().join("meminfo");
        fs::write(&meminfo, "MemTotal:       4096 kB\nMemFree: 1 kB\n").unwrap();
        let memory = || build_memory(dir.path(), &meminfo, Path::new("/"));
        assert_eq!(memory(), Some(4 << 20), "no slice");
        let slice = dir.path().join(AGENT_SLICE);
        fs::create_dir_all(&slice).unwrap();
        for (max, want) in [
            ("max\n", 4 << 20),
            ("1048576\n", 1 << 20),
            ("8388608\n", 4 << 20),
        ] {
            fs::write(slice.join("memory.max"), max).unwrap();
            assert_eq!(memory(), Some(want), "memory.max {max:?}");
        }
        let none = dir.path().join("none");
        assert_eq!(build_memory(dir.path(), &none, Path::new("/")), None);
        // A container's limit two levels above the worker's cgroup.
        let own = Path::new("/machine.slice/c1/payload");
        fs::create_dir_all(dir.path().join("machine.slice/c1/payload")).unwrap();
        fs::write(dir.path().join("machine.slice/c1/memory.max"), "524288\n").unwrap();
        let contained = build_memory(dir.path(), &meminfo, own);
        assert_eq!(contained, Some(512 << 10), "a container's limit");
    }

    #[test]
    fn uid_range_detection() {
        let mut a = BuildAssignment::default();
        assert!(!requires_uid_range(&a));
        a.required_features = vec!["big-parallel".into(), "uid-range".into()];
        assert!(requires_uid_range(&a));
    }

    /// NETWORK is advertised for every system, emulated or not, just
    /// where a build with network is not refused (agent_exec).
    #[test]
    fn network_is_advertised_where_a_build_with_network_is_not_refused() {
        let opts: WorkerConfig =
            toml::from_str("hub = \"x\"\nsystems = [\"x86_64-linux\", \"aarch64-linux\"]").unwrap();
        let advertised = |fod_isolation| {
            let dir = tempfile::tempdir().unwrap();
            let mut ctx = WorkerCtx::for_test(dir.path(), 1, vec![]).unwrap();
            let c = Arc::get_mut(&mut ctx).unwrap();
            c.fod_isolation = fod_isolation;
            c.emulators.insert("aarch64-linux".into(), "/qemu".into());
            system_caps(&opts, &ctx)
                .iter()
                .map(|c| c.features.iter().any(|f| f == NETWORK))
                .collect::<Vec<_>>()
        };
        assert_eq!(advertised(true), [true, true]);
        assert_eq!(advertised(false), [false, false]);
    }
}
