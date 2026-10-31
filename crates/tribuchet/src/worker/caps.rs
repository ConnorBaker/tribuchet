//! Advertised system capabilities and feature probing.

use std::env::consts;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use super::WorkerCtx;
use crate::config::WorkerConfig;
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
