//! hub.toml and worker.toml parsing.

use super::*;

#[test]
fn env_overrides_tls_paths() {
    let mut cfg: WorkerConfig = toml::from_str("hub = \"https://hub:7437\"").unwrap();
    // SAFETY: test-only; no other thread reads the environment here.
    unsafe { env::set_var("TRIBUCHET_KEY", "/run/credentials/w/worker-key") };
    cfg.apply_env_overrides();
    unsafe { env::remove_var("TRIBUCHET_KEY") };
    assert_eq!(cfg.key, PathBuf::from("/run/credentials/w/worker-key"));
    assert_eq!(cfg.cert, default_cert());
}

#[test]
fn worker_defaults_and_emulate_map() {
    let cfg: WorkerConfig = toml::from_str(
        r#"
        hub = "https://hub:7437"
        max-jobs = 2

        [emulate]
        aarch64-linux = "/nix/store/x-qemu/bin/qemu-aarch64"
        "#,
    )
    .unwrap();
    assert_eq!(cfg.hub, "https://hub:7437");
    assert_eq!(cfg.max_jobs, 2);
    assert_eq!(cfg.build_timeout_secs.get(), 24 * 3600);
    let err = toml::from_str::<WorkerConfig>("hub = \"x\"\nbuild-timeout-secs = 0").unwrap_err();
    assert!(err.to_string().contains("build-timeout-secs"), "{err}");
    assert_eq!(cfg.state_dir, PathBuf::from("/var/lib/tribuchet/worker"));
    assert_eq!(
        cfg.emulate.get("aarch64-linux"),
        Some(&PathBuf::from("/nix/store/x-qemu/bin/qemu-aarch64"))
    );
}

#[test]
fn sandbox_defaults_off() {
    let parse =
        |body: &str| toml::from_str::<WorkerConfig>(&format!("hub = \"x\"\n{body}")).unwrap();
    assert!(!parse("").sandbox);
    assert!(parse("sandbox = true").sandbox);
}

/// An unknown key fails the load, the removed holds, arbiter and
/// recursive-nix keys among them: none is read, nothing turns the CPU
/// arbiter off, and no worker seems to serve recursive Nix.
#[test]
fn unknown_keys_are_rejected() {
    let unknown = |set: &str| format!("unknown field `{}`", set.split(' ').next().unwrap());
    for set in [
        "max-jobs = 2",
        "cancel-grace-secs = 10",
        "clientless-hold-secs = 2002",
        "placement = {}",
        "max-job-attempts = 5",
        "max-resumed-losses = 20",
    ] {
        let err = toml::from_str::<HubConfig>(set).unwrap_err();
        assert!(err.to_string().contains(&unknown(set)), "{set}: {err}");
    }
    for set in [
        "cpu-arbiter = false",
        "cpu-arbiter-max-frozen-secs = 590",
        "recursive-nix = true",
    ] {
        let err = toml::from_str::<WorkerConfig>(&format!("hub = \"x\"\n{set}")).unwrap_err();
        assert!(err.to_string().contains(&unknown(set)), "{set}: {err}");
    }
}
