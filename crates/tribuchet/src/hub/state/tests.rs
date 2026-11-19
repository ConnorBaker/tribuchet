use std::sync::mpsc as std_mpsc;
use std::thread;

use harmonia_store_remote::{ConnectionPool, PoolConfig};
#[cfg(target_os = "linux")]
use rustix::fs::{CWD, FileType, Mode, mknodat};

use super::*;

impl HubState {
    /// No nix-daemon listens on its socket, whether or not the host
    /// runs one, and its journal is not read: tests behave the same in
    /// and out of the build sandbox, and each says what is read.
    pub(in crate::hub) fn for_test(worker_grace: Duration) -> Self {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let cache = ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap();
        let mut state = Self::new(worker_grace, None, Arc::new(cache));
        let socket = dir.path().join("no-daemon.sock");
        state.daemon_pool = ConnectionPool::new(&socket, PoolConfig::default());
        state.daemon_evidence = DaemonEvidence::Unread;
        state
    }
}

fn queued(system: &str, replay: Arc<Replay>) -> Job {
    Job {
        id: "j1".into(),
        key: "k1".into(),
        req: BuildRequest {
            system: system.into(),
            ..Default::default()
        },
        tmp_dir_pack: Arc::new(Vec::new()),
        features: vec![],
        replay,
        listing: Arc::default(),
        attempts: 0,
        resumed_losses: 0,
        queued_at: Instant::now(),
        loss: None,
    }
}

#[tokio::test]
async fn queued_job_survives_worker_restart_within_grace() {
    let mut state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    Arc::get_mut(&mut state).unwrap().started_at =
        Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
    let replay = Arc::new(Replay::default());
    let _rx = replay.subscribe().await;
    state
        .queue
        .lock()
        .await
        .push_back(queued("x86_64-linux", replay));
    state.record_departed(caps("x86_64-linux", &[]));
    state.hold_unservable().await;
    assert_eq!(state.queue.lock().await.len(), 1);
}

/// A held job whose client left is dropped, as anywhere in the queue.
#[tokio::test]
async fn held_job_is_dropped_once_its_client_leaves() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let replay = Arc::new(Replay::default());
    let rx = replay.subscribe().await;
    state
        .queue
        .lock()
        .await
        .push_back(queued("x86_64-linux", replay.clone()));
    state.hold_unservable().await;
    assert_eq!(state.queue.lock().await.len(), 1);
    drop(rx);
    state.hold_unservable().await;
    assert!(state.queue.lock().await.is_empty());
}

/// A requeued job's next attempt delivers its outputs anew: the last
/// delivery's chunks are left to the cache while it waits, held there
/// for as long as no worker serves it.
#[tokio::test]
async fn a_requeued_job_leaves_its_last_outputs_to_the_cache() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let replay = Arc::new(Replay::default());
    let _rx = replay.subscribe().await;
    let out = "/nix/store/00000000000000000000000000000000-out".to_owned();
    let pin = state.chunks.pin(Vec::new());
    let kept = KeptOutputs::new(state.chunks.clone(), vec![(out, Arc::default())], pin);
    replay.keep_outputs(kept).await;
    assert!(replay.outputs_pinned().await);
    state.requeue(queued("x86_64-linux", replay.clone())).await;
    assert_eq!(state.queue.lock().await.len(), 1, "not held");
    assert!(!replay.outputs_pinned().await, "pinned while it waits");
}

fn caps(system: &str, features: &[&str]) -> WorkerCaps {
    WorkerCaps {
        name: "w".into(),
        connected_at: Instant::now(),
        systems: [(
            system.to_owned(),
            features.iter().map(|f| (*f).to_owned()).collect(),
        )]
        .into(),
        max_jobs: 1,
    }
}

#[test]
fn startup_window_awaits_then_unseen_platform_declines() {
    // Within the startup window any platform is awaited (workers
    // have not re-registered yet); once it lapses a never-seen
    // platform with no departed worker declines at once.
    let within = HubState::for_test(Duration::from_secs(30));
    assert!(within.expected_deadline("aarch64-linux", &[]).is_some());
    let lapsed = HubState::for_test(Duration::ZERO);
    assert!(lapsed.expected_deadline("aarch64-linux", &[]).is_none());
}

#[test]
fn departed_worker_keeps_its_platform_expected() {
    // Past the startup window but inside the reconnect window.
    let mut state = HubState::for_test(Duration::from_secs(30));
    state.started_at = Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
    state.record_departed(caps("x86_64-linux", &["kvm"]));
    let kvm = vec!["kvm".to_owned()];
    let kvm_bp = vec!["kvm".to_owned(), "big-parallel".to_owned()];
    // The exact platform it served is still awaited.
    assert!(state.expected_deadline("x86_64-linux", &kvm).is_some());
    // A feature it never offered, or another system, is not.
    assert!(state.expected_deadline("x86_64-linux", &kvm_bp).is_none());
    assert!(state.expected_deadline("aarch64-linux", &[]).is_none());
}

#[test]
fn nix_config_union_systems_and_capped_oversubscribed_jobs() {
    let cfg = NixConfig {
        path: "/run/x".into(),
        attach_program: "/nix/store/attach".into(),
        oversubscribe_percent: 200,
        max_jobs_cap: 256,
    };
    let mut workers = HashMap::new();
    workers.insert(1, {
        let mut c = caps("x86_64-linux", &[]);
        c.max_jobs = 100;
        c
    });
    workers.insert(2, {
        let mut c = caps("aarch64-linux", &[]);
        c.max_jobs = 50;
        c
    });
    let out = render_nix_config(&workers, &cfg);
    assert!(
        out.contains(r#""systems":["aarch64-linux","x86_64-linux"]"#),
        "{out}"
    );
    assert!(out.contains(r#""program":"/nix/store/attach""#), "{out}");
    // (100 + 50) * 2 = 300, clamped to the cap.
    assert!(out.contains("max-jobs = 256\n"), "{out}");
}

#[test]
fn nix_config_json_escapes_peer_supplied_systems() {
    let cfg = NixConfig {
        path: "/run/x".into(),
        attach_program: r"/nix/store/at\tach".into(),
        oversubscribe_percent: 100,
        max_jobs_cap: 1,
    };
    let mut workers = HashMap::new();
    workers.insert(1, caps("x86_64-linux\u{2028}\"", &[]));
    let out = render_nix_config(&workers, &cfg);
    let json = out
        .strip_prefix("external-builders = ")
        .and_then(|s| s.lines().next())
        .unwrap();
    // Nix parses this as JSON; must be valid regardless of what a
    // worker registered or how the attach path is spelled.
    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    assert_eq!(v[0]["systems"][0], "x86_64-linux\u{2028}\"");
    assert_eq!(v[0]["program"], r"/nix/store/at\tach");
}

#[test]
fn nix_config_without_workers_omits_max_jobs() {
    let cfg = NixConfig {
        path: "/run/x".into(),
        attach_program: "/nix/store/attach".into(),
        oversubscribe_percent: 200,
        max_jobs_cap: 256,
    };
    let out = render_nix_config(&HashMap::new(), &cfg);
    assert!(out.contains(r#""systems":[]"#), "{out}");
    // No worker -> leave max-jobs to the base nix.conf default so a
    // local fallback build is not run at the offload capacity.
    assert!(!out.contains("max-jobs"), "{out}");
}

/// A nix.conf fragment write that does not finish holds neither
/// the register or deregister that asked for it nor `worker_caps`, which
/// every take_job reads. A FIFO stands for such a file: reading it waits
/// for a writer.
#[cfg(target_os = "linux")]
#[test]
fn a_stuck_nix_conf_write_holds_neither_its_caller_nor_worker_caps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nix.conf");
    mknodat(CWD, &path, FileType::Fifo, Mode::RWXU, 0).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut state = HubState::for_test(Duration::ZERO);
    state.nix_config = Some(NixConfig {
        path: path.clone(),
        attach_program: "/attach".into(),
        oversubscribe_percent: 100,
        max_jobs_cap: 1,
    });
    let state = Arc::new(state);
    let (tx, rx) = std_mpsc::channel();
    let (s, rt_handle) = (state.clone(), rt.handle().clone());
    thread::spawn(move || {
        let _rt = rt_handle.enter();
        s.regen_nix_config();
        tx.send(()).unwrap();
    });
    let returned = rx.recv_timeout(Duration::from_secs(10)).is_ok();
    let caps_free = state.worker_caps.try_lock().is_ok();
    // Let the read go: a writer opens the FIFO and closes it.
    drop(fs::OpenOptions::new().write(true).open(&path).unwrap());
    assert!(returned, "regen_nix_config waited on the fragment's file");
    assert!(
        caps_free,
        "worker_caps was held across the fragment's write"
    );
    let end = Instant::now() + Duration::from_secs(10);
    while !fs::symlink_metadata(&path).unwrap().is_file() {
        assert!(Instant::now() < end, "the fragment was never written");
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        fs::read_to_string(&path)
            .unwrap()
            .starts_with("external-builders")
    );
}

/// Renderings written out of order leave the newest.
#[test]
fn the_newest_nix_conf_rendering_is_the_one_left() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nix.conf");
    let written = StdMutex::new(0);
    write_newest(&written, 2, &path, "two");
    write_newest(&written, 1, &path, "one");
    assert_eq!(fs::read_to_string(&path).unwrap(), "two");
}

#[test]
fn worker_caps_feature_matching() {
    let caps = WorkerCaps {
        name: "w1".into(),
        connected_at: Instant::now(),
        systems: [
            ("x86_64-linux".to_owned(), ["kvm".to_owned()].into()),
            ("aarch64-linux".to_owned(), [].into()),
        ]
        .into(),
        max_jobs: 1,
    };
    assert!(caps.serves("x86_64-linux", &[]));
    assert!(caps.serves("x86_64-linux", &["kvm".into()]));
    assert!(!caps.serves("x86_64-linux", &["kvm".into(), "uid-range".into()]));
    assert!(caps.serves("aarch64-linux", &[]));
    // emulated system must not inherit the host's kvm
    assert!(!caps.serves("aarch64-linux", &["kvm".into()]));
    assert!(!caps.serves("i686-linux", &[]));
}
