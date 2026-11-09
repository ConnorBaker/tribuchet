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

pub(super) fn queued(system: &str, replay: Arc<Replay>) -> Job {
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
        deferred: None,
        expected: Expected::prior(false),
    }
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
    let _rt = rt.enter();
    let mut state = HubState::for_test(Duration::ZERO);
    state.nix_config = Some(HubState::nix_config_writer(NixConfig {
        path: path.clone(),
        attach_program: "/attach".into(),
        oversubscribe_percent: 100,
        max_jobs_cap: 1,
    }));
    let state = Arc::new(state);
    let (tx, rx) = std_mpsc::channel();
    let s = state.clone();
    thread::spawn(move || {
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
