//! The hub's usage line for a build.

use std::fs::{File, TryLockError};
use std::os::unix::fs::PermissionsExt;
use std::thread;

use super::*;
use crate::proto::{BuildResult, BuildUsage};

/// A build's usage line names the worker whose session ran it, its
/// system, and the scratch path of the output it is known by (see
/// `named_output`).
#[tokio::test]
async fn the_usage_line_names_the_worker_and_the_output_path() {
    const OUT: &str = "/nix/store/00000000000000000000000000000000-tt-usage";
    let log = Captured::default();
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let mut client = replay.subscribe().await;
    let mut j = job(0, replay);
    j.req.outputs.insert("out".to_owned(), OUT.to_owned());
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w1", 1, &[]);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let usage = Some(BuildUsage::default());
    let result = BuildResult {
        build_id,
        exit_code: 1,
        usage,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    let line = log.line("build usage");
    assert!(line.contains(r#"worker="w1""#), "{line}");
    assert!(line.contains(r#"system="x86_64-linux""#), "{line}");
    assert!(line.contains(&format!(r#"out_path="{OUT}""#)), "{line}");
}

/// What a build's run used is learned, here an OOM-killed run's peak: the
/// next build of its pname is dispatched expecting it, where the first
/// had only the prior. A failed run that was not OOM-killed teaches
/// nothing: the next build of its pname takes its prefix's, `tt`.
#[tokio::test]
async fn a_run_teaches_the_next_dispatch_of_its_pname() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = connect(&state, "w1", 1, &[]);
    for n in 0..4 {
        let (pname, oom_killed) = if n < 2 {
            ("learn", true)
        } else {
            ("fail", false)
        };
        let replay = Arc::new(Replay::default());
        let mut client = replay.subscribe().await;
        let mut j = job(n, replay);
        let out = format!("/nix/store/{H}-tt-{pname}-{n}.0");
        j.req.outputs.insert("out".to_owned(), out);
        state.queue.lock().await.push_back(j);
        w.to_hub.send(request_job(1, Some(n as u64))).await.unwrap();
        let build_id = w.assignment().await.expect("not assigned").build_id;
        let usage = BuildUsage {
            wall_ms: 2000,
            cpu_usec: 6_000_000,
            peak_memory_bytes: 5 << 30,
            oom_killed,
            ..BuildUsage::default()
        };
        let result = BuildResult {
            build_id,
            exit_code: 1,
            usage: Some(usage),
            ..Default::default()
        };
        w.send(worker_message::Msg::Result(result)).await;
        while client.recv().await.is_some() {}
    }
    let first = log.line(r#"drv="tt-learn-0.0""#);
    assert!(first.contains("learned=\"prior\""), "{first}");
    let second = log.line(r#"dispatching build id="00000000000000000000000000000001""#);
    for field in ["learned=\"key\"", "expect_mem=5368709120"] {
        assert!(second.contains(field), "{field} missing from {second}");
    }
    let failed = log.line(r#"dispatching build id="00000000000000000000000000000003""#);
    assert!(failed.contains("learned=\"prefix\""), "{failed}");
}

/// A build that ran and the usage it reported, to learn from.
fn learned_build() -> (Job, BuildUsage) {
    let mut j = job(0, Arc::new(Replay::default()));
    let out = format!("/nix/store/{H}-tt-saved-1.0");
    j.req.outputs.insert("out".to_owned(), out);
    let usage = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 3_000_000,
        peak_memory_bytes: 5 << 30,
        ..BuildUsage::default()
    };
    (j, usage)
}

/// What `state` expects of `j`: its level, and memory in bytes.
fn expected(state: &HubState, j: &Job) -> (&'static str, u64) {
    let e = state.placement.lock().unwrap().expect(j);
    (e.level.name(), placement::bytes(e.demand.memory))
}

/// What a hub learned it saves, in a directory it creates, and the next
/// hub loads, once the first has let go of the file's lock.
#[tokio::test]
async fn learning_survives_the_hubs_save_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache/tribuchet/placement-history.json");
    let state = HubState::for_test(Duration::from_secs(30));
    let file = placement::open_history(&state, path.clone()).await;
    let (j, usage) = learned_build();
    state.placement.lock().unwrap().learn(&j, 0, &usage);
    placement::save_history(&state, &file).await;
    let first = fs::read(&path).unwrap();
    state.placement.lock().unwrap().learn(&j, 0, &usage);
    placement::save_history(&state, &file).await;
    assert_ne!(fs::read(&path).unwrap(), first, "a later save not written");
    drop(file);
    let restarted = HubState::for_test(Duration::from_secs(30));
    let _file = placement::open_history(&restarted, path).await;
    assert_eq!(
        expected(&restarted, &j),
        ("key", 5 << 30),
        "after a restart"
    );
}

/// A hub that finds the lock held, by seed-history or another hub, loads
/// the file as it is and never saves.
#[tokio::test]
async fn a_hub_without_the_lock_loads_but_never_saves() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placement-history.json");
    let (first, (j, usage)) = (HubState::for_test(Duration::from_secs(30)), learned_build());
    let holder = placement::open_history(&first, path.clone()).await;
    first.placement.lock().unwrap().learn(&j, 0, &usage);
    placement::save_history(&first, &holder).await;
    let saved = fs::read(&path).unwrap();
    let state = HubState::for_test(Duration::from_secs(30));
    let file = placement::open_history(&state, path.clone()).await;
    assert_eq!(
        expected(&state, &j),
        ("key", 5 << 30),
        "the file not loaded"
    );
    state.placement.lock().unwrap().learn(&j, 0, &usage);
    placement::save_history(&state, &file).await;
    assert_eq!(fs::read(&path).unwrap(), saved, "saved without the lock");
}

/// A history file that does not load, here the previous format's, is the
/// only copy of what it holds: the hub starts empty and, at a periodic
/// save as at SIGTERM's (both save_history), never saves over it.
#[tokio::test]
async fn an_unreadable_history_is_never_saved_over() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placement-history.json");
    let old = r#"{"keys":{"x86_64-linux/foo":{"cores":1.0,"memory":2.0}}}"#;
    fs::write(&path, old).unwrap();
    let state = HubState::for_test(Duration::from_secs(30));
    let file = placement::open_history(&state, path.clone()).await;
    let (j, usage) = learned_build();
    for _ in 0..2 {
        state.placement.lock().unwrap().learn(&j, 0, &usage);
        placement::save_history(&state, &file).await;
    }
    assert_eq!(fs::read_to_string(&path).unwrap(), old, "saved over it");
}

/// A history path ending .tmp or .lock, a history file's own temporary
/// file's or lock's name, is not used: a history there is neither loaded
/// nor saved over.
#[tokio::test]
async fn a_history_named_as_a_temporary_file_or_lock_is_not_used() {
    let dir = tempfile::tempdir().unwrap();
    let (saved, (j, usage)) = (dir.path().join("h.json"), learned_build());
    let first = HubState::for_test(Duration::from_secs(30));
    let file = placement::open_history(&first, saved.clone()).await;
    first.placement.lock().unwrap().learn(&j, 0, &usage);
    placement::save_history(&first, &file).await;
    for name in ["r.json.tmp", "r.json.lock"] {
        let path = dir.path().join(name);
        fs::copy(&saved, &path).unwrap();
        let state = HubState::for_test(Duration::from_secs(30));
        let file = placement::open_history(&state, path.clone()).await;
        assert_eq!(expected(&state, &j).0, "prior", "loaded from {name}");
        state.placement.lock().unwrap().learn(&j, 0, &usage);
        placement::save_history(&state, &file).await;
        assert_eq!(
            fs::read(&path).unwrap(),
            fs::read(&saved).unwrap(),
            "saved at {name}"
        );
    }
}

/// The lock is the file's path and ".lock", the file the documented swap
/// takes with `flock -n LIVE.lock`.
#[test]
fn the_lock_is_the_files_path_and_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placement-history.json");
    let _hub = placement::lock_history(&path).unwrap().expect("free");
    let swap = File::open(dir.path().join("placement-history.json.lock")).unwrap();
    assert!(matches!(swap.try_lock(), Err(TryLockError::WouldBlock)));
}

/// A build that declares big-parallel, its key unseen, takes the prior
/// of the builds that do.
#[test]
fn a_declaring_build_takes_their_prior() {
    let state = HubState::for_test(Duration::from_secs(30));
    let (mut j, _) = learned_build();
    j.features = vec!["big-parallel".to_owned()];
    let e = state.placement.lock().unwrap().expect(&j);
    assert_eq!(e, placement::Expected::prior(true));
}

/// A lock file the hub may not write, as one a seeding left as another
/// user, still locks: it is opened to read.
#[test]
fn a_read_only_lock_file_locks() {
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("placement-history.json.lock");
    fs::write(&lock, "").unwrap();
    fs::set_permissions(&lock, fs::Permissions::from_mode(0o444)).unwrap();
    let got = placement::lock_history(&dir.path().join("placement-history.json"));
    assert!(matches!(got, Ok(Some(_))), "{got:?}");
}

/// What the hub learns it saves every SAVE_EVERY, not only as it stops.
#[tokio::test(start_paused = true)]
async fn the_history_is_saved_every_few_minutes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placement-history.json");
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let file = placement::open_history(&state, path.clone()).await;
    tokio::spawn(placement::keep_history(state.clone(), file));
    let (j, usage) = learned_build();
    state.placement.lock().unwrap().learn(&j, 0, &usage);
    tokio::time::sleep(placement::SAVE_EVERY).await;
    for _ in 0..500 {
        if path.exists() {
            break;
        }
        tokio::task::yield_now().await;
        thread::sleep(Duration::from_millis(10));
    }
    assert!(path.exists(), "not saved after SAVE_EVERY");
}
