//! The attach service: request validation, dedupe keys, admission.

use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::hub::state::WorkerCaps;
use crate::hub::tests::{submit, try_submit};

/// 32-char base32 hash part for synthetic store paths.
const H: &str = "00000000000000000000000000000000";

fn base_request() -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        args: vec!["-c".into(), "true".into()],
        env: BTreeMap::default(),
        outputs: [("out".to_string(), format!("/nix/store/{H}-out"))].into(),
        input_paths: vec![format!("/nix/store/{H}-dep")],
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        ..Default::default()
    }
}

#[test]
fn request_validation() {
    assert!(validate_request(&base_request()).is_ok());

    let mut req = base_request();
    req.store_dir = "/etc".into();
    req.input_paths = vec!["/etc/shadow".into()];
    req.outputs = [("out".to_string(), "/etc/out".to_string())].into();
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.builder = "-p".into();
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.tmp_dir_in_sandbox = "relative".into();
    assert!(validate_request(&req).is_err());

    // Darwin clients send the real per-build tmp dir path.
    let mut req = base_request();
    req.tmp_dir_in_sandbox = "/private/tmp/nix-build-foo.drv-0".into();
    assert!(validate_request(&req).is_ok());

    let mut req = base_request();
    req.tmp_dir_in_sandbox = "/build/../etc".into();
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.tmp_dir_in_sandbox = format!("/nix/store/{H}-x");
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.input_paths = vec![format!("/nix/store/{H}-dep"), format!("/nix/store/{H}-dep")];
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.outputs
        .insert("doc".into(), format!("/nix/store/{H}-out"));
    assert!(validate_request(&req).is_err());

    let mut req = base_request();
    req.outputs = [("out".to_string(), format!("/nix/store/{H}-dep"))].into();
    assert!(validate_request(&req).is_err());
}

#[test]
fn dedupe_key_binds_full_request() {
    let a = dedupe_key(&base_request(), b"");
    assert_eq!(a, dedupe_key(&base_request(), b""));
    let mut req = base_request();
    req.args = vec!["-c".into(), "false".into()];
    assert_ne!(a, dedupe_key(&req, b""));
    let mut req = base_request();
    req.env.insert("X".into(), "1".into());
    assert_ne!(a, dedupe_key(&req, b""));
    assert_ne!(a, dedupe_key(&base_request(), b"other .attrs.sh"));
    let req = BuildRequest {
        timeout_secs: 600,
        max_silent_secs: 60,
        can_local: Some(true),
        client: Some("boot/1/2".into()),
        ..base_request()
    };
    assert_eq!(a, dedupe_key(&req, b""));
}

/// A hub with one x86_64-linux worker registered.
fn hub() -> Arc<HubState> {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let caps = WorkerCaps {
        name: "w".into(),
        connected_at: Instant::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 1,
    };
    state.worker_caps.lock().unwrap().insert(1, caps);
    state
}

/// A submitted build's replay carries the id the build is queued
/// with, which the replay's warnings log.
#[tokio::test]
async fn a_submitted_builds_replay_carries_its_id() {
    let state = hub();
    let _events = submit(&state, base_request()).await;
    let queue = state.queue.lock().await;
    assert_eq!(queue[0].replay.id(), queue[0].id);
}

/// The same request submitted again while its build is in flight
/// joins that build (a chain of bounded clients).
/// So does one asking other limits: the build keeps those of the
/// request that dispatched it.
#[tokio::test]
async fn a_resubmission_joins_the_build_in_flight() {
    let state = hub();
    let limited = BuildRequest {
        timeout_secs: 600,
        max_silent_secs: 60,
        ..base_request()
    };
    let _first = submit(&state, limited).await;
    let second =
        tokio::time::timeout(Duration::from_secs(10), submit(&state, base_request())).await;
    assert!(second.is_ok(), "the resubmission did not join the build");
    assert_eq!(state.queue.lock().await.len(), 1, "queued twice");
}

/// canLocal is each client's: clients whose Nix can and cannot build a
/// request share its build, and each is recorded as it said, or by
/// today's rule (no network) if unsaid; a client gone counts no more.
#[tokio::test]
async fn each_client_of_a_build_says_whether_its_nix_builds_it() {
    let state = hub();
    let asks = [Some(true), Some(false), None].map(|can_local| BuildRequest {
        can_local,
        ..base_request()
    });
    let mut clients = Vec::new();
    for req in asks {
        clients.push(tokio::time::timeout(Duration::from_secs(10), submit(&state, req)).await);
    }
    assert_eq!(state.queue.lock().await.len(), 1, "not one build");
    let replay = state.queue.lock().await[0].replay.clone();
    assert_eq!(replay.can_local(), [true, false, true]);
    drop(clients.remove(0));
    for _ in (0..1000).take_while(|_| replay.can_local().len() > 2) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(replay.can_local(), [false, true]);
}

/// With no worker for its system connected (L11), a client whose Nix
/// can build the request is declined. One whose Nix cannot (a build
/// with network, or one that says so) is never declined: refused while
/// no known worker serves it, else queued, told why it waits.
#[tokio::test]
async fn with_no_worker_only_a_client_whose_nix_builds_it_is_declined() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let first = |mut events: tonic::Streaming<AttachEvent>| async move {
        let ev = tokio::time::timeout(Duration::from_secs(10), events.message()).await;
        ev.unwrap().unwrap().unwrap().event.unwrap()
    };
    let declined = first(submit(&state, base_request()).await).await;
    assert_eq!(declined, attach_event::Event::ExitCode(DECLINE_EXIT_CODE));
    let cannot = [
        BuildRequest {
            fixed_output: true,
            ..base_request()
        },
        BuildRequest {
            can_local: Some(false),
            ..base_request()
        },
    ];
    // A hub each: both requests have one output path.
    for req in cannot {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let Err(refused) = try_submit(&state, req.clone()).await else {
            panic!("not refused");
        };
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
        assert!(refused.message().contains("x86_64-linux"), "{refused}");
        state.known.learn(&WorkerCaps {
            name: "away".into(),
            connected_at: Instant::now(),
            systems: [("x86_64-linux".to_owned(), [NETWORK.to_owned()].into())].into(),
            max_jobs: 1,
        });
        let attach_event::Event::Log(told) = first(submit(&state, req).await).await else {
            panic!("not told it waits");
        };
        let told = String::from_utf8(told).unwrap();
        assert!(told.contains("waiting for one"), "{told}");
        assert_eq!(state.queue.lock().await.len(), 1);
    }
}

/// A build with network goes only to a worker that gives it a network
/// of its own, as it advertises: one that does not, though it serves
/// the build's system, would refuse it, failing its clients.
#[tokio::test]
async fn a_build_with_network_goes_only_to_a_worker_that_gives_it_one() {
    let state = hub();
    let without = state.worker_caps.lock().unwrap()[&1].clone();
    let with = WorkerCaps {
        systems: [("x86_64-linux".to_owned(), [NETWORK.to_owned()].into())].into(),
        ..without.clone()
    };
    // Known, so the build waits for it (L11).
    state.known.learn(&with);
    let fod = BuildRequest {
        fixed_output: true,
        ..base_request()
    };
    let _events = submit(&state, fod).await;
    assert!(
        state.take_job(&without, None).await.is_none(),
        "handed to a worker without network"
    );
    assert!(
        state.take_job(&with, None).await.is_some(),
        "not handed to one with"
    );
}

/// A build that requires recursive-nix, which no worker serves,
/// though one serves its system. With network it fails at submit,
/// naming why: queued, it would wait forever, and declined, Nix
/// would build it on the hub's host. Without, it is declined, as any
/// build no worker serves, and Nix builds it locally.
#[tokio::test]
async fn a_build_requiring_recursive_nix_fails_with_network_and_is_declined_without() {
    let state = hub();
    let fod = BuildRequest {
        fixed_output: true,
        required_features: vec!["recursive-nix".into()],
        ..base_request()
    };
    let Err(failed) = try_submit(&state, fod.clone()).await else {
        panic!("the build was taken");
    };
    assert_eq!(failed.code(), tonic::Code::FailedPrecondition);
    assert!(failed.message().contains("recursive-nix"), "{failed}");
    assert_eq!(state.queue.lock().await.len(), 0);
    let local = BuildRequest {
        fixed_output: false,
        ..fod
    };
    let mut events = try_submit(&state, local).await.expect("not declined");
    let first = tokio::time::timeout(Duration::from_secs(10), events.message()).await;
    let first = first.unwrap().unwrap().unwrap().event.unwrap();
    assert_eq!(first, attach_event::Event::ExitCode(DECLINE_EXIT_CODE));
}

/// Strings shifted between adjacent sections must not collide:
/// args `["-c", "K", "V"]` with no env and args `["-c"]` with
/// env `{K: V}` would feed identical bytes without section counts.
#[test]
fn dedupe_key_separates_sections() {
    let mut a = base_request();
    a.args = vec!["-c".into(), "K".into(), "V".into()];
    a.env.clear();
    let mut b = base_request();
    b.args = vec!["-c".into()];
    b.env = [("K".to_string(), "V".to_string())].into();
    assert_ne!(dedupe_key(&a, b""), dedupe_key(&b, b""));
}
