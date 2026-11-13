//! The semantic core beside the hub (shadow.rs): where the two answer a
//! client differently there is a warning, and where they agree none.

use super::*;
use crate::proto::{BuildResult, DECLINE_EXIT_CODE, Started};

fn request(system: &str, can_local: Option<bool>) -> BuildRequest {
    BuildRequest {
        system: system.into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        outputs: [("out".to_owned(), format!("/nix/store/{H}-tt-shadow"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        can_local,
        ..Default::default()
    }
}

/// With no worker for its system the hub declines a client, and the core
/// Defers it too if its Nix can build the key; if not, the core Refuses it.
#[tokio::test]
async fn declining_a_client_its_nix_cannot_build_is_a_divergence() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    for can_local in [true, false] {
        let mut client = submit(&state, request("aarch64-linux", Some(can_local))).await;
        while client.message().await.unwrap().is_some() {}
        assert_eq!(log.count("shadow divergence"), usize::from(!can_local));
    }
    assert!(log.has(&["shadow divergence", "no capable worker", "Refuse"]));
}

/// A run's own outcome answers its clients, in the core as in the hub.
#[tokio::test]
async fn a_build_run_to_its_outcome_is_no_divergence() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = connect(&state, "w1", 1, &[]);
    let mut client = submit(&state, request("x86_64-linux", None)).await;
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let BuildAssignment { build_id, run, .. } = w.assignment().await.expect("not assigned");
    let (id, ran) = (build_id.clone(), run.clone());
    w.send(worker_message::Msg::Started(Started { build_id, run }))
        .await;
    let result = BuildResult {
        build_id: id,
        exit_code: 1,
        run: ran,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.message().await.unwrap().is_some() {}
    for bad in ["shadow divergence", "shadow unmappable"] {
        assert_eq!(log.count(bad), 0, "{}", log.line(bad));
    }
}

/// A panic in the core turns the shadow off; the hub answers on.
#[tokio::test]
async fn a_panic_in_the_core_turns_the_shadow_off() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    state.shadow.with(|_| panic!("planted"));
    let mut client = submit(&state, request("aarch64-linux", Some(false))).await;
    let declined = client.message().await.unwrap().and_then(|e| e.event);
    assert_eq!(
        declined,
        Some(attach_event::Event::ExitCode(DECLINE_EXIT_CODE))
    );
    assert!(log.has(&["the semantic core panicked"]));
    assert!(!log.has(&["shadow divergence"]));
}
