//! The semantic core beside the hub (shadow.rs): where the two answer a
//! client differently there is a warning, and where they agree none.

use super::*;
use crate::proto::{BuildResult, DECLINE_EXIT_CODE, HeldRun, RunId, Started};

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
    assert!(log.has(&["shadow divergence", "no capable worker", "answer: Refuse"]));
}

/// A worker of the system without a feature the build requires does not
/// serve it in the core either: the core Refuses the client.
#[tokio::test]
async fn a_worker_without_a_required_feature_does_not_serve_the_build() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let _w = connect(&state, "w1", 1, &[]);
    while state.worker_caps.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    let mut req = request("x86_64-linux", Some(false));
    req.required_features = vec!["kvm".into()];
    let mut client = submit(&state, req).await;
    while client.message().await.unwrap().is_some() {}
    assert!(log.has(&["shadow divergence", "no capable worker", "answer: Refuse"]));
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

/// A dedupe key the core maps to `i`.
fn key_of(i: usize) -> String {
    format!("{i:08x}{}", "0".repeat(56))
}

/// The time a shadow call takes with `waiting` builds waiting: over 200
/// dispatches of five calls each, as probe C of the line review timed it.
async fn per_call(waiting: usize) -> Duration {
    let shadow = Arc::new(HubState::for_test(Duration::ZERO)).shadow.clone();
    let caps = vec![SystemCaps {
        system: "x86_64-linux".into(),
        features: vec![],
    }];
    let r = Register {
        worker_name: "w".into(),
        caps,
        ..Register::default()
    };
    shadow.register(&r);
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let waits: Vec<_> = (0..waiting + 200)
        .map(|i| shadow.submit(&key_of(i), sys, true))
        .collect();
    let mut jobs = Vec::new();
    for i in 0..200 {
        let mut j = job(i, Arc::new(Replay::default()));
        j.key = key_of(i);
        let client = j.replay.subscribe().await;
        jobs.push((j, client));
    }
    let start = std::time::Instant::now();
    for (i, (j, _)) in jobs.iter().enumerate() {
        let run = RunId {
            epoch: 1,
            seq: i as u64 + 1,
        };
        shadow.assigned(&run, &j.key, "w");
        shadow.need(Some(&run));
        shadow.started(Some(&run));
        shadow.result(j, Some(&run), 0);
        shadow.fetched(j, Some(&run));
    }
    let took = start.elapsed() / 1000;
    drop((waits, jobs));
    took
}

/// A shadow call costs the same however many builds wait: it frees what a
/// build had as it ends and scans nothing else. Before, each call scanned
/// every waiting build (253 µs a call with 20,000 waiting, in release).
#[tokio::test]
async fn a_shadow_call_costs_the_same_however_many_builds_wait() {
    let (few, many) = (per_call(100).await, per_call(20_000).await);
    eprintln!("a shadow call: {few:?} with 100 waiting, {many:?} with 20,000");
    assert!(
        many < few * 10,
        "a call: {few:?} with 100 waiting, {many:?} with 20,000"
    );
}

/// A divergence is logged once the shadow's lock is let go, and a warning
/// says how many runs a Register held, not which: before, each warning of
/// a key on a worker carried every run the worker held, under the lock.
#[tokio::test]
async fn a_divergence_is_logged_short_and_outside_the_shadows_lock() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let shadow = state.shadow.clone();
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let _client = shadow.submit(&key_of(0), sys, false);
    let held = (0..1000).map(|i| HeldRun {
        run: Some(RunId { epoch: 1, seq: i }),
        dedupe_key: key_of(i as usize),
        ..HeldRun::default()
    });
    let r = Register {
        worker_name: "w".into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec![],
        }],
        held: held.collect(),
        ..Register::default()
    };
    shadow.register(&r);
    shadow.register(&r);
    let busy = Arc::new(Mutex::new(Vec::new()));
    let (s, b) = (shadow.clone(), busy.clone());
    let sub = tracing_subscriber::fmt()
        .with_writer(move || {
            b.lock().unwrap().push(s.busy());
            io::sink()
        })
        .finish();
    {
        let _scoped = tracing::subscriber::set_default(sub);
        shadow.declined(&key_of(0), sys, false);
    }
    let busy = busy.lock().unwrap().clone();
    assert!(!busy.is_empty() && !busy.contains(&true), "{busy:?}");
    shadow.declined(&key_of(0), sys, false);
    let line = log.line("shadow divergence");
    assert!(line.contains("held: 1000"), "{line}");
    assert!(line.len() < 4096, "{} bytes", line.len());
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
