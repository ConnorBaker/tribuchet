//! The semantic core beside the hub (shadow.rs): where the two answer a
//! client differently there is a warning, and where they agree none.

use super::*;
use crate::proto::{BuildResult, ChunkFrame, DECLINE_EXIT_CODE, HeldRun, Manifest, RunId, Started};
use state::FLOOR;

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

/// With no worker ever known for its system (L11), the hub declines a client
/// whose Nix can build the key and refuses one whose Nix cannot, as the
/// core Defers the one and Refuses the other.
#[tokio::test]
async fn no_known_worker_defers_or_refuses_each_client_as_the_core_does() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let mut client = submit(&state, request("aarch64-linux", Some(true))).await;
    while client.message().await.unwrap().is_some() {}
    let refused = try_submit(&state, request("aarch64-linux", Some(false))).await;
    assert_eq!(
        refused.err().map(|s| s.code()),
        Some(tonic::Code::FailedPrecondition)
    );
    assert!(log.has(&["no known worker; refusing"]));
    assert_eq!(log.count("shadow divergence"), 0);
}

/// The hub refusing a key while a client whose Nix builds it waits gives
/// that client an answer §2 forbids (it is deferred first): the core takes
/// no such Refuse, so the shadow warns.
#[tokio::test]
async fn a_refusal_while_a_client_could_build_it_diverges() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let sys: (&str, &[String]) = ("aarch64-linux", &[]);
    let _local = state.shadow.submit(&key_of(0), sys, true);
    state.shadow.declined(&key_of(0), sys, false);
    assert_eq!(log.count("shadow divergence"), 1);
}

/// The hub's verdict (L12), before dispatch and after: it declines the
/// client whose Nix builds the key and refuses the other, as the core Defers
/// and then takes Cannot. Before, the core was fed Give Failed and diverged.
#[tokio::test]
async fn the_hubs_verdict_is_the_cores_cannot() {
    let log = Captured;
    let _guard = log.install();
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let caps = vec![SystemCaps {
        system: "x86_64-linux".into(),
        features: vec![],
    }];
    for run in [None, Some(RunId { epoch: 1, seq: 1 })] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let worker_name = "w".to_owned();
        let caps = caps.clone();
        state.shadow.register(&Register {
            worker_name,
            caps,
            ..Register::default()
        });
        let mut j = job(0, Arc::new(Replay::default()));
        j.key = key_of(0);
        let mut clients = Vec::new();
        for can_local in [true, false] {
            let c = state.shadow.submit(&j.key, sys, can_local);
            clients.push(j.replay.subscribe_as(c).await);
        }
        if let Some(r) = &run {
            state.shadow.assigned(r, &j.key, "w");
            state.shadow.started(Some(r));
        }
        losses::hub_cannot(&state, j, "hub-side error", "no".into()).await;
        let bad = "shadow divergence";
        assert_eq!(log.count(bad), 0, "{run:?}: {}", log.line(bad));
    }
}

/// The hub's verdict while a client whose Nix builds the key still waits
/// gives it an answer §2 forbids: the core takes no such Cannot.
#[tokio::test]
async fn a_verdict_while_a_client_could_build_it_diverges() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let mut j = job(0, Arc::new(Replay::default()));
    j.key = key_of(0);
    let c = state.shadow.submit(&j.key, ("aarch64-linux", &[]), true);
    let _client = j.replay.subscribe_as(c).await;
    state.shadow.cannot(&j, "hub-side error");
    assert_eq!(log.count("shadow divergence"), 1);
}

/// A worker of the system without a feature the build requires does not
/// serve it in the core either: the hub refuses the client, and the core
/// agrees (it takes a Refuse only while no worker serves the key).
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
    assert!(try_submit(&state, req).await.is_err(), "not refused");
    assert!(log.has(&["no known worker; refusing"]));
    assert_eq!(log.count("shadow divergence"), 0);
}

/// A worker that registered and left is still known (L11): a client whose
/// Nix cannot build its system waits for it, told why, and is not refused.
#[tokio::test]
async fn a_client_whose_nix_cannot_build_waits_for_a_worker_gone() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let w = connect(&state, "w1", 1, &[]);
    while state.worker_caps.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    drop(w);
    while !state.worker_caps.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    let mut client = submit(&state, request("x86_64-linux", Some(false))).await;
    let told = client.message().await.unwrap().and_then(|e| e.event);
    let Some(attach_event::Event::Log(told)) = told else {
        panic!("not told it waits: {told:?}");
    };
    assert!(String::from_utf8_lossy(&told).contains("waiting for one"));
    assert!(!log.has(&["no known worker; refusing"]));
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

/// A run its worker lost (its Register or heartbeat says so) ended with no
/// known cause, though the hub never heard it start, having heard its Need
/// or nothing at all: the core runs its key alone (D4). Before, the hub
/// read neither list, and such a key was never isolated.
#[tokio::test]
async fn a_run_its_worker_lost_isolates_its_key() {
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let r = Register {
        worker_name: "w".into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec![],
        }],
        ..Register::default()
    };
    for need in [true, false] {
        let shadow = Arc::new(HubState::for_test(Duration::ZERO)).shadow.clone();
        let _client = shadow.submit(&key_of(0), sys, false);
        shadow.register(&r);
        let run = RunId { epoch: 1, seq: 1 };
        shadow.assigned(&run, &key_of(0), "w");
        if need {
            shadow.need(Some(&run));
        }
        let lost = vec![run];
        shadow.register(&Register { lost, ..r.clone() });
        assert_eq!(shadow.isolated(&key_of(0)), Some(true), "need: {need}");
    }
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = connect(&state, "w1", 1, &[]);
    let _client = submit(&state, request("x86_64-linux", Some(false))).await;
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let a = w.assignment().await.expect("not assigned");
    let lost = Heartbeat {
        lost: a.run.into_iter().collect(),
        ..Heartbeat::default()
    };
    w.send(worker_message::Msg::Heartbeat(lost)).await;
    for _ in (0..200).take_while(|_| state.shadow.isolated(&a.dedupe_key) != Some(true)) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        state.shadow.isolated(&a.dedupe_key),
        Some(true),
        "by heartbeat"
    );
}

/// An assignment no worker answered is freed as its build ends, its last
/// client gone: no Place can follow it. Before, it was kept for good.
#[tokio::test]
async fn an_unanswered_assignment_is_freed_as_its_build_ends() {
    let shadow = Arc::new(HubState::for_test(Duration::ZERO)).shadow.clone();
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let client = shadow.submit(&key_of(0), sys, false);
    shadow.assigned(&RunId { epoch: 1, seq: 1 }, &key_of(0), "w");
    assert_eq!(shadow.pending(), 2, "not held while its build waits");
    drop(client);
    assert_eq!(shadow.pending(), 0);
}

/// A client resubmitting a key whose last client left joins the run still
/// going, in the core as in the hub (L9). Before, the core dropped the build
/// at the leave, so at the fetch it answered no one: a divergence (the hub,
/// 2026-11-13, keys 183cc4f5 and 3bf842b2).
#[tokio::test]
async fn a_resubmission_joins_the_run_its_left_build_kept() {
    let log = Captured;
    let _guard = log.install();
    let shadow = Arc::new(HubState::for_test(Duration::ZERO)).shadow.clone();
    let caps = vec![SystemCaps {
        system: "x86_64-linux".into(),
        features: vec![],
    }];
    let worker_name = "w".to_owned();
    shadow.register(&Register {
        worker_name,
        caps,
        ..Register::default()
    });
    let sys: (&str, &[String]) = ("x86_64-linux", &[]);
    let run = RunId { epoch: 1, seq: 1 };
    let first = shadow.submit(&key_of(0), sys, true);
    shadow.assigned(&run, &key_of(0), "w");
    shadow.started(Some(&run));
    drop(first);
    let mut j = job(0, Arc::new(Replay::default()));
    j.key = key_of(0);
    let rerun = shadow.submit(&j.key, sys, true);
    let _rerun = j.replay.subscribe_as(rerun).await;
    shadow.result(&j, Some(&run), 0);
    shadow.fetched(&j, Some(&run));
    let bad = "shadow divergence";
    assert_eq!(log.count(bad), 0, "{}", log.line(bad));
}

/// A clientless build whose run finished is not given up while its outputs
/// come: a client back past FLOOR joins it (L9). Before, it was unlisted,
/// and the key built anew diverged at both fetches (the hub, 2026-11-17,
/// 10 keys). One given up before its result came does not fetch it.
#[test]
fn a_finished_build_is_not_given_up_while_its_outputs_come() {
    for result_first in [true, false] {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(left_then(result_first));
    }
}

/// A build's client leaves after its run started; its result comes before
/// or after FLOOR, and then a client of its key.
async fn left_then(result_first: bool) {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w = connect(&state, "w1", 1, &[]);
    keep_alive(&w.to_hub);
    let first = submit(&state, request("x86_64-linux", None)).await;
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let BuildAssignment { build_id, run, .. } = w.assignment().await.expect("not assigned");
    let id = build_id.clone();
    w.send(worker_message::Msg::Started(Started { build_id, run }))
        .await;
    drop(first);
    let _wanting = wanting_a_slot(&state, 9).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let data = b"the build's output";
    let hash = blake3::hash(data).as_bytes().to_vec();
    let manifest = Manifest {
        store_path: format!("/nix/store/{H}-tt-shadow"),
        hashes: hash.clone(),
        sizes: vec![data.len() as u64],
        ..Default::default()
    };
    let (build_id, outputs) = (id.clone(), vec![manifest]);
    let result = BuildResult {
        build_id,
        outputs,
        run,
        ..Default::default()
    };
    let (zero, past) = (Duration::ZERO, 2 * FLOOR);
    let (before, after) = if result_first {
        (zero, past)
    } else {
        (past, zero)
    };
    tokio::time::sleep(before).await;
    w.send(worker_message::Msg::Result(result)).await;
    tokio::time::sleep(after).await;
    let mut next = submit(&state, request("x86_64-linux", None)).await;
    let queued = state.queue.lock().await.len();
    let frame = zstd::bulk::compress(data, 3).unwrap();
    for (zstd, hash, eof) in [(frame, hash, false), (vec![], vec![], true)] {
        let build_id = id.clone();
        let c = ChunkFrame {
            build_id,
            hash,
            zstd,
            eof,
        };
        w.send(worker_message::Msg::Chunk(c)).await;
    }
    let mut exit = None;
    while let Ok(Ok(Some(ev))) = tokio::time::timeout(Duration::from_mins(1), next.message()).await
    {
        if let Some(attach_event::Event::ExitCode(c)) = ev.event {
            exit = Some(c);
        }
    }
    let mut fetched = false;
    while let Ok(Ok(HubMessage { msg })) = w.from_hub.try_recv() {
        fetched |= matches!(msg, Some(hub_message::Msg::Need(_)));
    }
    w.hub.abort();
    let bad = "shadow divergence";
    assert_eq!(log.count(bad), 0, "{}", log.line(bad));
    let joined = (1, Some(0), true);
    let anew = (2, None, false);
    let want = if result_first { joined } else { anew };
    assert_eq!(
        (queued, exit, fetched),
        want,
        "result first: {result_first}"
    );
}

/// A panic in the core turns the shadow off; the hub answers on.
#[tokio::test]
async fn a_panic_in_the_core_turns_the_shadow_off() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    state.shadow.with(|_| panic!("planted"));
    let mut client = submit(&state, request("aarch64-linux", Some(true))).await;
    let declined = client.message().await.unwrap().and_then(|e| e.event);
    assert_eq!(
        declined,
        Some(attach_event::Event::ExitCode(DECLINE_EXIT_CODE))
    );
    assert!(log.has(&["the semantic core panicked"]));
    assert!(!log.has(&["shadow divergence"]));
}
