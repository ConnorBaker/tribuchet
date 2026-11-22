//! D4 isolation in the live hub (SEMANTICS-7 cut-over 10, I1): a key whose
//! run ended with no known cause is marked as the core marks it, runs alone
//! on its worker, and keeps the mark until it has an outcome (L10).

use super::*;
use crate::proto::{BuildResult, ChunkFrame, Manifest, ResultGone, RunId, Started};

/// A build of a key the core maps, and its client, whose Nix cannot build it.
async fn tracked(state: &Arc<HubState>) -> (Job, mpsc::Receiver<Result<AttachEvent, Status>>) {
    let mut j = job(0, Arc::new(Replay::default()));
    j.key = format!("{:08x}{}", 0, "0".repeat(56));
    let c = state.shadow.submit(&j.key, ("x86_64-linux", &[]), false);
    let client = j.replay.subscribe_as(c).await;
    (j, client)
}

/// How a started run of the key ends.
#[derive(Clone, Copy, Debug)]
enum Ends {
    /// Its worker is back without it.
    Back,
    /// Its worker is not back within the grace.
    Away,
    /// Its worker says it dropped its result.
    Gone,
    /// It was never answered, and its worker is not back: no run started,
    /// so no mark, in either.
    Unanswered,
    /// A producer's listener reached no attach (handoff_expired): a known
    /// cause, so no mark, in either (DD10).
    Expired,
}

/// A started run that ends with no known cause marks its key in the live
/// hub as in the core (D4), however it ends.
#[tokio::test]
async fn a_run_lost_with_no_known_cause_marks_its_key_as_the_core_does() {
    for ends in [
        Ends::Back,
        Ends::Away,
        Ends::Gone,
        Ends::Unanswered,
        Ends::Expired,
    ] {
        let state = Arc::new(HubState::for_test(Duration::from_millis(100)));
        let _other = register(&state, "w2", &[]);
        let w1 = Register {
            worker_name: "w1".into(),
            caps: vec![SystemCaps {
                system: "x86_64-linux".into(),
                features: vec![],
            }],
            ..Register::default()
        };
        state.shadow.register(&w1);
        let (j, _client) = tracked(&state).await;
        let (id, key) = (j.id.clone(), j.key.clone());
        let run = Some(RunId { epoch: 0, seq: 1 });
        let run_id = run;
        let mut said = vec![worker_message::Msg::Started(Started {
            build_id: id.clone(),
            run,
        })];
        if let Ends::Unanswered = ends {
            said.clear();
        }
        if let Ends::Expired = ends {
            said.push(worker_message::Msg::Result(BuildResult {
                build_id: id.clone(),
                run: run_id,
                exit_code: 1,
                handoff_expired: true,
                ..BuildResult::default()
            }));
        }
        if let Ends::Gone = ends {
            said.push(worker_message::Msg::ResultGone(ResultGone {
                build_id: id,
                run: run_id,
            }));
        }
        lose_session(&state, j, register(&state, "w1", &[]), said).await;
        let _back = match ends {
            Ends::Back => {
                state.shadow.register(&w1);
                Some(register(&state, "w1", &[]))
            }
            Ends::Away | Ends::Unanswered => {
                tokio::time::sleep(Duration::from_millis(150)).await;
                None
            }
            Ends::Gone | Ends::Expired => None,
        };
        let taken = state.take_job(&caps("w2"), None).await;
        assert!(taken.is_some(), "{ends:?}: not taken");
        let marked = !matches!(ends, Ends::Unanswered | Ends::Expired);
        assert_eq!(state.lost.has(&key), marked, "{ends:?}: the live hub");
        assert_eq!(
            state.shadow.isolated(&key),
            Some(marked),
            "{ends:?}: the core"
        );
    }
}

/// A lost key goes only to a worker holding nothing, the first busy one
/// that asks while it waits drains (takes nothing new) until it is empty,
/// and one holding a lost key's run takes nothing else (I1).
#[tokio::test]
async fn a_lost_key_runs_alone() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let (_a, _b) = (connect(&state, "a", 1, &[]), connect(&state, "b", 1, &[]));
    while state.worker_caps.lock().unwrap().len() < 2 {
        tokio::task::yield_now().await;
    }
    let mut ids: Vec<u64> = state.worker_caps.lock().unwrap().keys().copied().collect();
    ids.sort_unstable();
    let (a, b, caps) = (ids[0], ids[1], caps("w"));
    let replay = || Arc::new(Replay::default());
    let (lost, plain, other) = (job(1, replay()), job(2, replay()), job(3, replay()));
    let _clients = (
        lost.replay.subscribe().await,
        plain.replay.subscribe().await,
    );
    let _third = other.replay.subscribe().await;
    state.lost.mark(&lost.key);
    let busy = job(9, replay());
    state.placement.lock().unwrap().dispatched(a, &busy, false);
    state.queue.lock().await.push_back(lost);
    let take = |id| state.take_job(&caps, Some(id));
    assert!(take(a).await.is_none(), "a lost key went to a busy worker");
    assert_eq!(state.placement.lock().unwrap().drain, Some(a), "no drain");
    state.queue.lock().await.extend([plain, other]);
    assert!(take(a).await.is_none(), "a draining worker took a build");
    let alone = take(b)
        .await
        .expect("the lost key not given to an empty worker");
    assert_eq!(alone.key, "k1");
    state.placement.lock().unwrap().dispatched(b, &alone, true);
    assert!(
        take(b).await.is_none(),
        "a build went beside the lost key's"
    );
    assert_eq!(
        take(a).await.map(|j| j.key),
        Some("k2".into()),
        "still draining"
    );
    assert_eq!(state.placement.lock().unwrap().drain, None);
    state.placement.lock().unwrap().settled(b, &alone.id);
    assert_eq!(take(b).await.map(|j| j.key), Some("k3".into()));
}

/// An outcome of a lost key lifts its mark, failed or built (L10).
#[tokio::test]
async fn an_outcome_lifts_the_mark() {
    for code in [1, 0] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let mut w = connect(&state, "w1", 1, &[]);
        let output = format!("/nix/store/{H}-tt-isolation");
        let req = BuildRequest {
            system: "x86_64-linux".into(),
            builder: format!("/nix/store/{H}-bash/bin/bash"),
            outputs: [("out".to_owned(), output.clone())].into(),
            tmp_dir_in_sandbox: "/build".into(),
            store_dir: "/nix/store".into(),
            ..Default::default()
        };
        let mut client = submit(&state, req).await;
        w.to_hub.send(request_job(1, Some(0))).await.unwrap();
        let a = w.assignment().await.expect("not assigned");
        state.lost.mark(&a.dedupe_key);
        let (build_id, run) = (a.build_id.clone(), a.run);
        w.send(worker_message::Msg::Started(Started {
            build_id: build_id.clone(),
            run,
        }))
        .await;
        let data = b"the build's output";
        let hash = blake3::hash(data).as_bytes().to_vec();
        let manifest = Manifest {
            store_path: output,
            hashes: hash.clone(),
            sizes: vec![data.len() as u64],
            ..Default::default()
        };
        let outputs = if code == 0 { vec![manifest] } else { vec![] };
        let (id, exit_code) = (build_id.clone(), code);
        let result = BuildResult {
            build_id: id,
            exit_code,
            outputs,
            run,
            ..Default::default()
        };
        w.send(worker_message::Msg::Result(result)).await;
        if code == 0 {
            let frame = zstd::bulk::compress(data, 3).unwrap();
            for (zstd, hash, eof) in [(frame, hash, false), (vec![], vec![], true)] {
                let c = ChunkFrame {
                    build_id: build_id.clone(),
                    hash,
                    zstd,
                    eof,
                };
                w.send(worker_message::Msg::Chunk(c)).await;
            }
        }
        while client.message().await.unwrap().is_some() {}
        assert!(!state.lost.has(&a.dedupe_key), "exit {code}: still lost");
        w.hub.abort();
    }
}
