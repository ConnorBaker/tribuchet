//! A chain of clients under demand (hub/state/clientless.rs): clients
//! can be chains of `nix build`s of at most 600 s each
//! (nix-grpc-store's contrib/build-client.sh), so between one client of a
//! chain and the next a build has no client, and the chain's next
//! client, 55 s on, should still find the build running while another
//! waits for its slot. The hub cannot tell that client from any other: it
//! sees only `tribuchet attach`, which Nix runs on the hub's host with
//! the derivation's build.json and nix-daemon's environment, nothing of
//! the client's. So the client declares that
//! it will come back; the declaration reaches the scheduler that sends
//! the hub its builds, nix-grpc-store's, as a linger in its Want
//! (Want.linger_s, the client's `--option grpc-linger`), and that
//! scheduler keeps the build, its attach attached, until the
//! linger is over. So with a linger the hub never sees the gap, whether
//! the build waits in its queue or runs (nix-grpc-store's lease checks
//! run the chain through both, with a gap in each). What the hub
//! sees is a chain without one, or one whose linger is over: its build
//! loses its last client, and the hub's own rule decides, which gives
//! the next client no better chance than any: at FLOOR, such a build is
//! given up (state/clientless/tests.rs's chain row).

use super::clientless::sent;
use super::*;
use crate::proto::{BuildResult, ChunkFrame, Manifest, OutputNar};
use state::FLOOR;

/// A request for the chain's derivation, as the attach program sends it.
fn chained_request() -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        args: vec!["-c".into(), "true".into()],
        outputs: [("out".to_owned(), format!("/nix/store/{H}-chained-1.0"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        ..Default::default()
    }
}

/// A chain's link ends at its 590 s bound while another build waits for
/// the slot; the next asks for the same derivation 3 s, or 2 s inside
/// FLOOR, later. The hub has weighed nothing yet: the request joins the
/// build, neither cancelled nor dispatched again, and the new client
/// receives its output and exit. (In one deployment the next link
/// reached the remote store 11 to 17 s after the last ended, its
/// dispatch 17 to 26 s, and the hub's cancel 10 to 14 s: the test above.)
#[tokio::test(start_paused = true)]
async fn a_chained_client_back_within_the_floor_receives_the_output() {
    let out = format!("/nix/store/{H}-chained-1.0");
    let data = b"the build's output";
    let hash = *blake3::hash(data).as_bytes();
    let frame = zstd::bulk::compress(data, 3).unwrap();
    for gap in [
        Duration::from_secs(3),
        FLOOR.saturating_sub(Duration::from_secs(2)),
    ] {
        let mut s = session(Vec::new()).await;
        keep_alive(&s.to_hub);
        let first = submit(&s.state, chained_request()).await;
        s.to_hub.send(request_job(1, Some(0))).await.unwrap();
        let assigned = assignments(&mut s.from_hub, QUIET).await;
        assert_eq!(assigned.len(), 1, "gap {gap:?}");
        let _wanting = wanting_a_slot(&s.state, 1).await;
        tokio::time::sleep(Duration::from_secs(590)).await;
        drop(first);
        tokio::time::sleep(gap).await;
        let mut next = submit(&s.state, chained_request()).await;
        let queued = s.state.queue.lock().await.len();
        tokio::time::sleep(Duration::from_mins(1)).await;
        let (again, cancels) = sent(&mut s.from_hub);
        let id = &assigned[0];
        let result = BuildResult {
            build_id: id.clone(),
            outputs: vec![Manifest {
                store_path: out.clone(),
                hashes: hash.to_vec(),
                sizes: vec![data.len() as u64],
                ..Default::default()
            }],
            ..Default::default()
        };
        let chunk = |zstd: &[u8], eof| ChunkFrame {
            build_id: id.clone(),
            hash: if eof { Vec::new() } else { hash.to_vec() },
            zstd: zstd.to_vec(),
            eof,
        };
        for msg in [
            worker_message::Msg::Result(result),
            worker_message::Msg::Chunk(chunk(&frame, false)),
            worker_message::Msg::Chunk(chunk(&[], true)),
        ] {
            s.to_hub
                .send(WorkerMessage { msg: Some(msg) })
                .await
                .unwrap();
        }
        let mut received = Vec::new();
        while let Ok(Ok(Some(ev))) =
            tokio::time::timeout(Duration::from_mins(1), next.message()).await
        {
            match ev.event {
                Some(e @ attach_event::Event::ExitCode(_)) => {
                    received.push(e);
                    break;
                }
                Some(e @ attach_event::Event::Output(_)) => received.push(e),
                _ => {}
            }
        }
        s.hub.abort();
        assert_eq!(queued, 1, "gap {gap:?}: the request queued a build anew");
        assert_eq!((again, cancels), (vec![], vec![]), "gap {gap:?}");
        let output = |zstd_nar_chunk: &[u8], eof| {
            attach_event::Event::Output(OutputNar {
                store_path: out.clone(),
                zstd_nar_chunk: zstd_nar_chunk.to_vec(),
                eof,
            })
        };
        assert_eq!(
            received,
            [
                output(&frame, false),
                output(&[], true),
                attach_event::Event::ExitCode(0)
            ],
            "gap {gap:?}"
        );
    }
}
