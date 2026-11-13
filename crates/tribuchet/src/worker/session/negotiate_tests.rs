//! The session loop while the daemon stalls a build's negotiation.

use std::iter;

use futures_util::FutureExt as _;
use tokio::net::UnixListener;

use super::*;
use crate::proto::TmpDirArchive;
use crate::slowlink;
use crate::worker::build::tests::base_assignment;

/// A daemon that never answers stalls a build's negotiation; the loop
/// still sends its heartbeats and reads the hub: its tmp dir, which does
/// not start the build unnegotiated, and a cancel of that very build,
/// answered with its failure.
#[tokio::test(start_paused = true)]
async fn a_stalled_negotiation_leaves_the_loop_reading_the_hub() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut ctx = WorkerCtx::for_test(dir.path(), 1, vec![dir.path().join("down.sock")])?;
    let socket = dir.path().join("daemon.sock");
    let never_answers = UnixListener::bind(&socket)?;
    Arc::get_mut(&mut ctx).unwrap().daemon_socket = Some(socket);
    // A hub assigning a build and then cancelling it, which passes on all
    // it reads.
    let (read_tx, mut read) = mpsc::unbounded_channel();
    let stalled = slowlink::hub(move |(mut from_worker, to_worker), bulk| {
        let read = read_tx.clone();
        async move {
            let (_bulk, to_bulk) = bulk;
            let a = base_assignment();
            let (build_id, dedupe_key) = (a.build_id.clone(), a.dedupe_key.clone());
            let tmp_dir = hub_message::Msg::TmpDir(TmpDirArchive::eof(&build_id));
            let cancel = hub_message::Msg::Cancel(CancelBuild {
                build_id,
                dedupe_key,
            });
            let hub_msg = |m| Ok(HubMessage { msg: Some(m) });
            let _ = to_worker
                .send(hub_msg(hub_message::Msg::Assignment(a)))
                .await;
            // The tmp dir after the assignment, on the other connection.
            tokio::time::sleep(chunkio::HEARTBEAT).await;
            let _ = to_bulk.send(hub_msg(tmp_dir)).await;
            let _ = to_worker.send(hub_msg(cancel)).await;
            while let Ok(Some(m)) = from_worker.message().await {
                let _ = read.send(m);
            }
        }
    });
    let ((to_hub, mut from_hub), (to_bulk, mut from_bulk)) = slowlink::session(stalled).await;
    let active = &mut HashMap::new();
    let streams = (&mut from_hub, &mut from_bulk);
    let session = session_loop(streams, active, (&to_hub, &to_bulk), &ctx, Duration::ZERO);
    let _ = tokio::time::timeout(4 * chunkio::HEARTBEAT, session).await;
    let read: Vec<_> = iter::from_fn(|| read.try_recv().ok()).collect();
    let beat = |m: &&WorkerMessage| matches!(m.msg, Some(worker_message::Msg::Heartbeat(_)));
    let started = |m: &WorkerMessage| matches!(m.msg, Some(worker_message::Msg::Started(_)));
    let cancelled = |m: &WorkerMessage| match &m.msg {
        Some(worker_message::Msg::Result(r)) => r.error == CANCELLED,
        _ => false,
    };
    let beats = read.iter().filter(beat).count();
    let (started, cancelled) = (read.iter().any(started), read.iter().any(cancelled));
    let reached = never_answers
        .accept()
        .now_or_never()
        .is_some_and(|c| c.is_ok());
    assert!(reached, "the negotiation did not reach the daemon");
    assert!(beats >= 3, "{beats} heartbeats");
    assert!(
        !started && cancelled,
        "started: {started}; cancel answered: {cancelled}"
    );
    Ok(())
}
