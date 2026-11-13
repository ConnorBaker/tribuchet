//! The session loop over a slow link (crate::slowlink).

use super::*;
use crate::{hub, slowlink};

/// A hub sending inputs over a slow link to an otherwise quiet worker
/// keeps its session. Its keepalive would PING behind the inputs and
/// miss the timeout, but the worker's heartbeats give it something to
/// read first.
#[tokio::test(start_paused = true)]
async fn a_hub_sending_inputs_over_a_slow_link_keeps_its_session() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![])?;
    // Chunk frames as fast as the link takes them.
    let inputs = slowlink::hub(|mut from_worker, to_worker| async move {
        slowlink::flood(to_worker, || {
            let chunk = hub_message::Msg::Chunk(slowlink::bulk());
            Ok(HubMessage { msg: Some(chunk) })
        });
        while let Ok(Some(_)) = from_worker.message().await {}
    });
    let register = msg(worker_message::Msg::Register(Register::default()));
    let (to_hub, mut from_hub) = slowlink::session(inputs, register).await;
    let start = tokio::time::Instant::now();
    let active = &mut HashMap::new();
    let session = session_loop(&mut from_hub, active, &to_hub, &ctx, Duration::ZERO);
    if let Ok(e) = tokio::time::timeout(slowlink::LONG, session).await {
        panic!("the session died after {:?}: {e:?}", start.elapsed());
    }
    Ok(())
}

/// A worker sending outputs over a slow link keeps its session: its loop
/// reads the hub's beats while the outputs fill its channel. It reads all
/// else the hub sends too, a cancel here, once the hub takes no more
/// outputs and the channel stays full: a loop waiting for room in the
/// channel, for its heartbeat, would read nothing more.
#[tokio::test(start_paused = true)]
async fn a_worker_uploading_outputs_over_a_slow_link_keeps_its_session() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![])?;
    ResumableBuild::insert(&ctx, "k".into(), "b".into(), dir.path().join("b"), None);
    // hub::beat's beats; it reads all the worker sends for LONG / 4, then
    // nothing, and at LONG / 2 sends a cancel.
    let beats = slowlink::hub(|mut from_worker, to_worker| async move {
        tokio::spawn(hub::beat(to_worker.clone()));
        let read = async { while let Ok(Some(_)) = from_worker.message().await {} };
        let _ = tokio::time::timeout(slowlink::LONG / 4, read).await;
        tokio::time::sleep(slowlink::LONG / 4).await;
        let c = CancelBuild {
            build_id: "b".into(),
            dedupe_key: "k".into(),
        };
        let cancel = hub_message::Msg::Cancel(c);
        let _ = to_worker.send(Ok(HubMessage { msg: Some(cancel) })).await;
        // Held, unread, for as long as the session lasts.
        tokio::time::sleep(slowlink::LONG).await;
        drop(from_worker);
    });
    let register = msg(worker_message::Msg::Register(Register::default()));
    let (to_hub, mut from_hub) = slowlink::session(beats, register).await;
    slowlink::flood(to_hub.clone(), || {
        msg(worker_message::Msg::Chunk(slowlink::bulk()))
    });
    let start = tokio::time::Instant::now();
    let active = &mut HashMap::new();
    let session = session_loop(&mut from_hub, active, &to_hub, &ctx, Duration::ZERO);
    if let Ok(e) = tokio::time::timeout(slowlink::LONG, session).await {
        panic!("the session died after {:?}: {e:?}", start.elapsed());
    }
    assert!(
        ctx.cancelled.lock().unwrap().contains("k"),
        "no cancel read"
    );
    Ok(())
}
