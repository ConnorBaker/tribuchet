//! The session loop over a slow link (crate::slowlink).

use super::*;
use crate::proto::worker_hub_server::WorkerHub;
use crate::slowlink::{self, HubEnd};

/// A hub that reads the worker's Session (its outputs) for `reading`,
/// floods it with `inputs` if asked, and cancels the worker's build "b" on
/// its Control at LONG / 2.
fn cancelling(reading: Duration, inputs: bool) -> impl WorkerHub {
    slowlink::hub(
        move |(mut from_worker, to_worker): HubEnd, (mut bulk, to_bulk)| async move {
            let at = tokio::time::Instant::now() + slowlink::LONG / 2;
            hub_beats(&to_worker, &to_bulk);
            tokio::spawn(async move { while let Ok(Some(_)) = from_worker.message().await {} });
            if inputs {
                slowlink::flood(to_bulk.clone(), || {
                    Ok(HubMessage {
                        msg: Some(hub_message::Msg::Chunk(slowlink::bulk())),
                    })
                });
            }
            let read = async { while let Ok(Some(_)) = bulk.message().await {} };
            let _ = tokio::time::timeout(reading, read).await;
            tokio::time::sleep_until(at).await;
            let c = CancelBuild {
                build_id: "b".into(),
                dedupe_key: "k".into(),
            };
            let cancel = hub_message::Msg::Cancel(c);
            let _ = to_worker.send(Ok(HubMessage { msg: Some(cancel) })).await;
            // Held, unread, for as long as the session lasts.
            tokio::time::sleep(slowlink::LONG).await;
            drop((bulk, to_bulk));
        },
    )
}

/// The session loop on `hub` lives until the hub's cancel, and has taken
/// it once the link could have carried one more chunk.
async fn takes_the_cancel(hub: impl WorkerHub, outputs: bool) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![])?;
    ResumableBuild::insert(&ctx, "k".into(), "b".into(), dir.path().join("b"), None);
    let ((to_hub, mut from_hub), (to_bulk, mut from_bulk)) = slowlink::session(hub).await;
    if outputs {
        slowlink::flood(to_bulk.clone(), || {
            msg(worker_message::Msg::Chunk(slowlink::bulk()))
        });
    }
    let start = tokio::time::Instant::now();
    let (streams, active) = ((&mut from_hub, &mut from_bulk), &mut HashMap::new());
    let session = session_loop(streams, active, (&to_hub, &to_bulk), &ctx, Duration::ZERO);
    if let Ok(e) = tokio::time::timeout(slowlink::LONG / 2 + slowlink::one_chunk(), session).await {
        panic!("the session died after {:?}: {e:?}", start.elapsed());
    }
    assert!(
        ctx.cancelled.lock().unwrap().contains("k"),
        "no cancel read"
    );
    Ok(())
}

/// The hub's heartbeats on its Control and its Session, as it sends them.
fn hub_beats(ctl: &Tx, bulk: &Tx) {
    for tx in [ctl, bulk] {
        chunkio::beat(tx, || Ok(HubMessage::default()));
    }
}

type Tx = mpsc::Sender<std::result::Result<HubMessage, tonic::Status>>;

/// How long the session loop on `hub` lives, up to LONG.
async fn lives(hub: impl WorkerHub) -> Duration {
    let dir = tempfile::tempdir().unwrap();
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![]).unwrap();
    let ((to_hub, mut from_hub), (to_bulk, mut from_bulk)) = slowlink::session(hub).await;
    let (streams, active) = ((&mut from_hub, &mut from_bulk), &mut HashMap::new());
    let start = tokio::time::Instant::now();
    let session = session_loop(streams, active, (&to_hub, &to_bulk), &ctx, Duration::ZERO);
    let _ = tokio::time::timeout(slowlink::LONG, session).await;
    start.elapsed()
}

/// A hub that beats on its Control, its Session, both or neither, and is
/// else never heard from (its host gone: no FIN, no RST).
fn silent(on: [bool; 2]) -> impl WorkerHub {
    slowlink::hub(
        move |(_from_worker, ctl): HubEnd, (_bulk, bulk)| async move {
            let none = mpsc::channel(1).0;
            hub_beats(
                if on[0] { &ctl } else { &none },
                if on[1] { &bulk } else { &none },
            );
            tokio::time::sleep(slowlink::LONG * 2).await;
            drop((ctl, bulk));
        },
    )
}

/// The worker ends a session whose hub sends nothing on its Control, or
/// on its Session, for SILENCE_TIMEOUT, as the hub ends a silent worker's;
/// a hub beating on both keeps it, as does the hub itself.
#[tokio::test(start_paused = true)]
async fn a_hub_silent_on_either_stream_ends_the_session_after_its_silence() {
    let silence = chunkio::SILENCE_TIMEOUT..chunkio::SILENCE_TIMEOUT + slowlink::one_chunk();
    for on in [[false, false], [true, false], [false, true]] {
        let lived = lives(silent(on)).await;
        assert!(
            silence.contains(&lived),
            "beating on {on:?}: lived {lived:?}"
        );
    }
    assert_eq!(lives(silent([true, true])).await, slowlink::LONG);
    let hub = crate::hub::tests::slow_link::service();
    assert_eq!(lives(hub).await, slowlink::LONG, "the hub ended it");
}

/// A hub's cancel reaches the worker at once while its inputs fill the
/// link: the hub's Control waits behind none of them.
#[tokio::test(start_paused = true)]
async fn a_cancel_crosses_a_link_full_of_inputs() -> Result<()> {
    takes_the_cancel(cancelling(slowlink::LONG / 2, true), false).await
}

/// A worker uploading outputs over a slow link reads all the hub sends,
/// a cancel here, though the hub takes no more outputs and they wait:
/// the loop never waits on them.
#[tokio::test(start_paused = true)]
async fn a_worker_uploading_outputs_over_a_slow_link_keeps_its_session() -> Result<()> {
    takes_the_cancel(cancelling(slowlink::LONG / 4, false), true).await
}
