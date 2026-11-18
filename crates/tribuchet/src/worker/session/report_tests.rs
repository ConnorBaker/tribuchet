//! The session loop reports a staging that never finishes.

use super::*;
use crate::report::REPORT;
use crate::slowlink;
use crate::testlog::Captured;
use crate::worker::build::tests::base_assignment;

/// A build whose Need the hub never answers, the hub beating meanwhile:
/// the loop reports it once per REPORT, as the incident's staging, silent
/// for 4 h, did not.
#[tokio::test(start_paused = true)]
async fn the_loop_reports_a_staging_whose_need_is_never_answered() -> Result<()> {
    let log = Captured;
    let _guard = log.install();
    let beating = slowlink::hub(|(mut from_worker, to_worker), (_, to_bulk)| async move {
        let beat = || Ok(HubMessage { msg: None });
        loop {
            tokio::select! {
                m = from_worker.message() => if !matches!(m, Ok(Some(_))) { return },
                () = tokio::time::sleep(chunkio::HEARTBEAT) => {
                    let sent = (to_worker.send(beat()).await, to_bulk.send(beat()).await);
                    if sent.0.is_err() || sent.1.is_err() { return }
                }
            }
        }
    });
    let ((to_hub, mut from_hub), (to_bulk, mut from_bulk)) = slowlink::session(beating).await;
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![dir.path().join("down.sock")])?;
    let mut build = ActiveBuild::new(base_assignment(), ctx.clone(), mpsc::unbounded_channel().0)?;
    assert!(build.need(Vec::new(), vec![7; 32]).is_some());
    let id = build.assignment.build_id.clone();
    let active = &mut HashMap::from([(id.clone(), build)]);
    let streams = (&mut from_hub, &mut from_bulk);
    let session = session_loop(streams, active, (&to_hub, &to_bulk), &ctx, Duration::ZERO);
    let lived = tokio::time::timeout(REPORT + 2 * chunkio::HEARTBEAT, session).await;
    assert!(lived.is_err(), "the session ended: {lived:?}");
    assert_eq!(log.count("build not started"), 1);
    let line = log.line("build not started");
    for field in [
        &format!(r#"id="{id}""#),
        "negotiated=false",
        "tmp_dir_done=false",
        "pending=0",
        "parked=0",
        "importing=0",
        "waits=0",
        "needs_outstanding=1",
    ] {
        assert!(line.contains(field), "{field} in {line}");
    }
    Ok(())
}
