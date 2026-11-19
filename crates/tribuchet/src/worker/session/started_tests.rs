//! What a session tells the hub of a run: that it started, or that the
//! run it resumed is finished.

use super::*;
use crate::hub::msg_name;
use crate::worker::build::tests::base_assignment;

/// A build staged at once says it started after its Need, and its run
/// sends nothing before its Started: the loop queues both, in order, and
/// the run waits for the loop to send its Started.
#[tokio::test]
async fn a_run_says_it_started_after_its_need_and_first() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![dir.path().join("down.sock")])?;
    let build = ActiveBuild::new(base_assignment(), ctx.clone(), mpsc::unbounded_channel().0)?;
    let id = build.assignment.build_id.clone();
    // Cancelled, so that it ends instead of waiting for its agent, which
    // is down.
    ctx.cancelled.lock().unwrap().insert("test-key".into());
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let env = LaunchEnv {
        ctx: &ctx,
        out_tx: &out_tx,
        build_timeout: Duration::from_secs(60),
        assignments: AtomicU64::default(),
        outbox: Outbox::default(),
    };
    let mut active = HashMap::from([(id.clone(), build)]);
    let need = Need {
        build_id: id.clone(),
        ..Need::default()
    };
    let staged = Ok((Some(need), StagingStatus::Ready));
    advance_staging(&mut active, &mut VecDeque::new(), &id, staged, &env).await;
    let early = tokio::time::timeout(Duration::from_millis(500), out_rx.recv()).await;
    assert!(early.is_err(), "sent before the loop sent any: {early:?}");
    // As session_loop sends them.
    for (m, sent) in env.outbox.queue.take() {
        out_tx.send(m).await.unwrap();
        if let Some(s) = sent {
            let _ = s.send(());
        }
    }
    let mut got = vec![];
    while got.last() != Some(&"Result") {
        let m = tokio::time::timeout(Duration::from_secs(10), out_rx.recv()).await;
        let m = m.expect("no Result").unwrap();
        got.push(m.msg.as_ref().map_or("", msg_name));
    }
    assert_eq!(got, ["Need", "Started", "Result"]);
    Ok(())
}
