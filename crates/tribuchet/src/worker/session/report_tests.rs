//! The session loop reports a staging that never finishes.

use super::*;
use crate::report::REPORT;
use crate::slowlink;
use crate::testlog::Captured;
use crate::worker::build::tests::base_assignment;

/// A build whose Need the hub never answers, the hub beating meanwhile:
/// the loop reports it once per REPORT, as the incident's staging, silent
/// for 4 h, did not, and what it waits for in its one agent: none while
/// the agent is free or its own, one free while another build leases it,
/// one to answer while it is down. Neither incident's line told the last
/// two apart from a staging stuck.
#[tokio::test(start_paused = true)]
async fn the_loop_reports_a_staging_whose_need_is_never_answered() -> Result<()> {
    let cases = [
        (Agent::Free, 1, "no"),
        (Agent::Own, 1, "no"),
        (Agent::Leased, 1, "busy"),
        (Agent::Down, 0, "unreachable"),
    ];
    for (agent, slots, waiting) in cases {
        let log = Captured;
        let _guard = log.install();
        let line = staging_reported(&log, agent).await?;
        for field in [
            format!("slots={slots}"),
            format!(r#"agent_wait="{waiting}""#),
        ] {
            assert!(line.contains(&field), "{field} in {line} ({agent:?})");
        }
    }
    Ok(())
}

/// The state of the test's one agent.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Agent {
    Free,
    /// Leased by the build reported.
    Own,
    /// Leased by another build.
    Leased,
    Down,
}

/// The heartbeat gives the hub a slot per agent up: none with its one
/// agent down.
#[test]
fn the_heartbeat_has_no_slot_with_no_agent_up() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("down.sock");
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![socket.clone()])?;
    let slots = |ctx: &WorkerCtx| match beat(&HashMap::new(), ctx).msg {
        Some(worker_message::Msg::Heartbeat(h)) => h.max_jobs,
        m => panic!("not a heartbeat: {m:?}"),
    };
    assert_eq!(slots(&ctx), Some(1));
    assert!(ctx.agents.mark_down(&socket));
    assert_eq!(slots(&ctx), Some(0));
    Ok(())
}

/// The "build not started" line of the test above, with its `agent`.
async fn staging_reported(log: &Captured, agent: Agent) -> Result<String> {
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
    if agent == Agent::Down {
        assert!(ctx.agents.mark_down(&dir.path().join("down.sock")));
    }
    // Held to the end, as another build's lease.
    let other = if agent == Agent::Leased {
        ctx.agents.lease()
    } else {
        None
    };
    let mut build = ActiveBuild::new(base_assignment(), ctx.clone(), mpsc::unbounded_channel().0)?;
    if agent == Agent::Own {
        build.lease = ctx.agents.lease();
    }
    assert_eq!(
        other.is_some() || build.lease.is_some(),
        matches!(agent, Agent::Leased | Agent::Own)
    );
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
    Ok(line)
}
