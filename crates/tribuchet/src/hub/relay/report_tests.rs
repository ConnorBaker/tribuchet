//! run_job reports a build not started, and sums its Needs once it starts.

use super::*;
use crate::hub::losses::Dispatch;
use crate::hub::state::Replay;
use crate::hub::tests::job;
use crate::proto::{Need, Resumed, RunId, Started};
use crate::report::REPORT;
use crate::testlog::Captured;

const ID: &str = "00000000000000000000000000000009";

/// run_job, for REPORT and a tick, on a build its worker asked two Needs
/// of and then sent `then` of, or nothing more.
async fn negotiated(then: Option<worker_message::Msg>) {
    let state = HubState::for_test(Duration::from_secs(1));
    let j = job(9, Arc::new(Replay::default()));
    let (out_tx, _out_rx) = mpsc::channel(8);
    let (bulk_tx, _bulk_rx) = mpsc::channel(8);
    let sess = Arc::new(WorkerSession::new("w3".into(), bulk_tx));
    let (ctl_tx, ctl) = mpsc::unbounded_channel();
    let (_bulk_in, bulk) = mpsc::channel(1);
    for _ in 0..2 {
        let need = Need {
            build_id: j.id.clone(),
            ..Need::default()
        };
        ctl_tx.send(worker_message::Msg::Need(need)).unwrap();
    }
    if let Some(m) = then {
        ctl_tx.send(m).unwrap();
    }
    let mut dispatch = Dispatch::default();
    let worker = (&out_tx, "w3", 1);
    let run = run_job(&state, &j, worker, Inbox { ctl, bulk }, sess, &mut dispatch);
    tokio::pin!(run);
    tokio::select! {
        r = &mut run => panic!("run_job ended: {r:?}"),
        () = tokio::time::sleep(REPORT + Duration::from_secs(3)) => {}
    }
}

/// A build its worker negotiated and then said nothing of: run_job
/// reports it once per REPORT, as the incident's, silent for 4 h, did not.
#[tokio::test(start_paused = true)]
async fn run_job_reports_a_build_its_worker_never_starts() {
    let log = Captured;
    let _guard = log.install();
    negotiated(None).await;
    assert_eq!(log.count("build not started"), 1);
    let line = log.line("build not started");
    for field in [
        &format!(r#"id="{ID}""#),
        r#"worker="w3""#,
        "needs_received=2",
    ] {
        assert!(line.contains(field), "{field} in {line}");
    }
}

/// A build its worker started, or resumed, says nothing more of its
/// staging, and sums its Needs once, then, naming its run; the staging's
/// end adds no line.
#[tokio::test(start_paused = true)]
async fn run_job_sums_a_started_builds_needs_once() {
    let (build_id, run) = (ID.to_owned(), Some(RunId { epoch: 0, seq: 7 }));
    let started = Started {
        build_id: build_id.clone(),
        run,
    };
    let resumed = Resumed {
        build_id,
        run,
        finished: false,
    };
    for then in [
        worker_message::Msg::Started(started),
        worker_message::Msg::Resumed(resumed),
    ] {
        let log = Captured;
        let _guard = log.install();
        negotiated(Some(then)).await;
        let counts = (log.count("build not started"), log.count("needs served"));
        assert_eq!(counts, (0, 1));
        let line = log.line("needs served");
        for field in [&format!(r#"id="{ID}""#), r#"worker="w3""#, r#"run="0.7""#] {
            assert!(line.contains(field), "{field} in {line}");
        }
    }
}
