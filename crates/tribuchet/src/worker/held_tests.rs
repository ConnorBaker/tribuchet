//! What a worker's full report (Register, Heartbeat) lists as held.

use super::*;

/// A report lists each numbered run held with how far it got, but none
/// the hub had dropped: cancelled while it runs, or ended for a cancel.
#[test]
fn a_report_lists_the_runs_held_but_not_those_dropped() -> Result<()> {
    let state = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(state.path(), 1, vec![])?;
    let ended = |exit_code, error: &str| {
        let (usage, outputs, dir) = (resume::Usage::default(), vec![], PathBuf::new());
        let (error, finished_at) = (error.to_owned(), Instant::now());
        let fin = resume::FinishedBuild {
            exit_code,
            usage,
            error,
            outputs,
            dir,
            finished_at,
        };
        Some(Arc::new(fin))
    };
    let rows = [
        (Some(1), None, false),
        (Some(2), ended(1, "boom"), false),
        (Some(3), None, true),
        (Some(4), ended(1, CANCELLED), false),
        (None, None, false),
    ];
    for (n, (seq, finished, cancelled)) in rows.into_iter().enumerate() {
        let run = seq.map(|seq| RunId { epoch: 1, seq });
        let e = ResumableBuild {
            run,
            finished,
            cancelled,
            ..ResumableBuild::default()
        };
        ctx.resumable.lock().unwrap().insert(format!("k{n}"), e);
    }
    let mut held: Vec<_> = (ctx.held().into_iter())
        .map(|h| (h.run.map(|r| r.seq), h.started, h.exit_code))
        .collect();
    held.sort_unstable();
    assert_eq!(held, [(Some(1), true, None), (Some(2), true, Some(1))]);
    Ok(())
}
