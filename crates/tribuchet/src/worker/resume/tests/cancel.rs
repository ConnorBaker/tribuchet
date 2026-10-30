//! What a hub cancel does to a key's run, and what a later assignment of
//! the key then gets.

use super::*;

/// An entry for "k" in b1 for the session "old": running, held for its
/// agent, or finished with this error.
#[derive(Clone, Copy)]
enum Entry {
    Running,
    Held,
    Finished(&'static str),
}

/// What the registry is told, in turn: the hub cancels "old"; the held
/// build's wait fails it to the hub (fail_cancelled); the run's supervision
/// takes the cancel; an assignment from a session, adopted or not; the run
/// in b1 ends with this error; "k" runs anew in b2 for "new".
#[derive(Clone, Copy)]
enum Tell {
    Cancel,
    Fail,
    Take,
    Assign(&'static str, bool),
    End(&'static str),
    Anew,
}
use Tell::{Anew, Assign, Cancel, End, Fail, Take};

/// Then: each session's results, "k"'s entry (build id, finished), and
/// whether b1 is still there.
type Then = (
    &'static [(&'static str, &'static str)],
    &'static [(&'static str, &'static str)],
    Option<(&'static str, bool)>,
    bool,
);

const ROWS: &[(Entry, &[Tell], Then)] = &[
    // A run being killed for a cancel is no result for a later assignment
    // (the new client is not sent "build cancelled"): it starts anew, and
    // the killed run's failure goes to the session it was cancelled for.
    (
        Entry::Running,
        &[Cancel, Take, Assign("new", false), End(CANCELLED)],
        (&[("old", CANCELLED)], &[], Some(("old", true)), true),
    ),
    // ... and once it ends after its key ran anew, the new run is left be.
    (
        Entry::Running,
        &[Cancel, Take, Anew, End(CANCELLED)],
        (&[], &[], Some(("new", false)), false),
    ),
    // Nor is a cancelled run's result: its state goes.
    (
        Entry::Finished(CANCELLED),
        &[Assign("new", false)],
        (&[], &[], None, false),
    ),
    // An assignment voids a cancel not taken yet, also of a held build
    // whose wait failed it to the hub: the run goes on for it.
    (
        Entry::Running,
        &[Cancel, Assign("new", true), End("")],
        (&[], &[("new", "")], Some(("new", true)), true),
    ),
    (
        Entry::Held,
        &[Cancel, Fail, Assign("new", true)],
        (&[("old", CANCELLED)], &[], Some(("new", false)), true),
    ),
    // A finished run's result is the later assignment's to take.
    (
        Entry::Finished(""),
        &[Assign("new", true)],
        (&[], &[], Some(("new", true)), true),
    ),
    // A cancel of a finished run, or one that beat it to the finish, is no
    // flag that would fail the key's next run here.
    (
        Entry::Finished(""),
        &[Cancel],
        (&[], &[], Some(("old", true)), true),
    ),
    (
        Entry::Running,
        &[Cancel, End("")],
        (&[("old", "")], &[], Some(("old", true)), true),
    ),
];

/// The result of a run in `dir` that failed with `error` ("" for none).
fn ended(dir: &Path, error: &str) -> FinishedBuild {
    FinishedBuild {
        exit_code: i32::from(!error.is_empty()),
        usage: Usage::default(),
        error: error.into(),
        outputs: vec![],
        dir: dir.to_owned(),
        finished_at: Instant::now(),
    }
}

#[test]
fn every_cancel_and_assignment_ends_as_the_table_says() -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    for (row, &(entry, tells, (to_old, to_new, then, b1_there))) in ROWS.iter().enumerate() {
        let dir = tempfile::tempdir()?;
        let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
        let (b1, b2) = (dir.path().join("builds/b1"), dir.path().join("builds/b2"));
        fs::create_dir_all(&b1)?;
        // It logged: no limit is set, so no log size aborts it.
        fs::write(b1.join("build.log"), "running\n")?;
        let (old, mut old_rx) = tokio::sync::mpsc::channel(16);
        let (new, mut new_rx) = tokio::sync::mpsc::channel(16);
        let fin = match entry {
            Entry::Finished(error) => Some(ended(&b1, error)),
            _ => None,
        };
        ResumableBuild::insert(&ctx, "k".into(), "old".into(), b1.clone(), fin);
        if let Some(e) = ctx.resumable.lock().unwrap().get_mut("k") {
            (e.out_tx, e.adopting) = (Some(old.clone()), matches!(entry, Entry::Held));
        }
        for &tell in tells {
            match tell {
                Cancel => {
                    let c = CancelBuild {
                        build_id: "old".into(),
                        dedupe_key: "k".into(),
                    };
                    rt.block_on(handle_cancel(c, &mut HashMap::new(), &old, &ctx))?;
                }
                Fail => fail_cancelled(&ctx, "k"),
                Take => {
                    let taken = ctx.abort_reason("k", &b1.join("build.log"), None, None);
                    assert_eq!(taken.as_deref(), Some(CANCELLED), "row {row}");
                }
                Assign(s, adopted) => {
                    let tx = if s == "old" { &old } else { &new };
                    assert_eq!(
                        ctx.adopt_assignment(&assignment(s), tx),
                        adopted,
                        "row {row}"
                    );
                }
                End(error) => record_finished(&ctx, "k", ended(&b1, error)),
                Anew => {
                    fs::create_dir_all(&b2)?;
                    ResumableBuild::insert(&ctx, "k".into(), "new".into(), b2.clone(), None);
                    ctx.resumable.lock().unwrap().get_mut("k").unwrap().out_tx = Some(new.clone());
                }
            }
        }
        let results = |rx: &mut tokio::sync::mpsc::Receiver<WorkerMessage>| {
            thread::sleep(Duration::from_millis(200));
            let mut got = Vec::new();
            while let Ok(m) = rx.try_recv() {
                if let Some(worker_message::Msg::Result(r)) = m.msg {
                    got.push((r.build_id, r.error));
                }
            }
            got
        };
        let pairs = |want: &[(&str, &str)]| -> Vec<(String, String)> {
            want.iter()
                .map(|(a, b)| ((*a).into(), (*b).into()))
                .collect()
        };
        assert_eq!(results(&mut old_rx), pairs(to_old), "row {row}: old");
        assert_eq!(results(&mut new_rx), pairs(to_new), "row {row}: new");
        let entry = ctx
            .resumable
            .lock()
            .unwrap()
            .get("k")
            .map(|e| (e.build_id.clone(), e.finished.is_some()));
        assert_eq!(
            entry,
            then.map(|(id, f)| (id.to_owned(), f)),
            "row {row}: entry"
        );
        assert_eq!(b1.exists(), b1_there, "row {row}: b1");
        let next = ctx.abort_reason("k", &b1.join("build.log"), None, None);
        assert_eq!(next, None, "row {row}: the key's next run is aborted");
    }
    Ok(())
}

/// A cancel the hub sends as the kill of a held build it cancelled
/// before is decided (the first cancel failed to the hub after its agent
/// answered) leaves no flag: it goes with the key at that decision. Left
/// behind, it would fail the key's next run here "build cancelled".
#[test]
fn a_cancel_taken_as_a_held_builds_kill_is_decided_leaves_no_flag() -> Result<()> {
    let h = held_build(true, None, false)?;
    // Full once the session is told of the wait: the first cancel's
    // failure then waits, after the agent answered, until it is taken.
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    assert!(h.assign("old", &tx));
    let calls = || h.calls.asked.load(SeqCst);
    let before = calls();
    until("a call waiting at a full session", || {
        tx.capacity() == 0 && calls() > before
    });
    // A call to its agent waits at the gate, so the cancel is taken
    // after the agent answers it.
    h.cancel("old", &tx)?;
    h.allow.store(usize::MAX, SeqCst);
    let flagged = || h.ctx.cancelled.lock().unwrap().contains("k");
    until("the first cancel taken", || !flagged());
    assert_eq!(held(&h.ctx, "k"), Some((true, false)), "not held");
    h.cancel("old", &tx)?;
    assert!(flagged(), "the second cancel was not taken");
    let failed = thread::spawn(move || {
        let mut n = 0;
        while let Some(m) = rx.blocking_recv() {
            n += usize::from(
                matches!(m.msg, Some(worker_message::Msg::Result(r)) if r.error == CANCELLED),
            );
        }
        n
    });
    until("dropped", || held(&h.ctx, "k").is_none());
    until("handed back", || !h.ctx.agents.idle_sockets().is_empty());
    assert!(!flagged(), "the cancel taken at the decision outlived it");
    drop((tx, h));
    assert_eq!(
        failed.join().unwrap(),
        1,
        "the first cancel came before the answer"
    );
    Ok(())
}
