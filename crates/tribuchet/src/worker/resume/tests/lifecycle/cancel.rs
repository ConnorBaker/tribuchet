//! What a hub cancel does to a key's run, and what a later assignment of
//! the key then gets.

use super::*;
use crate::proto::Resumed;

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
    // the killed run's failure goes to the session it was cancelled for,
    // its result to its end as the key runs anew.
    (
        Entry::Running,
        &[Cancel, Take, Assign("new", false), End(CANCELLED), Anew],
        (&[("old", CANCELLED)], &[], Some(("new", false)), false),
    ),
    // ... and once it ends after its key ran anew, the new run is left be.
    (
        Entry::Running,
        &[Cancel, Take, Anew, End(CANCELLED)],
        (&[], &[], Some(("new", false)), false),
    ),
    // Nor is a cancelled run's result: it stays until the key runs anew.
    (
        Entry::Finished(CANCELLED),
        &[Assign("new", false)],
        (&[], &[], Some(("old", true)), true),
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
        (&[], &[("new", "")], Some(("new", true)), true),
    ),
    // A cancel of a finished run keeps its result: its dispatch wants it no
    // longer, and the next dispatch of its key takes it, with no rebuild.
    // One for an earlier adoption leaves the later's. None, nor one that
    // beat the run to the finish, is a flag that would fail the key's next
    // run here.
    (
        Entry::Finished(""),
        &[Cancel],
        (&[], &[], Some(("old", true)), true),
    ),
    (
        Entry::Finished(""),
        &[Cancel, Assign("new", true)],
        (&[], &[("new", "")], Some(("new", true)), true),
    ),
    (
        Entry::Finished(""),
        &[Assign("new", true), Cancel],
        (&[], &[("new", "")], Some(("new", true)), true),
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
                    // What it hands off the loop, e.g. an ack, runs by then.
                    thread::sleep(Duration::from_millis(100));
                }
                Fail => fail_cancelled(&ctx, "k"),
                Take => {
                    let taken = ctx.abort_reason("k", &b1.join("build.log"), None, None);
                    assert_eq!(taken.as_deref(), Some(CANCELLED), "row {row}");
                }
                // As handle_assignment: adopted, it is delivered to.
                Assign(s, adopted) => {
                    let tx = if s == "old" { &old } else { &new };
                    let adoption = ctx.adopt_assignment(&assignment(s), tx);
                    assert_eq!(adoption.is_some(), adopted, "row {row}");
                    if let Some(n) = adoption {
                        let ctx = ctx.clone();
                        bounded("delivered", move || try_deliver(&ctx, "k", Some(n)));
                    }
                }
                End(error) => {
                    let (ctx, fin) = (ctx.clone(), ended(&b1, error));
                    bounded("recorded", move || record_finished(&ctx, "k", fin));
                }
                Anew => {
                    fs::create_dir_all(&b2)?;
                    let mut anew = ResumableBuild::default();
                    (anew.build_id, anew.dir, anew.out_tx) =
                        ("new".into(), b2.clone(), Some(new.clone()));
                    let _rt = rt.enter();
                    ResumableBuild::launch(&ctx, "k".into(), anew);
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
        // A key goes before its dir (Ending).
        until(&format!("row {row}: b1"), || b1.exists() == b1_there);
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
        let next = ctx.abort_reason("k", &b1.join("build.log"), None, None);
        assert_eq!(next, None, "row {row}: the key's next run is aborted");
    }
    Ok(())
}

/// A cancel the hub sends as the kill of a held build it cancelled
/// before is decided (the first cancel failed to the hub after its agent
/// answered) leaves no flag: it goes at that decision. Left
/// behind, it would fail the key's next run here "build cancelled".
#[test]
fn a_cancel_taken_as_a_held_builds_kill_is_decided_leaves_no_flag() -> Result<()> {
    let h = held_build(true, None)?;
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
    h.cancel("old");
    h.allow.store(usize::MAX, SeqCst);
    let flagged = || h.ctx.cancelled.lock().unwrap().contains("k");
    until("the first cancel taken", || !flagged());
    assert_eq!(held(&h.ctx, "k"), Some((true, false)), "not held");
    h.cancel("old");
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

/// Run `f` on a thread until it returns, for 60 s at most: a delivery that
/// never ends fails its test instead of hanging the run.
fn bounded(what: &str, f: impl FnOnce() + Send + 'static) {
    let t = thread::spawn(f);
    until(what, || t.is_finished());
    t.join().unwrap();
}

/// A session: the build id and adoption of the assignment it was sent,
/// and its ends.
struct Session {
    id: &'static str,
    adoption: u64,
    tx: tokio::sync::mpsc::Sender<WorkerMessage>,
    rx: tokio::sync::mpsc::Receiver<WorkerMessage>,
}

impl Session {
    /// The hub assigns "k" anew as `id` on a new session: adopted.
    fn take_over(ctx: &Arc<WorkerCtx>, id: &'static str) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let adoption = ctx.adopt_assignment(&assignment(id), &tx);
        let adoption = adoption.expect("not held");
        Self {
            id,
            adoption,
            tx,
            rx,
        }
    }

    /// What handle_assignment does after adopting: send Resumed, then
    /// deliver for its adoption.
    fn resume(&self, ctx: &Arc<WorkerCtx>) {
        let resumed = Resumed {
            build_id: self.id.into(),
            finished: true,
        };
        self.tx
            .try_send(msg(worker_message::Msg::Resumed(resumed)))
            .unwrap();
        let (ctx, n) = (ctx.clone(), self.adoption);
        bounded("delivered", move || try_deliver(&ctx, "k", Some(n)));
    }

    /// The Resumed and Result messages it got so far, in order.
    fn got(&mut self) -> Vec<String> {
        let mut got = Vec::new();
        while let Ok(m) = self.rx.try_recv() {
            match m.msg {
                Some(worker_message::Msg::Resumed(r)) => {
                    got.push(format!("Resumed {}", r.build_id));
                }
                Some(worker_message::Msg::Result(r)) => got.push(format!("Result {}", r.build_id)),
                _ => {}
            }
        }
        got
    }
}

/// "k" finished in `dir`/b1 under `id`, no session told yet.
fn finished_k(dir: &Path, id: &str) -> Result<Arc<WorkerCtx>> {
    let ctx = WorkerCtx::for_test(dir, 4, vec![])?;
    let b1 = dir.join("builds/b1");
    fs::create_dir_all(&b1)?;
    let fin = Some(ended(&b1, ""));
    ResumableBuild::insert(&ctx, "k".into(), id.into(), b1, fin);
    Ok(ctx)
}

/// "k" finished in b1, its result being delivered to a session before
/// that takes nothing (full): the context and that session's receiver,
/// which `end_the_delivery_before` closes, failing the delivery.
struct Delivering {
    dir: tempfile::TempDir,
    ctx: Arc<WorkerCtx>,
    old_rx: tokio::sync::mpsc::Receiver<WorkerMessage>,
    first: Option<thread::JoinHandle<()>>,
}

fn delivering() -> Result<Delivering> {
    let dir = tempfile::tempdir()?;
    let ctx = finished_k(dir.path(), "old")?;
    let (old, old_rx) = tokio::sync::mpsc::channel(1);
    old.try_send(msg(worker_message::Msg::Resumed(Default::default())))
        .unwrap();
    ctx.resumable.lock().unwrap().get_mut("k").unwrap().out_tx = Some(old);
    let first_ctx = ctx.clone();
    let first = thread::spawn(move || try_deliver(&first_ctx, "k", None));
    until("delivering to the session before", || {
        ctx.resumable.lock().unwrap()["k"].delivering
    });
    Ok(Delivering {
        dir,
        ctx,
        old_rx,
        first: Some(first),
    })
}

impl Delivering {
    /// The session before goes, which ends its delivery. Bounded: a
    /// delivery that went on to a session that takes nothing would hang.
    fn end_the_delivery_before(&mut self) {
        self.old_rx.close();
        let first = self.first.take().unwrap();
        until("the delivery before ended", || first.is_finished());
        first.join().unwrap();
    }
}

/// A takeover told it resumed while the delivery to the session before
/// runs is sent the result as that ends, not left to the reaper.
#[test]
fn a_takeover_during_a_delivery_is_sent_the_result() -> Result<()> {
    let mut d = delivering()?;
    let mut new = Session::take_over(&d.ctx, "new");
    new.resume(&d.ctx);
    d.end_the_delivery_before();
    assert_eq!(new.got(), ["Resumed new", "Result new"], "new was sent");
    Ok(())
}

/// ... and one told it resumed after that ended is sent it once, after
/// Resumed: the delivery that ended makes none for it before.
#[test]
fn a_takeover_after_a_delivery_is_sent_the_result_once_after_resumed() -> Result<()> {
    let mut d = delivering()?;
    let mut new = Session::take_over(&d.ctx, "new");
    d.end_the_delivery_before();
    new.resume(&d.ctx);
    assert_eq!(new.got(), ["Resumed new", "Result new"], "new was sent");
    Ok(())
}

/// ... and once only, however often it asks: no delivery claims itself
/// again as it ends.
#[test]
fn a_takeover_after_a_delivery_is_sent_the_result_once() -> Result<()> {
    let mut d = delivering()?;
    let mut new = Session::take_over(&d.ctx, "new");
    d.end_the_delivery_before();
    let mut got = Vec::new();
    let (ctx, n) = (d.ctx.clone(), new.adoption);
    let asked = thread::spawn(move || {
        try_deliver(&ctx, "k", Some(n));
        try_deliver(&ctx, "k", Some(n));
    });
    until("delivered, once or more", || {
        got.extend(new.got());
        asked.is_finished() || got.len() > 1
    });
    got.extend(new.got());
    assert_eq!(got, ["Result new"], "new was sent");
    Ok(())
}

/// A second takeover during the delivery, not yet told it resumed as that
/// ends, is sent the result once, after its Resumed; the first, taken
/// over in turn, none.
#[test]
fn a_second_takeover_is_sent_the_result_once_after_resumed() -> Result<()> {
    let mut d = delivering()?;
    let mut new = Session::take_over(&d.ctx, "new");
    new.resume(&d.ctx);
    let mut third = Session::take_over(&d.ctx, "third");
    d.end_the_delivery_before();
    third.resume(&d.ctx);
    assert_eq!(
        third.got(),
        ["Resumed third", "Result third"],
        "third was sent"
    );
    assert_eq!(new.got(), ["Resumed new"], "new was sent");
    Ok(())
}

/// The same build id taken over on two sessions in turn: the first one's
/// delivery, asked late, sends the second nothing before its Resumed; the
/// second's sends it the result once, after.
#[test]
fn a_second_takeover_under_the_same_id_waits_for_its_resumed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = finished_k(dir.path(), "old")?;
    let mut s2 = Session::take_over(&ctx, "x");
    let mut s3 = Session::take_over(&ctx, "x");
    s2.resume(&ctx);
    assert_eq!(
        s3.got(),
        Vec::<String>::new(),
        "s3 was sent before its Resumed"
    );
    s3.resume(&ctx);
    assert_eq!(s3.got(), ["Resumed x", "Result x"], "s3 was sent");
    assert_eq!(s2.got(), ["Resumed x"], "s2 was sent");
    Ok(())
}

/// A takeover's delivery asked late, after a new run of the key replaced
/// its entry and was taken over in turn, sends that takeover nothing
/// before its own Resumed: adoption numbers are unique in the worker.
#[test]
fn a_late_delivery_for_a_replaced_entry_waits_for_the_new_resumed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = finished_k(dir.path(), "old")?;
    let s2 = Session::take_over(&ctx, "x");
    let b2 = dir.path().join("builds/b2");
    fs::create_dir_all(&b2)?;
    let fin = Some(ended(&b2, ""));
    ResumableBuild::insert(&ctx, "k".into(), "anew".into(), b2, fin);
    let mut s3 = Session::take_over(&ctx, "y");
    let (c, n) = (ctx.clone(), s2.adoption);
    bounded("delivered", move || try_deliver(&c, "k", Some(n)));
    assert_eq!(
        s3.got(),
        Vec::<String>::new(),
        "s3 was sent before its Resumed"
    );
    s3.resume(&ctx);
    assert_eq!(s3.got(), ["Resumed y", "Result y"], "s3 was sent");
    Ok(())
}

/// The hub requeues a job whose session died and assigns it again under
/// the same build id: the worker's next session is sent the result, after
/// its Resumed, whether the delivery to the session before failed (as the
/// run ended, or later) or reached it unacknowledged.
#[test]
fn a_same_id_takeover_after_a_delivery_is_sent_the_result() -> Result<()> {
    for before in ["failed at the end", "failed later", "unacknowledged"] {
        let dir = tempfile::tempdir()?;
        let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
        let b1 = dir.path().join("builds/b1");
        fs::create_dir_all(&b1)?;
        let fin = (before == "failed later").then(|| ended(&b1, ""));
        ResumableBuild::insert(&ctx, "k".into(), "x".into(), b1.clone(), fin);
        let (s1, mut s1_rx) = tokio::sync::mpsc::channel(16);
        ctx.resumable.lock().unwrap().get_mut("k").unwrap().out_tx = Some(s1);
        if before != "unacknowledged" {
            s1_rx.close();
        }
        let (c, fin) = (ctx.clone(), ended(&b1, ""));
        if before == "failed later" {
            bounded("delivered", move || try_deliver(&c, "k", None));
        } else {
            bounded("recorded", move || record_finished(&c, "k", fin));
        }
        let first = s1_rx.try_recv().ok().and_then(|m| m.msg);
        let reached = matches!(first, Some(worker_message::Msg::Result(_)));
        assert_eq!(reached, before == "unacknowledged", "{before}: s1 was sent");
        drop(s1_rx);
        let mut s2 = Session::take_over(&ctx, "x");
        s2.resume(&ctx);
        assert_eq!(s2.got(), ["Resumed x", "Result x"], "{before}: s2 was sent");
    }
    Ok(())
}

/// A takeover of a running build is sent nothing before its Resumed,
/// also when the run ends between: its result waits for that.
#[test]
fn a_run_that_ends_before_its_takeovers_resumed_waits_for_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
    let b1 = dir.path().join("builds/b1");
    fs::create_dir_all(&b1)?;
    ResumableBuild::insert(&ctx, "k".into(), "old".into(), b1.clone(), None);
    let mut new = Session::take_over(&ctx, "new");
    let (c, fin) = (ctx.clone(), ended(&b1, ""));
    bounded("recorded", move || record_finished(&c, "k", fin));
    assert_eq!(
        new.got(),
        Vec::<String>::new(),
        "new was sent before its Resumed"
    );
    new.resume(&ctx);
    assert_eq!(new.got(), ["Resumed new", "Result new"], "new was sent");
    Ok(())
}

/// A delivery that ends after a new run of its key replaced its entry
/// leaves that run's delivery marked as running: no second one starts
/// beside it.
#[test]
fn a_delivery_leaves_a_new_runs_entry_alone() -> Result<()> {
    let mut d = delivering()?;
    let b2 = d.dir.path().join("builds/b2");
    fs::create_dir_all(&b2)?;
    let fin = Some(ended(&b2, ""));
    ResumableBuild::insert(&d.ctx, "k".into(), "anew".into(), b2, fin);
    let (anew, mut anew_rx) = tokio::sync::mpsc::channel(1);
    anew.try_send(msg(worker_message::Msg::Resumed(Default::default())))
        .unwrap();
    d.ctx.resumable.lock().unwrap().get_mut("k").unwrap().out_tx = Some(anew);
    let second_ctx = d.ctx.clone();
    let second = thread::spawn(move || try_deliver(&second_ctx, "k", None));
    until("delivering the new run's result", || {
        d.ctx.resumable.lock().unwrap()["k"].delivering
    });
    d.end_the_delivery_before();
    let marked = d.ctx.resumable.lock().unwrap()["k"].delivering;
    anew_rx.close();
    until("the new run's delivery ended", || second.is_finished());
    assert!(marked, "the delivery before unmarked the new run's");
    Ok(())
}
