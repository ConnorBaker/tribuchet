//! A build waiting for a build agent: the wait, and what its client is
//! told.

use std::collections::HashMap;
use std::iter;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use super::tests::base_assignment;
use super::*;
use crate::proto::{WorkerMessage, worker_message};
use crate::worker::hostname;
use crate::worker::resume::ResumableBuild;

type Woken = mpsc::UnboundedReceiver<Wake>;

/// A worker whose only build agent is down, and a build in its
/// registry for the session behind `out_tx`, as launch_build leaves it.
pub(super) fn waiting_build(
    dir: &Path,
    a: BuildAssignment,
    out_tx: &mpsc::Sender<WorkerMessage>,
) -> Result<(Arc<WorkerCtx>, ActiveBuild, Woken)> {
    let ctx = WorkerCtx::for_test(dir, 1, vec![dir.join("down.sock")])?;
    let (wake, woken) = mpsc::unbounded_channel();
    let build = ActiveBuild::new(a, ctx.clone(), wake)?;
    let (key, id) = (
        build.assignment.dedupe_key.clone(),
        build.assignment.build_id.clone(),
    );
    ResumableBuild::insert(&ctx, key.clone(), id, build.dir.clone(), None);
    let mut map = ctx.resumable.lock().unwrap();
    let e = map.get_mut(&key).expect("just inserted");
    e.out_tx = Some(out_tx.clone());
    e.worker_name.clone_from(&build.assignment.worker_name);
    drop(map);
    Ok((ctx, build, woken))
}

/// The notes sent so far, as (build id, text).
fn notes(rx: &mut mpsc::Receiver<WorkerMessage>) -> Vec<(String, String)> {
    iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|m| match m.msg {
            Some(worker_message::Msg::Log(l)) => {
                Some((l.build_id, String::from_utf8(l.data).unwrap()))
            }
            _ => None,
        })
        .collect()
}

/// The notes `rx` gets: those within 0.5 s of the first, once there are
/// some (for 10 s), or none in 0.5 s.
fn told(rx: &mut mpsc::Receiver<WorkerMessage>, some: bool) -> Vec<(String, String)> {
    let end = Instant::now() + Duration::from_millis(if some { 10_000 } else { 500 });
    let mut got = notes(rx);
    while got.is_empty() && Instant::now() < end {
        thread::sleep(Duration::from_millis(10));
        got = notes(rx);
    }
    if !got.is_empty() {
        thread::sleep(Duration::from_millis(500));
        got.extend(notes(rx));
    }
    got
}

/// When another session, which knows the worker as "w-new", takes the
/// build over: never, before its wait starts, once the first session was
/// told, or while the first is full, which then takes it back.
#[derive(Clone, Copy, PartialEq)]
enum Takeover {
    Never,
    Before,
    After,
    Back,
}
use Takeover::{After, Back, Before, Never};

/// A build whose only agent is down waits for one, however short its
/// timeout, until it is cancelled (it does not fail once its timeout
/// passes), and its client is told it waits. Rows: the launching
/// session's capacity, the messages queued in it first, whether the
/// build is cancelled while it is full,
/// the hub's name for the worker ("": predating worker_name, hostname),
/// the takeover, the wait's restart, and whether each session is told.
const ROWS: &[(usize, usize, bool, &str, Takeover, bool, bool, bool)] = &[
    // Told once the session has room, after what was queued first.
    (
        2,
        2,
        false,
        "w-as-the-hub-knows-it",
        Never,
        false,
        true,
        false,
    ),
    // A full session blocks no cancel, and a wait that ended tells no one.
    (1, 1, true, "w", Never, false, false, false),
    // A session taking over a waiting build is told again: the note the
    // last one got may be lost, or its client gone.
    (4, 0, false, "", After, false, true, true),
    // One that took it over before the wait started is told then, under
    // its own build id, and the launching session is not.
    (4, 0, false, "", Before, false, false, true),
    // A wait that ends and starts again before the full session has room
    // is told once: the note of the one that ended is not sent.
    (1, 1, false, "w", Never, true, true, false),
    // A full session given the build back during the wait is told once:
    // the note pending from before the takeover is not sent.
    (1, 1, false, "w", Back, false, true, true),
];

#[test]
fn every_wait_is_told_as_the_table_says() -> Result<()> {
    let on = |name: &str| {
        format!(
            "tribuchet: no build agent on worker {name} is free or reachable; waiting for one\n"
        )
    };
    // The runtime the notes are sent from, as the worker's.
    let rt = tokio::runtime::Runtime::new()?;
    let _rt = rt.enter();
    for (row, &(cap, queued, full, name, takeover, restart, old_told, new_told)) in
        ROWS.iter().enumerate()
    {
        let dir = tempfile::tempdir()?;
        let (old_tx, mut old_rx) = mpsc::channel(cap);
        let a = BuildAssignment {
            worker_name: name.into(),
            ..base_assignment()
        };
        let (ctx, build, _woken) = waiting_build(dir.path(), a, &old_tx)?;
        for _ in 0..queued {
            old_tx.try_send(WorkerMessage::default()).unwrap();
        }
        let again = BuildAssignment {
            build_id: "0123456789abcdef0123456789abcde2".into(),
            worker_name: "w-new".into(),
            ..build.assignment.clone()
        };
        let (new_tx, mut new_rx) = mpsc::channel(4);
        let take = || assert!(ctx.adopt_assignment(&again, &new_tx), "row {row}: not held");
        if takeover == Before {
            take();
        }
        let old_id = build.assignment.build_id.clone();
        let (back, old_back) = (build.assignment.clone(), old_tx.clone());
        let on_rt = rt.handle().clone();
        let running = thread::spawn(move || {
            let _rt = on_rt.enter();
            build.execute(&old_tx, Duration::from_millis(1))
        });
        let waits = |m: &HashMap<String, ResumableBuild>| m["test-key"].waiting_for_agent;
        let end = Instant::now() + Duration::from_secs(10);
        while !waits(&ctx.resumable.lock().unwrap()) {
            let waiting = Instant::now() < end && !running.is_finished();
            assert!(waiting, "row {row}: the build did not wait");
            thread::sleep(Duration::from_millis(10));
        }
        if restart {
            ctx.wait_for_agent("test-key", false);
            ctx.wait_for_agent("test-key", true);
        }
        let mut new = Vec::new();
        if takeover == Back {
            take();
            new = told(&mut new_rx, true);
            assert!(
                ctx.adopt_assignment(&back, &old_back),
                "row {row}: not held"
            );
        }
        let cancel = || ctx.cancelled.lock().unwrap().insert("test-key".into());
        if full {
            cancel();
            let end = Instant::now() + Duration::from_secs(5);
            while !running.is_finished() {
                assert!(Instant::now() < end, "row {row}: stuck on the full session");
                thread::sleep(Duration::from_millis(10));
            }
        }
        for _ in 0..queued {
            assert_eq!(
                old_rx.try_recv().ok(),
                Some(WorkerMessage::default()),
                "row {row}: queued first"
            );
        }
        let old = told(&mut old_rx, old_told);
        if takeover == After {
            take();
        }
        new.extend(told(&mut new_rx, new_told && new.is_empty()));
        cancel();
        let res = running.join().unwrap().map_err(|e| chain(&e));
        assert_eq!(res.err().as_deref(), Some("build cancelled"), "row {row}");
        // Ends the log tailer a takeover started.
        ctx.resumable.lock().unwrap().remove("test-key");
        let named = if name.is_empty() {
            hostname()
        } else {
            name.into()
        };
        let want = |t, id: &str, name: &str| {
            if t {
                vec![(id.to_owned(), on(name))]
            } else {
                vec![]
            }
        };
        assert_eq!(
            old,
            want(old_told, &old_id, &named),
            "row {row}: launching session"
        );
        assert_eq!(
            new,
            want(new_told, &again.build_id, "w-new"),
            "row {row}: new session"
        );
    }
    Ok(())
}
