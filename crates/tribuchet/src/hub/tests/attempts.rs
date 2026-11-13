//! Worker-session losses of a dispatched build: which count against
//! MAX_JOB_ATTEMPTS, and which in transport against MAX_TRANSPORT_LOSSES.

use super::clientless::sent;
use super::fake_daemon::told;
use super::*;

use crate::proto::{self, BuildResult, DECLINE_EXIT_CODE, LogChunk, Need};
use Said::{Delivered, Finished, Nothing, Resumed, Staged, Started};
use Take::{Back, Fresh};
use Verdict::{Declined, Failed, Gone};
use losses::{MAX_JOB_ATTEMPTS, MAX_TRANSPORT_LOSSES};

/// A build with network or not, and whether its client's Nix builds it.
type Kind = (bool, bool);

/// One queued build of `kind`, its client attached.
async fn queued_build(
    state: &HubState,
    (fod, can_local): Kind,
) -> mpsc::Receiver<Result<AttachEvent, Status>> {
    let mut j = job(1, Arc::new(Replay::default()));
    j.req.fixed_output = fod;
    let client = j
        .replay
        .subscribe_as(shadow::Client::untracked(can_local))
        .await;
    state.queue.lock().await.push_back(j);
    client
}

/// What the worker sends of `job`, saying `said`.
fn said(job: &Job, said: Said) -> Vec<worker_message::Msg> {
    let build_id = job.id.clone();
    let started = worker_message::Msg::Started(proto::Started {
        build_id: job.id.clone(),
        run: None,
    });
    match said {
        // The first Need of a fresh staging, empty when no input is missing.
        Staged => vec![worker_message::Msg::Need(Need {
            build_id,
            paths: vec![],
            hashes: vec![],
        })],
        Started => vec![started],
        Resumed => vec![resumed(&job.id, false)],
        Finished => vec![resumed(&job.id, true)],
        // Its result arrived, and its delivery is under way.
        Delivered => vec![
            started,
            worker_message::Msg::Result(BuildResult {
                build_id,
                ..Default::default()
            }),
        ],
        Nothing => vec![],
    }
}

/// The worker `name` is back holding the build, and is handed it.
async fn back(state: &Arc<HubState>, name: &str) -> Option<(Job, CapsGuard)> {
    let worker = register(state, name, &["k1"]);
    let job = state.take_job_by_key(worker.id).await?;
    Some((job, worker))
}

/// What a client is told of a build no longer handed out.
#[derive(Debug, PartialEq)]
enum Verdict {
    /// The build failed with this error.
    Failed(String),
    /// Its Nix is to build it (DECLINE_EXIT_CODE).
    Declined,
    /// It was no longer handed out after this many of the losses.
    Gone(usize),
}

/// The client's verdict, once its stream ended.
async fn verdict(client: &mut mpsc::Receiver<Result<AttachEvent, Status>>) -> Option<Verdict> {
    let mut verdict = None;
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(AttachEvent { event })) = client.recv().await {
            match event {
                Some(attach_event::Event::Error(e)) => verdict = Some(Failed(e)),
                Some(attach_event::Event::ExitCode(DECLINE_EXIT_CODE)) => verdict = Some(Declined),
                _ => {}
            }
        }
    })
    .await
    .is_ok();
    assert!(ended, "the client's stream did not end at the verdict");
    verdict
}

/// How a dispatch of the build is taken.
#[derive(Clone, Copy)]
enum Take {
    /// From the queue, by a worker of this name that holds nothing.
    Fresh(&'static str),
    /// By key, by a worker of this name back holding it.
    Back(&'static str),
}

/// What the worker says of the build before its session dies.
#[derive(Clone, Copy)]
enum Said {
    /// It asks for its inputs: it starts the build over.
    Staged,
    /// Its run starts.
    Started,
    /// It holds a run of the build, unfinished.
    Resumed,
    /// It holds a run of the build, finished.
    Finished,
    /// Its run starts and succeeds.
    Delivered,
    Nothing,
}

/// The queued build of `kind`, taken as `take` after
/// dispatches taken and lost as `lost`: its genuine losses, losses in
/// transport and whether a loss is pending; or, once it
/// is no longer handed out, the client's verdict, which ends its stream,
/// the build counted as such and not queued.
async fn after_losses(
    kind: Kind,
    lost: &[(Take, Said)],
    take: Take,
) -> Result<(u32, u32, bool), Verdict> {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let mut client = queued_build(&state, kind).await;
    for (n, &(t, s)) in lost.iter().enumerate() {
        let Some((j, w)) = taken(&state, t).await else {
            return Err(Gone(n));
        };
        let msgs = said(&j, s);
        lose_session(&state, j, w, msgs).await;
    }
    if let Some((j, _)) = taken(&state, take).await {
        let pending = j.loss.is_some();
        return Ok((j.attempts, j.transport_losses, pending));
    }
    let verdict = verdict(&mut client).await.expect("no verdict");
    let counted = if verdict == Declined {
        &state.metrics.declined
    } else {
        &state.metrics.failed
    };
    assert_eq!(
        counted.load(Ordering::Relaxed),
        1,
        "{verdict:?} not counted"
    );
    assert!(
        state.queue.lock().await.is_empty(),
        "the build is queued after its verdict"
    );
    Err(verdict)
}

async fn taken(state: &Arc<HubState>, take: Take) -> Option<(Job, CapsGuard)> {
    match take {
        Take::Fresh(name) => {
            let w = register(state, name, &[]);
            let j = state.take_job(&caps(name), None).await?;
            Some((j, w))
        }
        Take::Back(name) => back(state, name).await,
    }
}

/// The build's run succeeds, and its delivery dies with the session
/// MAX_TRANSPORT_LOSSES + 1 times, each time but the first resumed.
fn delivery_flaps() -> Vec<(Take, Said)> {
    let mut flaps = vec![(Fresh("w1"), Delivered)];
    flaps.extend([(Back("w1"), Finished); 12]);
    flaps
}

/// The loss policy, each row a sequence of losses:
/// a build survives MAX_JOB_ATTEMPTS genuine losses and the next fails
/// it, and MAX_TRANSPORT_LOSSES losses in transport, the next declining it.
/// The caps are pinned by number, so that changing one is a decision.
#[tokio::test]
async fn each_sequence_of_losses_counts_as_decided() {
    let genuine = |n| {
        Err(Failed(format!(
            "build lost {n} times without a worker resuming it; giving up"
        )))
    };
    let rows = [
        // Each worker dies with the build staged and another takes it:
        // no run of it was lost, but its transfer, and the 13th such loss
        // declines it, its client to build it.
        (vec![(Fresh("w"), Staged); 13], Fresh("w"), Err(Declined)),
        (
            vec![(Fresh("w"), Staged); 12],
            Fresh("w"),
            Ok((0, 12, false)),
        ),
        // So does the 13th session dying before its worker answered.
        (vec![(Fresh("w"), Nothing); 13], Fresh("w"), Err(Declined)),
        // A run that started, its worker back without it, is a genuine
        // loss: one that kills its worker every time fails at the fourth.
        (vec![(Fresh("w"), Started)], Fresh("w"), Ok((1, 0, false))),
        // Its worker not back within the grace may run it still: a loss
        // in transport.
        (vec![(Fresh("w1"), Started)], Fresh("w2"), Ok((0, 1, false))),
        (vec![(Fresh("w"), Started); 4], Fresh("w"), genuine(4)),
        // A finished run whose delivery dies with the session every time
        // is lost in transport, never failed: the 13th declines it.
        (delivery_flaps(), Back("w1"), Err(Declined)),
        // Another worker holding the build takes it by key: the run the
        // first started may go on, a loss in transport.
        (vec![(Fresh("w1"), Started)], Back("w2"), Ok((0, 1, false))),
        // A worker staging afresh the run it resumed no longer had it: a
        // genuine loss, and the session's a loss in transport.
        (
            vec![(Back("w1"), Resumed), (Back("w1"), Staged)],
            Back("w1"),
            Ok((1, 2, true)),
        ),
        // A finished run whose delivery died, its worker back staging the
        // build afresh: it dropped the result, which no run lost, so only
        // the two sessions count, in transport.
        (
            vec![(Fresh("w1"), Delivered), (Back("w1"), Staged)],
            Back("w1"),
            Ok((0, 2, true)),
        ),
        // A worker holding the build resumes it, its session dying every
        // time: it may run it still, so each is a loss in transport, never
        // failed, and the 13th declines it.
        (
            vec![(Back("w1"), Resumed); 12],
            Back("w1"),
            Ok((0, 12, true)),
        ),
        (vec![(Back("w1"), Resumed); 13], Back("w1"), Err(Declined)),
        // Handed back the build it lost, a worker whose session dies
        // before it says whether it resumed or staged it settles
        // nothing: each is a loss in transport, or the build would be
        // handed back forever, and the 13th declines it.
        (
            [vec![(Fresh("w1"), Staged)], vec![(Back("w1"), Nothing); 12]].concat(),
            Back("w1"),
            Err(Declined),
        ),
        // Losses in transport count per build, across the workers it
        // moves between: seven on w1, its run taken over by w2 (w1 not
        // back), a start-over on w2, and the 13th on w2 declines it.
        (
            [
                vec![(Back("w1"), Resumed); 7],
                vec![(Fresh("w2"), Staged)],
                vec![(Back("w2"), Resumed); 4],
            ]
            .concat(),
            Back("w2"),
            Err(Declined),
        ),
    ];
    let mut wrong = Vec::new();
    for (n, (lost, take, verdict)) in rows.into_iter().enumerate() {
        let after = after_losses((false, true), &lost, take).await;
        if after != verdict {
            wrong.push(format!("row {n}: {after:?}, not {verdict:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A build with network the farm keeps losing in transport is never
/// declined, which would build it in the client host's own network: it
/// keeps waiting for a worker. So does one its client's Nix would not
/// build (canLocal false), which declining would fail.
#[tokio::test]
async fn a_build_with_network_lost_in_transport_keeps_waiting() {
    let rows = [
        (vec![(Fresh("w"), Staged); 13], Fresh("w"), false),
        (vec![(Fresh("w"), Nothing); 13], Fresh("w"), false),
        (delivery_flaps(), Back("w1"), true),
        (vec![(Back("w1"), Resumed); 13], Back("w1"), true),
    ];
    let mut wrong = Vec::new();
    for kind in [(true, true), (false, false)] {
        for (n, (lost, take, pending)) in rows.clone().into_iter().enumerate() {
            let after = after_losses(kind, &lost, take).await;
            if after != Ok((0, 13, pending)) {
                wrong.push(format!("{kind:?} row {n}: {after:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A worker whose session drops comes back holding the build and is
/// handed it; what it says then settles the loss before. Resuming it,
/// as a worker whose session flaps does, none is genuine (counting them
/// fails the build at the fourth): twice the genuine cap, so counting
/// all but one fails too, every other session dying between the build's
/// log line and its `Resumed`, the worker's order. Staging it afresh,
/// each is a loss in transport: declined, not failed, after the 13th.
#[tokio::test]
async fn what_a_returning_worker_says_settles_the_loss_before() {
    let max = MAX_JOB_ATTEMPTS;
    for (resumes, losses) in [(true, 2 * (max + 1)), (false, MAX_TRANSPORT_LOSSES)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let mut client = queued_build(&state, (false, true)).await;
        let mut w = connect(&state, "w1", 1, &[]);
        w.to_hub.send(request_job(1, Some(0))).await.unwrap();
        assert!(w.assignment().await.is_some(), "first dispatch");
        for loss in 1..=losses {
            drop(w);
            w = connect(&state, "w1", 1, &["k1"]);
            let Some(a) = w.assignment().await else {
                panic!("not redispatched after loss {loss}, resumes: {resumes}");
            };
            let build_id = a.build_id;
            if !resumes {
                // The first Need of a fresh staging, empty when no input
                // is missing.
                let need = Need {
                    build_id,
                    paths: vec![],
                    hashes: vec![],
                };
                w.send(worker_message::Msg::Need(need)).await;
                continue;
            }
            let data = b"still building\n".to_vec();
            let log = LogChunk {
                build_id: build_id.clone(),
                data,
            };
            w.send(worker_message::Msg::Log(log)).await;
            if loss % 2 == 0 {
                w.send(resumed(&build_id, false)).await;
            }
            assert_eq!(told(&mut client).await.1, None, "after loss {loss}");
        }
        drop(w);
        let mut w = connect(&state, "w1", 1, &["k1"]);
        let handed_out = w.assignment().await.is_some();
        let failed = told(&mut client).await.1.is_some();
        let declined = state.metrics.declined.load(Ordering::Relaxed) == 1;
        assert_eq!(
            (handed_out, failed, declined),
            (resumes, false, !resumes),
            "resumes: {resumes}"
        );
    }
}

/// An assignment whose send failed never reached the worker: the build
/// is requeued with nothing counted, and another worker taking it then
/// counts no loss either.
#[tokio::test]
async fn an_assignment_the_worker_never_got_counts_no_loss() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let mut client = queued_build(&state, (false, true)).await;
    let w = register(&state, "w1", &[]);
    let job = state
        .take_job(&caps("w1"), None)
        .await
        .expect("not handed out");
    let router = Router::default();
    let in_rx = router.register(&job.id);
    let (out_tx, out_rx) = mpsc::channel(64);
    drop(out_rx);
    let session = (w.id, w.caps.name.clone());
    let sess = Arc::new(WorkerSession::new("w".into(), out_tx.clone()));
    let admission = Arc::new(Mutex::new(Admission::default()));
    supervise_job(
        state.clone(),
        job,
        router,
        out_tx,
        in_rx,
        sess,
        session,
        admission,
    )
    .await;
    drop(w);
    let _w2 = register(&state, "w2", &[]);
    let taken = state.take_job(&caps("w2"), None).await;
    assert_eq!(
        taken.map(|j| (j.attempts, j.transport_losses, j.loss.is_some())),
        Some((0, 0, false))
    );
    assert_eq!(told(&mut client).await.1, None);
}

/// A build the hub ends with no verdict from a run of it that may go on
/// has that run cancelled once its worker is back listing it: declined
/// at the transport cap, failed at the genuine cap, dropped as its client
/// left. A build still wanted keeps it. No timeout ends a run nobody
/// cancels: before, each such run held its slot until it ended.
#[tokio::test]
async fn a_run_of_a_build_the_hub_ended_is_cancelled_on_its_worker() {
    let id = format!("{:032x}", 1);
    let cancel = vec![(id, "k1".to_owned())];
    let rows = [
        (vec![(Back("w1"), Resumed); 13], false, cancel.clone()),
        (vec![(Fresh("w1"), Started); 5], false, cancel.clone()),
        (vec![(Fresh("w1"), Started)], true, cancel),
        (vec![(Fresh("w1"), Started)], false, vec![]),
    ];
    let mut wrong = Vec::new();
    for (n, (lost, clientless, want)) in rows.into_iter().enumerate() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let client = queued_build(&state, (false, true)).await;
        for &(t, s) in &lost {
            let Some((j, w)) = taken(&state, t).await else {
                break;
            };
            let msgs = said(&j, s);
            lose_session(&state, j, w, msgs).await;
        }
        if clientless {
            drop(client);
            let _w2 = register(&state, "w2", &[]);
            assert!(state.take_job(&caps("w2"), None).await.is_none(), "row {n}");
        }
        let mut w = connect(&state, "w1", 1, &["k1"]);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let cancels = sent(&mut w.from_hub).1;
        w.hub.abort();
        if cancels != want {
            wrong.push(format!("row {n}: {cancels:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
