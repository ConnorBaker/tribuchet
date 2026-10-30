//! Worker-session losses of a dispatched build: which count against
//! MAX_JOB_ATTEMPTS, and which against MAX_RESUMED_LOSSES.

use super::fake_daemon::told;
use super::*;

use crate::proto::{self, LogChunk, Need};
use Said::{Nothing, Resumed, Staged};
use Take::{Back, Fresh};
use losses::MAX_JOB_ATTEMPTS;

/// One queued build with its client attached.
async fn queued_build(state: &HubState) -> mpsc::Receiver<Result<AttachEvent, Status>> {
    let j = job(1, Arc::new(Replay::default()));
    let client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    client
}

/// The first Need of a fresh staging, empty when no input is missing.
fn staged(job: &Job) -> Option<worker_message::Msg> {
    Some(worker_message::Msg::Need(Need {
        build_id: job.id.clone(),
        paths: vec![],
        hashes: vec![],
    }))
}

fn resume(job: &Job) -> Option<worker_message::Msg> {
    Some(worker_message::Msg::Resumed(proto::Resumed {
        build_id: job.id.clone(),
    }))
}

/// The worker `name` is back holding the build, and is handed it.
async fn back(state: &Arc<HubState>, name: &str) -> Option<(Job, CapsGuard)> {
    let worker = register(state, name, &["k1"]);
    let job = state.take_job_by_key(worker.id).await?;
    Some((job, worker))
}

/// The client's verdict, if it got one: the error it was sent, and
/// whether its stream then ended.
async fn verdict(
    client: &mut mpsc::Receiver<Result<AttachEvent, Status>>,
) -> (Option<String>, bool) {
    let mut error = None;
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(ev) = client.recv().await {
            if let Ok(AttachEvent {
                event: Some(attach_event::Event::Error(e)),
            }) = ev
            {
                error = Some(e);
            }
        }
    })
    .await
    .is_ok();
    (error, ended)
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
    Resumed,
    Nothing,
}

/// The queued build, taken as `take` after dispatches taken and lost as
/// `lost`: its genuine losses, losses on a resume and whether a loss is
/// pending; or, once it is no longer handed out, the client's verdict,
/// which ends its stream, the build counted failed and not queued.
async fn after_losses(lost: &[(Take, Said)], take: Take) -> Result<(u32, u32, bool), String> {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let mut client = queued_build(&state).await;
    for (n, &(t, said)) in lost.iter().enumerate() {
        let (j, w) = taken(&state, t)
            .await
            .ok_or_else(|| format!("not handed out after {n} losses"))?;
        let msg = match said {
            Said::Staged => staged(&j),
            Said::Resumed => resume(&j),
            Said::Nothing => None,
        };
        lose_session(&state, j, w, msg).await;
    }
    if let Some((j, _)) = taken(&state, take).await {
        return Ok((j.attempts, j.resumed_losses, j.loss.is_some()));
    }
    let (error, ended) = verdict(&mut client).await;
    assert!(ended, "the client's stream did not end at the verdict");
    assert_eq!(state.metrics.failed.load(Ordering::Relaxed), 1);
    assert!(
        state.queue.lock().await.is_empty(),
        "the failed build is queued"
    );
    Err(error.unwrap_or_default())
}

async fn taken(state: &Arc<HubState>, take: Take) -> Option<(Job, CapsGuard)> {
    match take {
        Take::Fresh(name) => {
            let w = register(state, name, &[]);
            let j = state.take_job(&caps(name)).await?;
            Some((j, w))
        }
        Take::Back(name) => back(state, name).await,
    }
}

/// The loss policy, each row a sequence of losses:
/// a build survives MAX_JOB_ATTEMPTS genuine losses and the next fails
/// it, and MAX_RESUMED_LOSSES losses on a resume,
/// with the same verdict. The caps are pinned by number, so that
/// changing one is a decision.
#[tokio::test]
async fn each_sequence_of_losses_counts_as_decided() {
    let genuine = |n| {
        Err(format!(
            "build lost {n} times without a worker resuming it; giving up"
        ))
    };
    let on_resume = |n| {
        Err(format!(
            "build lost {n} times while its worker resumed it; giving up"
        ))
    };
    let rows = [
        // Each worker dies with the build staged and another takes it,
        // the common genuine loss: the fourth fails it.
        (
            [Fresh("w0"), Fresh("w1"), Fresh("w2"), Fresh("w3")]
                .map(|t| (t, Staged))
                .to_vec(),
            Fresh("w4"),
            genuine(4),
        ),
        // Another worker holding the build takes it by key: the loss
        // stands, as when it takes it fresh.
        (vec![(Fresh("w1"), Nothing)], Back("w2"), Ok((1, 0, false))),
        // After a hub restart, the worker holding the build resumes it,
        // starts it over three times, and resumes it: the 13th loss on a
        // resume fails it.
        (
            [
                vec![(Back("w1"), Resumed)],
                vec![(Back("w1"), Staged); 3],
                vec![(Back("w1"), Resumed); 12],
            ]
            .concat(),
            Back("w1"),
            on_resume(13),
        ),
        // Handed back the build it lost, a worker whose session dies
        // before it says whether it resumed or staged it settles
        // nothing: each is a loss on a resume, or the build would be
        // handed back forever.
        (
            [vec![(Fresh("w1"), Staged)], vec![(Back("w1"), Nothing); 13]].concat(),
            Back("w1"),
            on_resume(13),
        ),
        // Losses on a resume count per build, across the workers it moves
        // between: seven on w1, a start-over on w2, six on w2.
        (
            [
                vec![(Back("w1"), Resumed); 7],
                vec![(Fresh("w2"), Staged)],
                vec![(Back("w2"), Resumed); 6],
            ]
            .concat(),
            Back("w2"),
            on_resume(13),
        ),
    ];
    for (n, (lost, take, verdict)) in rows.into_iter().enumerate() {
        assert_eq!(after_losses(&lost, take).await, verdict, "row {n}");
    }
}

/// A worker whose session drops comes back holding the build and is
/// handed it; what it says then settles the loss before. Resuming it,
/// as a worker whose session flaps does, none is genuine (counting them
/// fails the build at the fourth): twice the genuine cap, so counting
/// all but one fails too, every other session dying between the build's
/// log line and its `Resumed`, the worker's order. Starting it over (it
/// was gone by then), each is: the build is not handed out after the fifth.
#[tokio::test]
async fn what_a_returning_worker_says_settles_the_loss_before() {
    let max = MAX_JOB_ATTEMPTS;
    for (resumes, losses) in [(true, 2 * (max + 1)), (false, max + 1)] {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let mut client = queued_build(&state).await;
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
                w.send(worker_message::Msg::Resumed(proto::Resumed { build_id }))
                    .await;
            }
            assert_eq!(told(&mut client).1, None, "after loss {loss}");
        }
        drop(w);
        let mut w = connect(&state, "w1", 1, &["k1"]);
        let handed_out = w.assignment().await.is_some();
        let failed = told(&mut client).1.is_some();
        assert_eq!(
            (handed_out, failed),
            (resumes, !resumes),
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
    let mut client = queued_build(&state).await;
    let w = register(&state, "w1", &[]);
    let job = state.take_job(&caps("w1")).await.expect("not handed out");
    let router = Router::default();
    let in_rx = router.register(&job.id);
    let (out_tx, out_rx) = mpsc::channel(64);
    drop(out_rx);
    let session = (w.id, w.caps.name.clone());
    let sess = Arc::new(WorkerSession::new("w".into()));
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
    let taken = state.take_job(&caps("w2")).await;
    assert_eq!(
        taken.map(|j| (j.attempts, j.resumed_losses, j.loss.is_some())),
        Some((0, 0, false))
    );
    assert_eq!(told(&mut client).1, None);
}
