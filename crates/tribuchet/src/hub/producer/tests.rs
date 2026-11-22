//! Producers at the hub: DD8a (DT17, DT20) and R1 (DT19), driving the real
//! HubState through the attach service and supervise_job.

use std::collections::HashSet;
use std::io;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use super::super::caps::CapsGuard;
use super::super::losses::Run;
use super::super::placement::Size;
use super::super::relay::verify_set_for_test as verify;
use super::super::state::WorkerCaps;
use super::super::submit::dedupe_key;
use super::super::tests::{H, job};
use super::super::{Admission, Router, WorkerSession, supervise_job};
use super::*;
use crate::proto::{
    AttachEvent, BUILDER_RPC, BuildRequest, BuildResult, DECLINE_EXIT_CODE, NETWORK, Need,
    RelayReady, RelayStarted, ResultGone, Resumed, Started, TmpDirChunk, attach_event::Event,
    hub_message, worker_message,
};
use std::sync::Mutex;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tower::service_fn;

use super::super::submit::AttachSvc;
use crate::proto::attach_hub_client::AttachHubClient;
use crate::proto::attach_hub_server;

/// A producer's request from an attach whose client's Nix can build it
/// itself or not.
fn producer_request(can_local: Option<bool>) -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        outputs: [("out".to_owned(), format!("/nix/store/{H}-producer"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        required_features: vec![crate::proto::BUILDER_RPC.into()],
        attach_id: vec![7; 16],
        can_local,
        ..Default::default()
    }
}

/// A worker serving producers, `holding` the keys it lists.
fn worker(state: &Arc<HubState>, name: &str, holding: &[String]) -> CapsGuard {
    let caps = WorkerCaps {
        name: name.into(),
        connected_at: Instant::now(),
        systems: [(
            "x86_64-linux".to_owned(),
            HashSet::from([BUILDER_RPC.to_owned(), NETWORK.to_owned()]),
        )]
        .into(),
        max_jobs: 1,
    };
    let guard = CapsGuard::new(state.clone(), caps);
    state.hold(guard.id, holding, false);
    guard
}

/// A client of the attach service of `state` on a connection of its own
/// (as hub::tests::try_submit's).
async fn connect_attach(state: &Arc<HubState>) -> AttachHubClient<Channel> {
    let svc = attach_hub_server::AttachHubServer::new(AttachSvc {
        state: state.clone(),
    });
    let (client, server) = tokio::io::duplex(1 << 16);
    tokio::spawn(
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::once(Ok::<_, io::Error>(server))),
    );
    let mut client = Some(client);
    let channel = Endpoint::from_static("http://hub.invalid")
        .connect_with_connector(service_fn(move |_: Uri| {
            let io = client.take().map(TokioIo::new);
            async move { io.ok_or_else(|| io::Error::other("connected once")) }
        }))
        .await
        .unwrap();
    AttachHubClient::new(channel)
}

/// Submit `req` on a stream kept open, as a producer's attach does: the
/// sender for what it sends later, and the events it reads.
async fn submit_open(
    state: &Arc<HubState>,
    req: BuildRequest,
) -> (mpsc::Sender<BuildMessage>, Streaming<AttachEvent>) {
    try_submit_open(state, req).await.unwrap()
}

/// `submit_open`, or the hub's refusal.
async fn try_submit_open(
    state: &Arc<HubState>,
    req: BuildRequest,
) -> Result<(mpsc::Sender<BuildMessage>, Streaming<AttachEvent>), tonic::Status> {
    let (tx, rx) = mpsc::channel(8);
    tx.send(BuildMessage {
        msg: Some(build_message::Msg::Request(req)),
    })
    .await
    .unwrap();
    let eof = TmpDirChunk {
        zstd_chunk: Vec::new(),
        eof: true,
    };
    tx.send(BuildMessage {
        msg: Some(build_message::Msg::TmpDir(eof)),
    })
    .await
    .unwrap();
    let mut client = connect_attach(state).await;
    let events = client.build(ReceiverStream::new(rx)).await?;
    Ok((tx, events.into_inner()))
}

/// The run of `job` reaches its client: its start told the attach a
/// token, with which the attach's RelayStarted is acknowledged.
async fn relay_started(
    job: &Job,
    tx: &mpsc::Sender<BuildMessage>,
    events: &mut Streaming<AttachEvent>,
) {
    relay_ready(job, &[5; 16], true).await;
    let handoff_token = vec![5; 16];
    let relay = build_message::Msg::RelayStarted(RelayStarted { handoff_token });
    tx.send(BuildMessage { msg: Some(relay) }).await.unwrap();
    let acked = until(events, |e| *e == Event::RelayStartedAck(vec![5; 16])).await;
    assert!(acked, "RelayStarted not acknowledged");
}

/// Read `events` until `want` arrives; false if the stream ends first.
async fn until(events: &mut Streaming<AttachEvent>, want: impl Fn(&Event) -> bool) -> bool {
    let read = async {
        while let Ok(Some(ev)) = events.message().await {
            if ev.event.as_ref().is_some_and(&want) {
                return true;
            }
        }
        false
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .unwrap_or(false)
}

/// What a client of a build no longer handed out is told.
#[derive(Clone, Debug, PartialEq)]
enum Verdict {
    Declined,
    Failed(String),
}

/// The client's verdict, once its stream ended; None if none came.
async fn verdict(events: &mut Streaming<AttachEvent>) -> Option<Verdict> {
    let mut verdict = None;
    let read = async {
        while let Ok(Some(ev)) = events.message().await {
            match ev.event {
                Some(Event::ExitCode(DECLINE_EXIT_CODE)) => {
                    verdict = Some(Verdict::Declined);
                }
                Some(Event::Error(e)) => verdict = Some(Verdict::Failed(e)),
                _ => {}
            }
        }
    };
    let ended = tokio::time::timeout(Duration::from_secs(5), read).await;
    assert!(ended.is_ok(), "the client's stream did not end");
    verdict
}

/// Dispatch `job` to `w`, which says `said` about it before its session
/// dies: what the hub sent that worker.
async fn dispatch(
    state: &Arc<HubState>,
    job: Job,
    w: CapsGuard,
    said: Vec<worker_message::Msg>,
) -> Vec<hub_message::Msg> {
    let router = Router::default();
    let in_rx = router.register(&job.id);
    for m in said {
        router.route(m).await;
    }
    let (out_tx, mut out_rx) = mpsc::channel(64);
    let session = (w.id, w.caps.name.clone());
    router.close_all();
    drop(w);
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
    let mut sent = Vec::new();
    while let Ok(Ok(m)) = out_rx.try_recv() {
        sent.extend(m.msg);
    }
    sent
}

fn started(job: &Job) -> worker_message::Msg {
    worker_message::Msg::Started(Started {
        build_id: job.id.clone(),
        run: None,
    })
}

fn result_gone(job: &Job) -> worker_message::Msg {
    worker_message::Msg::ResultGone(ResultGone {
        build_id: job.id.clone(),
        run: None,
    })
}

fn need(job: &Job) -> worker_message::Msg {
    worker_message::Msg::Need(Need {
        build_id: job.id.clone(),
        ..Default::default()
    })
}

fn assignments(sent: &[hub_message::Msg]) -> usize {
    sent.iter()
        .filter(|m| matches!(m, hub_message::Msg::Assignment(_)))
        .count()
}

/// How the run of a dispatched producer is lost (hub/losses.rs `lost`
/// and the takes that settle a loss).
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Its worker says it no longer has the run (`gone`).
    Gone,
    /// The session dies with the run started; the worker is back without it.
    BackWithout,
    /// The session dies with the run started; the worker is back listing
    /// its key, and stages it afresh when handed it (`dispatch.staged`).
    StagedAfresh,
    /// The session dies with the run started; the worker is not back
    /// within its grace (a loss in transport).
    PastGrace,
}

/// A producer whose client can build it (`can_local`) or not, its run
/// relayed to the client (RelayStarted acknowledged) or not, lost as
/// `shape`: the assignments its first take sent, those every take after
/// sent (or None where no take had it), and the client's verdict.
async fn lose(
    shape: Shape,
    can_local: bool,
    relay: bool,
) -> (usize, Option<usize>, Option<Verdict>) {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let w1 = worker(&state, "w1", &[]);
    let (tx, mut events) = submit_open(&state, producer_request(Some(can_local))).await;
    let job = state.take_job(&w1.caps, None).await.expect("not queued");
    // Its run reaches the client, as attach says before its first write.
    if relay {
        relay_started(&job, &tx, &mut events).await;
    }
    let key = job.key.clone();
    let said = match shape {
        Shape::Gone => vec![started(&job), result_gone(&job)],
        _ => vec![started(&job)],
    };
    let first = assignments(&dispatch(&state, job, w1, said).await);
    let again = match shape {
        Shape::Gone => (state.take_job(&worker(&state, "w2", &[]).caps, None).await).map(|_| 1),
        Shape::BackWithout | Shape::PastGrace => {
            let back = worker(&state, "w1", &[]);
            state.take_job(&back.caps, None).await.map(|_| 1)
        }
        Shape::StagedAfresh => {
            let back = worker(&state, "w1", &[key]);
            match state.take_job_by_key(back.id).await {
                Some(j) => {
                    let n = need(&j);
                    let sent = dispatch(&state, j, back, vec![n]).await;
                    let staged = sent
                        .iter()
                        .any(|m| matches!(m, hub_message::Msg::TmpDir(_)));
                    if relay {
                        assert!(!staged, "a relayed producer's afresh staging went on");
                    }
                    // Staged and requeued, or settled.
                    (!state.queue.lock().await.is_empty()).then_some(1)
                }
                None => None,
            }
        }
    };
    drop(tx);
    if again.is_some() {
        return (first, again, None);
    }
    (first, again, verdict(&mut events).await)
}

const SHAPES: [Shape; 4] = [
    Shape::Gone,
    Shape::BackWithout,
    Shape::StagedAfresh,
    Shape::PastGrace,
];

/// DT20 (DD8a): after the hub acknowledged RelayStarted, a loss of the
/// producer's run in any shape `lost` and the takes tell apart leads to
/// no fresh run: a client whose Nix builds it is declined (222), one
/// whose Nix cannot gets hub_cannot's error, and nothing is requeued.
/// Positive control: the same losses without RelayStarted are dispatched
/// again under the loss policy.
#[tokio::test]
async fn a_relayed_producer_lost_is_never_dispatched_again() {
    let failed = Some(Verdict::Failed(RELAYED_LOST.to_owned()));
    for shape in SHAPES {
        let local = lose(shape, true, true).await;
        assert_eq!(
            local,
            (1, None, Some(Verdict::Declined)),
            "{shape:?} can_local"
        );
        let other = lose(shape, false, true).await;
        assert_eq!(other, (1, None, failed.clone()), "{shape:?} not can_local");
        let control = lose(shape, false, false).await;
        assert_eq!(
            control.1,
            Some(1),
            "{shape:?} without RelayStarted: not run again"
        );
    }
}

/// DT20, the fifth shape: a hub restart. The attach resubmits carrying
/// relay_started, which the new hub has never seen acknowledged: no
/// fresh take has it, a resume by key does, the key being the one the
/// worker holds, that of the submission before RelayStarted. Positive
/// control: without relay_started a fresh take has it.
#[tokio::test]
async fn a_relayed_producer_after_a_hub_restart_is_only_resumed() {
    for (relayed, can_local, fresh, verdict_want) in [
        (true, true, false, Some(Verdict::Declined)),
        (
            true,
            false,
            false,
            Some(Verdict::Failed(RELAYED_LOST.to_owned())),
        ),
        (false, false, true, None),
    ] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let w = worker(&state, "w1", &[]);
        let req = BuildRequest {
            relay_started: relayed,
            ..producer_request(Some(can_local))
        };
        let (tx, mut events) = submit_open(&state, req).await;
        let taken = state.take_job(&w.caps, None).await;
        assert_eq!(taken.is_some(), fresh, "relayed {relayed}: fresh take");
        drop(tx);
        if !fresh {
            assert_eq!(verdict(&mut events).await, verdict_want);
        }
        // The worker holding the run takes it back by key.
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let key = dedupe_key(&producer_request(Some(can_local)), b"");
        let req = BuildRequest {
            relay_started: relayed,
            ..producer_request(Some(can_local))
        };
        let back = worker(&state, "w1", &[key]);
        let _client = submit_open(&state, req).await;
        let resumed = state.take_job_by_key(back.id).await;
        assert!(resumed.is_some(), "relayed {relayed}: not resumed by key");
        let j = resumed.unwrap();
        let r = worker_message::Msg::Resumed(Resumed {
            build_id: j.id.clone(),
            finished: false,
            run: None,
        });
        let sent = dispatch(&state, j, back, vec![r]).await;
        assert_eq!(assignments(&sent), 1);
        assert_eq!(
            state.queue.lock().await.len(),
            1,
            "a resume is no loss of the run"
        );
    }
}

/// DT17 (DD8a): a loss after RelayStarted is answered by hub_cannot
/// unchanged: 222 for a client whose Nix builds it, which rebuilds it in a
/// fresh builder, the error for one whose Nix cannot. A loss before it is
/// answered by a fresh dispatch, failing nobody.
#[tokio::test]
async fn a_relayed_producer_is_never_rerun_behind_its_attach_and_is_declined_to_a_local_client() {
    assert_eq!(
        lose(Shape::Gone, true, true).await.2,
        Some(Verdict::Declined)
    );
    let failed = Verdict::Failed(RELAYED_LOST.to_owned());
    assert_eq!(lose(Shape::Gone, false, true).await.2, Some(failed));
    for can_local in [true, false] {
        let before = lose(Shape::Gone, can_local, false).await;
        assert_eq!(before, (1, Some(1), None), "before the first byte");
    }
}

/// The record's drv_path row: only where build.json carried drvPath.
#[test]
fn only_a_request_with_a_drv_path_is_recorded_with_one() {
    let mut j = job(1, Arc::new(Replay::default()));
    assert!(drv_path_row(&j).is_none());
    j.req.drv_path = Some(format!("/nix/store/{H}-p.drv"));
    let row = drv_path_row(&j).expect("no row");
    assert_eq!(row.values[3], format!("/nix/store/{H}-p.drv").into());
}

#[path = "admission_tests.rs"]
mod admission;

/// DT7 (DD9, DD6): another goal's attach submitting the same producer (the
/// same request but its attach_id) never joins its run and never waits on
/// it: each is queued as a build of its own. The same attach reconnecting
/// joins its own.
#[tokio::test]
async fn another_goal_never_joins_and_never_waits() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let _w = worker(&state, "w1", &[]);
    let first = producer_request(Some(true));
    let other = BuildRequest {
        attach_id: vec![8; 16],
        ..first.clone()
    };
    let submitted = |req| tokio::time::timeout(Duration::from_secs(5), submit_open(&state, req));
    let _a = submitted(first.clone()).await.expect("the first waits");
    let _b = submitted(other).await.expect("another goal's waits");
    assert_eq!(state.queue.lock().await.len(), 2, "another goal joined");
    let _again = submitted(first).await.expect("the same attach waits");
    assert_eq!(
        state.queue.lock().await.len(),
        2,
        "the same attach did not join"
    );
}

/// DT10 (DD6): a producer's result carries no output; one that does is
/// refused, one without is delivered. Every other build's result is
/// checked against its requested outputs.
#[tokio::test]
async fn a_producer_result_with_an_output_is_refused() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let _w = worker(&state, "w1", &[]);
    let _c = submit_open(&state, producer_request(Some(true))).await;
    let job = state.take_job(&serving_caps(), None).await.unwrap();
    let out = job.req.outputs["out"].clone();
    assert!(
        verify(vec![out.clone()], &outputs(&job)).is_err(),
        "an output"
    );
    assert!(verify(vec![], &outputs(&job)).is_ok(), "none");
    let mut plain = job;
    plain.req.required_features.clear();
    assert!(verify(vec![out], &outputs(&plain)).is_ok());
    assert!(verify(vec![], &outputs(&plain)).is_err());
}

fn serving_caps() -> WorkerCaps {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    worker(&state, "w", &[]).caps.clone()
}

/// DT15, the hub's half (DD10): a producer's run whose listener no attach
/// fetched (handoff_expired) is a loss in transport (how the farm moved
/// it, hub/losses.rs), settled within the caps by a fresh dispatch: never
/// a build result, never counted against the run, never isolating its key.
#[tokio::test]
async fn a_handoff_expired_is_settled_as_a_loss() {
    // A grace to wait out, were the run taken for one its worker may hold.
    let state = Arc::new(HubState::for_test(Duration::from_mins(1)));
    let w1 = worker(&state, "w1", &[]);
    let (_tx, _events) = submit_open(&state, producer_request(Some(true))).await;
    let job = state.take_job(&w1.caps, None).await.unwrap();
    // As the worker sends it (worker/resume/delivery.rs): a failure.
    let expired = worker_message::Msg::Result(BuildResult {
        build_id: job.id.clone(),
        exit_code: 1,
        error: "no attach fetched the producer's listener in time".into(),
        handoff_expired: true,
        ..Default::default()
    });
    let said = vec![started(&job), expired];
    let _ = dispatch(&state, job, w1, said).await;
    let queue = state.queue.lock().await;
    assert_eq!(queue.len(), 1, "not requeued");
    let losses = (queue[0].attempts, queue[0].transport_losses);
    assert_eq!(losses, (0, 1), "not counted as a loss in transport");
    assert!(!state.lost.has(&queue[0].key), "its key isolated");
    let run = queue[0]
        .loss
        .as_ref()
        .map(|l| (l.run, l.until > Instant::now()));
    assert_eq!(run, Some((Run::Unstarted, false)), "held for its worker");
    assert_eq!(
        state
            .metrics
            .succeeded
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[path = "relay_tests.rs"]
mod relay;

/// A relayed producer whose run is lost, taken afresh by a worker with a
/// placement session: the take commits it there (its key's hold and its
/// clients' charge, state/take.rs), and the loss settles it with no fresh
/// run (lost_relayed, losses::cannot), which ends both (HubState::finish):
/// the session holds nothing of it, no client is charged for it, and the
/// next build of its key is not held for it.
#[tokio::test]
async fn a_relayed_producer_lost_at_a_fresh_take_leaves_no_hold_or_charge() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let w1 = worker(&state, "w1", &[]);
    let (tx, mut events) = submit_open(&state, producer_request(Some(false))).await;
    let first = state.take_job(&w1.caps, None).await.expect("not queued");
    relay_started(&first, &tx, &mut events).await;
    let said = vec![started(&first)];
    assert_eq!(assignments(&dispatch(&state, first, w1, said).await), 1);
    let back = worker(&state, "w1", &[]);
    let size = Size {
        cores: 4.0,
        memory: None,
    };
    let admission = Arc::new(Mutex::new(Admission::default()));
    (state.placement.lock().unwrap()).open(back.id, admission, size);
    let before = state.placement.lock().unwrap().charges.clients();
    let taken = state.take_job(&back.caps, Some(back.id)).await;
    assert!(taken.is_none(), "a relayed producer was run afresh");
    let p = state.placement.lock().unwrap();
    assert!(p.sessions[&back.id].committed.is_empty(), "still committed");
    assert_eq!(p.charges.clients(), before, "a client still charged");
    let mut next = job(1, Arc::new(Replay::default()));
    next.req = producer_request(Some(false));
    assert!(!p.held_for_key(&next), "its key still held");
    drop(p);
    drop(tx);
}
