//! What a session tells the hub of a run: that it started, or that the
//! run it resumed is finished.

use std::iter;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::*;
use crate::hub::msg_name;
use crate::proto::worker_hub_server::WorkerHubServer;
use crate::slowlink;
use crate::worker::build::tests::base_assignment;
use crate::worker::resume::{FinishedBuild, Usage};

/// A build staged at once says it started after its Need, and its run
/// sends nothing before its Started: the loop sends the Need before it
/// launches the run, on the channel the run sends its Started on first.
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
    };
    let mut active = HashMap::from([(id.clone(), build)]);
    let need = Need {
        build_id: id.clone(),
        ..Need::default()
    };
    let staged = Ok((Some(need), StagingStatus::Ready));
    advance_staging(&mut active, &mut VecDeque::new(), &id, staged, &env).await?;
    let mut got = vec![];
    while got.last() != Some(&"Result") {
        let m = tokio::time::timeout(Duration::from_secs(10), out_rx.recv()).await;
        let m = m.expect("no Result").unwrap();
        got.push(m.msg.as_ref().map_or("", msg_name));
    }
    got.retain(|&m| m != "RequestJob");
    assert_eq!(got, ["Need", "Started", "Result"]);
    Ok(())
}

/// A session that resumes a finished result tells the hub it is
/// finished, and the session's end starts the result's reaper clock
/// (released).
#[tokio::test]
async fn a_session_resuming_a_finished_result_says_so_and_releases_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 1, vec![])?;
    let b1 = dir.path().join("builds/b1");
    fs::create_dir_all(&b1)?;
    let fin = FinishedBuild {
        exit_code: 0,
        usage: Usage::default(),
        error: String::new(),
        outputs: vec![],
        dir: b1.clone(),
        finished_at: Instant::now(),
    };
    ResumableBuild::insert(&ctx, "test-key".into(), "old".into(), b1, Some(fin));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let hub = listener.local_addr()?;
    let opts = format!("hub = \"http://{hub}\"\nauth = \"tailscale\"");
    let opts: WorkerConfig = toml::from_str(&opts).unwrap();
    // A hub assigning "test-key" anew, which ends the session once the
    // worker says it resumed it, and passes on all it read until then.
    let (read_tx, mut read) = mpsc::unbounded_channel();
    let resumes = slowlink::hub(move |(mut from_worker, to_worker), bulk| {
        let read = read_tx.clone();
        async move {
            let _bulk = bulk;
            let a = BuildAssignment {
                build_id: "new".into(),
                dedupe_key: "test-key".into(),
                ..Default::default()
            };
            let a = HubMessage {
                msg: Some(hub_message::Msg::Assignment(a)),
            };
            let _ = to_worker.send(Ok(a)).await;
            while let Ok(Some(m)) = from_worker.message().await {
                let resumed = matches!(m.msg, Some(worker_message::Msg::Resumed(_)));
                let _ = read.send(m);
                if resumed {
                    break;
                }
            }
        }
    });
    tokio::spawn(
        Server::builder()
            .add_service(WorkerHubServer::new(resumes))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    // It ends as the hub ends it, after the Resumed.
    let _ = session(&opts, &ctx).await;
    let read: Vec<_> = iter::from_fn(|| read.try_recv().ok()).collect();
    let result = |m: &WorkerMessage| matches!(m.msg, Some(worker_message::Msg::Result(_)));
    assert!(!read.iter().any(result), "a Result first");
    let Some(Some(worker_message::Msg::Resumed(resumed))) = read.last().map(|m| &m.msg) else {
        panic!("no Resumed");
    };
    assert!(resumed.finished, "unfinished");
    let released = ctx.resumable.lock().unwrap()["test-key"].released;
    assert!(released.is_some(), "not released");
    Ok(())
}
