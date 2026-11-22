//! A producer's relay through attach's own loop, against a fake hub: DD8's
//! RelayStarted and DD8a's resubmission, and DD7 at the hub's decline.

use super::*;

/// A hub relaying one producer: to the first submission it sends
/// RelayReady, acknowledges RelayStarted, and goes away once told; to the
/// next, the exit code (`then`). It records each submission's
/// relay_started, and the token each RelayStarted named. `resumed`: it
/// knows the run by a resume's token only (8), as a hub that restarted
/// does, and answers a RelayStarted naming the start's (7) with that
/// resume's RelayReady.
struct RelayingHub {
    submitted: Mutex<Vec<bool>>,
    started: Mutex<Vec<Vec<u8>>>,
    go_away: Notify,
    then: i32,
    resumed: bool,
}

impl RelayingHub {
    fn new(then: i32, resumed: bool) -> Arc<Self> {
        Arc::new(Self {
            submitted: Mutex::default(),
            started: Mutex::default(),
            go_away: Notify::new(),
            then,
            resumed,
        })
    }

    fn started(&self) -> Vec<Vec<u8>> {
        self.started.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl AttachHub for Arc<RelayingHub> {
    type BuildStream = ReceiverStream<StdResult<AttachEvent, Status>>;

    async fn build(
        &self,
        request: Request<Streaming<BuildMessage>>,
    ) -> StdResult<Response<Self::BuildStream>, Status> {
        let mut inbound = request.into_inner();
        let Ok(Some(BuildMessage {
            msg: Some(build_message::Msg::Request(req)),
        })) = inbound.message().await
        else {
            return Err(Status::invalid_argument("no request"));
        };
        let first = {
            let mut submitted = self.submitted.lock().unwrap();
            submitted.push(req.relay_started);
            submitted.len() == 1
        };
        let (tx, rx) = mpsc::channel(8);
        let send = |e| AttachEvent { event: Some(e) };
        if !first {
            let _ = tx
                .send(Ok(send(attach_event::Event::ExitCode(self.then))))
                .await;
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let ready = RelayReady {
            handoff_token: vec![7; 16],
            fresh: true,
        };
        let _ = tx
            .send(Ok(send(attach_event::Event::RelayReady(ready))))
            .await;
        let hub = self.clone();
        tokio::spawn(async move {
            let went = hub.go_away.notified();
            tokio::pin!(went);
            loop {
                tokio::select! {
                    () = &mut went => break,
                    m = inbound.message() => match m {
                        Ok(Some(BuildMessage { msg: Some(build_message::Msg::RelayStarted(r)) })) => {
                            hub.started.lock().unwrap().push(r.handoff_token.clone());
                            let ev = if hub.resumed && r.handoff_token == [7; 16] {
                                let handoff_token = vec![8; 16];
                                let ready = RelayReady { handoff_token, fresh: false };
                                attach_event::Event::RelayReady(ready)
                            } else {
                                attach_event::Event::RelayStartedAck(r.handoff_token)
                            };
                            let _ = tx.send(Ok(send(ev))).await;
                        }
                        Ok(Some(_)) => {}
                        _ => future::pending().await,
                    },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// An attach running `hub`'s producer in `dir`: its task, the client's
/// socket, and the producer's first connection, on which it wrote "rpc".
async fn attach_with(
    dir: &Path,
    hub: &Arc<RelayingHub>,
) -> (JoinHandle<Result<i32>>, UnixListener, UnixStream) {
    let top = dir.join("top");
    fs::create_dir_all(top.join("build")).unwrap();
    let at_client = UnixListener::bind(top.join("build/.nix-socket")).unwrap();
    let run = dir.join("run.sock");
    let handoff = handoff_server(dir, vec![(vec![7; 16], run.clone())]);
    let socket = dir.join("hub.sock");
    let incoming = UnixListenerStream::new(UnixListener::bind(&socket).unwrap());
    tokio::spawn(
        Server::builder()
            .add_service(AttachHubServer::new(hub.clone()))
            .serve_with_incoming(incoming),
    );
    let mut build = producer_build(Some(true));
    build.top_tmp_dir = top;
    let attach = tokio::spawn(run_async(build, socket, Some(handoff)));
    let mut producer = loop {
        match UnixStream::connect(&run).await {
            Ok(p) => break p,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    producer.write_all(b"rpc").await.unwrap();
    (attach, at_client, producer)
}

/// The client's connection of `at_client`, once "rpc" reached it.
async fn relayed(hub: &RelayingHub, at_client: &UnixListener) -> UnixStream {
    let (mut client, _) = tokio::time::timeout(WAIT, at_client.accept())
        .await
        .unwrap()
        .unwrap();
    let mut buf = [0u8; 3];
    if tokio::time::timeout(WAIT, client.read_exact(&mut buf))
        .await
        .is_err()
    {
        assert!(!hub.started().is_empty(), "the hub got no RelayStarted");
        panic!("the hub acknowledged RelayStarted, and the bytes were held");
    }
    assert_eq!(&buf, b"rpc");
    assert_eq!(hub.started(), [vec![7; 16]], "RelayStarted named the run");
    client
}

/// DT4 and DD8a through attach's own loop: the producer's first byte
/// reaches the client only once attach sent RelayStarted, naming the run,
/// and took the hub's acknowledgement; a resubmission after the hub went
/// away says RelayStarted was acknowledged.
#[tokio::test]
async fn attach_sends_relay_started_and_resubmits_it_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let hub = RelayingHub::new(0, false);
    let (attach, at_client, mut producer) = attach_with(dir.path(), &hub).await;
    producer.shutdown().await.unwrap();
    let mut client = relayed(&hub, &at_client).await;
    client.shutdown().await.unwrap();
    hub.go_away.notify_one();
    let code = tokio::time::timeout(Duration::from_secs(30), attach)
        .await
        .expect("attach did not exit")
        .unwrap()
        .unwrap();
    assert_eq!(code, 0);
    assert_eq!(
        *hub.submitted.lock().unwrap(),
        [false, true],
        "the resubmission forgot RelayStarted was acknowledged"
    );
}

/// DD7 at the hub's decline: the run was lost and may still run (a worker
/// past its grace, its agents alive). Attach exits with 222 at once,
/// aborting the run's splices, so nothing of it reaches the client after;
/// it does not wait for them.
#[tokio::test]
async fn a_decline_aborts_the_lost_runs_splices() {
    let dir = tempfile::tempdir().unwrap();
    let hub = RelayingHub::new(DECLINE_EXIT_CODE, false);
    let (attach, at_client, mut producer) = attach_with(dir.path(), &hub).await;
    let mut client = relayed(&hub, &at_client).await;
    hub.go_away.notify_one();
    let code = tokio::time::timeout(Duration::from_secs(30), attach).await;
    let code = code.expect("attach waited on the lost run's splice after the decline");
    assert_eq!(code.unwrap().unwrap(), DECLINE_EXIT_CODE);
    let mut rest = Vec::new();
    let ended = tokio::time::timeout(WAIT, producer.read_to_end(&mut rest)).await;
    assert!(ended.is_ok(), "the lost run's splice was not aborted");
    let ended = tokio::time::timeout(WAIT, client.read_to_end(&mut rest)).await;
    assert!(ended.is_ok(), "the client's connection was kept");
}

/// DD8 across a resume: a RelayStarted the hub does not take (it knows the
/// run by a resume's token only, as after a restart) is asked again once
/// that resume's RelayReady came, naming its token; then the bytes go.
#[tokio::test]
async fn relay_started_is_asked_again_under_a_resumes_token() {
    let dir = tempfile::tempdir().unwrap();
    let hub = RelayingHub::new(0, true);
    let (attach, at_client, mut producer) = attach_with(dir.path(), &hub).await;
    producer.shutdown().await.unwrap();
    let (mut client, _) = tokio::time::timeout(WAIT, at_client.accept())
        .await
        .unwrap()
        .unwrap();
    let mut buf = [0u8; 3];
    let read = tokio::time::timeout(WAIT, client.read_exact(&mut buf)).await;
    let asked = hub.started();
    assert!(
        read.is_ok(),
        "the bytes were held; RelayStarted named {asked:?}"
    );
    assert_eq!(asked, [vec![7; 16], vec![8; 16]], "RelayStarted named");
    client.shutdown().await.unwrap();
    hub.go_away.notify_one();
    let code = tokio::time::timeout(Duration::from_secs(30), attach).await;
    assert_eq!(code.expect("attach did not exit").unwrap().unwrap(), 0);
}

/// A hub that answers each submission with `0`'s events, then ends.
struct ScriptedHub(Vec<attach_event::Event>);

#[tonic::async_trait]
impl AttachHub for Arc<ScriptedHub> {
    type BuildStream = ReceiverStream<StdResult<AttachEvent, Status>>;

    async fn build(
        &self,
        _request: Request<Streaming<BuildMessage>>,
    ) -> StdResult<Response<Self::BuildStream>, Status> {
        let (tx, rx) = mpsc::channel(self.0.len().max(1));
        for e in self.0.clone() {
            let _ = tx.send(Ok(AttachEvent { event: Some(e) })).await;
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// DD10: a listener attach fails to fetch (a token the worker on this host
/// does not know) fails no build: the run is lost, which the hub settles
/// once that run's worker reports it expired. Attach goes on: the listener
/// of a fresh run, or of a resume of the run it never fetched, is fetched,
/// and the build ends with the hub's exit code.
#[tokio::test]
async fn a_listener_not_fetched_is_a_loss_not_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let top = dir.path().join("top");
    fs::create_dir_all(top.join("build")).unwrap();
    let (fresh, resumed) = (dir.path().join("7.sock"), dir.path().join("8.sock"));
    let runs = vec![(vec![7; 16], fresh.clone()), (vec![8; 16], resumed.clone())];
    let handoff = handoff_server(dir.path(), runs);
    let ready = |t: u8, fresh| {
        let handoff_token = vec![t; 16];
        attach_event::Event::RelayReady(RelayReady {
            handoff_token,
            fresh,
        })
    };
    let exit = attach_event::Event::ExitCode;
    let cases = [
        (
            vec![ready(9, true), exit(DECLINE_EXIT_CODE)],
            DECLINE_EXIT_CODE,
            None,
        ),
        (
            vec![ready(9, true), ready(7, true), exit(0)],
            0,
            Some(&fresh),
        ),
        (
            vec![ready(9, true), ready(8, false), exit(0)],
            0,
            Some(&resumed),
        ),
    ];
    for (i, (events, want, fetched)) in cases.into_iter().enumerate() {
        let socket = dir.path().join(format!("hub-{i}.sock"));
        let incoming = UnixListenerStream::new(UnixListener::bind(&socket).unwrap());
        let hub = Arc::new(ScriptedHub(events));
        tokio::spawn(
            Server::builder()
                .add_service(AttachHubServer::new(hub))
                .serve_with_incoming(incoming),
        );
        let mut build = producer_build(Some(true));
        build.top_tmp_dir.clone_from(&top);
        let attach = run_async(build, socket, Some(handoff.clone()));
        let code = tokio::time::timeout(Duration::from_secs(30), attach).await;
        let code = code
            .expect("attach did not exit")
            .map_err(|e| e.to_string());
        assert_eq!(code, Ok(want), "case {i}: after a listener not fetched");
        if let Some(run) = fetched {
            assert!(run.exists(), "case {i}: the next listener was not fetched");
        }
    }
}

/// A hub that sends RelayReady(7) and, at the first RelayStarted, the
/// worker's exit code 1 and the acknowledgement: the acknowledgement first
/// when `.0`, else the exit code first (the run ended before the hub read
/// RelayStarted: its relay reader and the run's are apart).
struct ExitAndAck(bool);

#[tonic::async_trait]
impl AttachHub for Arc<ExitAndAck> {
    type BuildStream = ReceiverStream<StdResult<AttachEvent, Status>>;

    async fn build(
        &self,
        request: Request<Streaming<BuildMessage>>,
    ) -> StdResult<Response<Self::BuildStream>, Status> {
        let ack_first = self.0;
        let mut inbound = request.into_inner();
        let _ = inbound.message().await;
        let (tx, rx) = mpsc::channel(8);
        let send = |e| AttachEvent { event: Some(e) };
        let ready = RelayReady {
            handoff_token: vec![7; 16],
            fresh: true,
        };
        let _ = tx
            .send(Ok(send(attach_event::Event::RelayReady(ready))))
            .await;
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(BuildMessage {
                        msg: Some(build_message::Msg::RelayStarted(r)),
                    })) => {
                        let ack = send(attach_event::Event::RelayStartedAck(r.handoff_token));
                        let exit = send(attach_event::Event::ExitCode(1));
                        let (a, b) = if ack_first { (ack, exit) } else { (exit, ack) };
                        let _ = tx.send(Ok(a)).await;
                        let _ = tx.send(Ok(b)).await;
                    }
                    Ok(Some(_)) => {}
                    _ => future::pending::<()>().await,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// What attach running `ExitAndAck(ack_first)`'s producer ends with, its
/// producer having written "rpc" and closed.
async fn exit_with(ack_first: bool) -> StdResult<i32, String> {
    let dir = tempfile::tempdir().unwrap();
    let top = dir.path().join("top");
    fs::create_dir_all(top.join("build")).unwrap();
    let at_client = UnixListener::bind(top.join("build/.nix-socket")).unwrap();
    // The client's Nix: reads each connection to its end, then closes it.
    tokio::spawn(async move {
        while let Ok((mut c, _)) = at_client.accept().await {
            tokio::spawn(async move {
                let mut v = Vec::new();
                let _ = c.read_to_end(&mut v).await;
            });
        }
    });
    let run = dir.path().join("run.sock");
    let handoff = handoff_server(dir.path(), vec![(vec![7; 16], run.clone())]);
    let socket = dir.path().join("hub.sock");
    let incoming = UnixListenerStream::new(UnixListener::bind(&socket).unwrap());
    tokio::spawn(
        Server::builder()
            .add_service(AttachHubServer::new(Arc::new(ExitAndAck(ack_first))))
            .serve_with_incoming(incoming),
    );
    let mut build = producer_build(Some(true));
    build.top_tmp_dir = top;
    let attach = tokio::spawn(run_async(build, socket, Some(handoff)));
    let mut producer = loop {
        match UnixStream::connect(&run).await {
            Ok(p) => break p,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    producer.write_all(b"rpc").await.unwrap();
    drop(producer);
    match tokio::time::timeout(Duration::from_secs(30), attach).await {
        Ok(r) => r.unwrap().map_err(|e| e.to_string()),
        Err(_) => Err("attach hung".into()),
    }
}

/// DD7, DD8: the worker's exit code while a splice waits at the gate for
/// an acknowledgement still to come: the run ended, so the gate opens,
/// the splice goes on and attach exits with the code. Control: the
/// acknowledgement first.
#[tokio::test]
async fn an_exit_code_while_a_splice_waits_at_the_gate_ends_attach() {
    assert_eq!(exit_with(true).await, Ok(1), "the acknowledgement first");
    assert_eq!(exit_with(false).await, Ok(1), "the exit code first");
}
