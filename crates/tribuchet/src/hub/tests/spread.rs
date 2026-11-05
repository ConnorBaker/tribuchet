//! Heavy builds (placement::heavy) and the workers' room for them.

use super::*;

use crate::proto::Resumed;

/// Worker `name` serving x86_64-linux with big-parallel and two slots.
fn heavy_worker(state: &Arc<HubState>, name: &str) -> Worker {
    let register = Register {
        worker_name: name.into(),
        caps: vec![SystemCaps {
            system: "x86_64-linux".into(),
            features: vec!["big-parallel".into()],
        }],
        resumable_keys: vec![],
        max_jobs: 2,
    };
    let (out_tx, from_hub) = mpsc::channel(256);
    let (to_hub, in_rx) = mpsc::channel(64);
    let hub = tokio::spawn(worker_loop(state.clone(), register, out_tx, in_rx));
    Worker {
        to_hub,
        from_hub,
        hub,
    }
}

/// A declaration of `capacity`, `free_slots` of it free slots, having
/// seen no assignment.
fn declare(capacity: u32, free_slots: u32) -> WorkerMessage {
    WorkerMessage {
        msg: Some(worker_message::Msg::RequestJob(RequestJob {
            capacity,
            assignments: Some(0),
            free_slots: Some(free_slots),
        })),
    }
}

/// Queue build `n`, named `name`, heavy or not, with a client attached.
async fn queue(state: &HubState, n: usize, name: &str, heavy: bool) -> Client {
    let mut j = job(n, Arc::new(Replay::default()));
    j.req.outputs = [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into();
    if heavy {
        j.features = vec!["big-parallel".into()];
    }
    let client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    state.notify.notify_waiters();
    client
}

type Client = mpsc::Receiver<Result<AttachEvent, Status>>;

/// A declaration's idle agents are its free slots less the assignments
/// it had not seen, never the lookahead; a worker predating free_slots
/// has none.
#[test]
fn idle_agents_are_free_slots_not_the_lookahead() {
    let rows = [
        (0, 3, Some(2), 2),
        (0, 1, Some(0), 0),
        (1, 3, Some(2), 1),
        (2, 3, Some(2), 0),
        (0, 3, None, 0),
    ];
    for (sent, capacity, free_slots, idle) in rows {
        let mut a = Admission {
            sent,
            ..Admission::default()
        };
        a.declare(&RequestJob {
            capacity,
            assignments: Some(0),
            free_slots,
        });
        assert_eq!(
            a.idle(),
            idle,
            "{sent} unseen; {capacity} declared, {free_slots:?} free"
        );
    }
}

/// A heavy build its worker resumes by key counts against that worker
/// too, and stops counting once its supervision ends in a hub-side
/// error: here, the worker no longer lists it, while its session lives.
#[tokio::test(start_paused = true)]
async fn a_resumed_heavy_build_counts_until_its_supervision_ends() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut j = job(0, Arc::new(Replay::default()));
    j.features = vec!["big-parallel".into()];
    let _client = j.replay.subscribe().await;
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w", 1, &["k0"]);
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let resumed = Resumed { build_id };
    w.send(worker_message::Msg::Resumed(resumed)).await;
    let heavy = || {
        let placement = state.placement.lock().unwrap();
        (placement.sessions.values())
            .map(|a| a.lock().unwrap().heavy)
            .sum::<u32>()
    };
    assert_eq!(heavy(), 1, "resumed");
    for keys in [vec!["k0".to_owned()], vec![]] {
        let listing = Heartbeat {
            resumable_keys: keys,
            ..Default::default()
        };
        w.send(worker_message::Msg::Heartbeat(listing)).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert_eq!(state.metrics.requeued.load(Ordering::Relaxed), 1);
    assert_eq!(heavy(), 0, "requeued");
    w.hub.abort();
}

/// A heavy build whose assignment is never sent stops counting against
/// its worker: here the hub cannot prepare it (its daemon does not answer
/// for a cached input) and gives it up, its client gone, for a build that
/// wants the slot.
#[tokio::test(start_paused = true)]
async fn a_heavy_build_never_sent_stops_counting() {
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let input = format!("/nix/store/{H}-cached");
    let mut unsendable = job(0, Arc::new(Replay::default()));
    (unsendable.req.input_paths, unsendable.features) =
        (vec![input.clone()], vec!["big-parallel".into()]);
    let mut client = unsendable.replay.subscribe().await;
    let wanting = job(1, Arc::new(Replay::default()));
    let _wanting = wanting.replay.subscribe().await;
    state.queue.lock().await.extend([unsendable, wanting]);
    state
        .chunks
        .store_recipe(input, vec![0], Arc::new(Vec::new()));
    let w = heavy_worker(&state, "w");
    keep_alive(&w.to_hub);
    w.to_hub.send(declare(1, 1)).await.unwrap();
    while let Some(Ok(AttachEvent { event })) = client.recv().await {
        if matches!(event, Some(attach_event::Event::Dispatched(_))) {
            break;
        }
    }
    let heavy = || {
        let placement = state.placement.lock().unwrap();
        (placement.sessions.values())
            .map(|a| a.lock().unwrap().heavy)
            .sum::<u32>()
    };
    assert_eq!(heavy(), 1, "dispatched");
    drop(client);
    tokio::time::sleep(FLOOR + Duration::from_secs(6)).await;
    assert_eq!(heavy(), 0, "given up unsent");
}

/// The dispatch line says what a build declares, whether it is heavy,
/// how many other workers that serve it have an idle agent, and each
/// worker's heavy builds in flight.
#[tokio::test]
async fn the_dispatch_line_counts_heavy_builds_and_idle_agents_elsewhere() {
    let log = Captured;
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let mut w1 = heavy_worker(&state, "w1");
    w1.to_hub.send(declare(3, 2)).await.unwrap();
    let _c0 = queue(&state, 0, "heavy-0", true).await;
    assert!(w1.assignment().await.is_some(), "heavy-0 not assigned");
    let mut w2 = heavy_worker(&state, "w2");
    w2.to_hub.send(declare(3, 2)).await.unwrap();
    tokio::time::sleep(QUIET).await;
    // Whichever takes it, the other has an idle agent.
    let _c1 = queue(&state, 1, "light-1", false).await;
    let (a1, a2) = tokio::join!(w1.assignment(), w2.assignment());
    assert!(a1.is_some() || a2.is_some(), "light-1 not assigned");
    for (drv, fields) in [
        (
            "heavy-0",
            [
                r#"features="big-parallel""#,
                "heavy=true",
                "idle_elsewhere=0",
                r#"heavy_builds="w1:1""#,
            ],
        ),
        (
            "light-1",
            [
                r#"features="""#,
                "heavy=false",
                "idle_elsewhere=1",
                r#"heavy_builds="w1:1,w2:0""#,
            ],
        ),
    ] {
        let line = log.line(&format!(r#"drv="{drv}""#));
        for field in fields {
            assert!(line.contains(field), "{field} missing from {line}");
        }
    }
}
