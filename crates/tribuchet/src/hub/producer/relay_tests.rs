//! Producers at the hub, as RelayStarted and RelayReady pass: DD8, DD8a at
//! the loss, DD9's refusal and DD10's replay.

use std::sync::Barrier;
use std::thread;

use tonic::Request;

use super::super::super::admin::AdminSvc;
use super::*;
use crate::proto::DecommissionRequest;
use crate::proto::hub_admin_server::HubAdmin;

/// The RelayReady events `events` brings, until one with token `last`.
async fn readies(events: &mut Streaming<AttachEvent>, last: &[u8]) -> Vec<RelayReady> {
    let mut got = Vec::new();
    let read = async {
        while let Ok(Some(ev)) = events.message().await {
            if let Some(Event::RelayReady(r)) = ev.event {
                let done = r.handoff_token == last;
                got.push(r);
                if done {
                    return;
                }
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(10), read).await;
    got
}

/// DD8a at the loss: a relayed producer's run its worker says is gone is
/// settled there, not requeued for a later take to settle; each client
/// is answered as its Nix said: canBuildLocally true is declined (222),
/// false or unsaid (OQ1's fallback (a)) fails, naming the cause.
#[tokio::test]
async fn a_relayed_producer_gone_is_settled_at_the_loss_as_its_nix_said() {
    let failed = Verdict::Failed(RELAYED_LOST.to_owned());
    for (can_local, want) in [
        (Some(true), Verdict::Declined),
        (Some(false), failed.clone()),
        (None, failed),
    ] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let w1 = worker(&state, "w1", &[]);
        let (tx, mut events) = submit_open(&state, producer_request(can_local)).await;
        let job = state.take_job(&w1.caps, None).await.expect("not queued");
        relay_started(&job, &tx, &mut events).await;
        let said = vec![started(&job), result_gone(&job)];
        let _ = dispatch(&state, job, w1, said).await;
        let queued = state.queue.lock().await.len();
        assert_eq!(
            queued, 0,
            "{can_local:?}: requeued, not settled at the loss"
        );
        drop(tx);
        assert_eq!(verdict(&mut events).await, Some(want), "{can_local:?}");
    }
}

/// DD8a with no known worker for it (L11′): a relayed producer requeued
/// as its worker's session died, then that worker retired (U4), is
/// answered as at a loss: canBuildLocally true is declined (222), false or
/// unsaid (OQ1's fallback (a)) fails, told why.
#[tokio::test]
async fn a_relayed_producer_no_known_worker_serves_is_answered_as_its_nix_said() {
    for (can_local, declined) in [(Some(true), true), (Some(false), false), (None, false)] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let w1 = worker(&state, "w1", &[]);
        let (tx, mut events) = submit_open(&state, producer_request(can_local)).await;
        let job = state.take_job(&w1.caps, None).await.expect("not queued");
        relay_started(&job, &tx, &mut events).await;
        let said = vec![started(&job)];
        let _ = dispatch(&state, job, w1, said).await;
        let queued = state.queue.lock().await.len();
        assert_eq!(queued, 1, "{can_local:?}: not requeued");
        let admin = AdminSvc {
            state: state.clone(),
        };
        let retire = DecommissionRequest {
            worker_name: "w1".into(),
        };
        admin.decommission(Request::new(retire)).await.unwrap();
        drop(tx);
        let got = verdict(&mut events).await;
        let got = got.map(|v| v == Verdict::Declined);
        assert_eq!(got, Some(declined), "{can_local:?}: declined");
    }
}

/// DD8a at admission: a relayed producer resubmitted to a hub no
/// connected worker serves (its own, known, not back since the hub
/// restarted) is declined only to a client whose Nix said it builds it;
/// one unsaid (OQ1's fallback (a)) or that cannot waits for the worker,
/// queued.
#[tokio::test]
async fn a_relayed_resubmission_with_no_worker_is_declined_only_as_its_nix_said() {
    for (can_local, declined) in [(Some(true), true), (Some(false), false), (None, false)] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        drop(worker(&state, "w1", &[]));
        let req = BuildRequest {
            relay_started: true,
            ..producer_request(can_local)
        };
        let (_tx, mut events) = submit_open(&state, req).await;
        let queued = state.queue.lock().await.len();
        assert_eq!(queued, usize::from(!declined), "{can_local:?}: queued");
        if declined {
            assert_eq!(verdict(&mut events).await, Some(Verdict::Declined));
        }
    }
}

/// DD8, DD10: the attach is told each dispatch's own handoff token as the
/// run starts, fresh, or is resumed, not fresh: the listener it holds is
/// that run's.
#[tokio::test]
async fn relay_ready_is_fresh_for_a_started_run_and_not_for_a_resumed_one() {
    for resumed in [false, true] {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let req = producer_request(Some(true));
        let held = if resumed {
            vec![dedupe_key(&req, b"")]
        } else {
            Vec::new()
        };
        let w = worker(&state, "w1", &held);
        let (_tx, mut events) = submit_open(&state, req).await;
        let job = if resumed {
            state.take_job_by_key(w.id).await
        } else {
            state.take_job(&w.caps, None).await
        };
        let job = job.expect("not taken");
        let said = if resumed {
            worker_message::Msg::Resumed(Resumed {
                build_id: job.id.clone(),
                finished: false,
                run: None,
            })
        } else {
            started(&job)
        };
        let sent = dispatch(&state, job, w, vec![said]).await;
        let token = sent.iter().find_map(|m| match m {
            hub_message::Msg::Assignment(a) => Some(a.handoff_token.clone()),
            _ => None,
        });
        let token = token.expect("no assignment");
        assert_eq!(token.len(), 16, "resumed {resumed}: the token");
        let want = RelayReady {
            handoff_token: token.clone(),
            fresh: !resumed,
        };
        let told = readies(&mut events, &token).await;
        assert_eq!(told.last(), Some(&want), "resumed {resumed}: RelayReady");
    }
}

/// DD10 on a rejoin: an attach reconnecting to the same hub joins its own
/// build and is replayed the latest fresh RelayReady and each resume's
/// after it, as published, none of a run a fresh start replaced: an attach
/// holding that run's listener keeps it, one holding a run before fetches
/// it. One published after it joined comes as published.
#[tokio::test]
async fn a_rejoin_is_replayed_the_latest_fresh_relay_ready_and_its_resumes() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let _w = worker(&state, "w1", &[]);
    let (_tx1, mut first) = submit_open(&state, producer_request(Some(true))).await;
    let replay = state.queue.lock().await[0].replay.clone();
    let ready = |t: u8, fresh| RelayReady {
        handoff_token: vec![t; 16],
        fresh,
    };
    let published = [
        ready(7, false),
        ready(8, true),
        ready(81, false),
        ready(9, true),
        ready(91, false),
        ready(92, false),
    ];
    for r in &published {
        replay.publish(Event::RelayReady(r.clone())).await;
    }
    let told = readies(&mut first, &[92; 16]).await;
    assert_eq!(told, published, "the first client");
    let (_tx2, mut second) = submit_open(&state, producer_request(Some(true))).await;
    assert_eq!(state.queue.lock().await.len(), 1, "the rejoin queued anew");
    replay.publish(Event::RelayReady(ready(10, true))).await;
    let told = readies(&mut second, &[10; 16]).await;
    let want = [
        ready(9, true),
        ready(91, false),
        ready(92, false),
        ready(10, true),
    ];
    assert_eq!(told, want, "the rejoin");
}

/// DD8: RelayStarted counts only with a token of the run current now. One
/// before any was told, or of a run a fresh start replaced (a stale
/// splice's), is neither recorded nor acknowledged; one of the current
/// run, its fresh start's or a resume's, is, and its acknowledgement
/// names it.
#[tokio::test]
async fn relay_started_counts_only_for_the_run_current_now() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let w = worker(&state, "w1", &[]);
    let (tx, mut events) = submit_open(&state, producer_request(Some(true))).await;
    let job = state.take_job(&w.caps, None).await.expect("not queued");
    let send = async |t: u8| {
        let started = RelayStarted {
            handoff_token: vec![t; 16],
        };
        let m = build_message::Msg::RelayStarted(started);
        tx.send(BuildMessage { msg: Some(m) }).await.unwrap();
    };
    send(1).await;
    relay_ready(&job, &[2; 16], true).await;
    relay_ready(&job, &[3; 16], true).await;
    send(2).await;
    relay_ready(&job, &[4; 16], false).await;
    send(3).await;
    send(4).await;
    let mut acks = Vec::new();
    let read = async {
        while let Ok(Some(ev)) = events.message().await {
            if let Some(Event::RelayStartedAck(t)) = ev.event {
                acks.push(t[0]);
                if acks.len() == 2 {
                    return;
                }
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(10), read).await;
    assert_eq!(acks, [3, 4], "the RelayStarted acknowledged");
    assert!(job.replay.relayed());
    // Once reached, a stale splice's RelayStarted after takes nothing back.
    assert!(!job.replay.relay_started_by(&[2; 16]));
    assert!(job.replay.relayed(), "a stale RelayStarted unset it");
}

/// DD9: a producer's request without an attach_id (from an attach that
/// relays nothing) is refused before it is queued, naming the cause.
#[tokio::test]
async fn a_producer_without_an_attach_id_is_refused() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let _w = worker(&state, "w1", &[]);
    let req = BuildRequest {
        attach_id: Vec::new(),
        ..producer_request(Some(true))
    };
    let refused = try_submit_open(&state, req).await.map(drop).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert_eq!(refused.message(), NO_ATTACH_ID);
    assert!(state.queue.lock().await.is_empty(), "queued");
}

/// DD8, DD8a: a run lost before RelayStarted was acknowledged is current
/// no more, whether its worker says it is gone or its session died (the
/// build requeued). Its splice's RelayStarted, arriving before or after a
/// fresh take has the build, is not acknowledged and marks nothing, so the
/// fresh run goes on with its own token, as the loss policy says.
#[tokio::test]
async fn a_lost_runs_relay_started_is_not_acknowledged_after_a_fresh_take() {
    for (gone, after_take) in [(true, true), (true, false), (false, false)] {
        let case = format!("gone {gone}, after the take {after_take}");
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let w1 = worker(&state, "w1", &[]);
        let (tx, mut events) = submit_open(&state, producer_request(Some(true))).await;
        let job = state.take_job(&w1.caps, None).await.expect("not queued");
        let mut said = vec![started(&job)];
        if gone {
            said.push(result_gone(&job));
        }
        let sent = dispatch(&state, job, w1, said).await;
        let lost = sent
            .iter()
            .find_map(|m| match m {
                hub_message::Msg::Assignment(a) => Some(a.handoff_token.clone()),
                _ => None,
            })
            .expect("no assignment");
        let w2 = worker(&state, "w2", &[]);
        let mut fresh = None;
        if after_take {
            fresh = state.take_job(&w2.caps, None).await;
        }
        let m = build_message::Msg::RelayStarted(RelayStarted {
            handoff_token: lost.clone(),
        });
        tx.send(BuildMessage { msg: Some(m) }).await.unwrap();
        let ack = until(&mut events, |e| *e == Event::RelayStartedAck(lost.clone()));
        let acked = tokio::time::timeout(Duration::from_secs(2), ack).await;
        assert!(
            !acked.unwrap_or(false),
            "{case}: the lost run's RelayStarted was acknowledged"
        );
        if !after_take {
            fresh = state.take_job(&w2.caps, None).await;
        }
        let fresh = fresh.expect("not requeued");
        assert!(!fresh.replay.relayed(), "{case}: RelayStarted counted");
        let s = started(&fresh);
        let sent = dispatch(&state, fresh, w2, vec![s]).await;
        assert_eq!(assignments(&sent), 1, "{case}: the fresh run");
    }
}

/// DD8a's one lock: a RelayStarted and a loss raced on two threads are
/// ordered: never acknowledged while the loss found the run not reached.
#[test]
fn an_acknowledgement_and_a_loss_are_ordered() {
    let token = vec![1u8; 16];
    let (mut torn, mut acked_first, mut lost_first) = (0, 0, 0);
    for i in 0..20_000 {
        let replay = Arc::new(Replay::new(format!("p{i}")));
        replay.relay.lock().unwrap().tokens.push(token.clone());
        let barrier = Arc::new(Barrier::new(2));
        let (r, b, t) = (replay.clone(), barrier.clone(), token.clone());
        let acker = thread::spawn(move || {
            b.wait();
            r.relay_started_by(&t)
        });
        barrier.wait();
        let reached = replay.run_lost();
        let acked = acker.join().unwrap();
        match (acked, reached) {
            (true, false) => torn += 1,
            (true, true) => acked_first += 1,
            _ => lost_first += 1,
        }
    }
    println!("torn {torn}, acknowledged first {acked_first}, lost first {lost_first}");
    assert_eq!(
        torn, 0,
        "acknowledged after a loss that found the run not reached"
    );
}

/// DD8: lost_relayed, finding a run not reached, ends it at once, not only
/// at a later requeue: a RelayStarted naming it is not counted in between.
#[tokio::test]
async fn a_loss_not_relayed_ends_the_run_at_once() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let w1 = worker(&state, "w1", &[]);
    let (_tx, _events) = submit_open(&state, producer_request(Some(true))).await;
    let job = state.take_job(&w1.caps, None).await.expect("not queued");
    relay_ready(&job, &[5; 16], true).await;
    let job = lost_relayed(&state, job, "gone").await.expect("settled");
    assert!(
        !job.replay.relay_started_by(&[5; 16]),
        "the lost run's RelayStarted counted before its requeue"
    );
}
