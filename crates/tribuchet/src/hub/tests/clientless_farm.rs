//! Builds with no attach client across workers and dispatches
//! (hub/state/clientless.rs). Each of their builds is given up at FLOOR.
//! Each test says where it failed before.

use super::clientless::sent;
use super::*;
use state::FLOOR;

fn farm() -> HubState {
    HubState::for_test(Duration::from_secs(30))
}

/// Worker `name`, with `slots`, registered holding the keys `held`, its
/// heartbeats listing `listed` every 30 s from 30 s on as keys it holds.
fn worker(state: &Arc<HubState>, name: &str, slots: u32, held: &[&str], listed: &[&str]) -> Worker {
    let keys: Vec<String> = listed.iter().map(|k| (*k).to_owned()).collect();
    let w = connect(state, name, slots, held);
    let beats = w.to_hub.clone();
    tokio::spawn(async move {
        let beat = || WorkerMessage {
            msg: Some(worker_message::Msg::Heartbeat(Heartbeat {
                resumable_keys: keys.clone(),
                ..Default::default()
            })),
        };
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if beats.send(beat()).await.is_err() {
                break;
            }
        }
    });
    w
}

/// Queue `jobs`, each with a client; their clients.
async fn queue(
    state: &HubState,
    jobs: Vec<Job>,
) -> Vec<mpsc::Receiver<Result<AttachEvent, Status>>> {
    let mut clients = Vec::new();
    for j in jobs {
        clients.push(j.replay.subscribe().await);
        state.queue.lock().await.push_back(j);
    }
    clients
}

/// A build whose client left is requeued when its worker's session
/// dies 8 s later and taken back by key by the worker's new session: it
/// is given up FLOOR after its client left, not FLOOR after the second
/// dispatch (the hold does not start again at each dispatch).
#[tokio::test(start_paused = true)]
async fn the_absence_runs_from_the_client_leaving_across_dispatches() {
    let state = Arc::new(farm());
    let clients = queue(&state, queued(1)).await;
    let mut first = worker(&state, "w", 1, &[], &[]);
    first.to_hub.send(request_job(1, Some(0))).await.unwrap();
    assert_eq!(assignments(&mut first.from_hub, QUIET).await.len(), 1);
    drop(clients);
    let left = tokio::time::Instant::now();
    let _wanting = wanting_a_slot(&state, 9).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    // The worker restarted: its new session registers, holding the
    // build, before the old one is seen to die.
    let mut again = worker(&state, "w", 1, &["k0"], &[]);
    again.to_hub.send(request_job(0, Some(0))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(first);
    let mut given_up = None;
    while left.elapsed() < Duration::from_secs(600) && given_up.is_none() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if !sent(&mut again.from_hub).1.is_empty() {
            given_up = Some(left.elapsed().as_secs());
        }
    }
    again.hub.abort();
    let floor = FLOOR.as_secs();
    assert!(
        given_up.is_some_and(|s| (floor..floor + 5).contains(&s)),
        "given up {given_up:?} s after its client left"
    );
}

/// A build its worker holds from before the hub's restart, which no
/// client asks for again, runs to completion though a queued build wants
/// its slot: its key is never cancelled. Such a build has no dispatch
/// to weigh, and is an exception to giving up clientless builds: it is
/// not given up at FLOOR. No code path cancels an undispatched key, and
/// this keeps it so.
#[tokio::test(start_paused = true)]
async fn a_build_held_from_before_a_restart_is_never_given_up() {
    let state = Arc::new(farm());
    let mut w = worker(&state, "w", 1, &["korphan"], &["korphan"]);
    w.to_hub.send(request_job(0, Some(0))).await.unwrap();
    let _wanting = wanting_a_slot(&state, 9).await;
    tokio::time::sleep(Duration::from_secs(600)).await;
    let cancels = sent(&mut w.from_hub).1;
    w.hub.abort();
    assert_eq!(cancels, []);
}

/// A build given up for waiting work stays dispatched until its worker
/// settles the cancel. Asked for again by a new client before then, it
/// is dispatched by key to that worker, which delivers to the new
/// dispatch: the old one ends, no longer counted as freeing a slot,
/// rather than wait, for as long as the session lives, for a result its
/// worker sent the new one.
#[tokio::test(start_paused = true)]
async fn a_given_up_build_asked_for_again_ends_its_old_dispatch() {
    let state = Arc::new(farm());
    let clients = queue(&state, queued(1)).await;
    let mut w = worker(&state, "w", 1, &[], &["k0"]);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    assert_eq!(assignments(&mut w.from_hub, QUIET).await.len(), 1);
    drop(clients);
    let _wanting = wanting_a_slot(&state, 9).await;
    // Past FLOOR, and past the first heartbeat, at 30 s, that lists k0.
    tokio::time::sleep(Duration::from_secs(40)).await;
    let old = format!("{:032x}", 0);
    assert_eq!(sent(&mut w.from_hub).1, [(old.clone(), "k0".to_owned())]);
    let kept = state.dispatched.lock().unwrap().contains_key(&old);
    assert!(kept, "given up, it ended before its worker settled");
    let mut again = job(5, Arc::new(Replay::default()));
    again.key = "k0".into();
    let _client = again.replay.subscribe().await;
    state.queue.lock().await.push_back(again);
    state.notify.notify_waiters();
    assert_eq!(
        assignments(&mut w.from_hub, Duration::from_secs(5)).await,
        [format!("{:032x}", 5)]
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    let still = state.dispatched.lock().unwrap().contains_key(&old);
    let later = sent(&mut w.from_hub).1;
    w.hub.abort();
    assert!(!still, "the old dispatch is still supervised");
    assert_eq!(later, [], "cancels after the new dispatch");
}
