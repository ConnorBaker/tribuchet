//! The hub's verdict reaches only the clients waiting at the instant it is
//! decided (cut-over 7's law, cut-over 10): one that comes after starts a
//! build of its own.

use std::cell::Cell;
use std::time::Duration;

use futures_util::poll;

use super::*;
use crate::hub::losses;
use crate::hub::state::HubState;
use crate::hub::testlog::Captured;
use crate::hub::tests::{H, connect, submit};
use crate::proto::BuildRequest;

fn request(can_local: bool) -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        outputs: [("out".to_owned(), format!("/nix/store/{H}-tt-verdict"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        can_local: Some(can_local),
        ..Default::default()
    }
}

/// The hub's verdict (L12′) on a build with a client whose Nix builds it
/// and one whose Nix cannot, stopped after each of its steps on the log in
/// turn; then a client whose Nix builds the key comes. Whenever it comes it
/// is never given the error, which §2 forbids it: before the decision it is
/// declined, after it starts a build of its own. Before, one that came
/// between the decline and the error joined the build and read the error.
#[tokio::test]
async fn a_verdict_reaches_only_the_clients_waiting_at_its_decision() {
    let log = Captured;
    let _guard = log.install();
    let (mut before, mut after) = (0, 0);
    for steps in 0.. {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let _w = connect(&state, "w1", 1, &[]);
        while state.worker_caps.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let _local = submit(&state, request(true)).await;
        let mut other = submit(&state, request(false)).await;
        let job = state.queue.lock().await.pop_front().expect("queued");
        let replay = job.replay.clone();
        let mut verdict = Box::pin(losses::hub_cannot(&state, job, "verdict", "no".into()));
        // The log held, the verdict waits at its next step on it.
        let mut held = replay.inner.lock().await;
        let mut done = poll!(&mut verdict).is_ready();
        for _ in 0..steps {
            let mut next = Box::pin(replay.inner.lock());
            assert!(poll!(&mut next).is_pending());
            drop(held);
            done = poll!(&mut verdict).is_ready();
            held = next.await;
            if done {
                break;
            }
        }
        let st = state.clone();
        let late = tokio::spawn(async move { submit(&st, request(true)).await });
        for _ in 0..400 {
            if replay.clients.lock().unwrap().len() > 2 || !state.queue.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(held);
        if !done {
            verdict.await;
        }
        let mut late = late.await.unwrap();
        let mut told = Vec::new();
        let wait = Duration::from_millis(300);
        while let Ok(Ok(Some(ev))) = tokio::time::timeout(wait, late.message()).await {
            told.extend(ev.event);
        }
        let error = told
            .iter()
            .any(|e| matches!(e, attach_event::Event::Error(_)));
        assert!(!error, "after {steps} steps, given the verdict: {told:?}");
        let mut refused = false;
        while let Some(ev) = other.message().await.unwrap() {
            refused |= matches!(ev.event, Some(attach_event::Event::Error(_)));
        }
        assert!(refused, "after {steps} steps, the other client not refused");
        let decline = attach_event::Event::ExitCode(DECLINE_EXIT_CODE);
        before += usize::from(told.contains(&decline));
        after += usize::from(state.queue.lock().await.len() == 1);
        if done {
            break;
        }
    }
    // Both kinds of late client came: before the decision and after.
    assert!(before > 0 && after > 0, "before {before}, after {after}");
    let bad = "shadow divergence";
    assert_eq!(log.count(bad), 0, "{}", log.line(bad));
}

/// A client that found the build listed before its answer was decided and
/// joins after is turned away, and no client is made (the core is told of
/// none): its submission then starts a build of its own.
#[tokio::test]
async fn a_client_joining_after_the_decision_is_turned_away() {
    let replay = Arc::new(Replay::default());
    let _waiting = replay.subscribe_as(Client::untracked(false)).await;
    let (declined, waiting) = replay.close(true, |_, _| {});
    assert_eq!((declined.len(), waiting), (0, 1));
    let made = Cell::new(false);
    let client = || {
        made.set(true);
        Client::untracked(true)
    };
    assert!(
        replay.join(client).await.is_none(),
        "joined after its answer"
    );
    assert!(!made.get(), "a client made");
}
