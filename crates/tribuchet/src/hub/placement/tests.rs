//! The history file's saves.

use std::time::Duration;

use super::*;

/// A succeeded run of `name`, learned.
fn learn(state: &HubState, name: &str) {
    let b = Build {
        system: "x86_64-linux",
        name: Some(name),
        declared: false,
    };
    let usage = BuildUsage {
        wall_ms: 1000,
        cpu_usec: 1_000_000,
        peak_memory_bytes: 1 << 30,
        ..BuildUsage::default()
    };
    let mut placement = state.placement.lock().unwrap();
    placement.history.record(&b, 0, &usage);
}

/// Of two copies saved out of order, the newer stays: overlapping saves
/// leave the newest, as the SIGTERM save must over a periodic one.
#[tokio::test]
async fn the_newer_of_two_saves_stays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("h.json");
    let state = HubState::for_test(Duration::from_secs(30));
    let file = open_history(&state, path.clone()).await;
    let copy = |name: &str| {
        learn(&state, name);
        let copy = state.placement.lock().unwrap().history.changed();
        copy.expect("changed")
    };
    let (older, newer) = (copy("a-1"), copy("b-1"));
    file.write(newer.0, &newer.1);
    file.write(older.0, &older.1);
    assert_eq!(
        History::load(&path).unwrap().len().0,
        2,
        "the older copy left"
    );
}

/// A save writes its copy under the copy's own number, which is what
/// lets write_newest keep the newer of two.
#[tokio::test]
async fn a_save_writes_its_copy_under_its_number() {
    let dir = tempfile::tempdir().unwrap();
    let state = HubState::for_test(Duration::from_secs(30));
    let file = open_history(&state, dir.path().join("h.json")).await;
    for name in ["a-1", "b-1"] {
        learn(&state, name);
        save_history(&state, &file).await;
    }
    let newest = *file.written.lock().unwrap();
    assert_eq!(newest, 2, "a copy saved under another number");
}
