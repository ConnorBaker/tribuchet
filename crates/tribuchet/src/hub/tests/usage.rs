//! The hub's usage line for a build.

use super::*;
use crate::proto::{BuildResult, BuildUsage};

/// A build's usage line names the worker whose session ran it, and the
/// scratch path of the output it is known by (see `named_output`).
#[tokio::test]
async fn the_usage_line_names_the_worker_and_the_output_path() {
    const OUT: &str = "/nix/store/00000000000000000000000000000000-tt-usage";
    let log = Captured::default();
    let _guard = log.install();
    let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
    let replay = Arc::new(Replay::default());
    let mut client = replay.subscribe().await;
    let mut j = job(0, replay);
    j.req.outputs.insert("out".to_owned(), OUT.to_owned());
    state.queue.lock().await.push_back(j);
    let mut w = connect(&state, "w1", 1, &[]);
    w.to_hub.send(request_job(1, Some(0))).await.unwrap();
    let build_id = w.assignment().await.expect("not assigned").build_id;
    let usage = Some(BuildUsage::default());
    let result = BuildResult {
        build_id,
        exit_code: 1,
        usage,
        ..Default::default()
    };
    w.send(worker_message::Msg::Result(result)).await;
    while client.recv().await.is_some() {}
    let line = log.line("build usage");
    assert!(line.contains(r#"worker="w1""#), "{line}");
    assert!(line.contains(&format!(r#"out_path="{OUT}""#)), "{line}");
}
