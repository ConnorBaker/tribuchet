//! The order a worker takes queued builds in (placement::objective): by
//! wait plus critical path, which a run teaches the hub from the builds
//! that depend on it. Each client's share is in share.rs.
#![allow(clippy::float_cmp)]

use super::share::placed;
use super::*;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use crate::hub::metrics;
use crate::hub::naming::drv_name;
use crate::hub::placement::objective::{Priority, enqueue};
use crate::hub::placement::{self as placement, Size};
use crate::hub::record::Rest;
use crate::hub::shadow::Client;
use crate::proto::BuildUsage;

pub(super) type Reader = mpsc::Receiver<Result<AttachEvent, Status>>;

/// Queue `j` for `owner`, in the queue's order: its reader.
async fn put(state: &HubState, j: Job, owner: &str) -> Reader {
    let reader = reads(&j.replay, owner).await;
    enqueue(&mut *state.queue.lock().await, j);
    reader
}

/// Queue build `n`, with `path` seconds learned to follow it, queued `ago`
/// seconds ago, for `owner`: its reader.
pub(super) async fn queued(state: &HubState, n: usize, path: f64, ago: u64, owner: &str) -> Reader {
    let mut j = job(n, Arc::new(Replay::default()));
    j.expected.below = path;
    j.queued_at = Instant::now()
        .checked_sub(Duration::from_secs(ago))
        .unwrap();
    put(state, j, owner).await
}

/// The number of the build a worker takes next.
async fn next(state: &HubState) -> usize {
    let job = state.take_job(&caps("w"), None).await.unwrap();
    usize::from_str_radix(&job.id, 16).unwrap()
}

/// The longest remaining path is taken first, though queued later.
#[tokio::test]
async fn the_longest_critical_path_is_taken_first() {
    let state = HubState::for_test(Duration::ZERO);
    let _r = [
        queued(&state, 0, 10.0, 0, "a").await,
        queued(&state, 1, 600.0, 0, "a").await,
        queued(&state, 2, 60.0, 0, "a").await,
    ];
    assert_eq!(next(&state).await, 1);
    assert_eq!(next(&state).await, 2);
    assert_eq!(next(&state).await, 0);
}

/// Of builds of equal path, one queued earlier goes first, though put
/// into the queue after a later one (a submission takes its time before
/// the queue's lock).
#[tokio::test]
async fn an_earlier_build_put_in_later_goes_first() {
    let state = HubState::for_test(Duration::ZERO);
    let _r = [
        queued(&state, 0, 0.0, 0, "a").await,
        queued(&state, 1, 0.0, 10, "a").await,
    ];
    assert_eq!(next(&state).await, 1);
}

/// The starvation bound: a build c queued after b, with a path longer by
/// 90 s, is taken first only if it came less than 90 s after b.
#[tokio::test]
async fn a_later_build_passes_only_within_its_path_difference() {
    for (gap, first) in [(89, 1), (91, 0)] {
        let state = HubState::for_test(Duration::ZERO);
        let _r = [
            queued(&state, 0, 10.0, 1_000, "a").await,
            queued(&state, 1, 100.0, 1_000 - gap, "a").await,
        ];
        assert_eq!(next(&state).await, first, "{gap} s after");
    }
}

/// The path is what was learned to follow a build, not its own seconds:
/// one expected to run long, with nothing learned to follow it, goes after
/// one that is followed.
#[tokio::test]
async fn the_path_is_what_follows_not_its_own_seconds() {
    let state = HubState::for_test(Duration::ZERO);
    let mut j = job(0, Arc::new(Replay::default()));
    j.expected.secs = Some(3_600.0);
    let _a = put(&state, j, "a").await;
    let mut j = job(1, Arc::new(Replay::default()));
    (j.expected.secs, j.expected.below) = (Some(10.0), 50.0);
    let _b = put(&state, j, "a").await;
    // Queued second, it is first in the queue.
    assert_eq!(terms(&state, 0).await.path, 50.0);
    assert_eq!(next(&state).await, 1);
}

/// Builds of equal path and wait are taken in the order they came, among
/// many of other paths, whatever their own seconds.
#[tokio::test]
async fn equal_builds_keep_the_order_they_came_in() {
    let state = HubState::for_test(Duration::ZERO);
    let at = Instant::now();
    let mut readers = Vec::new();
    for n in 0..30 {
        let mut j = job(n, Arc::new(Replay::default()));
        #[allow(clippy::cast_precision_loss)]
        let own = Some((n * 37 % 11) as f64);
        (j.expected.below, j.expected.secs, j.queued_at) = ([5.0, 7.0, 6.0][n % 3], own, at);
        readers.push(put(&state, j, "a").await);
    }
    let mut order = Vec::new();
    for _ in 0..30 {
        order.push(next(&state).await);
    }
    // The 7 s builds, then the 6 s, then the 5 s, each as they came.
    let each: Vec<usize> = [1, 2, 0]
        .into_iter()
        .flat_map(|r| (r..30).step_by(3))
        .collect();
    assert_eq!(order, each);
}

/// A run here of build `n`, of derivation `name`, `secs` long, with
/// `inputs`: its request.
fn ran(state: &HubState, n: usize, name: &str, secs: u64, inputs: Vec<String>) -> BuildRequest {
    let req = BuildRequest {
        outputs: [("out".to_owned(), format!("/nix/store/{H}-{name}"))].into(),
        input_paths: inputs,
        ..request()
    };
    let mut j = job(n, Arc::new(Replay::default()));
    j.req = req.clone();
    let usage = BuildUsage {
        peak_memory_bytes: 1 << 20,
        cpu_usec: secs * 1_000_000,
        wall_ms: secs * 1_000,
        ..BuildUsage::default()
    };
    (state.placement.lock().unwrap()).learn(&j, 0, &usage, Rest::of(&j, "w"));
    BuildRequest {
        input_paths: Vec::new(),
        ..req
    }
}

/// A submitted build goes into the queue by its path: one a 600 s build
/// was seen to follow, submitted later, is taken before one nothing does.
#[tokio::test]
async fn submitted_builds_are_taken_by_path() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    state.worker_caps.lock().unwrap().insert(1, caps("w"));
    let lib = ran(&state, 1, "lib", 10, vec![]);
    ran(&state, 2, "app", 600, vec![format!("/nix/store/{H}-lib")]);
    let other = ran(&state, 3, "other", 10, vec![]);
    let _o = submit(&state, other).await;
    let _l = submit(&state, lib).await;
    let job = state.take_job(&caps("w"), None).await.unwrap();
    assert_eq!(drv_name(&job.req), Some("lib"));
}

/// A requeued build takes its place by its path, not at the back.
#[tokio::test]
async fn a_requeued_build_takes_its_place_by_path() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    state.worker_caps.lock().unwrap().insert(1, caps("w"));
    let _a = queued(&state, 0, 10.0, 0, "a").await;
    let mut j = job(1, Arc::new(Replay::default()));
    j.expected.below = 600.0;
    let _b = reads(&j.replay, "a").await;
    state.requeue(j).await;
    assert_eq!(next(&state).await, 1);
}

/// A request the attach service takes.
fn request() -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: format!("/nix/store/{H}-bash/bin/bash"),
        outputs: [("out".to_owned(), format!("/nix/store/{H}-out"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        ..Default::default()
    }
}

/// Each client of a build is one it holds a share for: the first and one
/// joining it, each as its request names it; one whose reader is gone
/// no more.
#[tokio::test(start_paused = true)]
async fn each_client_of_a_build_is_its_owner() {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    state.worker_caps.lock().unwrap().insert(1, caps("w"));
    let asks = |c: &str| BuildRequest {
        client: Some(c.into()),
        ..request()
    };
    let _first = submit(&state, asks("first")).await;
    // Bounded: a second build of the same outputs would wait for the first.
    let joined = tokio::time::timeout(Duration::from_secs(5), submit(&state, asks("joined")))
        .await
        .expect("the second client did not join the first's build");
    let owners = || {
        let queue = state.queue.try_lock().unwrap();
        let mut o: Vec<String> = queue[0]
            .replay
            .owners()
            .iter()
            .flatten()
            .map(ToString::to_string)
            .collect();
        o.sort();
        o
    };
    assert_eq!(owners(), ["first", "joined"]);
    drop(joined);
    tokio::time::timeout(Duration::from_secs(10), async {
        while owners() != ["first"] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a client gone still owns the build");
}

/// A run teaches what follows them the keys of its inputs' builds, each
/// of its own system: a build of a key one depended on then expects the
/// dependant's seconds below it; another system's build of that pname,
/// and a key only a failed run depended on, nothing.
#[test]
fn a_run_teaches_its_inputs_what_follows_them() {
    let mut p = placement::Placement::default();
    let usage = BuildUsage {
        peak_memory_bytes: 1 << 20,
        cpu_usec: 300_000_000,
        wall_ms: 300_000,
        ..BuildUsage::default()
    };
    let built = |n: usize, system: &str, name: &str, inputs: Vec<String>| {
        let mut j = job(n, Arc::new(Replay::default()));
        j.req.system = system.into();
        let out = format!("/nix/store/{H}-{name}");
        j.req.outputs = [
            ("out".into(), out.clone()),
            ("dev".into(), format!("{out}-dev")),
        ]
        .into();
        j.req.input_paths = inputs;
        j
    };
    // An input of the same name made by another derivation (another
    // hash), not by a run here, teaches its pname nothing.
    let libqux = built(11, "x86_64-linux", "libqux-1", vec![]);
    p.learn(&libqux, 0, &usage, Rest::of(&libqux, "w"));
    let other = format!("/nix/store/{}-libqux-1", "1".repeat(32));
    let uses = built(12, "x86_64-linux", "uses-1", vec![other]);
    p.learn(&uses, 0, &usage, Rest::of(&uses, "w"));
    let libqux = built(13, "x86_64-linux", "libqux-2", vec![]);
    assert_eq!(p.expect(&libqux).below, 0.0, "matched by name");
    let arm = "aarch64-linux";
    let libfoo = built(1, arm, "libfoo-1.3", vec![]);
    p.learn(&libfoo, 0, &usage, Rest::of(&libfoo, "w"));
    let before = p.expect(&built(3, arm, "libfoo-1.4", vec![])).demand;
    let app = built(
        2,
        "x86_64-linux",
        "app-2.0",
        vec![format!("/nix/store/{H}-libfoo-1.3-dev")],
    );
    p.learn(&app, 0, &usage, Rest::of(&app, "w"));
    let after = p.expect(&built(3, arm, "libfoo-1.4", vec![])).demand;
    assert_eq!(before, after, "what follows a build changed its demand");
    assert_eq!(p.expect(&built(3, arm, "libfoo-1.4", vec![])).below, 300.0);
    let x86 = built(4, "x86_64-linux", "libfoo-1.4", vec![]);
    assert_eq!(p.expect(&x86).below, 0.0, "another system's libfoo");
    // A failed run teaches nothing of what follows.
    let libbar = built(5, "x86_64-linux", "libbar-1", vec![]);
    p.learn(&libbar, 0, &usage, Rest::of(&libbar, "w"));
    let tool = built(
        6,
        "x86_64-linux",
        "tool-1",
        vec![format!("/nix/store/{H}-libbar-1")],
    );
    p.learn(&tool, 1, &usage, Rest::of(&tool, "w"));
    assert_eq!(
        p.expect(&built(7, "x86_64-linux", "libbar-2", vec![]))
            .below,
        0.0
    );
    // Nor one its worker's kernel OOM-killed, though it exited 0 and its
    // key has run before: it did not finish.
    let killed = BuildUsage {
        oom_killed: true,
        ..usage.clone()
    };
    let check = built(10, "x86_64-linux", "check-1", vec![]);
    p.learn(&check, 0, &usage, Rest::of(&check, "w"));
    let check = built(
        8,
        "x86_64-linux",
        "check-2",
        vec![format!("/nix/store/{H}-libbar-1")],
    );
    p.learn(&check, 0, &killed, Rest::of(&check, "w"));
    assert_eq!(
        p.expect(&built(9, "x86_64-linux", "libbar-2", vec![]))
            .below,
        0.0
    );
}

/// The metrics count runs by whether they named a producer run here, and
/// give by system the learned seconds of each key something follows (lib,
/// not app).
#[tokio::test]
async fn the_metrics_count_runs_that_named_a_producer() {
    let state = HubState::for_test(Duration::ZERO);
    {
        let d = &mut state.placement.lock().unwrap().downstream;
        d.learn("x86_64-linux/lib", 1.0, [String::from("/o")], &[]);
        d.learn(
            "x86_64-linux/app",
            120.0,
            [String::from("/p")],
            &["/o".into()],
        );
    }
    let text = metrics::render(&state).await;
    let lines = [
        "tribuchet_hub_dependants_unmatched_total 1",
        "tribuchet_hub_dependants_matched_total 1",
        "tribuchet_hub_below_seconds_bucket{system=\"x86_64-linux\",le=\"60\"} 0",
        "tribuchet_hub_below_seconds_bucket{system=\"x86_64-linux\",le=\"300\"} 1",
        "tribuchet_hub_below_seconds_bucket{system=\"x86_64-linux\",le=\"86400\"} 1",
        "tribuchet_hub_below_seconds_count{system=\"x86_64-linux\"} 1",
        "tribuchet_hub_below_seconds_sum{system=\"x86_64-linux\"} 120",
    ];
    for line in lines {
        assert!(text.lines().any(|l| l == line), "{line} not in {text}");
    }
}

/// A worker session of 32 cores and 64 GiB, as session 7.
pub(super) fn open_session(state: &HubState) {
    let size = Size {
        cores: 32.0,
        memory: Some(64.0 * 1_073_741_824.0),
    };
    state
        .placement
        .lock()
        .unwrap()
        .open(7, Arc::default(), size);
}

/// A client named `owner` reading `replay`.
pub(super) async fn reads(replay: &Arc<Replay>, owner: &str) -> Reader {
    let mut c = Client::untracked(false);
    c.owner = Some(Arc::from(owner));
    replay.subscribe_as(c).await
}

/// The terms of queued build `i` now, as a dispatch logs them.
pub(super) async fn terms(state: &HubState, i: usize) -> Priority {
    let queue = state.queue.lock().await;
    let share = state.placement.lock().unwrap().share(&queue[i]);
    Priority::of(&queue[i], share, Instant::now())
}

/// The value of `name` in a log line's fields.
fn field(line: &str, name: &str) -> f64 {
    let token = line
        .split_whitespace()
        .find_map(|t| t.strip_prefix(&format!("{name}=")));
    token
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        .parse()
        .unwrap()
}

/// A dispatch logs the build's terms as they were before it.
#[tokio::test]
async fn a_dispatch_logs_the_builds_terms() {
    let log = Captured;
    let _guard = log.install();
    let state = HubState::for_test(Duration::ZERO);
    open_session(&state);
    let _c = placed(&state, 9, &["c"]).await;
    let mut j = job(1, Arc::new(Replay::default()));
    (j.expected.secs, j.expected.below) = (Some(600.0), 30.0);
    j.queued_at = Instant::now()
        .checked_sub(Duration::from_secs(120))
        .unwrap();
    let _d = reads(&j.replay, "c").await;
    placement::dispatch(&state, (7, "w"), &j, false, &StdMutex::default());
    let line = log.line("dispatching build");
    assert_eq!(field(&line, "share"), 0.031, "{line}");
    assert_eq!(field(&line, "path"), 30.0, "{line}");
    assert!((120.0..121.0).contains(&field(&line, "waited")), "{line}");
    assert_eq!(field(&line, "expect_below"), 30.0, "{line}");
}
