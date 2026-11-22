//! The lookup against a store of maps, shaped as blob-deps' per-file
//! observation is (per-file.nix): the program's bytes a fixed output,
//! the observer and stdenv input-addressed, two input sources.

use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::{Duration, Instant};

use super::*;
use crate::hub::state::WorkerCaps;
use crate::hub::tests::fake_daemon::{Daemon as Fake, hub_with};
use crate::hub::tests::submit;
use crate::testlog::Captured;

const H: &str = "00000000000000000000000000000000";

fn p(name: &str) -> String {
    format!("/nix/store/{H}-{name}")
}

/// A store's answers: derivers by output, references and whether
/// content-addressed by path (a valid path), outputs by derivation.
#[derive(Default)]
struct Maps {
    derivers: HashMap<String, Vec<String>>,
    infos: HashMap<String, (Vec<String>, bool)>,
    outputs: HashMap<String, Vec<String>>,
}

impl Store for Maps {
    async fn derivers(&mut self, path: &str) -> Option<Vec<String>> {
        self.derivers.get(path).cloned()
    }
    async fn info(&mut self, path: &str) -> Option<(Vec<String>, bool)> {
        self.infos.get(path).cloned()
    }
    async fn outputs(&mut self, drv: &str) -> Option<Vec<String>> {
        self.outputs.get(drv).cloned()
    }
}

/// An observation of program `file` by observer `obs`: its derivation
/// references two sources, bash (as `builtins.storePath` gives it), and
/// the derivations of the file (a fixed output), of stdenv and of the
/// observer, which also has a `doc` output the build does not consume
/// and the store does not have.
fn observation(file: &str, obs: &str) -> (Maps, BuildRequest) {
    let drv = p(&format!("{file}-{obs}-program-observation.json.drv"));
    let out = p(&format!("{file}-{obs}-program-observation.json"));
    let mut m = Maps::default();
    m.derivers.insert(out.clone(), vec![drv.clone()]);
    let refs = [
        p("source-stdenv.sh"),
        p("default-builder.sh"),
        p("bash"),
        p(&format!("{file}.drv")),
        p("stdenv.drv"),
        p(&format!("{obs}.drv")),
    ];
    m.infos.insert(drv, (refs.to_vec(), false));
    // Two sources, and bash, input-addressed, which `builtins.storePath`
    // made a source.
    for (src, ca) in [
        ("source-stdenv.sh", true),
        ("default-builder.sh", true),
        ("bash", false),
    ] {
        m.infos.insert(p(src), (vec![], ca));
    }
    for (d, outs, ca) in [
        (format!("{file}.drv"), vec![p(file)], true),
        ("stdenv.drv".to_owned(), vec![p("stdenv")], false),
        (
            format!("{obs}.drv"),
            vec![p(obs), p(&format!("{obs}-doc"))],
            false,
        ),
    ] {
        m.infos.insert(outs[0].clone(), (vec![], ca));
        m.outputs.insert(p(&d), outs);
    }
    let req = BuildRequest {
        outputs: [("out".to_owned(), out)].into(),
        input_paths: [
            p("source-stdenv.sh"),
            p("default-builder.sh"),
            p("bash"),
            p(file),
            p("stdenv"),
            p(obs),
        ]
        .into(),
        ..Default::default()
    };
    (m, req)
}

fn digest(paths: &[String]) -> String {
    let mut paths = paths.to_vec();
    paths.sort_unstable();
    hex::encode(Sha256::digest(paths.join("\n")))
}

fn run<T>(f: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

/// The content-addressed sources and the fixed output it consumes, not
/// the input-addressed inputs, nor an output it does not consume.
#[test]
fn the_sources_are_the_content_addressed_direct_inputs() {
    let (mut m, req) = observation("file-a", "observer-1");
    let want = digest(&[p("source-stdenv.sh"), p("default-builder.sh"), p("file-a")]);
    assert_eq!(run(sources(&mut m, &req)), Some(want));
}

/// The same program read by another observer has the same sources;
/// another program, others.
#[test]
fn the_same_bytes_under_another_observer_are_the_same_sources() {
    let of = |file, obs| {
        let (mut m, req) = observation(file, obs);
        run(sources(&mut m, &req))
    };
    assert_eq!(of("file-a", "observer-1"), of("file-a", "observer-2"));
    assert_ne!(of("file-a", "observer-1"), of("file-b", "observer-1"));
}

/// No deriver of its output (one already valid gets a scratch path), and
/// no content-addressed input, are no sources; so is a store that does
/// not answer for an input it consumes.
#[test]
fn none_where_the_store_knows_none() {
    let (mut m, req) = observation("file-a", "observer-1");
    m.derivers.clear();
    assert_eq!(run(sources(&mut m, &req)), None, "no deriver");
    let (mut m, req) = observation("file-a", "observer-1");
    let drv = m.derivers.values().next().unwrap()[0].clone();
    m.infos.insert(drv, (vec![p("stdenv.drv")], false));
    assert_eq!(
        run(sources(&mut m, &req)),
        None,
        "nothing content-addressed"
    );
    let (mut m, req) = observation("file-a", "observer-1");
    m.infos.remove(&p("file-a"));
    assert_eq!(run(sources(&mut m, &req)), None, "a consumed input unknown");
}

/// A daemon that takes no connection, or never answers, holds no build:
/// the lookup gives none and says why, in ANSWER at most.
#[tokio::test(start_paused = true)]
async fn a_daemon_that_does_not_answer_gives_none() {
    let log = Captured::default();
    let _guard = log.install();
    let (_, req) = observation("file-a", "observer-1");
    for (daemon, why) in [
        (Fake::Closes, "connecting to the daemon"),
        (Fake::Silent, "the daemon did not answer"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let state = hub_with(dir.path(), daemon, 1);
        let began = tokio::time::Instant::now();
        let got = tokio::time::timeout(2 * ANSWER, of(&state.daemon_pool, &req)).await;
        assert_eq!(got, Ok(None), "{why}");
        assert!(began.elapsed() <= ANSWER, "{why}: {:?}", began.elapsed());
        assert!(log.has(&["no content-addressed inputs read", why]), "{why}");
    }
}

/// A request the hub validates and queues.
fn request() -> BuildRequest {
    BuildRequest {
        system: "x86_64-linux".into(),
        builder: p("bash/bin/bash"),
        outputs: [("out".to_owned(), p("dedupe"))].into(),
        tmp_dir_in_sandbox: "/build".into(),
        store_dir: "/nix/store".into(),
        ..Default::default()
    }
}

/// A submission that joins a build in flight looks no sources up:
/// with a daemon that never answers, the first waits out its lookup,
/// and the second joins it at once.
#[tokio::test(start_paused = true)]
async fn a_resubmission_looks_no_sources_up() {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Fake::Silent, 1);
    let caps = WorkerCaps {
        name: "w".into(),
        connected_at: Instant::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 1,
    };
    state.worker_caps.lock().unwrap().insert(1, caps);
    let first = tokio::spawn({
        let state = state.clone();
        async move { submit(&state, request()).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = tokio::time::timeout(Duration::from_secs(10), submit(&state, request())).await;
    assert!(second.is_ok(), "the resubmission waited for a lookup");
    drop(first.await.unwrap());
    // The first's lookup ends, and its build is queued, in a task.
    tokio::time::sleep(2 * ANSWER).await;
    assert_eq!(state.queue.lock().await.len(), 1, "not queued once");
}

/// A first submission cancelled during its lookup, after a second
/// joined it, still queues the build: the second waits on it (the
/// review's probe found it unlisted and never queued).
#[tokio::test(start_paused = true)]
async fn a_cancelled_first_submission_still_queues_for_its_joiner() {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Fake::Silent, 1);
    let caps = WorkerCaps {
        name: "w".into(),
        connected_at: Instant::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 1,
    };
    state.worker_caps.lock().unwrap().insert(1, caps);
    let first = tokio::spawn({
        let state = state.clone();
        async move { submit(&state, request()).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = tokio::time::timeout(Duration::from_secs(10), submit(&state, request()))
        .await
        .expect("the second joins at once");
    first.abort();
    let _ = first.await;
    tokio::time::sleep(2 * ANSWER).await;
    let listed = state.inflight.lock().unwrap().by_key.len();
    let queued = state.queue.lock().await.len();
    assert_eq!((listed, queued), (1, 1), "listed, queued");
    drop(second);
}

/// A listed build whose queueing task ends before the push (here at a
/// poisoned placement lock) ends: its client's stream ends rather than
/// waiting on a build no queue holds.
#[tokio::test(start_paused = true)]
async fn a_build_never_queued_ends_its_stream() {
    let dir = tempfile::tempdir().unwrap();
    let state = hub_with(dir.path(), Fake::Closes, 1);
    let caps = WorkerCaps {
        name: "w".into(),
        connected_at: Instant::now(),
        systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
        max_jobs: 1,
    };
    state.worker_caps.lock().unwrap().insert(1, caps);
    let poison = state.clone();
    let poisoned = thread::spawn(move || {
        let _held = poison.placement.lock().unwrap();
        panic!("poisoning the placement lock");
    });
    assert!(poisoned.join().is_err());
    let mut stream = submit(&state, request()).await;
    let ended = tokio::time::timeout(2 * ANSWER, async {
        while let Ok(Some(_)) = stream.message().await {}
    })
    .await;
    assert!(ended.is_ok(), "the client waits on a build never queued");
    assert!(
        state.inflight.lock().unwrap().by_key.is_empty(),
        "still listed"
    );
}
