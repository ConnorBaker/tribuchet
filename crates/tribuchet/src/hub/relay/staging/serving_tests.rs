//! Chunk serving of several builds on one worker session.

use std::fs;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use super::tests::{info, test_job};
use super::*;
use crate::chunker::{Chunk, chunk_store_path};
use crate::chunkstore::io_hook;
use crate::report::REPORT;
use crate::testlog::Captured;

/// Recipe size of every test chunk.
const CHUNK: u64 = 1 << 20;

fn job_with_id(id: &str) -> Job {
    Job {
        id: id.into(),
        ..test_job()
    }
}

/// A staging that sent one manifest whose chunks are all cached.
fn staged<'a>(
    state: &'a HubState,
    job: &'a Job,
    sess: &'a WorkerSession,
    path: &str,
    chunks: Range<u8>,
) -> Staging<'a> {
    let recipe: Vec<(Hash, u64)> = chunks.map(|i| ([i; 32], CHUNK)).collect();
    io_hook::off_runtime(|| {
        for (h, _) in &recipe {
            state.chunks.admit(*h, &[h[0]]).unwrap();
        }
    });
    let mut staging = Staging::new(state, job, sess);
    staging
        .sent
        .push(Arc::new((info(path, &[]), Arc::new(recipe))));
    staging
}

/// Ask `staging` for every chunk of the last manifest it sent.
async fn need_all(staging: &mut Staging<'_>, out_tx: &mpsc::Sender<Result<HubMessage, Status>>) {
    let (_, recipe) = &**staging.sent.last().unwrap();
    let hashes = recipe.iter().flat_map(|(h, _)| *h).collect();
    let need = Need {
        build_id: staging.job.id.clone(),
        paths: vec![],
        hashes,
    };
    staging.handle_need(need, out_tx).await.unwrap();
}

/// Serve `first` until its first chunk is out, then `second` as well.
/// Returns every chunk frame the worker receives after that, as
/// (build id, eof).
async fn serve_overlapping(
    first: &mut Staging<'_>,
    second: &mut Staging<'_>,
) -> Vec<(String, bool)> {
    let (out_tx, mut out_rx) = mpsc::channel(1);
    need_all(first, &out_tx).await;
    loop {
        tokio::select! {
            r = first.progress(&out_tx), if first.busy() => r.unwrap(),
            m = out_rx.recv() => {
                m.unwrap().unwrap();
                break;
            }
        }
    }
    need_all(second, &out_tx).await;
    // The second Need's first frame comes from a blocking thread, which
    // races the first's frames. Hold the first (its send waits for room)
    // until the second has asked for the send permit.
    let asked = second.sess.asked();
    let deadline = Instant::now() + Duration::from_secs(20);
    while second.sess.asked() == asked {
        assert!(
            Instant::now() < deadline,
            "the second Need never asked for the send permit"
        );
        tokio::select! {
            r = second.progress(&out_tx), if second.busy() => r.unwrap(),
            () = tokio::task::yield_now() => {}
        }
    }
    let mut frames = Vec::new();
    let mut eofs = 0;
    while eofs < 2 {
        tokio::select! {
            r = first.progress(&out_tx), if first.busy() => r.unwrap(),
            r = second.progress(&out_tx), if second.busy() => r.unwrap(),
            m = out_rx.recv() => {
                if let Some(hub_message::Msg::Chunk(c)) = m.unwrap().unwrap().msg {
                    eofs += usize::from(c.eof);
                    frames.push((c.build_id, c.eof));
                }
            }
        }
    }
    frames
}

fn position(frames: &[(String, bool)], id: &str, eof: bool) -> usize {
    frames
        .iter()
        .position(|(i, e)| i == id && (*e || !eof))
        .unwrap()
}

/// Two builds on one worker need disjoint paths: one many chunks, one
/// a single chunk. The small one must not wait for the large transfer
/// to finish, as builds on a worker once waited minutes behind a 3.4 GB
/// input.
#[tokio::test(start_paused = true)]
async fn a_small_need_does_not_wait_for_a_large_one() {
    let state = HubState::for_test(Duration::from_secs(1));
    let sess = WorkerSession::new("w".into(), mpsc::channel(1).0);
    let big_job = job_with_id("0000000000000000000000000000000a");
    let small_job = job_with_id("0000000000000000000000000000000b");
    let mut big = staged(
        &state,
        &big_job,
        &sess,
        "/nix/store/00000000000000000000000000000000-big",
        0..64,
    );
    let mut small = staged(
        &state,
        &small_job,
        &sess,
        "/nix/store/11111111111111111111111111111111-small",
        64..65,
    );
    let frames = serve_overlapping(&mut big, &mut small).await;
    assert!(
        position(&frames, &small_job.id, true) < position(&frames, &big_job.id, true),
        "the small build's chunks waited for the large transfer"
    );
}

/// A worker sends a Need per late manifest, so a Need per input path:
/// one build staging 18316 inputs logged 12,119 lines in 90 s, and
/// journald dropped the hub's other lines. A build's Needs log one line.
#[tokio::test]
async fn a_builds_needs_log_one_line_with_their_sums() {
    let log = Captured::default();
    let _guard = log.install();
    let state = HubState::for_test(Duration::from_secs(1));
    let (job, sess) = (
        test_job(),
        WorkerSession::new("w1".into(), mpsc::channel(1).0),
    );
    let mut staging = Staging::new(&state, &job, &sess);
    let (out_tx, mut out_rx) = mpsc::channel(1);
    for i in 0..3 {
        // a chunk the recipe repeats: its plaintext counts once
        let recipe = Arc::new(vec![([i; 32], CHUNK), ([i; 32], CHUNK)]);
        let path = format!("/nix/store/{i}-x");
        staging.sent.push(Arc::new((info(&path, &[]), recipe)));
        io_hook::off_runtime(|| state.chunks.admit([i; 32], &[i]).unwrap());
        need_all(&mut staging, &out_tx).await;
    }
    let mut eofs = 0;
    while eofs < 3 {
        tokio::select! {
            r = staging.progress(&out_tx), if staging.busy() => r.unwrap(),
            m = out_rx.recv() => if let Some(hub_message::Msg::Chunk(c)) = m.unwrap().unwrap().msg {
                eofs += usize::from(c.eof);
            }
        }
    }
    drop(staging);
    assert!(!log.has(&["INFO", "need served"]), "a Need's own INFO line");
    assert_eq!(log.count("needs served"), 1);
    let line = log.line("needs served");
    // three chunks of CHUNK plaintext bytes, sent as one-byte frames
    let sums = "needs: 3, cached: 3, repacked: 0, plain_bytes: 3145728, wire_bytes: 3,";
    assert!(line.contains(sums), "{line}");
    assert!(line.contains(r#"worker="w1""#), "{line}");
}

/// A staging whose Need is never answered said nothing while it lasted:
/// one served for 4 h with no line after its first Need. It reports its
/// counts once per REPORT.
#[tokio::test(start_paused = true)]
async fn a_staging_that_never_finishes_reports_where_it_stands() {
    let log = Captured;
    let _guard = log.install();
    let state = HubState::for_test(Duration::from_secs(1));
    let path = "/nix/store/22222222222222222222222222222222-stuck";
    let mut job = job_with_id("0000000000000000000000000000000c");
    job.req.input_paths = vec![path.into()];
    let sess = WorkerSession::new("w2".into(), mpsc::channel(1).0);
    let mut staging = staged(&state, &job, &sess, path, 0..2);
    // Never polled, the Need being served sends nothing.
    let (out_tx, _out_rx) = mpsc::channel(8);
    need_all(&mut staging, &out_tx).await;
    staging.tasks.push(Box::pin(pending()));
    tokio::time::advance(REPORT).await;
    // Its worker, session 2, is gone: not known to reach no agent.
    staging.report_if_due(2);
    let line = log.line("build not started");
    for field in [
        r#"id="0000000000000000000000000000000c""#,
        r#"worker="w2""#,
        "staging_s=600",
        "computing=1",
        "needs_received=1",
        "serving=true",
        "needs: 0,",
        "slots=None",
        r#"worker_agents="unknown""#,
    ] {
        assert!(line.contains(field), "{field} in {line}");
    }
}

/// A manifest for a file of `len` bytes that does not repeat, as its own
/// path: the NAR the packer scans, of chunks the cache lacks.
fn file_manifest(dir: &Path, len: usize) -> Arc<(Info, Recipe)> {
    let file = dir.join("input");
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let bytes: Vec<u8> = (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect();
    fs::write(&file, bytes).unwrap();
    let mut recipe = Vec::new();
    let mut chunk = |c: Chunk| {
        recipe.push((c.hash, c.data.len() as u64));
        Ok(true)
    };
    chunk_store_path(&file, &mut chunk).unwrap();
    let msg = PathInfoMsg {
        nar_size: recipe.iter().map(|(_, n)| n).sum(),
        ..PathInfoMsg::default()
    };
    let store_path = file.to_str().unwrap().into();
    Arc::new((Info { store_path, msg }, Arc::new(recipe)))
}

/// A sum of `served`'s, by name.
fn sum(served: &Served, name: &str) -> u64 {
    let served = format!("{served:?}");
    let at = served.find(&format!("{name}: ")).unwrap() + name.len() + 2;
    served[at..]
        .split([',', ' '])
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

/// A Need's two waits to send show apart: for the session's send permit,
/// held here by another Need until this one asks for it and 200 ms on,
/// and for room in the session's stream, read only 500 ms after that.
#[tokio::test(start_paused = true)]
async fn a_need_being_served_tells_its_wait_for_the_permit_from_the_link() {
    let state = HubState::for_test(Duration::from_secs(1));
    let job = test_job();
    let sess = WorkerSession::new("w".into(), mpsc::channel(1).0);
    let dir = tempfile::tempdir().unwrap();
    let mut staging = Staging::new(&state, &job, &sess);
    staging.sent.push(file_manifest(dir.path(), 300_000));
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let held = sess.send_permit().await;
    let asked = sess.asked();
    need_all(&mut staging, &out_tx).await;
    let released = AtomicBool::new(false);
    let serve = async {
        while staging.busy() {
            staging.progress(&out_tx).await.unwrap();
        }
    };
    let release = async {
        while sess.asked() == asked {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(held);
        released.store(true, Ordering::Relaxed);
    };
    let read = async {
        while !released.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        while let Some(m) = out_rx.recv().await {
            if let Some(hub_message::Msg::Chunk(c)) = m.unwrap().msg
                && c.eof
            {
                break;
            }
        }
    };
    tokio::join!(serve, release, read);
    let s = &staging.served;
    let (permit, send) = (sum(s, "permit_us"), sum(s, "send_us"));
    assert!((200_000..300_000).contains(&permit), "{s:?}");
    assert!((500_000..600_000).contains(&send), "{s:?}");
}
