//! Chunk serving of several builds on one worker session.

use std::ops::Range;
use std::time::{Duration, Instant};

use super::tests::{info, test_job};
use super::*;
use crate::chunkstore::io_hook;

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

/// Ask `staging` for every chunk of the one manifest it sent.
async fn need_all(staging: &mut Staging<'_>, out_tx: &mpsc::Sender<Result<HubMessage, Status>>) {
    let hashes = staging.sent[0].1.iter().flat_map(|(h, _)| *h).collect();
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
    let sess = WorkerSession::new("w".into());
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
