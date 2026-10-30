//! Serving the chunks of a Need: cached frames as stored, uncached
//! chunks by re-serializing the path that holds them.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Instant;

use futures_util::StreamExt as _;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use zstd::bulk::compress;

use crate::chunker::{Chunk, chunk_store_path};
use crate::chunkstore::Hash;
use crate::errors::{Result, err_ctx, err_msg};
use crate::hub::chunkcache::{ChunkCache, Disposition, Recipe};
use crate::hub::relay::send;
use crate::hub::relay::staging::{Info, WorkerSession};
use crate::hub::state::Job;
use crate::proto::{ChunkFrame, HubMessage, hub_message};

/// Each needed chunk ships once, from the first manifest holding it,
/// each frame when this Need has the session's send permit.
pub(super) async fn serve_need(
    cache: &Arc<ChunkCache>,
    job: &Job,
    sent: &[Arc<(Info, Recipe)>],
    needed: HashSet<Hash>,
    out_tx: &mpsc::Sender<Result<HubMessage, Status>>,
    sess: &WorkerSession,
) -> Result<()> {
    let t0 = Instant::now();
    let mut sizes: HashMap<Hash, u64> = HashMap::new();
    for e in sent {
        for (h, size) in e.1.iter().filter(|(h, _)| needed.contains(h)) {
            sizes.entry(*h).or_insert(*size);
        }
    }
    let plain_bytes: u64 = sizes.values().sum();
    let mut served: HashSet<Hash> = HashSet::new();
    let (mut cached, mut repacked, mut wire_bytes) = (0usize, 0usize, 0u64);
    for e in sent {
        let (info, recipe) = &**e;
        let todo: HashSet<Hash> = recipe
            .iter()
            .map(|(h, _)| *h)
            .filter(|h| needed.contains(h) && !served.contains(h))
            .collect();
        if todo.is_empty() {
            continue;
        }
        served.extend(&todo);
        let n = todo.len();
        let rest = serve_cached(cache, &job.id, todo, out_tx, sess, &mut wire_bytes).await?;
        cached += n - rest.len();
        if rest.is_empty() {
            continue;
        }
        repacked += rest.len();
        let path = &info.store_path;
        stream_path_chunks(cache, job, path, rest, out_tx, sess, &mut wire_bytes).await?;
    }
    if served.len() != needed.len() {
        return Err(err_msg("worker needs chunks outside every manifest"));
    }
    // Per Need: what the hub sent each worker, and how fast. elapsed
    // includes the frames of the session's other Needs sent meanwhile.
    tracing::info!(
        id = job.id,
        worker = sess.worker,
        chunks = served.len(),
        cached,
        repacked,
        plain_bytes,
        wire_bytes,
        elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
        "need served"
    );
    Ok(())
}

fn cores() -> usize {
    thread::available_parallelism().map_or(4, usize::from)
}

/// One core per pack: a large closure must not flood the blocking pool.
static PACK_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(cores()));

/// Serve a recipe from the in-memory cache or pack and chunk the NAR.
pub(super) async fn compute_recipe(
    cache: &ChunkCache,
    store_path: &str,
    nar_sha256: &[u8],
) -> Result<Recipe> {
    if let Some(r) = cache.recipe(store_path, nar_sha256) {
        return Ok(r);
    }
    let _permit = PACK_PERMITS.acquire().await.expect("pack semaphore closed");
    let t0 = Instant::now();
    let path = store_path.to_string();
    let recipe = spawn_blocking(move || -> Result<Vec<(Hash, u64)>> {
        let mut out = Vec::new();
        chunk_store_path(Path::new(&path), |c| {
            out.push((c.hash, c.data.len() as u64));
            Ok(true)
        })?;
        Ok(out)
    })
    .await
    .map_err(err_ctx("recipe task panicked"))??;
    tracing::debug!(
        path = store_path,
        chunks = recipe.len(),
        elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
        "recipe computed"
    );
    let recipe = Arc::new(recipe);
    cache.store_recipe(store_path.to_string(), nar_sha256.to_vec(), recipe.clone());
    Ok(recipe)
}

async fn send_frame(
    out_tx: &mpsc::Sender<Result<HubMessage, Status>>,
    sess: &WorkerSession,
    build_id: &str,
    hash: &Hash,
    zstd: Vec<u8>,
) -> Result<()> {
    let _permit = sess.send_permit().await;
    send(
        out_tx,
        hub_message::Msg::Chunk(ChunkFrame {
            build_id: build_id.to_string(),
            hash: hash.to_vec(),
            zstd,
            eof: false,
        }),
    )
    .await
}

/// Serve needed chunks from the cache, returning what still needs a
/// repack. Adds the frame bytes sent to `wire`. A blocking thread reads
/// the frames, one ahead of the sends.
async fn serve_cached(
    cache: &ChunkCache,
    build_id: &str,
    todo: HashSet<Hash>,
    out_tx: &mpsc::Sender<Result<HubMessage, Status>>,
    sess: &WorkerSession,
    wire: &mut u64,
) -> Result<HashSet<Hash>> {
    let Some(frames) = cache.locate_all(&todo) else {
        return Ok(todo);
    };
    let (tx, mut rx) = mpsc::channel(1);
    let reader = spawn_blocking(move || {
        for (hash, fref) in frames {
            // A failed read: the repack picks up this chunk and the rest.
            let Ok(frame) = fref.read_blocking() else {
                return;
            };
            if tx.blocking_send((hash, frame)).is_err() {
                return;
            }
        }
    });
    let mut rest = todo;
    while let Some((hash, frame)) = rx.recv().await {
        *wire += frame.len() as u64;
        send_frame(out_tx, sess, build_id, &hash, frame).await?;
        rest.remove(&hash);
    }
    reader.await.map_err(err_ctx("cache read task panicked"))?;
    Ok(rest)
}

/// Re-pack the path and send its needed chunks in recipe order,
/// compressing up to one chunk per core in parallel. Adds the frame
/// bytes sent to `wire`.
async fn stream_path_chunks(
    cache: &Arc<ChunkCache>,
    job: &Job,
    store_path: &str,
    todo: HashSet<Hash>,
    out_tx: &mpsc::Sender<Result<HubMessage, Status>>,
    sess: &WorkerSession,
    wire: &mut u64,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<Chunk>(cores());
    let path = store_path.to_string();
    let packer = spawn_blocking(move || {
        chunk_store_path(Path::new(&path), |c| {
            if todo.contains(&c.hash) && tx.blocking_send(c).is_err() {
                return Err(err_msg("chunk consumer gone"));
            }
            Ok(true)
        })
    });
    let mut frames = ReceiverStream::new(rx)
        .map(|chunk| {
            let cache = cache.clone();
            spawn_blocking(move || frame_for(&cache, &chunk).map(|f| (chunk.hash, f)))
        })
        .buffered(cores());
    while let Some(frame) = frames.next().await {
        let (hash, frame) = frame.map_err(err_ctx("compress task panicked"))??;
        *wire += frame.len() as u64;
        send_frame(out_tx, sess, &job.id, &hash, frame).await?;
    }
    packer.await.map_err(err_ctx("pack task panicked"))?
}

/// The cached frame if there is one, else compress, admitting to the
/// cache on second sighting.
fn frame_for(cache: &ChunkCache, chunk: &Chunk) -> Result<Vec<u8>> {
    let admit = match cache.classify(&chunk.hash) {
        Disposition::Cached(fref) => match fref.read_blocking() {
            Ok(frame) => return Ok(frame),
            Err(_) => false,
        },
        Disposition::FirstSeen => false,
        Disposition::Admit => true,
    };
    let frame = compress(&chunk.data, 3).map_err(err_ctx("compressing chunk"))?;
    if admit {
        // only loses future hits
        if let Err(e) = cache.admit(chunk.hash, &frame) {
            tracing::warn!(error = %e, "chunk cache write failed");
        }
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkstore::io_hook;
    use crate::hub::state::Replay;
    use crate::hub::testlog::Captured;
    use crate::hub::tests::job;
    use crate::proto::PathInfoMsg;

    /// Every chunk of the Need is cached: the line counts them as
    /// cached and carries the recipe's plaintext and the frames' bytes.
    #[tokio::test]
    async fn the_need_line_carries_worker_chunks_and_bytes() {
        let log = Captured::default();
        let _guard = log.install();
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(ChunkCache::open(dir.path().to_path_buf(), 1 << 20).unwrap());
        let (a, b): (Hash, Hash) = ([1; 32], [2; 32]);
        let (fa, fb) = (
            compress(b"hello", 3).unwrap(),
            compress(b"world!!", 3).unwrap(),
        );
        let wire = fa.len() + fb.len();
        io_hook::off_runtime(|| {
            cache.admit(a, &fa).unwrap();
            cache.admit(b, &fb).unwrap();
        });
        let job = job(1, Arc::new(Replay::default()));
        let info = Info {
            store_path: "/nix/store/x".into(),
            msg: PathInfoMsg::default(),
        };
        let sent = [Arc::new((info, Arc::new(vec![(a, 5), (b, 7), (a, 5)])))];
        let (tx, _rx) = mpsc::channel(8);
        let sess = WorkerSession::new("w1".into());
        serve_need(&cache, &job, &sent, [a, b].into(), &tx, &sess)
            .await
            .unwrap();
        let line = log.line("need served");
        for field in [
            r#"worker="w1""#.to_owned(),
            "chunks=2".to_owned(),
            "cached=2".to_owned(),
            "repacked=0".to_owned(),
            "plain_bytes=12".to_owned(),
            format!("wire_bytes={wire}"),
        ] {
            assert!(line.contains(&field), "{field} missing from {line}");
        }
    }

    #[tokio::test]
    async fn serve_cached_skips_repack_and_reports_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::open(dir.path().to_path_buf(), 1 << 20).unwrap();
        let a: Hash = [1; 32];
        let b: Hash = [2; 32];
        io_hook::off_runtime(|| cache.admit(a, &compress(b"hello", 3).unwrap()).unwrap());

        let (tx, mut rx) = mpsc::channel(8);
        let sess = WorkerSession::new("w".into());

        let todo: HashSet<Hash> = [a, b].into();
        let mut wire = 0;
        let rest = serve_cached(&cache, "b1", todo.clone(), &tx, &sess, &mut wire)
            .await
            .unwrap();
        assert_eq!(rest, todo);

        let rest = serve_cached(&cache, "b1", [a].into(), &tx, &sess, &mut wire)
            .await
            .unwrap();
        assert!(rest.is_empty());
        let msg = rx.recv().await.unwrap().unwrap();
        let Some(hub_message::Msg::Chunk(c)) = msg.msg else {
            panic!("expected ChunkFrame");
        };
        assert_eq!(c.hash, a.to_vec());
        assert_eq!(zstd::bulk::decompress(&c.zstd, 1 << 20).unwrap(), b"hello");
    }
}
