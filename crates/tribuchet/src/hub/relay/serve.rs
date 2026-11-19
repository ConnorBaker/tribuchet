//! Serving the chunks of a Need: cached frames as stored, uncached
//! chunks by re-serializing the path that holds them.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
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

/// What the hub served a build, summed over its Needs as they are
/// served; chunks = cached + repacked. serve_ms includes the frames of
/// the session's other Needs sent meanwhile. A Need cut off (the staging
/// dropped mid-Need): plain_bytes and repacked count the chunks planned
/// for the manifests it reached, cached only those of a finished cache
/// pass, wire_bytes each frame handed to a send; needs and serve_ms
/// leave it out.
#[derive(Debug, Default)]
pub(super) struct Served {
    needs: AtomicU64,
    cached: AtomicU64,
    repacked: AtomicU64,
    plain_bytes: AtomicU64,
    wire_bytes: AtomicU64,
    serve_ms: AtomicU64,
}

/// Each needed chunk ships once, from the first manifest holding it,
/// each frame when this Need has the session's send permit.
pub(super) async fn serve_need(
    cache: &Arc<ChunkCache>,
    job: &Job,
    sent: &[Arc<(Info, Recipe)>],
    needed: HashSet<Hash>,
    out_tx: &mpsc::Sender<Result<HubMessage, Status>>,
    sess: &WorkerSession,
    total: &Served,
) -> Result<()> {
    let t0 = Instant::now();
    let mut served: HashSet<Hash> = HashSet::new();
    for e in sent {
        let (info, recipe) = &**e;
        let mut todo = HashSet::new();
        for (h, size) in recipe.iter().filter(|(h, _)| needed.contains(h)) {
            if served.insert(*h) {
                todo.insert(*h);
                total.plain_bytes.fetch_add(*size, Relaxed);
            }
        }
        if todo.is_empty() {
            continue;
        }
        let n = todo.len();
        let rest = serve_cached(cache, &job.id, todo, out_tx, sess, &total.wire_bytes).await?;
        total.cached.fetch_add((n - rest.len()) as u64, Relaxed);
        if rest.is_empty() {
            continue;
        }
        total.repacked.fetch_add(rest.len() as u64, Relaxed);
        let path = &info.store_path;
        stream_path_chunks(cache, job, path, rest, out_tx, sess, &total.wire_bytes).await?;
    }
    if served.len() != needed.len() {
        return Err(err_msg("worker needs chunks outside every manifest"));
    }
    let ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
    total.serve_ms.fetch_add(ms, Relaxed);
    total.needs.fetch_add(1, Relaxed);
    // The build's sums so far: a Need's own are the step from the last.
    tracing::debug!(id = job.id, worker = sess.worker, ?total, "need served");
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
    wire: &AtomicU64,
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
        wire.fetch_add(frame.len() as u64, Relaxed);
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
    wire: &AtomicU64,
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
        wire.fetch_add(frame.len() as u64, Relaxed);
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
        let wire = AtomicU64::default();
        let rest = serve_cached(&cache, "b1", todo.clone(), &tx, &sess, &wire)
            .await
            .unwrap();
        assert_eq!(rest, todo);

        let rest = serve_cached(&cache, "b1", [a].into(), &tx, &sess, &wire)
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
