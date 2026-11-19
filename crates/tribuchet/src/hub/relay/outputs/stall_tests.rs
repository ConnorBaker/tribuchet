//! Runtime workers that relay outputs and do chunk-store file I/O
//! themselves stall the runtime: one stalled 14 s in a pread's swap-in
//! until the hub's watchdog killed it. That I/O belongs
//! on the blocking pool: the store's io_hook fails a test that does it
//! on a runtime's own thread. An arriving chunk's admit is driven by the
//! chained-client tests; a cached chunk's read only here.

use std::sync::Arc;

use super::*;
use crate::chunkstore::io_hook;

#[tokio::test]
async fn a_cached_output_chunk_is_read_off_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().to_path_buf(), 1 << 20).unwrap());
    let data = b"cached output chunk".repeat(64);
    let hash = *blake3::hash(&data).as_bytes();
    let zstd = zstd::bulk::compress(&data, 3).unwrap();
    io_hook::off_runtime(|| cache.admit(hash, &zstd)).unwrap();
    let (_to_hub, mut in_rx) = mpsc::channel(1);
    let mut src = ChunkSource {
        cache: &cache,
        needed: HashSet::new(),
        in_rx: &mut in_rx,
    };
    let frame = src.get(&hash, data.len()).await.unwrap();
    assert_eq!(zstd::bulk::decompress(&frame, data.len()).unwrap(), data);
}
