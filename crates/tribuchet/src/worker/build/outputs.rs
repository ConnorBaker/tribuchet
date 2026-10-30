//! Output packing: chunked NARs for outputs.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::num::NonZero;
use std::path::Path;
use std::thread;
use std::time::Instant;

use super::super::resume::{OutChunk, PackedOutput};
use super::super::sandbox;
use super::store_base;
use crate::chunker::{Chunk, chunk_store_path};
use crate::errors::{Result, err_ctx, err_msg};
use crate::fsutil::io_ctx;
use crate::proto::MAX_NAR_BYTES;

pub(in crate::worker) fn pack_outputs(
    dir: &Path,
    spec: &sandbox::SandboxSpec,
    deadline: Instant,
) -> Result<Vec<PackedOutput>> {
    let mut packed = Vec::new();
    for scratch in &spec.outputs {
        let host_path = sandbox::output_host_path(spec, scratch);
        // lstat: a symlink output whose target only resolves inside
        // the sandbox is still a valid output.
        if host_path.symlink_metadata().is_err() {
            return Err(err_msg(format!("builder did not produce output {scratch}")));
        }
        let frames_file = dir.join(format!("{}.frames", store_base(scratch)));
        let chunks = pack_one_nar(&host_path, &frames_file, deadline)
            .map_err(err_ctx(format!("packing output {scratch}")))?;
        packed.push(PackedOutput {
            scratch: scratch.clone(),
            frames_file,
            chunks,
        });
    }
    Ok(packed)
}

/// Chunks compressed per parallel batch.
const BATCH: usize = 64;

/// Chunk `host_path`'s NAR into a file of zstd frames. Blocking.
fn pack_one_nar(host_path: &Path, frames_path: &Path, deadline: Instant) -> Result<Vec<OutChunk>> {
    let mut frames = FrameWriter {
        file: BufWriter::new(File::create(frames_path).map_err(io_ctx("creating", frames_path))?),
        chunks: Vec::new(),
        off: 0,
    };
    let mut batch: Vec<Chunk> = Vec::with_capacity(BATCH);
    let mut total = 0u64;
    chunk_store_path(host_path, |c| {
        // A builder can exit instantly leaving a multi-TB sparse output.
        if Instant::now() >= deadline {
            return Err(err_msg("build timed out"));
        }
        total += c.data.len() as u64;
        if total > MAX_NAR_BYTES {
            return Err(err_msg(format!("NAR exceeds {MAX_NAR_BYTES} bytes")));
        }
        batch.push(c);
        if batch.len() == BATCH {
            frames.write(&mut batch)?;
        }
        Ok(true)
    })?;
    frames.write(&mut batch)?;
    frames
        .file
        .flush()
        .map_err(io_ctx("writing", frames_path))?;
    Ok(frames.chunks)
}

struct FrameWriter {
    file: BufWriter<File>,
    chunks: Vec<OutChunk>,
    off: u64,
}

impl FrameWriter {
    /// zstd-3 does ~330 MB/s per core, so compress a batch in parallel.
    fn write(&mut self, batch: &mut Vec<Chunk>) -> Result<()> {
        let threads = thread::available_parallelism()
            .map_or(1, NonZero::get)
            .min(4);
        let per = batch.len().div_ceil(threads).max(1);
        let parts: Vec<io::Result<Vec<Vec<u8>>>> = thread::scope(|s| {
            let handles: Vec<_> = batch
                .chunks(per)
                .map(|part| {
                    s.spawn(move || {
                        part.iter()
                            .map(|c| zstd::bulk::compress(&c.data, 3))
                            .collect()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut frames = Vec::with_capacity(batch.len());
        for p in parts {
            frames.append(&mut p.map_err(err_ctx("compressing chunk"))?);
        }
        for (c, frame) in batch.drain(..).zip(frames) {
            self.file.write_all(&frame)?;
            self.chunks.push(OutChunk {
                hash: c.hash,
                size: u32::try_from(c.data.len()).expect("chunk size bounded"),
                off: self.off,
                len: u32::try_from(frame.len()).expect("frame size bounded"),
            });
            self.off += frame.len() as u64;
        }
        Ok(())
    }
}
