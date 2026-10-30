//! Append-only pack files: records of (len, hash, zstd frame) with a
//! sidecar index written at seal. Queue order equals pack order, so
//! S3-FIFO eviction is one unlink per pack.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::Hash;

/// bytes preceding the frame payload: u32 len + 32 B hash
const HEADER_LEN: usize = 4 + 32;
const HEADER: u64 = HEADER_LEN as u64;

pub(super) fn pack_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:016x}.pack"))
}

pub(super) fn index_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:016x}.idx"))
}

pub(super) struct PackWriter {
    /// Also the pack's read handle, so readers never open files.
    file: Arc<File>,
    pub(super) id: u64,
    pub(super) len: u64,
    /// (hash, payload offset, payload len) in append order
    entries: Vec<(Hash, u64, u32)>,
}

impl PackWriter {
    pub(super) fn create(dir: &Path, id: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(pack_path(dir, id))?;
        Ok(Self {
            file: Arc::new(file),
            id,
            len: 0,
            entries: Vec::new(),
        })
    }

    /// Append one record at the tracked end, so a failed partial write
    /// is overwritten by the next record. Returns the payload offset.
    pub(super) fn append(&mut self, hash: &Hash, frame: &[u8]) -> io::Result<u64> {
        let len = u32::try_from(frame.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "oversized chunk frame"))?;
        let mut rec = Vec::with_capacity(HEADER_LEN + frame.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(hash);
        rec.extend_from_slice(frame);
        #[cfg(test)]
        io_hook::fire(io_hook::Op::Write);
        self.file.write_all_at(&rec, self.len)?;
        let offset = self.len + HEADER;
        self.len += HEADER + u64::from(len);
        self.entries.push((*hash, offset, len));
        Ok(offset)
    }

    /// Fsync the data and write the sidecar index. The index is the
    /// seal marker: a pack without one is recovered by scanning.
    pub(super) fn seal(self, dir: &Path) -> io::Result<()> {
        // drop a torn tail left by a failed last append
        self.file.set_len(self.len)?;
        #[cfg(test)]
        io_hook::fire(io_hook::Op::Sync);
        self.file.sync_data()?;
        write_index(dir, self.id, &self.entries)
    }
}

fn write_index(dir: &Path, id: u64, entries: &[(Hash, u64, u32)]) -> io::Result<()> {
    let tmp = dir.join(format!("{id:016x}.idx.tmp"));
    let mut f = File::create(&tmp)?;
    let mut buf = Vec::with_capacity(entries.len() * 44);
    for (hash, offset, len) in entries {
        buf.extend_from_slice(hash);
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&len.to_le_bytes());
    }
    f.write_all(&buf)?;
    #[cfg(test)]
    io_hook::fire(io_hook::Op::IndexSync);
    f.sync_data()?;
    fs::rename(tmp, index_path(dir, id))
}

pub(super) fn load_index(dir: &Path, id: u64) -> io::Result<Vec<(Hash, u64, u32)>> {
    #[cfg(test)]
    io_hook::fire(io_hook::Op::IndexLoad);
    let data = fs::read(index_path(dir, id))?;
    let mut out = Vec::with_capacity(data.len() / 44);
    for rec in data.chunks_exact(44) {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&rec[..32]);
        let offset = u64::from_le_bytes(rec[32..40].try_into().unwrap());
        let len = u32::from_le_bytes(rec[40..44].try_into().unwrap());
        out.push((hash, offset, len));
    }
    Ok(out)
}

/// Scan an unsealed pack up to the last intact record and truncate the
/// rest (a torn tail from a crash), then seal it. Appended pages can
/// land out of order, so each frame is verified against its hash, not
/// just its framing.
pub(super) fn recover(dir: &Path, id: u64) -> io::Result<Vec<(Hash, u64, u32)>> {
    let path = pack_path(dir, id);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let total = file.metadata()?.len();
    let mut entries = Vec::new();
    let mut pos = 0u64;
    let mut header = [0u8; HEADER_LEN];
    while pos + HEADER <= total {
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut header)?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap());
        if pos + HEADER + u64::from(len) > total {
            break;
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&header[4..]);
        let mut frame = vec![0u8; len as usize];
        file.read_exact(&mut frame)?;
        let ok = zstd::stream::decode_all(frame.as_slice())
            .is_ok_and(|data| *blake3::hash(&data).as_bytes() == hash);
        if !ok {
            break;
        }
        entries.push((hash, pos + HEADER, len));
        pos += HEADER + u64::from(len);
    }
    file.set_len(pos)?;
    file.sync_data()?;
    write_index(dir, id, &entries)?;
    Ok(entries)
}

pub(super) fn open_pack(dir: &Path, id: u64) -> io::Result<File> {
    File::open(pack_path(dir, id))
}

pub(super) fn read_frame(file: &File, offset: u64, len: u32) -> io::Result<Vec<u8>> {
    #[cfg(test)]
    io_hook::fire(io_hook::Op::Read);
    let mut buf = vec![0u8; len as usize];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

impl PackWriter {
    pub(super) fn sync(&self) -> io::Result<()> {
        #[cfg(test)]
        io_hook::fire(io_hook::Op::CopySync);
        self.file.sync_data()
    }

    pub(super) fn file(&self) -> Arc<File> {
        self.file.clone()
    }
}

/// Test seam: a per-thread callback before each pack read, write or
/// fsync, each sidecar index's fsync or read, and each retire's
/// unlinks, to hold a thread inside chunk-store file I/O, and before a
/// retire takes the index lock to decide what it keeps.
#[cfg(test)]
pub(crate) mod io_hook {
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::thread;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Op {
        Read,
        Write,
        /// A seal's fsync of its pack.
        Sync,
        Decide,
        /// A retire's fsync of the chunks it copied, before it unlinks
        /// the pack they came from.
        CopySync,
        /// The fsync of a sealed pack's sidecar index, before its rename.
        IndexSync,
        /// A read of a sealed pack's sidecar index, at open or as a
        /// retire's first step.
        IndexLoad,
        /// A retire's unlink of the pack it retires, or of that pack's
        /// index.
        Unlink,
    }

    pub(crate) type Hook = Arc<dyn Fn(Op) + Send + Sync>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// `f`, run on a thread of its own, off any runtime: a test's setup
    /// that writes the store from async code (fire).
    pub(crate) fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> T {
        thread::scope(|s| s.spawn(f).join().unwrap())
    }

    /// Run `hook` before each of this thread's ops above.
    pub(crate) fn install(hook: Hook) {
        HOOK.with(|h| *h.borrow_mut() = Some(hook));
    }

    pub(in crate::chunkstore) fn fire(op: Op) {
        // Never on a runtime's worker, the thread that drives its tasks:
        // there tokio refuses to block_on ("Cannot start a runtime from
        // within a runtime"), and the test fails. Elsewhere it is a no-op.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.block_on(async {});
        }
        let hook = HOOK.with(|h| h.borrow().clone());
        if let Some(hook) = hook {
            hook(op);
        }
    }
}
