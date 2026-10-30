//! Log-structured chunk cache with S3-FIFO eviction, shared by hub
//! and worker. Never a source of truth at rest: losing a chunk costs a
//! re-transfer, the daemon's NAR-hash check backstops correctness. A
//! hub output delivery reads the chunks it pinned back, though, and
//! fails if one is lost (crate::hub::relay::outputs).
//!
//! S3-FIFO because cold stagings are one-hit-wonder scans that would
//! evict the hot working set under LRU. Its FIFO queues merge with
//! append-only pack files (queue order = pack order): eviction
//! retires the oldest pack with one unlink, and only re-hit chunks
//! are ever copied forward.
//!
//! No lock that a lookup or a pin takes is ever held across file I/O.
//! The writer lock serialises appends, seals and evictions and is held
//! across their reads, writes and fsyncs, so only blocking threads may
//! take it. The index and the pin counts are held for in-memory updates
//! only. A retire's pread under one shared lock would leave hub runtime
//! workers waiting in `pin` until the watchdog killed the hub.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::task::spawn_blocking;

mod pack;
#[cfg(test)]
mod stall_tests;
#[cfg(test)]
mod tests;

use pack::PackWriter;
#[cfg(test)]
pub(crate) use pack::io_hook;

/// BLAKE3 of the uncompressed chunk bytes.
pub type Hash = [u8; 32];

/// A located frame, read without holding any store lock.
pub struct FrameRef {
    file: Arc<File>,
    offset: u64,
    len: u32,
}

impl FrameRef {
    /// A read error is a miss, like `get`. Waits on the disk, and on a
    /// swap-in of the buffer: async callers use [`FrameRef::read`].
    pub fn read_blocking(&self) -> io::Result<Vec<u8>> {
        pack::read_frame(&self.file, self.offset, self.len)
    }

    /// The pread on tokio's blocking pool, off the runtime's workers.
    pub async fn read(self) -> io::Result<Vec<u8>> {
        spawn_blocking(move || self.read_blocking())
            .await
            .map_err(io::Error::other)?
    }
}

const SMALL_FRACTION: u64 = 10;
const FREQ_MAX: u8 = 3;

/// Ghosts are an admission hint: a truncated-key collision at worst
/// promotes one cold chunk, so 8 bytes suffice.
fn ghost_key(hash: &Hash) -> u64 {
    u64::from_le_bytes(hash[..8].try_into().expect("hash is 32 bytes"))
}

#[derive(Clone, Copy, PartialEq)]
enum Queue {
    Small,
    Main,
}

struct Entry {
    pack: u64,
    offset: u64,
    len: u32,
    freq: u8,
}

/// Where a written record came from, which decides how it is indexed.
#[derive(Clone, Copy)]
enum Origin {
    Admitted,
    /// Copied forward out of pack `from`, retired from `queue`.
    Survivor {
        from: u64,
        queue: Queue,
    },
}

pub struct ChunkStore {
    dir: PathBuf,
    /// Appends, seals and evictions, one at a time, across their I/O.
    writer: Mutex<Writer>,
    /// Where each chunk is. Never held across file I/O.
    index: Mutex<Index>,
    /// Never held across file I/O, and taken after `index`.
    pins: Arc<Mutex<HashMap<Hash, u32>>>,
}

#[derive(Default)]
struct Index {
    map: HashMap<Hash, Entry>,
    /// Read handles, opened with their pack, dropped at its eviction.
    /// The Arc keeps an evicted pack readable for in-flight FrameRefs.
    readers: HashMap<u64, Arc<File>>,
}

impl Index {
    fn frame_ref(&self, hash: &Hash) -> Option<FrameRef> {
        let entry = self.map.get(hash)?;
        Some(FrameRef {
            file: self.readers.get(&entry.pack)?.clone(),
            offset: entry.offset,
            len: entry.len,
        })
    }

    /// Publish a record written at `at`.
    fn place(&mut self, hash: Hash, mut at: Entry, origin: Origin) {
        match origin {
            Origin::Admitted => {
                self.map.insert(hash, at);
            }
            // forget() may have dropped the chunk during the copy.
            Origin::Survivor { from, queue } => {
                if let Some(entry) = self.map.get_mut(&hash)
                    && entry.pack == from
                {
                    at.freq = match queue {
                        Queue::Small => entry.freq,
                        Queue::Main => entry.freq.saturating_sub(1),
                    };
                    *entry = at;
                }
            }
        }
    }
}

/// State only writers touch.
#[derive(Default)]
struct Writer {
    budget: u64,
    seal_bytes: u64,
    /// sealed pack id -> file size
    packs: HashMap<u64, u64>,
    small: VecDeque<u64>,
    main: VecDeque<u64>,
    active_small: Option<PackWriter>,
    active_main: Option<PackWriter>,
    small_bytes: u64,
    main_bytes: u64,
    ghost: VecDeque<u64>,
    ghost_set: HashSet<u64>,
    next_id: u64,
}

impl Writer {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            // 64 MiB packs for small budgets keep eviction fine-grained.
            seal_bytes: (budget / 16).clamp(4 << 20, 256 << 20),
            ..Self::default()
        }
    }

    fn active(&mut self, queue: Queue) -> &mut Option<PackWriter> {
        match queue {
            Queue::Small => &mut self.active_small,
            Queue::Main => &mut self.active_main,
        }
    }

    fn bytes(&mut self, queue: Queue) -> &mut u64 {
        match queue {
            Queue::Small => &mut self.small_bytes,
            Queue::Main => &mut self.main_bytes,
        }
    }

    fn ghost_push(&mut self, hash: Hash) {
        let key = ghost_key(&hash);
        if self.ghost_set.insert(key) {
            self.ghost.push_back(key);
        }
    }

    fn ghost_remove(&mut self, hash: &Hash) -> bool {
        // Lazy removal: the queue entry stays and is skipped at trim.
        self.ghost_set.remove(&ghost_key(hash))
    }

    fn trim_ghost(&mut self, live: usize) {
        let cap = live.max(1024);
        while self.ghost.len() > cap {
            let key = self.ghost.pop_front().unwrap();
            self.ghost_set.remove(&key);
        }
    }
}

/// Keeps a set of hashes exempt from eviction while alive.
pub struct PinGuard {
    pins: Arc<Mutex<HashMap<Hash, u32>>>,
    hashes: Vec<Hash>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let mut pins = self.pins.lock().unwrap();
        for h in &self.hashes {
            if let Some(n) = pins.get_mut(h) {
                *n -= 1;
                if *n == 0 {
                    pins.remove(h);
                }
            }
        }
    }
}

impl ChunkStore {
    pub fn open(dir: PathBuf, budget: u64) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let mut writer = Writer::new(budget);
        let mut index = Index::default();
        load(&dir, &mut writer, &mut index)?;
        Ok(Self {
            dir,
            writer: Mutex::new(writer),
            index: Mutex::new(index),
            pins: Arc::default(),
        })
    }

    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap()
    }

    /// The index lock a retire decides under. It reads the pins only
    /// once it holds it, so a pin taken before keeps its chunks.
    fn index_to_decide(&self) -> MutexGuard<'_, Index> {
        #[cfg(test)]
        io_hook::fire(io_hook::Op::Decide);
        self.index()
    }

    /// Existence only. No frequency bump, so probing for a Need
    /// does not fake reuse.
    pub fn contains(&self, hash: &Hash) -> bool {
        self.index().map.contains_key(hash)
    }

    /// Drop a chunk found corrupt or unreadable. The bytes stay dead in
    /// their pack until eviction. On reopen the pack index resurrects
    /// the entry, and the next import check forgets it again.
    pub fn forget(&self, hash: &Hash) {
        self.index().map.remove(hash);
    }

    /// Locate a frame for reading outside the store lock: the pread
    /// dominates get() and would otherwise convoy concurrent callers.
    pub fn locate(&self, hash: &Hash) -> Option<FrameRef> {
        self.locate_all([hash])?.pop().map(|(_, f)| f)
    }

    /// Locate every hash under one lock. None unless all are stored.
    pub fn locate_all<'h>(
        &self,
        hashes: impl IntoIterator<Item = &'h Hash>,
    ) -> Option<Vec<(Hash, FrameRef)>> {
        let mut index = self.index();
        hashes
            .into_iter()
            .map(|h| {
                let entry = index.map.get_mut(h)?;
                entry.freq = (entry.freq + 1).min(FREQ_MAX);
                index.frame_ref(h).map(|f| (*h, f))
            })
            .collect()
    }

    pub fn peek(&self, hash: &Hash) -> Option<FrameRef> {
        self.index().frame_ref(hash)
    }

    /// Exempt `hashes` (present or admitted later) from eviction until
    /// the guard drops. Waits on no I/O: eviction decides under the
    /// index and pin locks together, then copies without them.
    pub fn pin(&self, hashes: impl IntoIterator<Item = Hash>) -> PinGuard {
        let hashes: Vec<Hash> = hashes.into_iter().collect();
        let mut pins = self.pins.lock().unwrap();
        for h in &hashes {
            *pins.entry(*h).or_default() += 1;
        }
        drop(pins);
        PinGuard {
            pins: self.pins.clone(),
            hashes,
        }
    }

    /// Admit one zstd frame. A ghost hit goes straight to main. Does
    /// file I/O and may fsync: call it from a blocking thread.
    pub fn insert(&self, hash: Hash, frame: &[u8]) -> io::Result<()> {
        let mut w = self.writer.lock().unwrap();
        // Only writers add entries, so a miss here holds until we append.
        if self.contains(&hash) {
            return Ok(());
        }
        let queue = if w.ghost_remove(&hash) {
            Queue::Main
        } else {
            Queue::Small
        };
        self.append(&mut w, queue, &hash, frame, Origin::Admitted)?;
        self.evict(&mut w)?;
        let live = self.index().map.len();
        w.trim_ghost(live);
        Ok(())
    }

    /// Write one record with no index lock held, then publish it.
    fn append(
        &self,
        w: &mut Writer,
        queue: Queue,
        hash: &Hash,
        frame: &[u8],
        origin: Origin,
    ) -> io::Result<()> {
        if w.active(queue).is_none() {
            let id = w.next_id;
            w.next_id += 1;
            let created = PackWriter::create(&self.dir, id)?;
            self.index().readers.insert(id, created.file());
            *w.active(queue) = Some(created);
        }
        let seal_bytes = w.seal_bytes;
        let active = w.active(queue).as_mut().expect("just created");
        let before = active.len;
        let offset = active.append(hash, frame)?;
        let (pack, grown, full) = (active.id, active.len - before, active.len >= seal_bytes);
        let at = Entry {
            pack,
            offset,
            len: u32::try_from(frame.len()).expect("append checked the frame size"),
            freq: 0,
        };
        self.index().place(*hash, at, origin);
        *w.bytes(queue) += grown;
        if full {
            self.seal(w, queue)?;
        }
        Ok(())
    }

    fn seal(&self, w: &mut Writer, queue: Queue) -> io::Result<()> {
        let Some(sealing) = w.active(queue).take() else {
            return Ok(());
        };
        let (id, size) = (sealing.id, sealing.len);
        sealing.seal(&self.dir)?;
        w.packs.insert(id, size);
        match queue {
            Queue::Small => w.small.push_back(id),
            Queue::Main => w.main.push_back(id),
        }
        Ok(())
    }

    /// Stops once a full round over main freed nothing (all pinned or
    /// hot).
    fn evict(&self, w: &mut Writer) -> io::Result<()> {
        let mut unfreed_round = 0;
        while w.small_bytes + w.main_bytes > w.budget {
            if w.small_bytes > w.budget / SMALL_FRACTION {
                if w.small.is_empty() {
                    self.seal(w, Queue::Small)?;
                }
                if let Some(id) = w.small.pop_front() {
                    self.retire(w, id, Queue::Small)?;
                    continue;
                }
            }
            if w.main.is_empty() {
                self.seal(w, Queue::Main)?;
            }
            let Some(id) = w.main.pop_front() else {
                // Only unsealed small data left. Nothing to evict.
                break;
            };
            let before = w.small_bytes + w.main_bytes;
            self.retire(w, id, Queue::Main)?;
            if w.small_bytes + w.main_bytes < before {
                unfreed_round = 0;
            } else {
                unfreed_round += 1;
                if unfreed_round > w.main.len() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Evict one pack: small survivors (freq > 0) promote to main,
    /// the rest leave a ghost. Main survivors are copied forward with
    /// freq - 1 (S3-FIFO reinsertion as log compaction). Cold unpinned
    /// chunks leave the index at once, so a later pin finds them gone;
    /// survivors stay readable here until their copy is published.
    fn retire(&self, w: &mut Writer, id: u64, queue: Queue) -> io::Result<()> {
        let entries = pack::load_index(&self.dir, id)?;
        let (file, survivors) = {
            let mut index = self.index_to_decide();
            let pins = self.pins.lock().unwrap();
            let mut survivors = Vec::new();
            for (hash, ..) in entries {
                // Promotion rewrites entry.pack. Only current owners count.
                let Some(entry) = index.map.get(&hash) else {
                    continue;
                };
                if entry.pack != id {
                    continue;
                }
                if entry.freq == 0 && !pins.contains_key(&hash) {
                    index.map.remove(&hash);
                    if queue == Queue::Small {
                        w.ghost_push(hash);
                    }
                    continue;
                }
                survivors.push((hash, entry.offset, entry.len));
            }
            let file = index.readers.get(&id).cloned();
            (file.expect("a live pack has a reader"), survivors)
        };
        let promoted = !survivors.is_empty();
        for (hash, offset, len) in survivors {
            let frame = pack::read_frame(&file, offset, len)?;
            let origin = Origin::Survivor { from: id, queue };
            self.append(w, Queue::Main, &hash, &frame, origin)?;
        }
        let size = w.packs.remove(&id).expect("retiring an unknown pack");
        *w.bytes(queue) -= size;
        // Fsync survivors before unlinking their source, so a crash
        // never loses a chunk the map claims to have.
        if promoted && let Some(active) = &w.active_main {
            active.sync()?;
        }
        self.index().readers.remove(&id);
        #[cfg(test)]
        io_hook::fire(io_hook::Op::Unlink);
        fs::remove_file(pack::pack_path(&self.dir, id))?;
        #[cfg(test)]
        io_hook::fire(io_hook::Op::Unlink);
        let _ = fs::remove_file(pack::index_path(&self.dir, id));
        Ok(())
    }

    /// Small packs and budget, so a few inserts retire a pack.
    #[cfg(test)]
    pub(crate) fn set_limits(&self, budget: u64, seal_bytes: u64) {
        let mut w = self.writer.lock().unwrap();
        (w.budget, w.seal_bytes) = (budget, seal_bytes);
    }
}

/// Everything on disk loads into main with freq 0.
fn load(dir: &Path, w: &mut Writer, index: &mut Index) -> io::Result<()> {
    let mut ids: Vec<u64> = Vec::new();
    for e in fs::read_dir(dir)? {
        let name = e?.file_name();
        if let Some(hex) = name.to_str().and_then(|n| n.strip_suffix(".pack"))
            && let Ok(id) = u64::from_str_radix(hex, 16)
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    for id in ids {
        let entries = match pack::load_index(dir, id) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => pack::recover(dir, id)?,
            Err(e) => return Err(e),
        };
        let size = fs::metadata(pack::pack_path(dir, id))?.len();
        for (hash, offset, len) in entries {
            let entry = Entry {
                pack: id,
                offset,
                len,
                freq: 0,
            };
            index.map.insert(hash, entry);
        }
        index
            .readers
            .insert(id, Arc::new(pack::open_pack(dir, id)?));
        w.packs.insert(id, size);
        w.main.push_back(id);
        w.main_bytes += size;
        w.next_id = w.next_id.max(id + 1);
    }
    Ok(())
}
