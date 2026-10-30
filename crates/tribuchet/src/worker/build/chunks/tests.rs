//! Chunk staging beside the import feeds: what a feed reads while the
//! session stores and evicts. That neither waits for the other's file
//! I/O is rows of chunkstore/stall_tests.rs.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::thread;
use std::time::Duration;

use zstd::bulk::compress;

use super::*;
use crate::chunkstore::io_hook::{self, Op};
use crate::stalltest::Gate;

#[test]
fn feed_import_forgets_corrupt_and_missing_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::open(dir.path().to_path_buf(), 1 << 20).unwrap();
    let good = b"good chunk".to_vec();
    let good_hash: Hash = *blake3::hash(&good).as_bytes();
    let liar: Hash = [7; 32];
    let missing: Hash = [9; 32];
    store
        .insert(good_hash, &compress(&good, 3).unwrap())
        .unwrap();
    store
        .insert(liar, &compress(b"not what the hash says", 3).unwrap())
        .unwrap();
    let recipe = [(good_hash, good.len()), (liar, 22), (missing, 1)];
    let (tx, mut rx) = mpsc::channel(8);
    let bad = feed_import(&store, &recipe, &tx);
    assert_eq!(bad, vec![liar, missing]);
    assert_eq!(rx.try_recv().unwrap().as_ref(), good.as_slice());
    assert!(rx.try_recv().is_err());
    assert!(store.contains(&good_hash));
    assert!(!store.contains(&liar));
}

/// One build's input: its recipe, the frames the hub sends, and the
/// bytes the daemon import must read.
#[derive(Clone)]
pub(crate) struct Input {
    recipe: Vec<(Hash, usize)>,
    frames: Vec<Vec<u8>>,
    bodies: Vec<Vec<u8>>,
}

/// `n` incompressible chunks of `len` bytes, tagged `tag`, so packs
/// fill and retire at sizes the limits set.
pub(crate) fn input(tag: u8, n: u32, len: usize) -> Input {
    let mut inp = Input {
        recipe: Vec::new(),
        frames: Vec::new(),
        bodies: Vec::new(),
    };
    for i in 0..n {
        let mut xof = blake3::Hasher::new();
        xof.update(&[tag]);
        xof.update(&i.to_le_bytes());
        let mut body = vec![0u8; len];
        xof.finalize_xof().fill(&mut body);
        inp.recipe.push((*blake3::hash(&body).as_bytes(), len));
        inp.frames.push(compress(&body, 3).unwrap());
        inp.bodies.push(body);
    }
    inp
}

/// The session's staging of `inp` as `path`: the manifest (pin, then
/// probe the store), then each requested frame as it arrives.
fn stage(staging: &mut ChunkStaging, path: &str, inp: &Input) -> Vec<u8> {
    let hashes: Vec<u8> = inp.recipe.iter().flat_map(|(h, _)| *h).collect();
    let sizes: Vec<u64> = inp.recipe.iter().map(|(_, s)| *s as u64).collect();
    let (need, mut ready) = staging
        .add_recipe(path.to_string(), &hashes, &sizes)
        .unwrap();
    let (needed, _) = need.as_chunks::<32>();
    for ((hash, _), frame) in inp.recipe.iter().zip(&inp.frames) {
        if needed.contains(hash) {
            ready |= staging
                .ingest(hash, frame)
                .unwrap()
                .iter()
                .any(|p| p == path);
        }
    }
    assert!(ready, "{path} never became ready");
    need
}

/// The session task's ingest of each frame of `inp`, awaited by none.
pub(crate) fn ingest(store: Arc<ChunkStore>, inp: &Input) {
    let mut staging = ChunkStaging::new(store);
    for ((hash, _), frame) in inp.recipe.iter().zip(&inp.frames) {
        assert!(staging.ingest(hash, frame).unwrap().is_empty());
    }
}

/// feed_import as import_worker runs it: the chunks it found bad, and
/// what the daemon import read.
fn feed_collect(store: &ChunkStore, recipe: &[(Hash, usize)]) -> (Vec<Hash>, Vec<Vec<u8>>) {
    let (tx, mut rx) = mpsc::channel::<Bytes>(8);
    thread::scope(|s| {
        let drain = s.spawn(move || {
            let mut read = Vec::new();
            while let Some(b) = rx.blocking_recv() {
                read.push(b.to_vec());
            }
            read
        });
        let bad = feed_import(store, recipe, &tx);
        drop(tx);
        (bad, drain.join().unwrap())
    })
}

/// A feed of `inp` that must read it intact.
pub(crate) fn feed(store: &ChunkStore, inp: &Input) {
    let (bad, read) = feed_collect(store, &inp.recipe);
    assert!(
        bad.is_empty() && read == inp.bodies,
        "a feed read other bytes"
    );
}

/// `inp` staged as `path` by a session of its own: pinned while the
/// returned session lives.
pub(crate) fn pinned(store: &Arc<ChunkStore>, path: &str, inp: &Input) -> Box<dyn Send> {
    let mut staging = ChunkStaging::new(store.clone());
    stage(&mut staging, path, inp);
    Box::new(staging)
}

/// The outer mutex made a feed's lookup and its pread one step, so no
/// eviction ran between them. Without it, the session's ingest can
/// retire the pack a feed just looked a chunk up in: copy the pinned
/// chunk forward, drop the pack's read handle and unlink its file,
/// while the feed still holds the frame it located. The feed must
/// read that chunk intact, and the copy must stay indexed.
#[test]
fn a_feed_reads_its_chunk_after_a_retire_moved_it_and_unlinked_its_pack() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::open(dir.path().to_path_buf(), 1 << 30).unwrap());
    store.set_limits(256 << 10, 32 << 10);
    // Build a's four chunks fill pack 0, which seals.
    let a = input(10, 4, 8 << 10);
    let mut session = ChunkStaging::new(store.clone());
    assert_eq!(stage(&mut session, "/nix/store/a", &a).len(), 4 * 32);
    let pack0 = dir.path().join(format!("{:016x}.pack", 0));
    assert!(pack0.exists(), "a's chunks are not in pack 0");

    let gate = Arc::new(Gate::default());
    gate.arm();
    let feeder = {
        let (store, gate, recipe) = (store.clone(), gate.clone(), a.recipe.clone());
        thread::spawn(move || {
            let first = AtomicBool::new(true);
            io_hook::install(Arc::new(move |op| {
                if op == Op::Read && first.swap(false, SeqCst) {
                    gate.pass();
                }
            }));
            feed_collect(&store, &recipe)
        })
    };
    gate.wait_entered();
    // The feed holds the frame of a's first chunk, located in pack 0.
    // The session stores another build's chunks until pack 0 is gone.
    let (decides, copies) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    {
        let (decides, copies) = (decides.clone(), copies.clone());
        io_hook::install(Arc::new(move |op| match op {
            Op::Decide => {
                decides.fetch_add(1, SeqCst);
            }
            Op::Read => {
                copies.fetch_add(1, SeqCst);
            }
            Op::Write | Op::Sync | Op::CopySync | Op::IndexSync | Op::IndexLoad | Op::Unlink => {}
        }));
    }
    let b = input(11, 256, 8 << 10);
    let mut other = ChunkStaging::new(store.clone());
    for ((hash, _), frame) in b.recipe.iter().zip(&b.frames) {
        assert!(other.ingest(hash, frame).unwrap().is_empty());
        if !pack0.exists() {
            break;
        }
    }
    let unlinked = !pack0.exists();
    let indexed = a.recipe.iter().all(|(h, _)| store.peek(h).is_some());
    gate.release();
    let (bad, read) = feeder.join().unwrap();
    let (decides, copies) = (decides.load(SeqCst), copies.load(SeqCst));
    assert!(
        unlinked && decides > 0,
        "vacuous: pack 0 unlinked {unlinked}, {decides} retire decides"
    );
    assert!(indexed, "a pinned chunk left the index in a retire");
    assert!(copies > 0, "the retire copied nothing forward");
    assert!(
        bad.is_empty(),
        "the feed found {} of a's {} pinned chunks corrupt or missing after a retire \
         moved them and unlinked pack 0 under it",
        bad.len(),
        a.recipe.len()
    );
    assert!(read == a.bodies, "the feed read other bytes than a's");
}

/// Feed `inp` over and over until `stop`: the rounds fed, or the first
/// round that read a chunk corrupt, missing or other than `inp`'s.
fn feed_until(store: &ChunkStore, inp: &Input, stop: &AtomicBool) -> Result<usize, String> {
    let mut rounds = 0usize;
    while !stop.load(SeqCst) {
        let (bad, read) = feed_collect(store, &inp.recipe);
        if !bad.is_empty() || read != inp.bodies {
            let intact = read.iter().zip(&inp.bodies).filter(|(r, b)| r == b);
            return Err(format!(
                "round {rounds}: {} chunks corrupt or missing, {} of {} read intact",
                bad.len(),
                intact.count(),
                inp.bodies.len()
            ));
        }
        rounds += 1;
    }
    Ok(rounds)
}

/// Four feeds read their pinned inputs over and over while the session
/// stores another build's chunks. The pinned inputs exceed the budget,
/// so each insert's eviction copies them forward and unlinks their packs
/// under the feeds, and now and then a manifest pins and probes them.
/// Every feed must read every chunk intact, and no pinned chunk may go.
#[test]
fn feeds_racing_the_sessions_evictions_read_every_pinned_chunk_intact() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::open(dir.path().to_path_buf(), 1 << 30).unwrap());
    // Four inputs of eight 8 KiB chunks pin 256 KiB, over the budget.
    store.set_limits(192 << 10, 64 << 10);
    let inputs: Vec<Arc<Input>> = (0..4u8)
        .map(|b| Arc::new(input(20 + b, 8, 8 << 10)))
        .collect();
    let mut builds = Vec::new();
    for (b, inp) in inputs.iter().enumerate() {
        let mut staging = ChunkStaging::new(store.clone());
        stage(&mut staging, &format!("/nix/store/in{b}"), inp);
        builds.push(staging);
    }
    let stop = Arc::new(AtomicBool::new(false));
    // Retires begun, and how many began while a feed held a chunk it
    // had located but not yet read.
    let decides = Arc::new(AtomicUsize::new(0));
    let overlapped = Arc::new(AtomicUsize::new(0));
    let feeders: Vec<_> = inputs
        .iter()
        .map(|inp| {
            let (store, inp, stop) = (store.clone(), inp.clone(), stop.clone());
            let (decides, overlapped) = (decides.clone(), overlapped.clone());
            thread::spawn(move || {
                io_hook::install(Arc::new(move |op| {
                    if op == Op::Read {
                        let before = decides.load(SeqCst);
                        thread::sleep(Duration::from_micros(20));
                        if decides.load(SeqCst) != before {
                            overlapped.fetch_add(1, SeqCst);
                        }
                    }
                }));
                feed_until(&store, &inp, &stop)
            })
        })
        .collect();
    let copies = Arc::new(AtomicUsize::new(0));
    {
        let (decides, copies) = (decides.clone(), copies.clone());
        io_hook::install(Arc::new(move |op| match op {
            Op::Decide => {
                decides.fetch_add(1, SeqCst);
            }
            Op::Read => {
                copies.fetch_add(1, SeqCst);
            }
            // Widen the window between a record's write and its
            // publication, as a slow disk would.
            Op::Write => thread::sleep(Duration::from_micros(50)),
            Op::Sync | Op::CopySync | Op::IndexSync | Op::IndexLoad | Op::Unlink => {}
        }));
    }
    let stream = input(30, 200, 8 << 10);
    let mut other = ChunkStaging::new(store.clone());
    let mut late_missing = 0;
    for (i, ((hash, _), frame)) in stream.recipe.iter().zip(&stream.frames).enumerate() {
        assert!(other.ingest(hash, frame).unwrap().is_empty());
        if i % 20 == 19 {
            let path = format!("/nix/store/late{i}");
            let pinned = &inputs[(i / 20) % 4].recipe[..2];
            let hashes: Vec<u8> = pinned.iter().flat_map(|(h, _)| *h).collect();
            let sizes: Vec<u64> = pinned.iter().map(|(_, s)| *s as u64).collect();
            let (need, ready) = other.add_recipe(path.clone(), &hashes, &sizes).unwrap();
            late_missing += need.len() / 32 + usize::from(!ready);
            other.forget_path(&path);
        }
    }
    stop.store(true, SeqCst);
    let rounds: Vec<_> = feeders.into_iter().map(|f| f.join().unwrap()).collect();
    for r in &rounds {
        if let Err(e) = r {
            panic!("a feed raced the session's evictions and lost: {e}");
        }
    }
    assert_eq!(late_missing, 0, "a manifest found pinned chunks missing");
    for inp in &inputs {
        assert!(
            inp.recipe.iter().all(|(h, _)| store.peek(h).is_some()),
            "a pinned chunk left the index"
        );
    }
    let (decides, overlapped, copies) = (
        decides.load(SeqCst),
        overlapped.load(SeqCst),
        copies.load(SeqCst),
    );
    eprintln!(
        "{decides} retire decides, {overlapped} inside a feed's lookup-to-read window, \
         {copies} copies forward, feed rounds {rounds:?}"
    );
    assert!(
        overlapped > 0 && copies > 0 && rounds.iter().all(|r| r.as_ref().is_ok_and(|n| *n > 0)),
        "vacuous: {decides} retire decides, {overlapped} inside a feed's \
         lookup-to-read window, {copies} copies forward, feed rounds {rounds:?}"
    );
    drop(builds);
}
