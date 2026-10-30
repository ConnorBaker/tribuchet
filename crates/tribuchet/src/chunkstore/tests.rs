use std::path::Path;
use std::sync::Arc;
use std::thread;

use super::*;
use crate::chunkstore::io_hook::{self, Op};
use crate::stalltest::Gate;

fn h(n: u8) -> Hash {
    let mut hash = [0u8; 32];
    hash[0] = n;
    hash
}

fn store(dir: &Path, budget: u64) -> ChunkStore {
    ChunkStore::open(dir.to_path_buf(), budget).unwrap()
}

fn get(s: &ChunkStore, hash: &Hash) -> Option<Vec<u8>> {
    s.locate(hash)?.read_blocking().ok()
}

#[test]
fn roundtrip_and_contains() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path(), 1 << 20);
    s.insert(h(1), b"frame-one").unwrap();
    assert!(get(&s, &h(2)).is_none());
    assert_eq!(get(&s, &h(1)).unwrap(), b"frame-one");
}

/// blake3 hash and zstd frame of `data`, as production inserts
/// store them. Recovery verifies both.
fn frame(data: &[u8]) -> (Hash, Vec<u8>) {
    let hash = *blake3::hash(data).as_bytes();
    (hash, zstd::stream::encode_all(data, 3).unwrap())
}

fn pack_file(dir: &Path) -> PathBuf {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "pack"))
        .unwrap()
}

#[test]
fn survives_reopen_via_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (ha, fa) = frame(b"aaa");
    let (hb, fb) = frame(b"bbb");
    let s = store(dir.path(), 1 << 20);
    s.insert(ha, &fa).unwrap();
    s.insert(hb, &fb).unwrap();
    drop(s);
    let s = store(dir.path(), 1 << 20);
    assert_eq!(get(&s, &ha).unwrap(), fa);
    assert_eq!(get(&s, &hb).unwrap(), fb);
}

#[test]
fn reopen_truncates_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let (ha, fa) = frame(b"aaa");
    let s = store(dir.path(), 1 << 20);
    s.insert(ha, &fa).unwrap();
    drop(s);
    let pack = pack_file(dir.path());
    let mut data = fs::read(&pack).unwrap();
    data.extend_from_slice(&[7, 0, 0, 0, 9]); // half a record
    fs::write(&pack, data).unwrap();
    let s = store(dir.path(), 1 << 20);
    assert_eq!(get(&s, &ha).unwrap(), fa);
}

/// A torn write can leave a record with intact framing but a
/// corrupt payload. Recovery must drop it, not just short tails.
#[test]
fn reopen_drops_corrupt_payload() {
    let dir = tempfile::tempdir().unwrap();
    let (ha, fa) = frame(b"aaa");
    let (hb, fb) = frame(b"bbb");
    let s = store(dir.path(), 1 << 20);
    s.insert(ha, &fa).unwrap();
    s.insert(hb, &fb).unwrap();
    drop(s);
    let pack = pack_file(dir.path());
    let mut data = fs::read(&pack).unwrap();
    let tail = data.len() - fb.len();
    for b in &mut data[tail..] {
        *b = 0;
    }
    fs::write(&pack, data).unwrap();
    let s = store(dir.path(), 1 << 20);
    assert_eq!(get(&s, &ha).unwrap(), fa);
    assert!(get(&s, &hb).is_none());
}

#[test]
fn pinned_survives_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path(), 64 << 20);
    s.set_limits(64 << 20, 64 << 10);
    let frame = vec![0u8; 8 << 10];
    let pinned: Vec<Hash> = (0..20u8).map(h).collect();
    for p in &pinned {
        s.insert(*p, &frame).unwrap();
    }
    let guard = s.pin(pinned.clone());
    s.set_limits(128 << 10, 64 << 10);
    for n in 20..200u8 {
        s.insert(h(n), &frame).unwrap();
    }
    assert!(
        pinned.iter().all(|p| s.peek(p).is_some()),
        "pinned chunk evicted"
    );
    assert!(s.peek(&h(20)).is_none());
    drop(guard);
    for n in 200..255u8 {
        s.insert(h(n), &frame).unwrap();
    }
    assert!(
        pinned.iter().any(|p| s.peek(p).is_none()),
        "unpinned chunks never evicted"
    );
}

/// One-hit wonders age out with zero copies while a re-hit chunk
/// is promoted and survives the same eviction wave.
#[test]
fn scan_resistance() {
    let dir = tempfile::tempdir().unwrap();
    let budget = 64 << 20;
    let s = store(dir.path(), budget);
    s.set_limits(budget, 64 << 10);
    let frame = vec![0u8; 8 << 10];
    s.insert(h(255), &frame).unwrap();
    get(&s, &h(255)).unwrap(); // freq bump: hot
    // scan: enough one-hit chunks to overflow the small queue
    for n in 0..200u8 {
        let mut hash = h(n);
        hash[1] = 1;
        s.insert(hash, &frame).unwrap();
    }
    // force eviction pressure well past the budget
    s.set_limits(128 << 10, 64 << 10);
    s.insert(h(254), &frame).unwrap();
    assert!(get(&s, &h(255)).is_some(), "hot chunk evicted by the scan");
    let survivors = (0..200u8)
        .filter(|n| {
            let mut hash = h(*n);
            hash[1] = 1;
            get(&s, &hash).is_some()
        })
        .count();
    assert!(survivors < 200, "scan chunks were never evicted");
}

/// A ghost hit admits straight to main and survives small-queue
/// pressure afterwards.
#[test]
fn ghost_readmission_goes_to_main() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path(), 256 << 10);
    s.set_limits(256 << 10, 32 << 10);
    let frame = vec![0u8; 8 << 10];
    s.insert(h(1), &frame).unwrap();
    // push it out of small without a hit -> ghost
    for n in 10..80u8 {
        s.insert(h(n), &frame).unwrap();
    }
    assert!(get(&s, &h(1)).is_none(), "expected h(1) evicted to ghost");
    s.insert(h(1), &frame).unwrap();
    // another scan. A small-queue resident would be evicted again
    for n in 100..170u8 {
        s.insert(h(n), &frame).unwrap();
    }
    assert!(get(&s, &h(1)).is_some(), "ghost readmission did not stick");
}

/// A chunk pinned and then found present just before a retire decides
/// stays until unpinned, although nothing read it: the retire reads the
/// pins under the index lock it decides under. A retire that read them
/// before taking the lock would drop it.
#[test]
fn a_chunk_pinned_before_the_decide_stays() {
    let dir = tempfile::tempdir().unwrap();
    let s = Arc::new(store(dir.path(), 1 << 20));
    s.set_limits(128 << 10, 32 << 10);
    let frame = |n: u8| vec![n; 8 << 10];
    // The first pack to retire starts with these two, both cold.
    s.insert(h(0), &frame(0)).unwrap();
    s.insert(h(1), &frame(1)).unwrap();
    let gate = Arc::new(Gate::default());
    gate.arm();
    let admitter = {
        let (s, gate) = (s.clone(), gate.clone());
        thread::spawn(move || {
            io_hook::install(Arc::new(move |op| {
                if op == Op::Decide {
                    gate.pass();
                }
            }));
            for n in 2..100 {
                s.insert(h(n), &frame(n)).unwrap();
            }
        })
    };
    gate.wait_entered();
    let pin = s.pin([h(0)]);
    assert!(s.peek(&h(0)).is_some(), "dropped before the retire decided");
    gate.release();
    admitter.join().unwrap();
    assert!(s.peek(&h(1)).is_none(), "the retire kept a cold chunk");
    let kept = s
        .peek(&h(0))
        .expect("a chunk pinned before the decide was dropped");
    assert_eq!(kept.read_blocking().unwrap(), frame(0));
    drop(pin);
}
