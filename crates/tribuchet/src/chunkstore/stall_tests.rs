//! The store's liveness rule, as both its users rely on it: no lock that
//! a lookup or a pin takes is held across file I/O. Each row holds one
//! thread inside a file op of the store and requires every call of the
//! others to finish meanwhile. The hub cache's rows are the path on which
//! a retire's pread held hub runtime workers in `pin` until the watchdog
//! killed the hub; the worker's are the path on which a mutex around the
//! store stalled the session with a feed's pread.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;

use super::io_hook::{self, Op};
use super::{ChunkStore, Hash, PinGuard};
use crate::hub::ChunkCache;
use crate::stalltest::{Gate, PATIENCE};
use crate::worker::chunk_tests::{Input, feed, ingest, input, pinned};
use Act::{AddRecipe, Admit, Classify, Evict, Feed, HasRecipe, Ingest, Locate, Pin, Unpin};

/// Every op the store's io_hook fires.
const OPS: [Op; 8] = [
    Op::Write,
    Op::Sync,
    Op::IndexSync,
    Op::IndexLoad,
    Op::Decide,
    Op::Read,
    Op::CopySync,
    Op::Unlink,
];

/// What a thread does with the store. The hub cache admits chunks on a
/// blocking thread while runtime tasks pin, unpin, locate and classify
/// them and look up recipes. A worker's import feeds read pinned chunks
/// while the session ingests arriving ones (into a store that seals and
/// evicts, for Evict) and stages each manifest (its pin and probe).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Admit,
    Pin,
    Unpin,
    Locate,
    Classify,
    HasRecipe,
    Feed,
    Ingest,
    AddRecipe,
    Evict,
}

/// Each row: the act held inside its first `op`, and the acts that must
/// finish while it is.
#[test]
fn no_call_waits_for_another_threads_store_io() {
    let cache: &[Act] = &[Pin, Unpin, Locate, Classify, HasRecipe];
    let mut rows: Vec<(Act, Op, &[Act])> = OPS.map(|op| (Admit, op, cache)).to_vec();
    rows.push((Feed, Op::Read, &[Feed, Ingest, AddRecipe]));
    rows.push((Ingest, Op::Write, &[Feed]));
    rows.push((Feed, Op::Read, &[Evict]));
    rows.extend(OPS.map(|op| (Evict, op, &[Feed][..])));
    let failed: Vec<String> = rows
        .into_iter()
        .flat_map(|(held, op, acts)| waited(held, op, acts))
        .collect();
    assert_eq!(failed, Vec::<String>::new());
}

fn h(n: u16) -> Hash {
    let mut hash = [0u8; 32];
    hash[..2].copy_from_slice(&n.to_le_bytes());
    hash
}

/// What an act found wrong, if anything.
type Call = Box<dyn FnOnce() -> Option<String> + Send>;

/// The hub's cache, a hit chunk pinned twice in it, or a worker's store
/// with two inputs pinned (and a third that retires copy forward, in a
/// store that seals and evicts).
enum Scene {
    Cache(Arc<ChunkCache>, Arc<Mutex<Vec<PinGuard>>>),
    Store {
        store: Arc<ChunkStore>,
        mine: Input,
        other: Input,
        _sessions: Vec<Box<dyn Send>>,
    },
}

impl Scene {
    fn new(dir: &Path, held: Act, acts: &[Act]) -> Self {
        if held == Admit {
            let cache = ChunkCache::open(dir.to_path_buf(), 1 << 20).unwrap();
            // 32 KiB packs under a 128 KiB budget: about the 17th 8 KiB
            // admit retires the first pack, whose hit, pinned chunk survives.
            cache.set_limits(128 << 10, 32 << 10);
            cache.admit(h(0), &[0u8; 8 << 10]).unwrap();
            cache.locate(&h(0)).expect("just admitted");
            let pins = vec![cache.pin([h(0)]), cache.pin([h(0)])];
            return Self::Cache(Arc::new(cache), Arc::new(Mutex::new(pins)));
        }
        let store = Arc::new(ChunkStore::open(dir.to_path_buf(), 1 << 30).unwrap());
        let evicting = held == Evict || acts.contains(&Evict);
        if evicting {
            // 32 KiB packs under a 128 KiB budget: a seal every few 8 KiB
            // chunks, and an ingest of 32 overruns the budget many times.
            store.set_limits(128 << 10, 32 << 10);
        }
        let (mine, other) = (input(40, 4, 8 << 10), input(41, 4, 8 << 10));
        let mut sessions = vec![
            pinned(&store, "/nix/store/mine", &mine),
            pinned(&store, "/nix/store/other", &other),
        ];
        if evicting {
            // Three chunks leave the pack they go to unsealed, so a
            // retire that copies them forward fsyncs the copies itself.
            sessions.push(pinned(&store, "/nix/store/fresh", &input(43, 3, 8 << 10)));
        }
        Self::Store {
            store,
            mine,
            other,
            _sessions: sessions,
        }
    }

    /// `act`, to run on a thread of its own: as the held thread, whose
    /// hook holds it, or as a call made meanwhile.
    fn act(&self, act: Act, held: bool) -> Call {
        match self {
            Self::Cache(cache, pins) => {
                let (c, pins) = (cache.clone(), pins.clone());
                match act {
                    Admit => Box::new(move || {
                        for n in 1..200 {
                            c.admit(h(n), &[0u8; 8 << 10]).unwrap();
                        }
                        None
                    }),
                    Pin => Box::new(move || {
                        drop(c.pin([h(0), h(500)]));
                        None
                    }),
                    Unpin => Box::new(move || {
                        drop(pins.lock().unwrap().pop());
                        None
                    }),
                    Locate => Box::new(move || {
                        let found = c.locate(&h(0)).is_some();
                        (!found).then(|| "a pinned survivor was not readable".to_owned())
                    }),
                    Classify => Box::new(move || {
                        c.classify(&h(501));
                        None
                    }),
                    HasRecipe => Box::new(move || {
                        c.has_recipe("/nix/store/x");
                        None
                    }),
                    Feed | Ingest | AddRecipe | Evict => unreachable!("{act:?} is a worker's"),
                }
            }
            Self::Store {
                store, mine, other, ..
            } => {
                let s = store.clone();
                // Held, a feed reads its own input; meanwhile, the other.
                let inp = if held { mine.clone() } else { other.clone() };
                match act {
                    Feed => Box::new(move || {
                        feed(&s, &inp);
                        None
                    }),
                    Ingest => Box::new(move || {
                        ingest(s, &input(if held { 45 } else { 42 }, 4, 8 << 10));
                        None
                    }),
                    AddRecipe => Box::new(move || {
                        drop(pinned(&s, "/nix/store/again", &inp));
                        None
                    }),
                    Evict if held => Box::new(move || {
                        ingest(s, &input(44, 32, 8 << 10));
                        None
                    }),
                    Evict => Box::new(move || {
                        let seen = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
                        let counts = seen.clone();
                        io_hook::install(Arc::new(move |op| match op {
                            Op::Sync => {
                                counts[0].fetch_add(1, SeqCst);
                            }
                            Op::Decide => {
                                counts[1].fetch_add(1, SeqCst);
                            }
                            _ => {}
                        }));
                        ingest(s, &input(44, 32, 8 << 10));
                        let (syncs, decides) = (seen[0].load(SeqCst), seen[1].load(SeqCst));
                        let vacuous = syncs == 0 || decides == 0;
                        vacuous.then(|| format!("vacuous: {syncs} fsyncs, {decides} retires"))
                    }),
                    Admit | Pin | Unpin | Locate | Classify | HasRecipe => {
                        unreachable!("{act:?} is the hub cache's")
                    }
                }
            }
        }
    }
}

/// What waited, or went wrong, while `held` was held inside its first
/// `op`, of `acts` each on a thread of its own.
fn waited(held: Act, op: Op, acts: &[Act]) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let scene = Scene::new(dir.path(), held, acts);
    let gate = Arc::new(Gate::default());
    gate.arm();
    let holder = {
        let (gate, act) = (gate.clone(), scene.act(held, true));
        thread::spawn(move || {
            io_hook::install(Arc::new(move |o| {
                if o == op {
                    gate.pass();
                }
            }));
            act()
        })
    };
    let entered = gate.entered().is_some();
    assert!(entered, "nothing reached the gate: {held:?} at {op:?}");
    let (done_tx, done_rx) = mpsc::channel();
    let threads: Vec<_> = acts
        .iter()
        .map(|&a| {
            let (call, done_tx) = (scene.act(a, false), done_tx.clone());
            thread::spawn(move || done_tx.send((a, call())).unwrap())
        })
        .collect();
    let deadline = Instant::now() + PATIENCE;
    let mut failed = Vec::new();
    let mut finished = 0;
    while finished < threads.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        match done_rx.recv_timeout(left) {
            Ok((act, problem)) => {
                finished += 1;
                failed.extend(problem.map(|p| format!("{act:?} with {held:?} at {op:?}: {p}")));
            }
            Err(_) => {
                let waiting = threads.len() - finished;
                failed.push(format!(
                    "{waiting} of {acts:?} waited for {held:?} at {op:?}"
                ));
                break;
            }
        }
    }
    gate.release();
    for t in threads {
        t.join().unwrap();
    }
    failed.extend(holder.join().unwrap());
    if let Scene::Cache(cache, _) = &scene
        && cache.locate(&h(0)).is_none()
    {
        failed.push(format!("the pinned survivor was lost at {op:?}"));
    }
    failed
}
