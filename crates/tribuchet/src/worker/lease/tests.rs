//! The pool's laws after every step of any sequence, and leases taken at
//! once from many threads.

use std::collections::BTreeSet;
use std::hint;
use std::pin::pin;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::task::{Context, Wake, Waker};
use std::thread;

use proptest::collection::vec;
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};
use proptest::test_runner::{Config, RngSeed, TestRunner};

use super::*;

/// Sockets 0..6 are configured; 6 and 7 are not, as the agent of a build
/// adopted after its socket left the config.
const CONFIGURED: usize = 6;

fn path(k: usize) -> PathBuf {
    PathBuf::from(format!("/agents/{k}.sock"))
}

/// The lease's agent; None while it waits for one.
fn on(lease: &Lease) -> Option<usize> {
    let name = lease.socket.as_ref()?.file_stem()?.to_str()?;
    name.parse().ok()
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Lease,
    /// A build on agent k runs already: adopted, held, killed or swept.
    LeaseThis(usize),
    /// The lease at an index, modulo the leases, ends.
    Drop(usize),
    /// Its agent does not answer the start; cancelled if none is free.
    Unreachable(usize),
    MarkDown(usize),
    ProbeUp(usize),
    Discover(usize),
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    let (k, i) = (|| 0..CONFIGURED + 2, || 0..64usize);
    let op = prop_oneof![
        4 => Just(Op::Lease),
        1 => k().prop_map(Op::LeaseThis),
        3 => i().prop_map(Op::Drop),
        2 => i().prop_map(Op::Unreachable),
        2 => k().prop_map(Op::MarkDown),
        2 => k().prop_map(Op::ProbeUp),
        1 => k().prop_map(Op::Discover),
    ];
    vec(op, 1..60)
}

/// The pool's agents by index: all, free, down.
type Seen = [BTreeSet<usize>; 3];

fn seen(pool: &Pool) -> Seen {
    let agents = pool.lock();
    let index = |p: &PathBuf| -> Option<usize> { p.file_stem()?.to_str()?.parse().ok() };
    [&agents.all, &agents.free, &agents.down].map(|v| {
        let s: BTreeSet<usize> = v.iter().filter_map(index).collect();
        assert_eq!(s.len(), v.len(), "a socket twice");
        s
    })
}

/// Apply `op` to the pool seen as `b` with room `r`, checking what its law
/// says it returns; what its law says the agents become.
/// Each lease with whether its build ran already (`lease_this`).
type Leases = Vec<(Lease, bool)>;

fn step(pool: &Arc<Pool>, leases: &mut Leases, b: &Seen, r: Room, op: Op) -> Seen {
    let [all, free, _] = b;
    let mut want = b.clone();
    let n = leases.len().max(1);
    let held = |leases: &Leases, k, ran: bool| {
        let on_k = |(l, r): &(Lease, bool)| on(l) == Some(k) && (ran || !r);
        leases.iter().any(on_k)
    };
    match op {
        // A lease exactly while a slot is free, on an agent that was free.
        Op::Lease => match pool.lease() {
            Some(lease) => {
                assert!(r.free > 0 && want[1].remove(&on(&lease).unwrap()));
                leases.push((lease, false));
            }
            None => assert_eq!(r.free, 0, "no lease, a slot free"),
        },
        // Always, its agent out of free and down; never an agent `lease()`
        // gave, but two running builds may claim one.
        Op::LeaseThis(k) if !held(leases, k, false) => {
            leases.push((pool.lease_this(&path(k)), true));
            want[1].remove(&k);
            want[2].remove(&k);
        }
        // Its agent back to free if in the pool and no other lease has it.
        Op::Drop(i) if !leases.is_empty() => {
            let k = on(&leases.remove(i % n).0);
            if let Some(k) = k.filter(|k| all.contains(k) && !held(leases, *k, true)) {
                want[1].insert(k);
            }
        }
        // Its agent down, and a free one taken in its place if there is one.
        // Only a lease from `lease()` starts.
        Op::Unreachable(i) if !leases.is_empty() && !leases[i % n].1 => {
            let lease = &mut leases[i % n].0;
            want[2].extend(on(lease));
            let took = lease.next_agent(&|| true, &mut None::<fn()>).is_ok();
            assert_eq!(took, !free.is_empty(), "took an agent");
            assert!(!took || want[1].remove(&on(lease).unwrap()));
        }
        Op::MarkDown(k) => {
            assert_eq!(pool.mark_down(&path(k)), free.contains(&k));
            if want[1].remove(&k) {
                want[2].insert(k);
            }
        }
        Op::ProbeUp(k) => {
            pool.probe_up(&path(k));
            if want[2].remove(&k) {
                want[1].insert(k);
            }
        }
        // New, down; an outside lease's agent not until its lease ends.
        Op::Discover(k) => {
            pool.discover(vec![path(k)]);
            if !held(leases, k, true) && want[0].insert(k) {
                want[2].insert(k);
            }
        }
        _ => {}
    }
    want
}

/// What every state `a` keeps, with room `r` of `max_jobs`, the room
/// before `before`, and what a wake of the session loop found (`woken`):
/// free slots, free agents.
fn laws(
    leases: &Leases,
    a: &Seen,
    r: Room,
    before: Room,
    woken: Option<(usize, usize)>,
    max_jobs: usize,
) {
    let [all, free, down] = a;
    // Free and down are disjoint agents of the pool; a lease's agent is
    // neither (so `lease()`, which takes a free one, shares none).
    assert!(free.is_disjoint(down) && free.union(down).all(|k| all.contains(k)));
    let held: Vec<usize> = leases.iter().filter_map(|(l, _)| on(l)).collect();
    let idle = |k: &usize| free.contains(k) || down.contains(k);
    assert!(!held.iter().any(idle), "a leased agent free or down");
    // One slot per agent not down, at most max-jobs; each lease takes one;
    // one is free only with an agent free.
    let capacity = max_jobs.min(all.len() - down.len());
    let free_slots = capacity.saturating_sub(leases.len());
    assert_eq!(
        (r.capacity, r.free, r.leases),
        (capacity, free_slots, leases.len())
    );
    assert!(r.free == 0 || !free.is_empty(), "a slot free, no agent");
    // The session loop is woken whenever the slots free go from none to
    // some, and the wake finds them, and their agents, free.
    assert!(
        woken.is_some() || before.free > 0 || r.free == 0,
        "not woken"
    );
    assert!(woken.is_none_or(|w| w == (r.free, free.len())), "{woken:?}");
    // The hub is asked for the free slots, and one more staged ahead with
    // an agent up.
    for lookahead in [false, true] {
        let asked = r.request(lookahead, 0);
        let more = usize::from(lookahead && r.capacity > 0);
        let want = (
            u32::try_from(r.free + more).unwrap(),
            u32::try_from(r.free).ok(),
        );
        assert_eq!((asked.capacity, asked.free_slots), want, "{lookahead}");
    }
}

/// The free slots and agents the session loop finds when it is woken.
struct Woken(Arc<Pool>, Mutex<Option<(usize, usize)>>);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let free = (self.0.room().free, self.0.idle_sockets().len());
        *self.1.lock().unwrap() = Some(free);
    }
}

/// Any steps, max-jobs below, at and above the agents: each step's law
/// and every state's; 256 cases under each of four seeds.
#[test]
fn the_pool_keeps_its_laws_after_any_steps() {
    for seed in [1, 42, 2026, 31_337] {
        let config = Config {
            cases: 256,
            failure_persistence: None,
            rng_seed: RngSeed::Fixed(seed),
            ..Config::default()
        };
        let cases = (1..=CONFIGURED + 2, ops());
        let ran = TestRunner::new(config).run(&cases, |(max_jobs, ops)| {
            let pool = Pool::new((0..CONFIGURED).map(path).collect(), max_jobs);
            let woken = Arc::new(Woken(pool.clone(), Mutex::new(None)));
            let waker = Waker::from(woken.clone());
            let mut leases = Vec::new();
            for op in ops {
                // Waiting, as the session loop: a wake left from an earlier
                // step is taken first.
                let mut wait = pin!(pool.changed.notified());
                while wait
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_ready()
                {
                    wait.set(pool.changed.notified());
                }
                *woken.1.lock().unwrap() = None;
                let (b, before) = (seen(&pool), pool.room());
                let want = step(&pool, &mut leases, &b, before, op);
                let a = seen(&pool);
                assert_eq!(a, want, "after {op:?}");
                let found = woken.1.lock().unwrap().take();
                laws(&leases, &a, pool.room(), before, found, max_jobs);
            }
            Ok(())
        });
        assert!(ran.is_ok(), "seed {seed}: {ran:?}");
    }
}

/// Leases taken at once from four threads never outnumber the capacity,
/// nor share an agent, with the agents binding and with max-jobs binding.
/// A lease checking the slots, then taking an agent under another lock,
/// lets two callers both pass the check.
#[test]
fn leases_at_once_never_outnumber_the_capacity() {
    for (agents, max_jobs) in [(1, 8), (4, 1)] {
        let pool = Pool::new((0..agents).map(path).collect(), max_jobs);
        let (held, most) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let busy: Vec<AtomicBool> = (0..agents).map(|_| AtomicBool::new(false)).collect();
        thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for _ in 0..50_000 {
                        let Some(lease) = pool.lease() else { continue };
                        let k = on(&lease).unwrap();
                        assert!(!busy[k].swap(true, SeqCst), "agent {k} leased twice");
                        most.fetch_max(held.fetch_add(1, SeqCst) + 1, SeqCst);
                        thread::yield_now();
                        held.fetch_sub(1, SeqCst);
                        busy[k].store(false, SeqCst);
                        drop(lease);
                    }
                });
            }
        });
        assert_eq!(most.load(SeqCst), 1, "most leases at once, {agents} agents");
    }
}

/// The agent a build that runs already leases is counted with its slot in
/// one step: a build asking meanwhile gets none. With a lease outside the
/// pool, an agent taken out of down before its lease is counted would free
/// a slot for that build.
#[test]
fn a_running_builds_lease_is_counted_with_its_agent() {
    let pool = Pool::new(vec![path(0), path(1)], 8);
    let _outside = pool.lease_this(&path(7));
    let (rounds, mut lost) = (2_000, 0);
    for _ in 0..rounds {
        pool.mark_down(&path(0));
        let (asking, done) = (AtomicBool::new(false), AtomicBool::new(false));
        let (running, taken) = thread::scope(|s| {
            let taker = s.spawn(|| {
                loop {
                    asking.store(true, SeqCst);
                    let lease = pool.lease();
                    if lease.is_some() || done.load(SeqCst) {
                        break lease;
                    }
                }
            });
            while !asking.load(SeqCst) {
                hint::spin_loop();
            }
            let running = pool.lease_this(&path(0));
            done.store(true, SeqCst);
            (running, taker.join().unwrap())
        });
        lost += usize::from(taken.is_some());
        drop((taken, running));
    }
    assert_eq!(lost, 0, "a build took a slot in {lost} of {rounds} rounds");
}

/// Two builds that run already on one agent (two held for an agent that
/// does not answer): the agent stays leased until both leases end, then is
/// free once; outside the pool, discovery leaves it out until then.
#[test]
fn an_agent_of_two_running_builds_stays_leased_until_both_end() {
    for k in [0, 7] {
        let pool = Pool::new(vec![path(0)], 1);
        let (first, second) = (pool.lease_this(&path(k)), pool.lease_this(&path(k)));
        let at = |v: Vec<PathBuf>| v.iter().filter(|s| **s == path(k)).count();
        drop(first);
        pool.discover(vec![path(k)]);
        let idle = (at(pool.idle_sockets()), at(pool.down_sockets()));
        assert_eq!(idle, (0, 0), "agent {k} free or down after the first lease");
        drop(second);
        let free = usize::from(k == 0);
        assert_eq!(at(pool.idle_sockets()), free, "agent {k} free after both");
    }
}
