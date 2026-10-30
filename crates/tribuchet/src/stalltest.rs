//! Test harness for runtime stalls (as a hub's watchdog kills them): a
//! small multi-thread runtime, a task woken by socket
//! readiness, a timer whose ticks are recorded, and a gate that holds
//! a thread until the test lets it go.
//!
//! The socket wake matters. tokio hands its I/O and timer driver to a
//! worker only while that worker parks, and the worker an I/O event
//! wakes runs the woken task itself, waking no other worker unless it
//! queued two tasks. If that task then blocks, nobody polls the driver:
//! timers and sockets stall although the other workers are idle (tokio
//! 1.53.2, scheduler/multi_thread/park.rs:132-147, worker.rs:915-917).

use std::future::{Future, poll_fn};
use std::io::Write as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tokio::net::UnixStream;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

use crate::{rt, sd};

/// Three of the four workers stay idle throughout.
pub(crate) const WORKERS: usize = 4;
/// How long a test waits for progress while a thread is held. A stall
/// shows as no progress for all of it.
pub(crate) const HOLD: Duration = Duration::from_secs(3);
/// How long a test of a fix waits for [`BEATS`] beats while a thread is
/// held. A loaded host only delays them; under the hazard none come
/// while the thread is held, however long the wait.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);
/// Beats a running timer shows while a thread is held: more than the
/// two periods' worth of pings sent past the last tick.
pub(crate) const BEATS: usize = 10;
/// How much longer a runtime's longest beat gap over a hold may be than
/// a [`clock`]'s over the same hold. A runtime whose timer stays
/// attended exceeds it by at most about one of the tests' 50 ms periods
/// of phase between the two clocks when a stop delays both; a driver
/// handed over late runs further over.
pub(crate) const SLACK: Duration = Duration::from_millis(300);
/// Upper bound on any hold, so a broken test fails instead of hanging.
const SAFETY: Duration = Duration::from_secs(20);

/// Times of a recurring event, awaitable from a plain thread.
#[derive(Default)]
pub(crate) struct Beats {
    at: Mutex<Vec<Instant>>,
    cv: Condvar,
}

impl Beats {
    pub(crate) fn beat(&self) {
        self.at.lock().unwrap().push(Instant::now());
        self.cv.notify_all();
    }

    /// Whether `n` beats came after `since` within `timeout`.
    pub(crate) fn wait_after(&self, since: Instant, n: usize, timeout: Duration) -> bool {
        let at = self.at.lock().unwrap();
        let (at, _) = self
            .cv
            .wait_timeout_while(at, timeout, |at| {
                at.iter().filter(|t| **t > since).count() < n
            })
            .unwrap();
        at.iter().filter(|t| **t > since).count() >= n
    }

    /// The longest interval between consecutive beats that overlaps
    /// [from, to]. Without a beat on either side, the edge counts.
    pub(crate) fn max_gap(&self, from: Instant, to: Instant) -> Duration {
        let at = self.at.lock().unwrap();
        let before = at.iter().rev().find(|t| **t <= from).copied();
        let mut prev = before.unwrap_or(from);
        let mut gap = Duration::ZERO;
        for t in at.iter().filter(|t| **t > from) {
            gap = gap.max(*t - prev);
            prev = *t;
            if *t >= to {
                return gap;
            }
        }
        gap.max(to - prev)
    }
}

/// Beats of a plain thread every `period`, as long as the returned
/// beats are held: the reference for a runtime's beats over a hold. A
/// host that deschedules the process delays both alike; a runtime that
/// leaves its timer unattended delays only its own.
pub(crate) fn clock(period: Duration) -> Arc<Beats> {
    let beats = Arc::<Beats>::default();
    let weak = Arc::downgrade(&beats);
    thread::spawn(move || {
        loop {
            thread::sleep(period);
            let Some(beats) = weak.upgrade() else {
                return;
            };
            beats.beat();
        }
    });
    beats
}

/// Ticks of an `interval(period)` running on `rt`.
pub(crate) fn ticker(rt: &Handle, period: Duration) -> Arc<Beats> {
    let beats = Arc::<Beats>::default();
    let b = beats.clone();
    rt.spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            b.beat();
        }
    });
    beats
}

/// The other end of an [`on_io_wake`] task's socket.
pub(crate) struct Trigger(StdUnixStream);

impl Trigger {
    /// Wake the task right after a tick, so its socket event is the
    /// only one in the driver's turn: a timer expiring in the same turn
    /// queues a second task, and tokio then wakes another worker that
    /// takes the driver over (seen in 1 of 8 unaligned trials).
    pub(crate) fn fire_after_tick(mut self, ticks: &Beats) {
        assert!(ticks.wait_after(Instant::now(), 1, SAFETY), "no tick");
        self.0.write_all(b"x").unwrap();
    }
}

/// Spawn `f` behind a socket read, as relay tasks wake on worker
/// traffic. Returns once the task waits on the socket.
pub(crate) fn on_io_wake<F>(rt: &Handle, f: F) -> (Trigger, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (trigger, rx) = StdUnixStream::pair().unwrap();
    rx.set_nonblocking(true).unwrap();
    let (armed_tx, armed_rx) = std_mpsc::channel();
    let task = {
        let _rt = rt.enter();
        let rx = UnixStream::from_std(rx).unwrap();
        rt.spawn(async move {
            let mut armed = Some(armed_tx);
            poll_fn(|cx| {
                let ready = rx.poll_read_ready(cx);
                if ready.is_pending()
                    && let Some(tx) = armed.take()
                {
                    tx.send(()).unwrap();
                }
                ready
            })
            .await
            .unwrap();
            f.await
        })
    };
    armed_rx.recv_timeout(SAFETY).expect("task never polled");
    (Trigger(trigger), task)
}

#[derive(Default)]
struct GateState {
    armed: bool,
    entered: Option<Instant>,
    held: usize,
    released: bool,
}

/// Holds every thread that passes it while armed, until released.
#[derive(Default)]
pub(crate) struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    pub(crate) fn arm(&self) {
        self.state.lock().unwrap().armed = true;
    }

    /// Block here while armed and not yet released.
    pub(crate) fn pass(&self) {
        let mut s = self.state.lock().unwrap();
        if !s.armed || s.released {
            return;
        }
        s.entered.get_or_insert_with(Instant::now);
        s.held += 1;
        self.cv.notify_all();
        let (s, timeout) = self
            .cv
            .wait_timeout_while(s, SAFETY, |s| !s.released)
            .unwrap();
        assert!(!timeout.timed_out() || s.released, "gate never released");
    }

    /// When the first thread was held, if one was within SAFETY.
    pub(crate) fn entered(&self) -> Option<Instant> {
        let s = self.state.lock().unwrap();
        let (s, _) = self
            .cv
            .wait_timeout_while(s, SAFETY, |s| s.entered.is_none())
            .unwrap();
        s.entered
    }

    /// When the first thread was held.
    pub(crate) fn wait_entered(&self) -> Instant {
        self.entered().expect("nothing reached the gate")
    }

    /// Wait until `n` threads are held at once or were.
    pub(crate) fn wait_held(&self, n: usize) -> Instant {
        let s = self.state.lock().unwrap();
        let (s, _) = self
            .cv
            .wait_timeout_while(s, SAFETY, |s| s.held < n)
            .unwrap();
        assert!(
            s.held >= n,
            "only {} of {n} threads reached the gate",
            s.held
        );
        Instant::now()
    }

    pub(crate) fn release(&self) -> Instant {
        self.state.lock().unwrap().released = true;
        self.cv.notify_all();
        Instant::now()
    }
}

/// Pings from `crate::sd::feed` on `rt`, every `period`: a quarter of
/// the watchdog timeout it is given.
fn fed(rt: &Runtime, period: Duration) -> Arc<Beats> {
    let pings = Arc::<Beats>::default();
    let p = pings.clone();
    let _rt = rt.enter();
    sd::feed(4 * period, move || p.beat());
    assert!(pings.wait_after(Instant::now(), 2, HOLD), "no ping");
    pings
}

/// Test A, on the runtime every role runs (crate::rt). An I/O-woken
/// task blocks one of four workers until the systemd ping and the
/// runtime's own timer beat BEATS times each, or PATIENCE passed.
/// Without eager driver hand-off neither beats while the worker is held,
/// and a late hand-off lets the timer's gap exceed a plain thread's by
/// more than SLACK.
#[test]
fn one_blocked_worker_stops_neither_pings_nor_timers() {
    const TICK: Duration = Duration::from_millis(50);
    let rt = rt::builder().worker_threads(WORKERS).build().unwrap();
    let pings = fed(&rt, TICK);
    let ticks = ticker(rt.handle(), TICK);
    let reference = clock(TICK);
    let gate = Arc::new(Gate::default());
    gate.arm();
    let g = gate.clone();
    let (trigger, task) = on_io_wake(rt.handle(), async move { g.pass() });
    trigger.fire_after_tick(&ticks);
    let entered = gate.wait_entered();
    let pinged = pings.wait_after(entered, BEATS, PATIENCE);
    let left = PATIENCE.saturating_sub(entered.elapsed());
    let ticked = ticks.wait_after(entered, BEATS, left);
    let released = gate.release();
    pings.wait_after(released, 2, HOLD);
    ticks.wait_after(released, 2, HOLD);
    reference.wait_after(released, 1, HOLD);
    let ping_gap = pings.max_gap(entered, released);
    let tick_gap = ticks.max_gap(entered, released);
    let ref_gap = reference.max_gap(entered, released);
    rt.block_on(task).unwrap();
    eprintln!(
        "one worker held: ping gap {ping_gap:?}, timer gap {tick_gap:?}, reference gap {ref_gap:?}"
    );
    assert!(
        pinged,
        "pings stopped while a worker was held: gap {ping_gap:?}"
    );
    assert!(
        ticked,
        "runtime timer stopped while a worker was held: gap {tick_gap:?}"
    );
    assert!(
        tick_gap <= ref_gap + SLACK,
        "runtime timer late while a worker was held: gap {tick_gap:?}, a plain thread's {ref_gap:?}"
    );
}

/// The ping stays meaningful: a runtime whose every worker blocks runs
/// no timer, sends no ping, and so would be restarted.
#[test]
fn a_wedged_runtime_gets_no_ping() {
    const PERIOD: Duration = Duration::from_millis(50);
    let rt = rt::builder().worker_threads(WORKERS).build().unwrap();
    let pings = fed(&rt, PERIOD);
    let gate = Arc::new(Gate::default());
    gate.arm();
    // One at a time: a worker pulls several queued tasks at once.
    let tasks: Vec<_> = (1..=WORKERS)
        .map(|n| {
            let g = gate.clone();
            let task = rt.spawn(async move { g.pass() });
            gate.wait_held(n);
            task
        })
        .collect();
    let wedged = Instant::now();
    let pinged = pings.wait_after(wedged + 3 * PERIOD, 1, 10 * PERIOD);
    let released = gate.release();
    let resumed = pings.wait_after(released, 2, HOLD);
    for task in tasks {
        rt.block_on(task).unwrap();
    }
    assert!(!pinged, "pinged a wedged runtime");
    assert!(resumed, "pings did not resume");
}

/// A stalled runtime is killed only after about WatchdogSec: it is
/// pinged every quarter of it, at most 1 s apart. Over 3.5 s of a 30 s
/// watchdog (WatchdogSec = 30, as the hub's), pings at 0, 1, 2 and 3 s;
/// at half of it, one, and a stall
/// of half the timeout could be killed.
#[tokio::test(start_paused = true)]
async fn pings_come_every_quarter_of_the_timeout_and_at_most_a_second_apart() {
    let pings = Arc::new(AtomicUsize::new(0));
    let p = pings.clone();
    sd::feed(Duration::from_secs(30), move || {
        p.fetch_add(1, Ordering::SeqCst);
    });
    tokio::time::sleep(Duration::from_millis(3500)).await;
    let n = pings.load(Ordering::SeqCst);
    assert_eq!(n, 4, "pings in 3.5 s of a 30 s watchdog");
}
