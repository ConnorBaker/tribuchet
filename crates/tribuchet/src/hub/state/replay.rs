//! Buffered per-build event replay for dedupe subscribers.

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::Instant;
use tonic::Status;

/// How long after its build ends a client catching up can count on the
/// outputs staying in the hub's cache. UNMEASURED: no client stalled in
/// the deployed hub journal it could be sized from. Past it the
/// catch-up goes on while the chunks last, else the client resubmits.
const CATCH_UP_PIN: Duration = Duration::from_mins(10);

use super::{EventTx, MAX_REPLAY_BYTES, SUB_CHANNEL_SLACK};
use crate::chunker::Recipe;
use crate::chunkstore::{Hash, PinGuard};
use crate::hub::chunkcache::ChunkCache;
use crate::proto::{AttachEvent, OutputNar, attach_event};

/// Said to a client that missed log lines, before what follows them.
const TRUNCATED: &str =
    "tribuchet: part of this build's log was dropped for this client; outputs follow in full\n";

/// Said to a client behind what the log keeps while the build runs: a
/// resubmit would come back to this same log (upstream's status and words).
const EXHAUSTED: &str = "build output exceeded the replay buffer; retry after it finishes";

/// Buffered event log of one in-flight build; late identical submissions
/// (dedupe) replay the buffer and then follow live. The buffer holds the
/// compressed output chunks too, capped at MAX_REPLAY_BYTES. Each client
/// reads at its own pace (`read`), publish waits for none; past the cap it
/// may miss log lines, told so, but gets each chunk from the cache, unless
/// it is behind even what the log keeps of those: then it is refused
/// while the build runs, and resubmits once it ended.
#[derive(Default)]
pub(in crate::hub) struct Replay {
    /// The build's id, for the warnings.
    id: String,
    inner: Mutex<ReplayInner>,
    /// Woken at each event and at the build's end.
    grew: Notify,
    /// The waits (`Wait` bits) the clients were told of since the build
    /// was last dispatched.
    told_waiting: AtomicU8,
    /// Since when it has had no client, and when its current run
    /// started: kept across its dispatches (state/clientless.rs).
    hold: StdMutex<Hold>,
}

#[derive(Default)]
struct Hold {
    clientless: Option<Instant>,
    run: Option<Instant>,
}

/// What a build can wait for, told to its clients once per dispatch.
#[derive(Clone, Copy)]
pub(in crate::hub) enum Wait {
    /// A connected worker that builds its system.
    Worker = 1,
    /// The hub's own nix-daemon.
    Daemon = 2,
}

/// What an event held costs beyond the bytes it holds (`cost`).
const HELD: usize = mem::size_of::<Held>();

#[derive(Default)]
struct ReplayInner {
    /// The log from `base` on; the events before it went past the cap.
    events: VecDeque<Held>,
    base: usize,
    /// Of those that went, all but the log lines and the chunks a restart
    /// superseded: a chunk without its bytes (read back from the cache),
    /// restarts, ends, verdicts.
    went: VecDeque<Held>,
    /// One past the last log line that went.
    lost: usize,
    /// One past the last event that went from `went` too, once it alone
    /// was past the cap: a client behind it cannot have the outputs whole.
    forgot: usize,
    /// The `cost` of what `events` and `went` hold, at most `cap` (else
    /// MAX_REPLAY_BYTES). Not counted: the two deques' spare room, which
    /// neither gives back; at worst about 2.7 times the cap (a cap's worth
    /// of empty lines, then of chunk records).
    bytes: usize,
    cap: Option<usize>,
    /// Per output: its last restart, and its chunks since.
    paths: HashMap<String, Output>,
    /// The latest delivery's outputs, read from the cache for a chunk
    /// whose bytes went.
    outputs: Option<Arc<KeptOutputs>>,
    /// When the build ended (`finish`).
    ended: Option<Instant>,
    subs: Vec<EventTx>,
    /// The cap was reached: warned once.
    overflowed: bool,
}

struct Held {
    /// Its place in the log.
    seq: usize,
    event: AttachEvent,
    /// A chunk's place in its output since the output's last restart.
    nth: usize,
}

/// What holding `ev` costs: the event and the bytes it holds.
fn cost(ev: &AttachEvent) -> usize {
    HELD + match ev.event.as_ref() {
        Some(attach_event::Event::Log(l)) => l.len(),
        Some(attach_event::Event::Output(o)) => o.store_path.len() + o.zstd_nar_chunk.len(),
        Some(
            attach_event::Event::Error(s)
            | attach_event::Event::OutputRestart(s)
            | attach_event::Event::Dispatched(s),
        ) => s.len(),
        Some(attach_event::Event::ExitCode(_)) | None => 0,
    }
}

#[derive(Default)]
struct Output {
    restart: usize,
    chunks: usize,
}

/// A delivery's outputs, pinned in the hub's chunk cache so a
/// subscriber that missed them can be sent them again.
pub(in crate::hub) struct KeptOutputs {
    cache: Arc<ChunkCache>,
    outputs: Vec<(String, Arc<Recipe>)>,
    /// Released at a requeue, or CATCH_UP_PIN after the build ends.
    pin: StdMutex<Option<PinGuard>>,
}

impl KeptOutputs {
    pub(in crate::hub) fn new(
        cache: Arc<ChunkCache>,
        outputs: Vec<(String, Arc<Recipe>)>,
        pin: PinGuard,
    ) -> Self {
        Self {
            cache,
            outputs,
            pin: StdMutex::new(Some(pin)),
        }
    }

    /// Leave the outputs' chunks to the cache.
    fn unpin(&self) {
        drop(self.pin.lock().unwrap().take());
    }
}

fn event(ev: attach_event::Event) -> AttachEvent {
    AttachEvent { event: Some(ev) }
}

/// Where one client is in the log.
#[derive(Default)]
struct Cursor {
    at: usize,
    /// Told of the lines missed since the last line sent.
    told: bool,
    /// It missed some of the log, or came after the build's end.
    behind: bool,
}

enum Next {
    Send(AttachEvent),
    /// A chunk whose bytes went, from the cache.
    Read(Arc<KeptOutputs>, String, Hash, usize),
    /// The verdict, and when the build ended if the client is behind.
    Verdict(AttachEvent, Option<Instant>),
    Resubmit,
    /// Behind what the log keeps, the build still on.
    Exhausted,
    Wait,
}

impl ReplayInner {
    /// The step after `c`.
    fn next(&self, c: &mut Cursor) -> Next {
        loop {
            let (held, gone) = if c.at < self.base {
                if c.at < self.forgot {
                    return match self.ended {
                        Some(_) => Next::Resubmit,
                        None => Next::Exhausted,
                    };
                }
                if c.at < self.lost && !c.told {
                    (c.told, c.behind) = (true, true);
                    return Next::Send(event(attach_event::Event::Log(TRUNCATED.into())));
                }
                let i = self.went.partition_point(|h| h.seq < c.at);
                match self.went.get(i) {
                    Some(held) => (held, true),
                    None => {
                        c.at = self.base;
                        continue;
                    }
                }
            } else {
                match self.events.get(c.at - self.base) {
                    Some(held) => (held, false),
                    None => break,
                }
            };
            let i = held.seq;
            c.at = i + 1;
            match held.event.event.as_ref() {
                Some(attach_event::Event::Log(_)) => {
                    c.told = false;
                    return Next::Send(held.event.clone());
                }
                Some(attach_event::Event::Output(o)) => {
                    let path = &o.store_path;
                    // One a later restart superseded: that restart follows.
                    if self.superseded(path, i) {
                        continue;
                    }
                    if !gone || o.eof {
                        return Next::Send(held.event.clone());
                    }
                    let recipe = self.outputs.as_ref().and_then(|kept| {
                        let (_, recipe) = kept.outputs.iter().find(|(p, _)| p == path)?;
                        Some((kept.clone(), *recipe.get(held.nth)?))
                    });
                    return match recipe {
                        Some((kept, (hash, size))) => Next::Read(kept, path.clone(), hash, size),
                        None => Next::Resubmit,
                    };
                }
                Some(attach_event::Event::ExitCode(_) | attach_event::Event::Error(_)) => {
                    let ended = self.ended.filter(|_| c.behind);
                    return Next::Verdict(held.event.clone(), ended);
                }
                _ => return Next::Send(held.event.clone()),
            }
        }
        if self.ended.is_some() {
            Next::Resubmit
        } else {
            Next::Wait
        }
    }

    /// Whether a restart of `path` after `i` superseded its event `i`.
    fn superseded(&self, path: &str, i: usize) -> bool {
        self.paths.get(path).is_some_and(|p| i < p.restart)
    }

    /// Past the cap, drop the oldest events: a log line for good, a chunk's
    /// bytes to be read back from the cache; then, if what went alone is
    /// past it, the oldest of that, a client behind it resubmitting.
    fn trim(&mut self, id: &str) {
        while self.bytes > self.cap.unwrap_or(MAX_REPLAY_BYTES) {
            if let Some(mut held) = self.events.pop_front() {
                self.base += 1;
                self.bytes -= cost(&held.event);
                match held.event.event.as_mut() {
                    Some(attach_event::Event::Log(_)) => self.lost = held.seq + 1,
                    Some(attach_event::Event::Output(o))
                        if self.superseded(&o.store_path, held.seq) => {}
                    ev => {
                        if let Some(attach_event::Event::Output(o)) = ev {
                            o.zstd_nar_chunk = Vec::new();
                        }
                        self.bytes += cost(&held.event);
                        self.went.push_back(held);
                    }
                }
            } else if let Some(held) = self.went.pop_front() {
                self.bytes -= cost(&held.event);
                self.forgot = held.seq + 1;
            } else {
                return;
            }
            if !mem::replace(&mut self.overflowed, true) {
                tracing::warn!(
                    id,
                    "replay buffer cap reached; clients behind it miss its oldest log lines, \
                     and once only chunk records are left, are refused until the build ends"
                );
            }
        }
    }
}

/// One client reading the log from the start at its own pace, until the
/// verdict. Its stream ends without one once the build ended without one,
/// or a chunk it needs left the cache: attach then resubmits (attach.rs).
/// Behind what the log keeps while the build runs, it is refused
/// (EXHAUSTED), which attach does not retry.
async fn read(replay: Arc<Replay>, tx: EventTx) {
    let id = &replay.id;
    let mut c = Cursor {
        behind: replay.inner.lock().await.ended.is_some(),
        ..Cursor::default()
    };
    loop {
        let grew = replay.grew.notified();
        tokio::pin!(grew);
        grew.as_mut().enable();
        let next = replay.inner.lock().await.next(&mut c);
        let ev = match next {
            Next::Send(ev) => ev,
            Next::Read(kept, path, hash, size) => {
                let Ok(frame) = kept.cache.read_checked(&hash, size).await else {
                    tracing::warn!(
                        id,
                        path,
                        "output chunk left the hub's cache; client resubmits"
                    );
                    break;
                };
                event(attach_event::Event::Output(OutputNar {
                    store_path: path,
                    zstd_nar_chunk: frame,
                    eof: false,
                }))
            }
            Next::Verdict(ev, ended) => {
                if tx.send(Ok(ev)).await.is_ok()
                    && let Some(ended) = ended
                {
                    tracing::info!(
                        id,
                        after_end_ms =
                            u64::try_from(ended.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "attach subscriber caught up"
                    );
                }
                break;
            }
            Next::Resubmit => break,
            Next::Exhausted => {
                let _ = tx.send(Err(Status::resource_exhausted(EXHAUSTED))).await;
                break;
            }
            Next::Wait => {
                tokio::select! {
                    () = &mut grew => continue,
                    () = tx.closed() => break,
                }
            }
        };
        if tx.send(Ok(ev)).await.is_err() {
            break;
        }
    }
    let mut inner = replay.inner.lock().await;
    inner.subs.retain(|s| !s.same_channel(&tx));
}

/// Release a finished build's outputs from the cache's pin once
/// CATCH_UP_PIN has passed, whoever still holds them.
async fn unpin_after(kept: Weak<KeptOutputs>) {
    tokio::time::sleep(CATCH_UP_PIN).await;
    if let Some(kept) = kept.upgrade() {
        kept.unpin();
    }
}

impl Replay {
    pub(in crate::hub) fn new(id: String) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    /// Append `ev` to the log; waits for no client.
    pub(in crate::hub) async fn publish(&self, ev: attach_event::Event) {
        if matches!(ev, attach_event::Event::Dispatched(_)) {
            self.told_waiting.store(0, Ordering::Relaxed);
        }
        let ev = AttachEvent { event: Some(ev) };
        let mut inner = self.inner.lock().await;
        let (i, mut nth) = (inner.base + inner.events.len(), 0);
        match &ev.event {
            Some(attach_event::Event::Output(o)) if !o.eof => {
                let out = inner.paths.entry(o.store_path.clone()).or_default();
                (nth, out.chunks) = (out.chunks, out.chunks + 1);
            }
            Some(attach_event::Event::OutputRestart(path)) => {
                let restart = Output {
                    restart: i,
                    ..Output::default()
                };
                inner.paths.insert(path.clone(), restart);
                // The chunks it supersedes need not be read back.
                let mut freed = 0;
                inner.went.retain(|h| match h.event.event.as_ref() {
                    Some(attach_event::Event::Output(o)) if o.store_path == *path => {
                        freed += cost(&h.event);
                        false
                    }
                    _ => true,
                });
                inner.bytes -= freed;
            }
            _ => {}
        }
        inner.bytes += cost(&ev);
        inner.events.push_back(Held {
            seq: i,
            event: ev,
            nth,
        });
        inner.trim(&self.id);
        drop(inner);
        self.grew.notify_waiters();
    }

    pub(in crate::hub) async fn subscribe(
        self: &Arc<Self>,
    ) -> mpsc::Receiver<Result<AttachEvent, Status>> {
        self.hold.lock().unwrap().clientless = None;
        let (tx, rx) = mpsc::channel(SUB_CHANNEL_SLACK);
        // Before the reader, which takes it out when it ends.
        self.inner.lock().await.subs.push(tx.clone());
        tokio::spawn(read(self.clone(), tx));
        rx
    }

    /// Tell the clients the build waits for `wait`, once until it is
    /// dispatched. True if this call told them.
    pub(in crate::hub) async fn tell_waiting(&self, wait: Wait, note: String) -> bool {
        let bit = wait as u8;
        if self.told_waiting.fetch_or(bit, Ordering::Relaxed) & bit != 0 {
            return false;
        }
        self.publish(attach_event::Event::Log(note.into_bytes()))
            .await;
        true
    }

    /// Note whether the build has a client at the check at `now`: it has
    /// had none since the first check that found none, across
    /// dispatches, until a client subscribes again (clientless).
    pub(in crate::hub) fn checked(&self, has_client: bool, now: Instant) {
        let mut h = self.hold.lock().unwrap();
        if has_client {
            h.clientless = None;
        } else {
            h.clientless.get_or_insert(now);
        }
    }

    /// Since when the build has had no client, as of the latest check.
    pub(in crate::hub) fn clientless(&self) -> Option<Instant> {
        self.hold.lock().unwrap().clientless
    }

    /// When the build's current run started, as of its dispatch at `now`:
    /// a resume keeps the run, a fresh dispatch starts one.
    pub(in crate::hub) fn run_since(&self, resumed: bool, now: Instant) -> Instant {
        let mut h = self.hold.lock().unwrap();
        match h.run {
            Some(run) if resumed => run,
            _ => *h.run.insert(now),
        }
    }

    /// Keep the outputs a delivery is about to send, for subscribers
    /// that miss them.
    pub(in crate::hub) async fn keep_outputs(&self, outputs: KeptOutputs) {
        self.inner.lock().await.outputs = Some(Arc::new(outputs));
    }

    /// A requeue restarted the latest delivery's outputs (every chunk sent
    /// so far is superseded), and the next attempt delivers its own: leave
    /// their chunks to the cache.
    pub(in crate::hub) async fn unpin_outputs(&self) {
        self.inner.lock().await.outputs = None;
    }

    /// The build ended: a client still reading gets the rest, then the
    /// verdict, or its stream ends without one.
    pub(in crate::hub) async fn finish(&self) {
        let mut inner = self.inner.lock().await;
        inner.ended = Some(Instant::now());
        if let Some(kept) = &inner.outputs {
            tokio::spawn(unpin_after(Arc::downgrade(kept)));
        }
        drop(inner);
        self.grew.notify_waiters();
    }

    /// Any attach client still listening? Subscribers whose stream was
    /// dropped count as gone even before their readers prune them.
    pub(in crate::hub) async fn has_subscribers(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.subs.iter().any(|tx| !tx.is_closed())
    }
}

#[cfg(test)]
#[path = "replay/tests.rs"]
mod tests;
