//! Buffered per-build event replay for dedupe subscribers.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{iter, mem};

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;
use tonic::Status;

const SUB_STALL: Duration = Duration::from_mins(1);

/// How long after its build ends a client catching up can count on the
/// outputs staying in the hub's cache. UNMEASURED: no client stalled in
/// the deployed hub journal it could be sized from. Past it the
/// catch-up goes on while the chunks last, else the client resubmits.
const CATCH_UP_PIN: Duration = Duration::from_mins(10);

use super::{EventTx, MAX_REPLAY_BYTES, SUB_CHANNEL_SLACK};
use crate::chunker::Recipe;
use crate::chunkstore::PinGuard;
use crate::hub::chunkcache::ChunkCache;
use crate::proto::{AttachEvent, OutputNar, attach_event};

/// Said to a subscriber that missed events, before it is served again.
const TRUNCATED: &str =
    "tribuchet: part of this build's log was dropped for this client; outputs follow in full\n";

/// Buffered event log of one in-flight build; late identical submissions
/// (dedupe) replay the buffer and then follow live. The buffer holds the
/// compressed output chunks too, capped at MAX_REPLAY_BYTES. A
/// subscriber that misses events (it stalled, or came after the cap)
/// is served the outputs from the hub's chunk cache when the build ends.
#[derive(Default)]
pub(in crate::hub) struct Replay {
    /// The build's id, for the warnings.
    id: String,
    inner: Mutex<ReplayInner>,
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

#[derive(Default)]
struct ReplayInner {
    events: Vec<AttachEvent>,
    bytes: usize,
    /// Buffer cap hit: the backlog is incomplete, so late dedupe
    /// subscribers cannot replay it.
    overflowed: bool,
    subs: Vec<EventTx>,
    /// Attached but missing events: get no live ones, and catch_up
    /// once the build ends. While no output is being delivered they
    /// rejoin once they have room, the outputs they may hold part of
    /// restarted.
    behind: Vec<EventTx>,
    /// Back from `behind`. Sent live events without waiting: one full
    /// again goes back behind at once instead of holding the build up
    /// for SUB_STALL each time it stalls.
    rejoined: Vec<EventTx>,
    /// An output's chunks are going out: since the last Output with no
    /// OutputRestart (a requeue) after it.
    streaming: bool,
    /// Verdict events, kept past a buffer overflow.
    ending: Vec<AttachEvent>,
    /// The latest delivery's outputs, for catch_up.
    outputs: Option<Arc<KeptOutputs>>,
    /// When the build ended (`finish`).
    ended: Option<Instant>,
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

/// What a subscriber that missed events is sent before anything else:
/// the note, and a restart of every output it may hold part of (only
/// kept outputs ever went out).
fn missed(outputs: Option<&KeptOutputs>) -> Vec<AttachEvent> {
    let restarts = outputs
        .into_iter()
        .flat_map(|kept| &kept.outputs)
        .map(|(path, _)| event(attach_event::Event::OutputRestart(path.clone())));
    iter::once(event(attach_event::Event::Log(TRUNCATED.into())))
        .chain(restarts)
        .collect()
}

/// Bring a subscriber that missed events to the end of the build:
/// `missed`, every output from the start out of the chunk cache if the
/// build succeeded, then the verdict. Without a verdict it is told to
/// resubmit. Once the verdict is sent, and with it the last chunk read,
/// it logs how long after the build `ended`.
async fn catch_up(
    tx: EventTx,
    id: String,
    outputs: Option<Arc<KeptOutputs>>,
    ending: Vec<AttachEvent>,
    ended: Instant,
) {
    let gone = || {
        Err(Status::unavailable(
            "build is no longer in flight; resubmit",
        ))
    };
    for ev in missed(outputs.as_deref()) {
        if tx.send(Ok(ev)).await.is_err() {
            return;
        }
    }
    let succeeded = ending
        .iter()
        .any(|e| matches!(e.event, Some(attach_event::Event::ExitCode(0))));
    if ending.is_empty() || (succeeded && outputs.is_none()) {
        let _ = tx.send(gone()).await;
        return;
    }
    if let Some(kept) = outputs.filter(|_| succeeded) {
        for (path, recipe) in &kept.outputs {
            let nar = |zstd_nar_chunk, eof| {
                Ok(event(attach_event::Event::Output(OutputNar {
                    store_path: path.clone(),
                    zstd_nar_chunk,
                    eof,
                })))
            };
            for (hash, size) in recipe.iter() {
                let Ok(frame) = kept.cache.read_checked(hash, *size).await else {
                    tracing::warn!(
                        id,
                        path,
                        "output chunk left the hub's cache; client resubmits"
                    );
                    let _ = tx.send(gone()).await;
                    return;
                };
                if tx.send(nar(frame, false)).await.is_err() {
                    return;
                }
            }
            if tx.send(nar(Vec::new(), true)).await.is_err() {
                return;
            }
        }
    }
    for ev in ending {
        if tx.send(Ok(ev)).await.is_err() {
            return;
        }
    }
    tracing::info!(
        id,
        after_end_ms = u64::try_from(ended.elapsed().as_millis()).unwrap_or(u64::MAX),
        "attach subscriber caught up"
    );
}

/// Release a finished build's outputs from the cache's pin once
/// CATCH_UP_PIN has passed, whoever still holds them.
async fn unpin_after(kept: Weak<KeptOutputs>) {
    tokio::time::sleep(CATCH_UP_PIN).await;
    if let Some(kept) = kept.upgrade() {
        kept.unpin();
    }
}

fn event_size(ev: &attach_event::Event) -> usize {
    match ev {
        attach_event::Event::Log(d) => d.len(),
        attach_event::Event::Output(o) => o.zstd_nar_chunk.len(),
        attach_event::Event::OutputRestart(p) | attach_event::Event::Dispatched(p) => p.len(),
        attach_event::Event::Error(e) => e.len(),
        attach_event::Event::ExitCode(_) => 0,
    }
    .saturating_add(64)
}

impl Replay {
    pub(in crate::hub) fn new(id: String) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    pub(in crate::hub) async fn publish(&self, ev: attach_event::Event) {
        let sz = event_size(&ev);
        if matches!(ev, attach_event::Event::Dispatched(_)) {
            self.told_waiting.store(0, Ordering::Relaxed);
        }
        let streaming = match ev {
            attach_event::Event::Output(_) => Some(true),
            attach_event::Event::OutputRestart(_) => Some(false),
            _ => None,
        };
        let ending = matches!(
            ev,
            attach_event::Event::ExitCode(_) | attach_event::Event::Error(_)
        );
        let ev = AttachEvent { event: Some(ev) };
        let mut inner = self.inner.lock().await;
        if let Some(streaming) = streaming {
            inner.streaming = streaming;
        }
        if ending {
            inner.ending.push(ev.clone());
        }
        if !inner.streaming && !inner.behind.is_empty() {
            let preamble = missed(inner.outputs.as_deref());
            for tx in mem::take(&mut inner.behind) {
                // Room for the preamble and this event, or it stays.
                if tx.capacity() > preamble.len() {
                    for ev in &preamble {
                        let _ = tx.try_send(Ok(ev.clone()));
                    }
                    inner.rejoined.push(tx);
                } else if !tx.is_closed() {
                    inner.behind.push(tx);
                }
            }
        }
        for tx in mem::take(&mut inner.subs) {
            match tokio::time::timeout(SUB_STALL, tx.send(Ok(ev.clone()))).await {
                Ok(Ok(())) => inner.subs.push(tx),
                Ok(Err(_)) => {}
                Err(_) => {
                    // Slow, not gone: it still counts as attached.
                    tracing::warn!(
                        id = self.id,
                        "attach subscriber stalled; it catches up later"
                    );
                    inner.behind.push(tx);
                }
            }
        }
        for tx in mem::take(&mut inner.rejoined) {
            match tx.try_send(Ok(ev.clone())) {
                Ok(()) => inner.rejoined.push(tx),
                Err(TrySendError::Full(_)) => inner.behind.push(tx),
                Err(TrySendError::Closed(_)) => {}
            }
        }
        if inner.overflowed {
            return;
        }
        if inner.bytes + sz > MAX_REPLAY_BYTES {
            tracing::warn!(
                id = self.id,
                "replay buffer cap reached; late subscribers miss the log so far"
            );
            inner.overflowed = true;
            inner.events.clear();
            inner.bytes = 0;
            return;
        }
        inner.bytes += sz;
        inner.events.push(ev);
    }

    pub(in crate::hub) async fn subscribe(&self) -> mpsc::Receiver<Result<AttachEvent, Status>> {
        self.hold.lock().unwrap().clientless = None;
        let mut inner = self.inner.lock().await;
        if inner.overflowed {
            // The backlog is gone. Follow live from here; joining in
            // the middle of an output, wait behind like a stalled one.
            let (tx, rx) = mpsc::channel(SUB_CHANNEL_SLACK);
            if let Some(ended) = inner.ended {
                let (outputs, ending) = (inner.outputs.clone(), inner.ending.clone());
                tokio::spawn(catch_up(tx, self.id.clone(), outputs, ending, ended));
            } else if inner.streaming {
                inner.behind.push(tx);
            } else {
                let _ = tx.try_send(Ok(event(attach_event::Event::Log(TRUNCATED.into()))));
                inner.subs.push(tx);
            }
            return rx;
        }
        // Enough capacity for the whole backlog plus live slack (and
        // one error slot), so the snapshot below cannot drop events.
        let (tx, rx) = mpsc::channel(inner.events.len() + SUB_CHANNEL_SLACK);
        for ev in &inner.events {
            let _ = tx.try_send(Ok(ev.clone()));
        }
        if inner.ended.is_some() {
            // Finished without a verdict in the backlog (e.g. the job
            // was dropped as abandoned between the dedupe lookup and
            // this subscribe): an error beats a silently empty stream.
            let concluded = inner.events.iter().any(|e| {
                matches!(
                    e.event,
                    Some(attach_event::Event::ExitCode(_) | attach_event::Event::Error(_))
                )
            });
            if !concluded {
                let _ = tx.try_send(Err(Status::unavailable(
                    "build is no longer in flight; resubmit",
                )));
            }
        } else {
            inner.subs.push(tx);
        }
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

    /// A requeue restarted the latest delivery's outputs, and the next
    /// attempt delivers its own: leave their chunks to the cache.
    /// `missed` needs only their paths.
    pub(in crate::hub) async fn unpin_outputs(&self) {
        if let Some(kept) = &self.inner.lock().await.outputs {
            kept.unpin();
        }
    }

    /// Close all subscriber streams; those behind catch up first.
    pub(in crate::hub) async fn finish(&self) {
        let mut inner = self.inner.lock().await;
        let ended = Instant::now();
        inner.ended = Some(ended);
        inner.subs.clear();
        inner.rejoined.clear();
        for tx in mem::take(&mut inner.behind) {
            if !tx.is_closed() {
                let (outputs, ending) = (inner.outputs.clone(), inner.ending.clone());
                tokio::spawn(catch_up(tx, self.id.clone(), outputs, ending, ended));
            }
        }
        if let Some(kept) = &inner.outputs {
            tokio::spawn(unpin_after(Arc::downgrade(kept)));
        }
    }

    /// Any attach client still listening? Subscribers whose stream was
    /// dropped count as gone even before publish() prunes them.
    pub(in crate::hub) async fn has_subscribers(&self) -> bool {
        let inner = self.inner.lock().await;
        inner
            .subs
            .iter()
            .chain(&inner.rejoined)
            .chain(&inner.behind)
            .any(|tx| !tx.is_closed())
    }
}

#[cfg(test)]
#[path = "replay/tests.rs"]
mod tests;
