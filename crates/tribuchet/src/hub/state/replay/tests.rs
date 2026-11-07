//! Clients that read at once, slowly, late or not at all.

use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use super::*;
use crate::chunkstore::io_hook;
use crate::hub::model;
use crate::hub::state::HubState;
use crate::hub::tests::job;
use Step::{
    Attach, Corrupt, Exit0, Exit1, Fill, Finish, Keep, Log, Out, Read1, ReadAll, Requeue, Tick,
    Wait,
};

mod laws;
mod schedules;

impl Replay {
    /// Whether the latest delivery's outputs are pinned in the cache.
    pub(in crate::hub) async fn outputs_pinned(&self) -> bool {
        let log = self.inner.lock().await;
        let kept = log.outputs.as_ref();
        kept.is_some_and(|k| k.pin.lock().unwrap().is_some())
    }

    /// The build's id, for the warnings.
    pub(in crate::hub) fn id(&self) -> &str {
        &self.id
    }
}

/// A client that does not read holds up neither publish nor a client that
/// reads: both are done at once, however full the first one's channel.
#[tokio::test(start_paused = true)]
async fn a_stalled_client_holds_up_neither_publish_nor_another_client() {
    let replay = Arc::new(Replay::default());
    let _stalled = replay.subscribe().await;
    let mut reading = replay.subscribe().await;
    let n = SUB_CHANNEL_SLACK + 2;
    let start = Instant::now();
    let publisher = {
        let replay = replay.clone();
        tokio::spawn(async move {
            for k in 0..n {
                let line = format!("{k}\n").into_bytes();
                replay.publish(attach_event::Event::Log(line)).await;
            }
            start.elapsed()
        })
    };
    for k in 0..n {
        let line = format!("{k}\n").into_bytes();
        let read = tokio::time::timeout(Duration::from_secs(3600), reading.recv());
        let ev = read
            .await
            .expect("the client that reads got nothing")
            .unwrap()
            .unwrap();
        assert_eq!(ev.event, Some(attach_event::Event::Log(line)));
    }
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "the client that reads waited on the one that does not"
    );
    let held = publisher.await.unwrap();
    assert_eq!(
        held,
        Duration::ZERO,
        "publish waited on a client that does not read"
    );
}

const OUT: &str = "/nix/store/00000000000000000000000000000000-out";
const OUT2: &str = "/nix/store/11111111111111111111111111111111-out2";

/// The replay's cap in these tests: about one and a half output chunks.
const CAP: usize = 24 * 1024;

/// A step of a build's delivery or of one of its clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// The delivery keeps OUT (three chunks) and OUT2 (two), its bytes its
    /// own; or with OUT's first chunk held wrong in the cache.
    Keep,
    Corrupt,
    /// The delivery's next event: a chunk, or an output's end.
    Out,
    /// A log line goes out; one past the replay's cap; more than a client
    /// has room for, so that one that does not read stalls.
    Log,
    Big,
    Fill,
    /// A requeue (HubState::requeue): the outputs restart.
    Requeue,
    /// The verdict goes out; the build ends.
    Exit0,
    Exit1,
    Finish,
    /// A client attaches; reads one event; reads until it has all there
    /// is; goes away.
    Attach,
    Read1,
    ReadAll,
    Drop,
    /// CATCH_UP_PIN less a second passes; two seconds do.
    Wait,
    Tick,
}

/// What a client's stream comes to: the verdict, 0 or 1, every output
/// whole and none unfinished; its end without one, which attach
/// resubmits; that end where OUT's first chunk was next, which a corrupt
/// delivery may cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Got {
    Passed,
    Failed,
    Resubmit,
    Lost,
}

type Seen = Vec<Result<attach_event::Event, String>>;
type Rx = mpsc::Receiver<Result<AttachEvent, Status>>;
/// Each output's frames, as one delivery sends them.
type Frames = HashMap<String, Vec<Vec<u8>>>;

/// A build delivered step by step to its clients.
struct Run {
    state: Arc<HubState>,
    replay: Arc<Replay>,
    /// Each client's stream, none once it went away, and what it read.
    clients: Vec<(Option<Rx>, Seen)>,
    /// Every delivery's frames, the latest last, and its events in order.
    frames: Vec<Frames>,
    plan: Vec<attach_event::Event>,
    logs: Vec<Vec<u8>>,
    sent: usize,
    /// A delivery was corrupt; a client that ended on it stays ended, past
    /// any clean delivery after it.
    corrupt: bool,
    finished: bool,
    /// The hub dropped the build at a requeue (no client's stream was
    /// open), when it had `at_drop` clients.
    dropped: bool,
    at_drop: usize,
    /// The last event that went out that a client can be sent.
    last: Option<attach_event::Event>,
}

impl Run {
    fn new() -> Self {
        let replay = Replay::new("b1".into());
        replay.inner.try_lock().unwrap().cap = Some(CAP);
        Self {
            state: Arc::new(HubState::for_test(Duration::ZERO)),
            replay: Arc::new(replay),
            clients: Vec::new(),
            frames: Vec::new(),
            plan: Vec::new(),
            logs: Vec::new(),
            sent: 0,
            corrupt: false,
            finished: false,
            dropped: false,
            at_drop: 0,
            last: None,
        }
    }

    /// Publish `ev`, done at its first poll whatever the clients do.
    fn publish(&mut self, ev: attach_event::Event) -> Result<(), String> {
        model::at_once(self.replay.publish(ev.clone()))
            .ok_or(format!("publish waited for a client: {ev:?}"))?;
        self.last = Some(ev);
        Ok(())
    }

    /// Take `step`, client `k`'s if it is a client's, then let every
    /// client's read go as far as it can.
    async fn step(&mut self, step: Step, k: usize) -> Result<(), String> {
        match step {
            Keep | Corrupt => self.keep(step == Corrupt).await,
            Out => {
                self.sent += 1;
                self.publish(self.plan[self.sent - 1].clone())?;
            }
            Log => {
                let ev = line(&mut self.logs);
                self.publish(ev)?;
            }
            Step::Big => {
                // Its own line, past the cap: no client need get it, nor
                // follows live up to it.
                let mut big = vec![b'x'; CAP + 1];
                big[..8].copy_from_slice(&self.logs.len().to_le_bytes());
                self.logs.push(big.clone());
                self.publish(attach_event::Event::Log(big))?;
                self.last = None;
            }
            Fill => {
                for _ in 0..=SUB_CHANNEL_SLACK {
                    let ev = line(&mut self.logs);
                    self.publish(ev)?;
                }
            }
            Requeue => {
                let mut j = job(1, self.replay.clone());
                j.req.outputs = [("out", OUT), ("out2", OUT2)]
                    .map(|(n, p)| (n.to_owned(), p.to_owned()))
                    .into();
                // Held for want of a worker, its clients are told so; with
                // none, it is dropped: a client's stream may have ended
                // without a Drop, on a chunk wrong in the cache.
                let (open, _) = self.open().await;
                self.state.requeue(j).await;
                (self.sent, self.last) = (0, None);
                if !self.dropped {
                    self.dropped = self.replay.inner.lock().await.ended.is_some();
                    self.at_drop = self.clients.len();
                    if self.dropped != (open == 0) {
                        return Err(format!("dropped: {}, {open} streams open", self.dropped));
                    }
                }
            }
            Exit0 | Exit1 => {
                self.publish(attach_event::Event::ExitCode(i32::from(step == Exit1)))?
            }
            Finish => {
                self.replay.finish().await;
                self.finished = true;
            }
            Attach => {
                let rx = self.replay.subscribe().await;
                self.clients.push((Some(rx), Vec::new()));
            }
            Read1 => {
                let (rx, seen) = &mut self.clients[k];
                seen.extend(rx.as_mut().unwrap().try_recv().ok().map(as_read));
            }
            ReadAll => self.catch_up(k).await?,
            Step::Drop => self.clients[k].0 = None,
            Wait | Tick => {
                let secs = if step == Wait {
                    CATCH_UP_PIN.as_secs() - 1
                } else {
                    2
                };
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
        model::settle().await;
        Ok(())
    }

    /// Keep a delivery of OUT and OUT2 whose bytes are its own, OUT's
    /// first chunk held wrong in the cache if `corrupt`.
    async fn keep(&mut self, corrupt: bool) {
        let cache = &self.state.chunks;
        let d = u8::try_from(self.frames.len()).unwrap();
        let (mut out, mut frames) = chunks(cache, [d, 0], 3);
        if corrupt {
            let raw = [d; 4096];
            let hash = *blake3::hash(&raw).as_bytes();
            let wrong = zstd::bulk::compress(&[!d; 4096], 3).unwrap();
            io_hook::off_runtime(|| cache.admit(hash, &wrong).unwrap());
            (out[0], frames[0]) = ((hash, raw.len()), zstd::bulk::compress(&raw, 3).unwrap());
        }
        let (out2, frames2) = chunks(cache, [d, 1], 2);
        let pin = cache.pin(out.iter().chain(&out2).map(|(h, _)| *h));
        let kept = vec![
            (OUT.to_owned(), Arc::new(out)),
            (OUT2.to_owned(), Arc::new(out2)),
        ];
        self.replay
            .keep_outputs(KeptOutputs::new(cache.clone(), kept, pin))
            .await;
        self.plan = [(OUT, &frames), (OUT2, &frames2)]
            .iter()
            .flat_map(|(path, f)| {
                let chunks = f.iter().map(|frame| nar(path, frame.clone(), false));
                chunks.chain([nar(path, Vec::new(), true)])
            })
            .collect();
        let frames = [(OUT, frames), (OUT2, frames2)];
        self.frames
            .push(frames.map(|(p, f)| (p.to_owned(), f)).into());
        (self.sent, self.corrupt) = (0, self.corrupt || corrupt);
    }

    /// Client `k` reads until there is nothing more for it; then, the build
    /// and its stream still on, it follows live: the last event that went
    /// out reached it.
    async fn catch_up(&mut self, k: usize) -> Result<(), String> {
        let (rx, seen) = &mut self.clients[k];
        let rx = rx.as_mut().unwrap();
        for _ in 0..1_000 {
            let read = seen.len();
            seen.extend(iter::from_fn(|| rx.try_recv().ok()).map(as_read));
            if seen.len() == read {
                // Past it, only the note of a line dropped since may follow.
                let note = Ok(attach_event::Event::Log(TRUNCATED.into()));
                let latest = seen.iter().rev().find(|ev| **ev != note);
                return match &self.last {
                    Some(ev)
                        if !self.finished && !rx.is_closed() && latest != Some(&Ok(ev.clone())) =>
                    {
                        Err(format!("client {k} behind after {} events", seen.len()))
                    }
                    _ => Ok(()),
                };
            }
            model::settle().await;
        }
        Err(format!("client {k} never caught up"))
    }

    /// What client `k` comes to once its stream ends; an error if it does
    /// not end within the hour.
    async fn verdict(&mut self, k: usize) -> Result<Got, String> {
        let (rx, seen) = &mut self.clients[k];
        seen.extend(drain(rx.as_mut().unwrap()).await?);
        judged(seen, &self.frames, &self.logs, self.corrupt)
    }

    /// Clients whose stream is open, and the hub's readers.
    async fn open(&self) -> (usize, usize) {
        let open = self
            .clients
            .iter()
            .flat_map(|(rx, _)| rx)
            .filter(|rx| !rx.is_closed());
        (open.count(), self.replay.inner.lock().await.subs.len())
    }
}

/// `n` chunks of 16 KiB from `seed`, in `cache`: the recipe and the frames.
fn chunks(cache: &ChunkCache, seed: [u8; 2], n: u8) -> (Recipe, Vec<Vec<u8>>) {
    io_hook::off_runtime(|| {
        (0..n)
            .map(|i| {
                let mut raw = vec![0u8; 16 * 1024];
                let mut h = blake3::Hasher::new();
                h.update(&seed).update(&[i]).finalize_xof().fill(&mut raw);
                let hash = *blake3::hash(&raw).as_bytes();
                let frame = zstd::bulk::compress(&raw, 3).unwrap();
                cache.admit(hash, &frame).unwrap();
                ((hash, raw.len()), frame)
            })
            .unzip()
    })
}

/// What `rx` gets until its stream ends, which must be within the hour.
async fn drain(rx: &mut Rx) -> Result<Seen, String> {
    let mut seen = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(3600), rx.recv()).await {
            Ok(Some(ev)) => seen.push(as_read(ev)),
            Ok(None) => return Ok(seen),
            Err(_) => return Err(format!("stream still open after {} events", seen.len())),
        }
    }
}

/// The next log line, each its own, recorded in `logs`.
fn line(logs: &mut Vec<Vec<u8>>) -> attach_event::Event {
    logs.push(format!("{}\n", logs.len()).into_bytes());
    attach_event::Event::Log(logs[logs.len() - 1].clone())
}

fn nar(path: &str, zstd_nar_chunk: Vec<u8>, eof: bool) -> attach_event::Event {
    attach_event::Event::Output(OutputNar {
        store_path: path.into(),
        zstd_nar_chunk,
        eof,
    })
}

fn as_read(ev: Result<AttachEvent, Status>) -> Result<attach_event::Event, String> {
    ev.map(|e| e.event.unwrap()).map_err(|s| s.to_string())
}

/// `seen` as attach reads it, against each delivery's `frames` and the
/// `logs` that went out: an error unless each output's chunks continue one
/// delivery's frames of it from their start (as of the output's last
/// restart), exit 0 comes with the latest delivery's whole, and any log
/// line missed is told first by the note.
fn judged(seen: &Seen, frames: &[Frames], logs: &[Vec<u8>], corrupt: bool) -> Result<Got, String> {
    // Each output's chunks since its last restart, and whether it is open.
    let mut out = HashMap::<&str, (Vec<Vec<u8>>, bool)>::new();
    let (mut told, mut next) = (false, 0);
    for (i, ev) in seen.iter().enumerate() {
        match ev {
            Err(_) => return Ok(Got::Resubmit),
            Ok(attach_event::Event::Log(l)) if *l == TRUNCATED.as_bytes() => told = true,
            Ok(attach_event::Event::Log(l)) => {
                let Some(k) = logs.iter().position(|p| p == l) else {
                    continue;
                };
                if k < next || (k > next && !told) {
                    return Err(format!("log line {k} after {next}, told: {told}"));
                }
                (next, told) = (k + 1, false);
            }
            Ok(attach_event::Event::OutputRestart(p)) => _ = out.remove(p.as_str()),
            Ok(attach_event::Event::Output(o)) => {
                let (got, open) = out.entry(&o.store_path).or_default();
                *open = !o.eof;
                if o.eof {
                    continue;
                }
                got.push(o.zstd_nar_chunk.clone());
                if !frames.iter().any(|f| f[&o.store_path].starts_with(got)) {
                    return Err(format!(
                        "{} not continued from its start at {i}",
                        o.store_path
                    ));
                }
            }
            Ok(attach_event::Event::ExitCode(code)) => {
                if next < logs.len() && !told {
                    return Err(format!("log lines from {next} missed, untold"));
                }
                let whole = |f: &Frames| {
                    f.iter()
                        .all(|(p, f)| out.get(p.as_str()) == Some(&(f.clone(), false)))
                };
                if out.values().any(|o| o.1) || (*code == 0 && !frames.last().is_some_and(whole)) {
                    return Err(format!("outputs unfinished at the verdict {code}"));
                }
                return Ok(if *code == 0 { Got::Passed } else { Got::Failed });
            }
            Ok(_) => {}
        }
    }
    let first_next = out.get(OUT).is_none_or(|o| o.0.is_empty());
    Ok(if corrupt && first_next {
        Got::Lost
    } else {
        Got::Resubmit
    })
}
