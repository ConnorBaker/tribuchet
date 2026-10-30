//! Replay subscribers that keep up, stall, rejoin or catch up.

use std::sync::Arc;

use super::*;
use crate::chunkstore::io_hook;
use crate::hub::state::HubState;
use crate::hub::testlog::Captured;
use crate::hub::tests::{cached_output, job};
use Step::{
    Attach, Attached, Big, Corrupt, Ended, Eof, Exit0, Exit1, Failed, Fill, Finish, Keep, Live,
    Log, Out, Passed, Pinned, Read1, ReadAll, Requeue, Resubmit, Tick, Unpinned, Wait,
};

impl Replay {
    /// Whether the latest delivery's outputs are pinned in the cache.
    pub(in crate::hub) async fn outputs_pinned(&self) -> bool {
        let inner = self.inner.lock().await;
        let kept = inner.outputs.as_ref();
        kept.is_some_and(|k| k.pin.lock().unwrap().is_some())
    }
}

#[tokio::test(start_paused = true)]
async fn slow_subscriber_paces_publish() {
    let replay = Arc::new(Replay::default());
    let mut rx = replay.subscribe().await;
    let n = SUB_CHANNEL_SLACK + 10;
    let publisher = {
        let replay = replay.clone();
        tokio::spawn(async move {
            for _ in 0..n {
                replay
                    .publish(attach_event::Event::Log(b"x".to_vec()))
                    .await;
            }
        })
    };
    for _ in 0..n {
        tokio::time::sleep(Duration::from_millis(10)).await;
        rx.recv().await.unwrap().unwrap();
    }
    publisher.await.unwrap();
    assert!(replay.has_subscribers().await);
}

const OUT: &str = "/nix/store/00000000000000000000000000000000-out";

/// A step of a build's delivery or of its one client, or, last in a
/// row, what the client's stream comes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// The delivery keeps OUT, four cached chunks; one chunk whose cached
    /// frame does not match the recipe.
    Keep,
    Corrupt,
    /// OUT's next chunk goes out; OUT's end.
    Out,
    Eof,
    /// A log line goes out; one past the replay's cap; as many as the
    /// client has room for, so that the next event stalls it.
    Log,
    Big,
    Fill,
    /// A requeue (HubState::requeue): OUT restarts.
    Requeue,
    /// The verdict goes out; the build ends.
    Exit0,
    Exit1,
    Finish,
    /// The client attaches; reads one event, or all it has; still counts
    /// as attached.
    Attach,
    Read1,
    ReadAll,
    Attached,
    /// CATCH_UP_PIN less a second passes; two seconds do.
    Wait,
    Tick,
    /// OUT's chunks are pinned in the hub's cache, or not.
    Pinned,
    Unpinned,
    /// Not ended, it follows live (the last event that went out reached
    /// it); the verdict, 0 or 1, every output whole and none unfinished;
    /// told to resubmit; ended without either.
    Live,
    Passed,
    Failed,
    Resubmit,
    Ended,
}

/// What a client of a build delivered as `steps` gets: an error unless
/// each of OUT's chunks it is sent continues OUT from its start (as of
/// OUT's last restart), and any log line it missed is told first by the
/// note, which is followed by OUT's restart once OUT is kept.
async fn delivered(steps: &[Step]) -> Result<Step, String> {
    let state = Arc::new(HubState::for_test(Duration::ZERO));
    let replay = Arc::new(Replay::new("b1".into()));
    let (mut client, mut seen): (Option<mpsc::Receiver<_>>, _) = (None, Vec::new());
    let (mut frames, mut logs, mut last) = (Vec::new(), Vec::new(), None);
    let (mut sent, mut kept) = (0, false);
    for &step in steps {
        let ev = match step {
            Keep | Corrupt => {
                let recipe = if step == Keep {
                    let recipe;
                    (recipe, frames) = cached_output(&state.chunks, 4);
                    recipe
                } else {
                    let raw = [7u8; 4096];
                    let hash = *blake3::hash(&raw).as_bytes();
                    let wrong = zstd::bulk::compress(&[8u8; 4096], 3).unwrap();
                    io_hook::off_runtime(|| state.chunks.admit(hash, &wrong).unwrap());
                    vec![(hash, raw.len())]
                };
                let pin = state.chunks.pin(recipe.iter().map(|(h, _)| *h));
                let outputs = vec![(OUT.to_owned(), Arc::new(recipe))];
                let outputs = KeptOutputs::new(state.chunks.clone(), outputs, pin);
                replay.keep_outputs(outputs).await;
                kept = true;
                None
            }
            Out => {
                sent += 1;
                Some(nar(frames[sent - 1].clone(), false))
            }
            Eof => Some(nar(Vec::new(), true)),
            Log => Some(line(&mut logs)),
            Big => {
                logs.push(Vec::new());
                let big = vec![b'x'; MAX_REPLAY_BYTES];
                replay.publish(attach_event::Event::Log(big)).await;
                None
            }
            Fill => {
                while client.as_ref().unwrap().capacity() > 0 {
                    replay.publish(line(&mut logs)).await;
                }
                last = logs.last().cloned().map(attach_event::Event::Log);
                None
            }
            Requeue => {
                let mut j = job(1, replay.clone());
                j.req.outputs = [("out".to_owned(), OUT.to_owned())].into();
                state.requeue(j).await;
                last = None;
                None
            }
            Exit0 | Exit1 => Some(attach_event::Event::ExitCode(i32::from(step == Exit1))),
            Finish => {
                replay.finish().await;
                None
            }
            Attach => {
                client = Some(replay.subscribe().await);
                None
            }
            Read1 | ReadAll => {
                let rx = client.as_mut().unwrap();
                let n = if step == Read1 { 1 } else { usize::MAX };
                seen.extend(iter::from_fn(|| rx.try_recv().ok()).take(n).map(as_read));
                None
            }
            Attached if !replay.has_subscribers().await => {
                return Err(format!("counted as gone after {} events", seen.len()));
            }
            Wait | Tick => {
                let secs = if step == Wait {
                    CATCH_UP_PIN.as_secs() - 1
                } else {
                    2
                };
                tokio::time::sleep(Duration::from_secs(secs)).await;
                None
            }
            Pinned | Unpinned if replay.outputs_pinned().await != (step == Pinned) => {
                return Err(format!("not {step:?}"));
            }
            Attached | Pinned | Unpinned => None,
            Live | Passed | Failed | Resubmit | Ended => unreachable!("{step:?} is no step"),
        };
        if let Some(ev) = ev {
            replay.publish(ev.clone()).await;
            last = Some(ev);
        }
    }
    let mut rx = client.expect("no client attached");
    if steps.contains(&Finish) {
        while let Some(ev) = rx.recv().await {
            seen.push(as_read(ev));
        }
        return judged(&seen, &frames, &logs, kept);
    }
    seen.extend(iter::from_fn(|| rx.try_recv().ok()).map(as_read));
    judged(&seen, &frames, &logs, kept)?;
    // Not ended, it follows live: not behind, and the last event that
    // went out reached it.
    let behind = !replay.inner.lock().await.behind.is_empty();
    if behind || last.is_some_and(|ev| seen.last() != Some(&Ok(ev))) {
        return Err(format!("behind after {} events", seen.len()));
    }
    Ok(Live)
}

/// The next log line, each its own, recorded in `logs`.
fn line(logs: &mut Vec<Vec<u8>>) -> attach_event::Event {
    logs.push(format!("{}\n", logs.len()).into_bytes());
    attach_event::Event::Log(logs[logs.len() - 1].clone())
}

fn nar(zstd_nar_chunk: Vec<u8>, eof: bool) -> attach_event::Event {
    attach_event::Event::Output(OutputNar {
        store_path: OUT.into(),
        zstd_nar_chunk,
        eof,
    })
}

fn as_read(ev: Result<AttachEvent, Status>) -> Result<attach_event::Event, String> {
    ev.map(|e| e.event.unwrap()).map_err(|s| s.to_string())
}

/// `seen` as attach reads it, against OUT's `frames` and the `logs` that
/// went out.
fn judged(
    seen: &[Result<attach_event::Event, String>],
    frames: &[Vec<u8>],
    logs: &[Vec<u8>],
    kept: bool,
) -> Result<Step, String> {
    let restart = Ok(attach_event::Event::OutputRestart(OUT.into()));
    let (mut out, mut open, mut told, mut next) = (Vec::new(), false, false, 0);
    for (i, ev) in seen.iter().enumerate() {
        match ev {
            Err(_) => return Ok(Resubmit),
            Ok(attach_event::Event::Log(l)) if *l == TRUNCATED.as_bytes() => {
                if kept && seen.get(i + 1) != Some(&restart) {
                    return Err(format!("the note at {i} without OUT's restart"));
                }
                told = true;
            }
            Ok(attach_event::Event::Log(l)) => {
                let Some(k) = logs.iter().position(|p| p == l) else {
                    continue;
                };
                if k < next || (k > next && !told) {
                    return Err(format!("log line {k} after {next}, told: {told}"));
                }
                (next, told) = (k + 1, false);
            }
            Ok(attach_event::Event::OutputRestart(_)) => (out, open) = (Vec::new(), false),
            Ok(attach_event::Event::Output(o)) if o.eof => open = false,
            Ok(attach_event::Event::Output(o)) => {
                out.push(o.zstd_nar_chunk.clone());
                open = true;
                if !frames.starts_with(&out) {
                    return Err(format!("OUT not continued from its start at {i}"));
                }
            }
            Ok(attach_event::Event::ExitCode(code)) => {
                if next < logs.len() && !told {
                    return Err(format!("log lines from {next} missed, untold"));
                }
                if open || (*code == 0 && out != frames) {
                    return Err(format!("OUT unfinished at the verdict {code}"));
                }
                return Ok(if *code == 0 { Passed } else { Failed });
            }
            Ok(_) => {}
        }
    }
    Ok(Ended)
}

/// A client that is slow, stalls or attaches late still gets every
/// output whole, or is told to resubmit when the cache lost a chunk;
/// only log lines may be lost, and it is told so first.
#[tokio::test(start_paused = true)]
async fn every_client_gets_each_output_whole_or_is_told_to_resubmit() {
    let rows: &[&[Step]] = &[
        // Stalled a minute over log lines, it rejoins once it has room.
        // Back, it counts as attached: counted as gone, it would have its
        // build cancelled the cancel grace later while it reads.
        &[Attach, Fill, Log, Attached, ReadAll, Log, Attached, Live],
        // Stalled mid-output, it stays behind while OUT goes out, and
        // rejoins once a requeue restarts it.
        &[Keep, Attach, Out, Fill, Out, ReadAll, Out, Requeue, Live],
        // It rejoins only with room for the note and the restart: with
        // less, the restart is dropped, and the next attempt's chunks
        // would be appended to its partial unpack of OUT.
        &[
            Keep, Attach, Out, Fill, Out, Requeue, Read1, Log, ReadAll, Log, Finish, Ended,
        ],
        // Back from a stall, with no room for OUT's first chunk: it goes
        // behind again. Kept on and sent the rest, it would take OUT with
        // a chunk missing for whole.
        &[
            Keep, Attach, Fill, Log, ReadAll, Log, Fill, Out, ReadAll, Out, Out, Out, Eof, Exit0,
            Finish, Passed,
        ],
        // A cached frame that does not match the recipe is never sent.
        &[Corrupt, Attach, Fill, Log, Exit0, Finish, Resubmit],
        // Back long after its build ended: OUT is left to the cache after
        // CATCH_UP_PIN, and sent while the cache still holds it.
        &[
            Keep, Attach, Fill, Log, Exit0, Finish, Wait, Pinned, Tick, Unpinned, Wait, Passed,
        ],
        // A resubmission attaching after the log passed the cap, in the
        // middle of OUT: joined live, it would get OUT from the middle; a
        // hub that refuses it says "exceeded the replay buffer".
        &[
            Keep, Big, Out, Attach, Out, Out, Out, Eof, Exit0, Finish, Passed,
        ],
        // The same, with no output going out, follows live. A hub that
        // refuses it cancels, with no client left, the build of a chain of
        // bounded nix-build clients.
        &[Big, Attach, Log, Attached, Live],
        // Behind at a requeue, it is sent OUT's restart before the next
        // attempt's verdict: a requeue leaves only OUT's chunks to the
        // cache, not the path the restart is sent from.
        &[Keep, Attach, Out, Fill, Out, Requeue, Exit1, Finish, Failed],
    ];
    for (n, row) in rows.iter().enumerate() {
        let (got, steps) = row.split_last().unwrap();
        assert_eq!(delivered(steps).await, Ok(*got), "row {n}");
    }
}

/// A client that stalls, reads a little and stalls again holds publish,
/// and with it the build's relay, for SUB_STALL the first time only,
/// and is not dropped after the first stall.
#[tokio::test(start_paused = true)]
async fn a_restalling_subscriber_holds_publish_only_once() {
    let replay = Replay::default();
    let mut rx = replay.subscribe().await;
    for _ in 0..=SUB_CHANNEL_SLACK {
        replay
            .publish(attach_event::Event::Log(b"x".to_vec()))
            .await;
    }
    for _ in 0..10 {
        rx.try_recv().unwrap().unwrap();
    }
    let t = tokio::time::Instant::now();
    for _ in 0..20 {
        replay
            .publish(attach_event::Event::Log(b"y".to_vec()))
            .await;
    }
    let held = t.elapsed();
    assert!(
        held < SUB_STALL,
        "publish held {held:?} by a client that stalled before"
    );
}

/// A client that attaches after the cap, 5 minutes after its build
/// ended, is caught up; the line names the build and counts from its end.
#[tokio::test(start_paused = true)]
async fn a_late_clients_catch_up_counts_from_the_build_end() {
    let log = Captured::default();
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap());
    let pin = cache.pin(iter::empty());
    let replay = Replay::new("b1".into());
    replay
        .publish(attach_event::Event::Log(vec![0; MAX_REPLAY_BYTES]))
        .await;
    let kept = vec![(OUT.to_owned(), Arc::new(Vec::new()))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    replay.publish(attach_event::Event::ExitCode(0)).await;
    replay.finish().await;
    tokio::time::sleep(Duration::from_mins(5)).await;
    let mut rx = replay.subscribe().await;
    while rx.recv().await.is_some() {}
    let line = log.line("caught up");
    assert!(line.contains(r#"id="b1""#), "{line}");
    assert!(line.contains("after_end_ms=300000"), "{line}");
}

/// With no client catching up, a finished build's outputs go with its
/// replay, not CATCH_UP_PIN later.
#[tokio::test(start_paused = true)]
async fn outputs_nobody_catches_up_on_go_with_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap());
    let pin = cache.pin(iter::empty());
    let replay = Replay::default();
    let kept = vec![(OUT.to_owned(), Arc::new(Vec::new()))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    let _rx = replay.subscribe().await;
    replay.publish(attach_event::Event::ExitCode(0)).await;
    let kept = Arc::downgrade(replay.inner.lock().await.outputs.as_ref().unwrap());
    replay.finish().await;
    drop(replay);
    tokio::task::yield_now().await;
    assert!(kept.upgrade().is_none(), "held with no client catching up");
}

impl Replay {
    /// The build's id, for the warnings.
    pub(in crate::hub) fn id(&self) -> &str {
        &self.id
    }
}

/// The replay's warnings name the build: its buffer reaching the cap,
/// a client's stall, and the client's catch-up running out of chunks.
#[tokio::test(start_paused = true)]
async fn the_replays_warnings_name_the_build() {
    let log = Captured::default();
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap());
    let pin = cache.pin(iter::empty());
    let replay = Replay::new("b1".into());
    // Past the cap with no subscriber yet: nothing copies it.
    replay
        .publish(attach_event::Event::Log(vec![0; MAX_REPLAY_BYTES]))
        .await;
    // A chunk the cache never held.
    let kept = vec![(OUT.to_owned(), Arc::new(vec![([1; 32], 1)]))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    let mut rx = replay.subscribe().await;
    for _ in 0..=SUB_CHANNEL_SLACK {
        replay
            .publish(attach_event::Event::Log(b"x".to_vec()))
            .await;
    }
    replay.publish(attach_event::Event::ExitCode(0)).await;
    replay.finish().await;
    while rx.recv().await.is_some() {}
    for warning in ["cap reached", "stalled", "left the hub's cache"] {
        let line = log.line(warning);
        assert!(line.contains(r#"id="b1""#), "{line}");
    }
}
