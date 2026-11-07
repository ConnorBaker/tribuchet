//! The replay's laws, a test each.

use futures_util::FutureExt;

use super::*;
use crate::hub::testlog::Captured;

/// A client that attaches 5 minutes after its build ended is caught up;
/// the line names the build and counts from its end.
#[tokio::test(start_paused = true)]
async fn a_late_clients_catch_up_counts_from_the_build_end() {
    let log = Captured::default();
    let _guard = log.install();
    let replay = Arc::new(Replay::new("b1".into()));
    replay.publish(attach_event::Event::ExitCode(0)).await;
    replay.finish().await;
    tokio::time::sleep(Duration::from_mins(5)).await;
    let mut rx = replay.subscribe().await;
    drain(&mut rx).await.unwrap();
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
    let replay = Arc::new(Replay::default());
    let kept = vec![(OUT.to_owned(), Arc::new(Vec::new()))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    let _rx = replay.subscribe().await;
    replay.publish(attach_event::Event::ExitCode(0)).await;
    let kept = Arc::downgrade(replay.inner.lock().await.outputs.as_ref().unwrap());
    replay.finish().await;
    drop(replay);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(kept.upgrade().is_none(), "held with no client catching up");
}

/// The replay's warnings name the build: its buffer reaching the cap,
/// and a client's catch-up running out of chunks.
#[tokio::test(start_paused = true)]
async fn the_replays_warnings_name_the_build() {
    let log = Captured::default();
    let _guard = log.install();
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap());
    let pin = cache.pin(iter::empty());
    let replay = Arc::new(Replay::new("b1".into()));
    // Room for the verdict and the two chunks' records, not their bytes.
    replay.inner.try_lock().unwrap().cap = Some(3 * HELD + 2 * OUT.len());
    // A chunk the cache never held.
    let kept = vec![(OUT.to_owned(), Arc::new(vec![([1; 32], 1)]))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    replay.publish(nar(OUT, vec![0; 8], false)).await;
    replay.publish(nar(OUT, vec![1; 8], false)).await;
    // Past the cap: the chunks' bytes and the line go, at one publish.
    replay
        .publish(attach_event::Event::Log(b"x".to_vec()))
        .await;
    replay.publish(attach_event::Event::ExitCode(0)).await;
    replay.finish().await;
    let mut rx = replay.subscribe().await;
    drain(&mut rx).await.unwrap();
    for warning in ["cap reached", "left the hub's cache"] {
        let line = log.line(warning);
        assert!(line.contains(r#"id="b1""#), "{line}");
        assert_eq!(log.count(warning), 1, "{warning}");
    }
}

/// Each client's stream ends at its verdict, or without one once the build
/// ended without one, and the build then has no client: also when
/// `subscribe` yields at its lock (tokio's cooperative budget spent), so
/// that the reader may run to its end first.
#[tokio::test(start_paused = true)]
async fn a_clients_stream_ends_however_subscribe_is_polled() {
    for (verdict, spent) in [(false, false), (false, true), (true, false), (true, true)] {
        let replay = Arc::new(Replay::new("b1".into()));
        if verdict {
            replay.publish(attach_event::Event::ExitCode(0)).await;
        } else {
            replay.finish().await;
        }
        if spent {
            let spend = tokio::sync::Mutex::new(());
            while spend.lock().now_or_never().is_some() {}
        }
        let mut rx = replay.subscribe().await;
        let got = drain(&mut rx).await.map(|seen| seen.len());
        let case = format!("verdict {verdict}, budget spent {spent}");
        assert_eq!(got, Ok(usize::from(verdict)), "{case}");
        assert!(!replay.has_subscribers().await, "{case}");
    }
}

/// The cap bounds what the log holds, however small its events, what it
/// keeps of chunks that went included (dropped only once no event is left);
/// a restart drops what it kept of the chunks it supersedes.
#[tokio::test(start_paused = true)]
async fn the_cap_bounds_what_the_log_holds() {
    let replay = Replay::new("b1".into());
    replay.inner.try_lock().unwrap().cap = Some(1024);
    let held = || {
        let log = replay.inner.try_lock().unwrap();
        let out = |h: &&Held| matches!(h.event.event, Some(attach_event::Event::Output(_)));
        (
            log.events.len() + log.went.len(),
            log.went.iter().filter(out).count(),
        )
    };
    for _ in 0..100_000 {
        replay.publish(attach_event::Event::Log(Vec::new())).await;
    }
    assert!(held().0 <= 1024 / HELD, "{} held, of lines", held().0);
    // Chunks gone by the restart, then chunks still in the log at it.
    for n in [100_000, 3] {
        for _ in 0..n {
            replay.publish(nar(OUT, vec![7; 8], false)).await;
        }
        assert!(held().0 <= 1024 / HELD, "{} held, of chunks", held().0);
        replay
            .publish(attach_event::Event::OutputRestart(OUT.into()))
            .await;
        // A line past the cap: what was before it goes.
        let big = attach_event::Event::Log(vec![b'x'; 1024]);
        replay.publish(big).await;
        assert_eq!(held().1, 0, "chunks a restart superseded kept, of {n}");
    }
    // What is kept of OUT2's chunk stays at OUT's restart.
    replay.publish(nar(OUT2, vec![7; 8], false)).await;
    replay
        .publish(attach_event::Event::Log(vec![b'x'; 1024]))
        .await;
    replay
        .publish(attach_event::Event::OutputRestart(OUT.into()))
        .await;
    assert_eq!(held().1, 1, "OUT2's chunk dropped at OUT's restart");
}

/// A client further behind than what the log keeps of the chunks that went
/// does not get an output with a gap: while the build runs it is refused,
/// so attach does not resubmit into the same log, and it resubmits once
/// the build ended. One read OUT's first chunk, and the record of the
/// second went; one attaches after that; one after the end.
#[tokio::test(start_paused = true)]
async fn a_client_behind_what_the_log_keeps_is_refused_until_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 22).unwrap());
    let (recipe, frames) = chunks(&cache, [9, 9], 10);
    let pin = cache.pin(recipe.iter().map(|(h, _)| *h));
    let replay = Arc::new(Replay::new("b1".into()));
    // Room for eight chunks' records.
    replay.inner.try_lock().unwrap().cap = Some(8 * (HELD + OUT.len()));
    let kept = vec![(OUT.to_owned(), Arc::new(recipe))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    let mut rx = replay.subscribe().await;
    let first = nar(OUT, frames[0].clone(), false);
    replay.publish(first.clone()).await;
    model::settle().await;
    for frame in &frames[1..] {
        replay.publish(nar(OUT, frame.clone(), false)).await;
    }
    let refused = Err(Status::resource_exhausted(EXHAUSTED).to_string());
    assert_eq!(drain(&mut rx).await, Ok(vec![Ok(first), refused.clone()]));
    let mut late = replay.subscribe().await;
    assert_eq!(drain(&mut late).await, Ok(vec![refused]));
    replay.finish().await;
    let mut after = replay.subscribe().await;
    assert_eq!(drain(&mut after).await, Ok(Vec::new()));
}

/// A client that read every line there was is not told one went: it read
/// L, then L and the chunk after it went past the cap before it read on.
#[tokio::test(start_paused = true)]
async fn a_client_that_missed_no_line_is_told_none() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 22).unwrap());
    let (recipe, frames) = chunks(&cache, [9, 9], 1);
    let pin = cache.pin(recipe.iter().map(|(h, _)| *h));
    let replay = Arc::new(Replay::new("b1".into()));
    replay.inner.try_lock().unwrap().cap = Some(1024);
    let kept = vec![(OUT.to_owned(), Arc::new(recipe))];
    replay
        .keep_outputs(KeptOutputs::new(cache, kept, pin))
        .await;
    let mut rx = replay.subscribe().await;
    let line = attach_event::Event::Log(b"L\n".to_vec());
    replay.publish(line.clone()).await;
    model::settle().await;
    replay.publish(nar(OUT, frames[0].clone(), false)).await;
    replay.publish(attach_event::Event::ExitCode(0)).await;
    replay.finish().await;
    let seen = drain(&mut rx).await.unwrap();
    let chunk = nar(OUT, frames[0].clone(), false);
    assert_eq!(
        seen,
        [line, chunk, attach_event::Event::ExitCode(0)].map(Ok)
    );
}

/// A log exactly at the cap loses nothing (H9: past it, it drops): what
/// each event costs is its size and the bytes it holds.
#[tokio::test(start_paused = true)]
async fn nothing_goes_at_the_cap() {
    let replay = Arc::new(Replay::new("b1".into()));
    let (line, chunk) = (b"0123456\n".to_vec(), vec![7; 8]);
    let cap = 3 * HELD + line.len() + OUT.len() + chunk.len();
    replay.inner.try_lock().unwrap().cap = Some(cap);
    let want = [
        attach_event::Event::Log(line),
        nar(OUT, chunk, false),
        attach_event::Event::ExitCode(0),
    ];
    for ev in want.clone() {
        replay.publish(ev).await;
    }
    replay.finish().await;
    let mut rx = replay.subscribe().await;
    assert_eq!(drain(&mut rx).await, Ok(want.map(Ok).into()));
}
