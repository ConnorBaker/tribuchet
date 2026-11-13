//! Taking a build's registry entry and build dir down while other
//! things touch them.

use std::os::unix::fs::PermissionsExt;

use rustix::fs::{CWD, FileType, Mode, mknodat};
use rustix::process::geteuid;

use super::*;

/// A build dir whose removal stops part-way, as when the worker stops
/// meanwhile, leaves no build to adopt: its state.json goes first. Here
/// the removal stops at once, the dir being unlistable. Before, a
/// cancelled held build's run could be adopted again from what remained.
#[test]
fn a_build_dir_removed_in_part_leaves_no_build_to_adopt() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let build = dir.path().join("builds/b1");
    fs::create_dir_all(&build)?;
    resume_state("b1", dir.path().join("1.sock")).persist(&build)?;
    // Searchable and writable, not listable (unless run as root).
    fs::set_permissions(&build, fs::Permissions::from_mode(0o300))?;
    remove_build_dir(&build);
    let kept = build.join("state.json").exists();
    if build.exists() {
        fs::set_permissions(&build, fs::Permissions::from_mode(0o700))?;
    }
    assert!(!kept, "the part-removed build's state is kept");
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
    tokio::runtime::Runtime::new()?.block_on(adopt_builds(&ctx));
    assert_eq!(held(&ctx, "k"), None, "a part-removed build was adopted");
    Ok(())
}

/// A successful result of a run in `dir`.
fn finished(dir: &Path) -> FinishedBuild {
    FinishedBuild {
        exit_code: 0,
        usage: Usage::default(),
        error: String::new(),
        outputs: vec![],
        dir: dir.to_owned(),
        finished_at: Instant::now(),
    }
}

/// Results the reaper drops leave nothing to adopt once their keys are
/// gone: each state.json goes with its key, not after the dirs before it
/// are removed (slow here, each full). Before, the second result's state
/// outlived its key by the first's removal.
#[test]
fn reaped_results_leave_nothing_to_adopt_once_their_keys_are_gone() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
    let mut builds = Vec::new();
    for key in ["k", "k2"] {
        let build = dir.path().join(format!("builds/{key}"));
        fill(&build, "top/build")?;
        delivery::persist_finished(key, ("old", None), &finished(&build));
        let fin = Some(finished(&build));
        ResumableBuild::insert(&ctx, key.into(), "old".into(), build.clone(), fin);
        builds.push(build);
    }
    thread::sleep(Duration::from_millis(1));
    let reaper_ctx = ctx.clone();
    let reaper = thread::spawn(move || delivery::reap(&reaper_ctx, Duration::ZERO));
    let end = Instant::now() + Duration::from_mins(1);
    while !ctx.resumable.lock().unwrap().is_empty() {
        assert!(Instant::now() < end, "never reaped");
        thread::sleep(Duration::from_millis(1));
    }
    let kept: Vec<&PathBuf> = builds
        .iter()
        .filter(|b| b.join("state.json").exists())
        .collect();
    assert!(kept.is_empty(), "state outlived its key: {kept:?}");
    reaper.join().unwrap();
    Ok(())
}

/// A result is kept while the session it is held for is open, however
/// long ago it finished, and the reaper's clock starts at that session's
/// close, or at a cancel of its dispatch: a given-up build's result goes
/// a ttl on, its session open. An ack for an earlier adoption leaves it.
/// Before, it went 5 min after its finish, the hub's chunk request still
/// on its way.
#[test]
fn a_result_is_kept_while_its_session_is_open() -> Result<()> {
    for cancelled in [false, true] {
        let dir = tempfile::tempdir()?;
        let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
        let build = dir.path().join("builds/k");
        fs::create_dir_all(&build)?;
        let mut fin = finished(&build);
        fin.finished_at = Instant::now().checked_sub(Duration::from_mins(10)).unwrap();
        ResumableBuild::insert(&ctx, "k".into(), "b".into(), build, Some(fin));
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        ctx.resumable.lock().unwrap().get_mut("k").unwrap().out_tx = Some(tx.clone());
        let ttl = Duration::from_mins(5);
        delivery::reap(&ctx, ttl);
        delivery::acked(&ctx, "k", "earlier");
        let kept = held(&ctx, "k");
        assert_eq!(kept, Some((false, true)), "{cancelled}: gone while open");
        if cancelled {
            let c = CancelBuild {
                build_id: "b".into(),
                dedupe_key: "k".into(),
            };
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(handle_cancel(c, &mut HashMap::new(), &tx, &ctx))?;
        } else {
            delivery::release(&ctx, &tx);
        }
        delivery::reap(&ctx, ttl);
        let kept = held(&ctx, "k");
        assert_eq!(
            kept,
            Some((false, true)),
            "{cancelled}: gone before its ttl"
        );
        thread::sleep(Duration::from_millis(1));
        delivery::reap(&ctx, Duration::ZERO);
        assert_eq!(held(&ctx, "k"), None, "{cancelled}: kept past its ttl");
    }
    Ok(())
}

/// A chunk request the worker cannot serve is answered ResultGone: for a
/// result it does not have, and for one it cannot read, which it drops
/// first so that no later assignment adopts it again. Before, the first
/// went unanswered, the hub waiting for the session to die, and the
/// second was answered as a failed build.
#[tokio::test]
async fn a_chunk_request_it_cannot_serve_is_answered_result_gone() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
    let build = dir.path().join("builds/k");
    fs::create_dir_all(&build)?;
    let (mut fin, hash) = (finished(&build), [7; 32]);
    let chunks = vec![OutChunk {
        hash,
        size: 1,
        off: 0,
        len: 1,
    }];
    let (scratch, frames_file) = ("/nix/store/out".into(), build.join("gone"));
    fin.outputs = vec![PackedOutput {
        scratch,
        frames_file,
        chunks,
    }];
    ResumableBuild::insert(&ctx, "k".into(), "b".into(), build, Some(fin));
    for (id, kept) in [("unknown", true), ("b", false)] {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        serve_chunks(&ctx, id.into(), &hash, (tx.clone(), tx));
        let answer = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
        let Ok(Some(WorkerMessage {
            msg: Some(worker_message::Msg::ResultGone(gone)),
        })) = answer
        else {
            panic!("{id}: answered {answer:?}");
        };
        let after = (gone.build_id.as_str(), held(&ctx, "k").is_some());
        assert_eq!(after, (id, kept));
    }
    Ok(())
}

/// A worker holding the finished result "b" of key "k", one chunk of it
/// on disk, whose hash it returns.
fn servable(dir: &Path) -> Result<(Arc<WorkerCtx>, [u8; 32])> {
    let ctx = WorkerCtx::for_test(dir, 4, vec![])?;
    let build = dir.join("builds/k");
    fs::create_dir_all(&build)?;
    let (mut fin, hash) = (finished(&build), [7; 32]);
    let frames_file = build.join("frames");
    fs::write(&frames_file, [1])?;
    let chunks = vec![OutChunk {
        hash,
        size: 1,
        off: 0,
        len: 1,
    }];
    let scratch = "/nix/store/out".into();
    fin.outputs = vec![PackedOutput {
        scratch,
        frames_file,
        chunks,
    }];
    ResumableBuild::insert(&ctx, "k".into(), "b".into(), build, Some(fin));
    Ok((ctx, hash))
}

/// The chunks a Need asks of a finished result, and their eof, go on the
/// session's Session and none on its Control, where control would queue
/// behind bulk again.
#[tokio::test]
async fn output_chunks_go_on_the_session_not_the_control() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (ctx, hash) = servable(dir.path())?;
    let (ctl, mut ctl_rx) = tokio::sync::mpsc::channel(4);
    let (bulk, mut bulk_rx) = tokio::sync::mpsc::channel(4);
    serve_chunks(&ctx, "b".into(), &hash, (ctl, bulk));
    let mut eofs = Vec::new();
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(10), bulk_rx.recv()).await {
        if let Some(worker_message::Msg::Chunk(c)) = m.msg {
            eofs.push(c.eof);
        }
    }
    assert_eq!(eofs, [false, true], "not the chunk and its eof");
    assert!(ctl_rx.try_recv().is_err(), "something on the Control");
    Ok(())
}

/// Chunks sent on a Session the hub takes nothing from, its Control
/// alive, wait no longer than the hub's silence may last: then the
/// session is gone, and the result is kept for the next one.
#[tokio::test(start_paused = true)]
async fn a_chunk_request_waits_on_a_silent_session_no_longer_than_its_silence() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (ctx, hash) = servable(dir.path())?;
    let (ctl, mut ctl_rx) = tokio::sync::mpsc::channel(4);
    let (bulk, bulk_rx) = tokio::sync::mpsc::channel(1);
    serve_chunks(&ctx, "b".into(), &hash, (ctl, bulk));
    // The chunk fills the Session; its eof waits.
    while bulk_rx.is_empty() {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(100));
    // A blocking task holds the paused clock still unless it is advanced.
    tokio::time::advance(crate::chunkio::SILENCE_TIMEOUT + Duration::from_secs(1)).await;
    for _ in (0..1000).take_while(|_| bulk_rx.sender_strong_count() > 0) {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(bulk_rx.sender_strong_count(), 0, "still waiting");
    assert!(ctl_rx.try_recv().is_err(), "said more than the chunks");
    assert!(held(&ctx, "k").is_some(), "the result went");
    Ok(())
}

/// A result whose state.json the reaper or an ack cannot remove keeps its
/// key: the hub, told the key is gone, may run the build again under the
/// same id, which a restarted worker would take for this result. Here its
/// dir is not writable; root writes it all the same, so the test shows
/// nothing as root and is skipped.
#[test]
fn an_ended_result_whose_state_stays_keeps_its_key() -> Result<()> {
    if geteuid().is_root() {
        return Ok(());
    }
    let ends: [fn(&Arc<WorkerCtx>); 2] = [
        |c| delivery::reap(c, Duration::ZERO),
        |c| delivery::acked(c, "k", "old"),
    ];
    for end in ends {
        let dir = tempfile::tempdir()?;
        let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
        let build = dir.path().join("builds/k");
        fs::create_dir_all(&build)?;
        delivery::persist_finished("k", ("old", None), &finished(&build));
        let fin = Some(finished(&build));
        ResumableBuild::insert(&ctx, "k".into(), "old".into(), build.clone(), fin);
        thread::sleep(Duration::from_millis(1));
        fs::set_permissions(&build, fs::Permissions::from_mode(0o500))?;
        end(&ctx);
        fs::set_permissions(&build, fs::Permissions::from_mode(0o700))?;
        assert!(build.join("state.json").exists(), "its state.json went");
        assert_eq!(held(&ctx, "k"), Some((false, true)), "its key went");
    }
    Ok(())
}

/// The reaper's file I/O holds no runtime worker, nor does an ack's. Here
/// the registry they read is held, as a lock behind slow I/O would be; the
/// runtime answers every probe while they wait for it.
#[test]
fn ending_a_result_holds_no_runtime_worker() -> Result<()> {
    let starts: [fn(_); 2] = [spawn_resumable_reaper, |c| ack_delivery(&c, "k", "old")];
    for start in starts {
        let dir = tempfile::tempdir()?;
        let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
        let build = dir.path().join("builds/k");
        fs::create_dir_all(&build)?;
        let mut fin = finished(&build);
        fin.finished_at = Instant::now().checked_sub(Duration::from_mins(10)).unwrap();
        ResumableBuild::insert(&ctx, "k".into(), "old".into(), build, Some(fin));
        let (probe_tx, mut probe_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (ack_tx, ack_rx) = mpsc::channel();
        let rt_ctx = ctx.clone();
        let runtime = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap();
            rt.block_on(async {
                start(rt_ctx);
                while probe_rx.recv().await.is_some() {
                    ack_tx.send(()).unwrap();
                }
            });
        });
        let registry = ctx.resumable.lock().unwrap();
        let answered = (0..20).all(|_| {
            thread::sleep(Duration::from_millis(50));
            probe_tx.send(()).is_ok() && ack_rx.recv_timeout(Duration::from_secs(10)).is_ok()
        });
        drop(registry);
        let end = Instant::now() + Duration::from_mins(1);
        while !ctx.resumable.lock().unwrap().is_empty() {
            assert!(Instant::now() < end, "never reaped");
            thread::sleep(Duration::from_millis(1));
        }
        drop(probe_tx);
        runtime.join().unwrap();
        assert!(answered, "the runtime stopped while its ending waited");
    }
    Ok(())
}

/// Recording a result holds the registry across no file I/O, so
/// no assignment or cancel waits on a disk. Here the result's
/// state.json.tmp is a fifo filled and not drained, so the write blocks;
/// the registry is free meanwhile, and the result recorded after it.
#[test]
fn recording_a_result_holds_the_registry_across_no_write() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let ctx = WorkerCtx::for_test(dir.path(), 4, vec![])?;
    let build = dir.path().join("builds/k");
    fs::create_dir_all(&build)?;
    let tmp = build.join("state.json.tmp");
    mknodat(CWD, &tmp, FileType::Fifo, Mode::RUSR | Mode::WUSR, 0).map_err(io::Error::from)?;
    ResumableBuild::insert(&ctx, "k".into(), "b".into(), build.clone(), None);
    // More than any pipe holds (fs.pipe-max-size, 1 MiB by default).
    let mut fin = finished(&build);
    fin.error = "x".repeat(4 << 20);
    let recorder_ctx = ctx.clone();
    let recorder = thread::spawn(move || record_finished(&recorder_ctx, "k", fin));
    // Opened once the recorder opened it to write; a Timeout if not in 10 s.
    let ((open_tx, open_rx), path) = (mpsc::channel(), tmp.clone());
    thread::spawn(move || open_tx.send(fs::File::open(path)));
    let mut fifo = open_rx.recv_timeout(Duration::from_secs(10)).unwrap()?;
    let (free_tx, free_rx) = mpsc::channel();
    let probe_ctx = ctx.clone();
    thread::spawn(move || {
        drop(probe_ctx.resumable.lock().unwrap());
        let _ = free_tx.send(());
    });
    let free = free_rx.recv_timeout(Duration::from_secs(10)).is_ok();
    io::copy(&mut fifo, &mut io::sink())?;
    recorder.join().unwrap();
    assert!(free, "the registry was held across the result's write");
    assert_eq!(held(&ctx, "k"), Some((false, true)), "recorded");
    Ok(())
}
