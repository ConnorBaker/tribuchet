//! Taking a build's registry entry and build dir down while other
//! things touch them.

use std::os::unix::fs::PermissionsExt;

use rustix::fs::{CWD, FileType, Mode, mknodat};

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
        delivery::persist_finished(key, "old", &finished(&build));
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

/// The reaper's file I/O holds no runtime worker. Here the
/// registry it reads is held, as a lock behind slow I/O would be; the
/// runtime answers every probe while the reaper waits for it.
#[test]
fn the_reaper_holds_no_runtime_worker() -> Result<()> {
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
            spawn_resumable_reaper(rt_ctx);
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
    assert!(answered, "the runtime stopped while the reaper waited");
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
