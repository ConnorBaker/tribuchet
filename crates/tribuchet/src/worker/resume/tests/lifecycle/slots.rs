//! Builds that run already hold a slot, whatever is free.

use super::*;

/// Max-jobs `max_jobs`, the agents `pool` (of 1.sock and 2.sock): B1
/// adopted on 1.sock and running; `then` leaves a build that runs already
/// on 2.sock; B1 killed. Once B1 is recorded, the slots free and the capacity:
/// what is on 2.sock still holds its slot then. A failure fails the test
/// (B1 not recorded within 60 s too), not hangs it: the runtime is shut
/// down however the test ends (`Down`).
fn once_b1_ends(
    max_jobs: usize,
    pool: &[&str],
    then: impl FnOnce(&Arc<WorkerCtx>, &tokio::runtime::Runtime, &Path) -> Result<()>,
) -> Result<(usize, usize)> {
    let dir = tempfile::tempdir()?;
    let s1 = dir.path().join("1.sock");
    let _b1 = fake_agent(&s1, "b1", None, answers(usize::MAX), String::new)?;
    let pool = pool.iter().map(|s| dir.path().join(s)).collect();
    let ctx = WorkerCtx::for_test(dir.path(), max_jobs, pool)?;
    let down = Down(Some(tokio::runtime::Runtime::new()?));
    let rt = down.0.as_ref().unwrap();
    let (st, build) = running(dir.path(), "b1")?;
    let answer = agents::AgentBuild::adopt(&s1, "b1");
    rt.block_on(settle(&ctx, st, build, answer));
    then(&ctx, rt, dir.path())?;
    agents::kill(&s1, "b1")?;
    let recorded = || {
        let map = ctx.resumable.lock().unwrap();
        map.get("k1").is_some_and(|e| e.finished.is_some())
    };
    let end = Instant::now() + Duration::from_mins(1);
    while !recorded() && Instant::now() < end {
        thread::sleep(Duration::from_millis(1));
    }
    let room = ctx.agents.room();
    let slots = (room.free, room.capacity);
    // B2, if adopted, ends, and its supervisor with it (a kill call to an
    // agent that does not answer would wait for good).
    let map = ctx.resumable.lock().unwrap();
    let adopted = map.get("k2").is_some_and(|e| !e.waiting_for_agent);
    drop(map);
    if adopted {
        let _ = agents::kill(&dir.path().join("2.sock"), "b2");
    }
    assert!(recorded(), "B1 not recorded in 60 s");
    Ok(slots)
}

/// A test's runtime, shut down when it goes, its blocking tasks not waited
/// for: what a stand-in agent runs may run on for good.
struct Down(Option<tokio::runtime::Runtime>);

impl Drop for Down {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

/// A stand-in agent's `allow`: it answers `n` calls.
fn answers(n: usize) -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(n))
}

/// Build `id` ("b1", "b2"), keyed "k1", "k2", running on `id`'s number's
/// socket under `root`, its state persisted.
fn running(root: &Path, id: &str) -> Result<(ResumeState, PathBuf)> {
    let build = root.join("builds").join(id);
    fs::create_dir_all(&build)?;
    fs::write(build.join("build.log"), "")?;
    let socket = root.join(format!("{}.sock", &id[1..]));
    let mut st = resume_state(id, socket);
    st.dedupe_key = format!("k{}", &id[1..]);
    st.persist(&build)?;
    Ok((st, build))
}

/// B2 adopted on 2.sock, as adopt_builds does after B1.
fn adopt_b2(ctx: &Arc<WorkerCtx>, rt: &tokio::runtime::Runtime, root: &Path) -> Result<()> {
    let s2 = root.join("2.sock");
    fake_agent(&s2, "b2", None, answers(usize::MAX), String::new)?;
    let (st, build) = running(root, "b2")?;
    rt.block_on(settle(ctx, st, build, agents::AgentBuild::adopt(&s2, "b2")));
    Ok(())
}

const BOTH: &[&str] = &["1.sock", "2.sock"];

/// An adopted build runs whether or not a slot was free for it, so it
/// holds one: once B1 ends, B2 still holds the only slot.
#[test]
fn adopted_builds_beyond_max_jobs_leave_no_slot_free() -> Result<()> {
    assert_eq!(once_b1_ends(1, BOTH, adopt_b2)?, (0, 1));
    Ok(())
}

/// The same with B2's agent outside the pool, max-jobs 4: one agent, one
/// slot, which B2 still holds: a build on an agent outside the pool
/// counts against the pool's bound too (W13).
#[test]
fn an_adopted_build_on_an_agent_outside_the_pool_holds_a_slot() -> Result<()> {
    assert_eq!(once_b1_ends(4, &["1.sock"], adopt_b2)?, (0, 1));
    Ok(())
}

/// Positive control: with two slots, B2 holds one and one is free.
#[test]
fn adopted_builds_within_max_jobs_leave_the_rest_free() -> Result<()> {
    assert_eq!(once_b1_ends(2, BOTH, adopt_b2)?, (1, 2));
    Ok(())
}

/// A build held for its agent (wait_for_agent: no agent at 2.sock) holds a
/// slot while it waits, beyond max-jobs too.
#[test]
fn a_build_held_for_its_agent_beyond_max_jobs_holds_a_slot() -> Result<()> {
    let mut held = None;
    let slots = once_b1_ends(1, BOTH, |ctx, rt, root| {
        let (st, build) = running(root, "b2")?;
        let answer = agents::AgentBuild::adopt(&st.agent_socket, "b2");
        rt.block_on(settle(ctx, st, build, answer));
        let map = ctx.resumable.lock().unwrap();
        held = map.get("k2").map(|e| e.waiting_for_agent);
        Ok(())
    })?;
    assert_eq!(held, Some(true), "B2 is not held for its agent");
    assert_eq!(slots, (0, 1));
    Ok(())
}

/// An orphan's cleanup (sweep_orphaned_agent_builds) holds a slot while it
/// runs, beyond max-jobs too: the orphan's agent answers the status call,
/// not the kill.
#[test]
fn an_orphans_cleanup_beyond_max_jobs_holds_a_slot() -> Result<()> {
    let slots = once_b1_ends(1, BOTH, |ctx, rt, root| {
        let s2 = root.join("2.sock");
        fake_agent(&s2, "orphan", None, answers(1), String::new)?;
        let _rt = rt.enter();
        sweep_orphaned_agent_builds(ctx, vec![s2]);
        Ok(())
    })?;
    assert_eq!(slots, (0, 1));
    Ok(())
}
