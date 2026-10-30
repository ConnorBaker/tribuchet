use std::fs;
use std::path::Path;

use super::*;

/// An arbiter for 4 CPUs.
fn arbiter() -> Arc<Arbiter> {
    Arbiter::with_cpus(true, 4)
}

/// A build cgroup listing the calling thread `threads` times: running
/// while it samples, so each counts as runnable. It counts no frozen time
/// yet (Linux 6.18's cgroup.stat.local), which a test may change, holds
/// no memory, and reaches frozen once asked (cgroup.events).
#[cfg(target_os = "linux")]
fn busy_cgroup(dir: &Path, name: &str, threads: usize) -> PathBuf {
    let cg = dir.join(name);
    fs::create_dir(&cg).unwrap();
    let me = format!("{}\n", rustix::thread::gettid().as_raw_nonzero());
    fs::write(cg.join("cgroup.threads"), me.repeat(threads)).unwrap();
    fs::write(cg.join("cgroup.stat.local"), "frozen_usec 0\n").unwrap();
    fs::write(cg.join("memory.current"), "0\n").unwrap();
    fs::write(cg.join("cgroup.events"), "populated 1\nfrozen 1\n").unwrap();
    cg
}

/// A build's frozen time, which its build timeout leaves out, is its
/// cgroup's own count: zero without one, and what it was once read when
/// it can no longer be.
#[cfg(target_os = "linux")]
#[test]
fn a_tracked_build_reports_its_cgroups_frozen_time() {
    let dir = tempfile::tempdir().unwrap();
    let a = arbiter();
    let cg = busy_cgroup(dir.path(), "b", 1);
    let freeze = fs::File::create(cg.join("cgroup.freeze")).unwrap();
    a.insert("b", "b", 0, Some(cg.clone()), Some(freeze));
    let t = Tracking {
        arbiter: a.clone(),
        key: "b".into(),
        most_usec: AtomicU64::new(0),
    };
    fs::remove_file(cg.join("cgroup.stat.local")).unwrap();
    assert_eq!(t.frozen(), Duration::ZERO);
    fs::write(cg.join("cgroup.stat.local"), "frozen_usec 7250000\n").unwrap();
    assert_eq!(t.frozen(), Duration::from_millis(7250));
    fs::remove_file(cg.join("cgroup.stat.local")).unwrap();
    assert_eq!(t.frozen(), Duration::from_millis(7250));
    drop(t);
    assert!(a.lock().builds.is_empty());
}

/// A change to the cgroup files: a build's memory.current, frozen_usec,
/// whether cgroup.events says frozen, or (before it is adopted) whether its
/// cgroup.freeze asks for frozen or its agent sent no handle; the slice's
/// memory.max and memory.current.
#[derive(Clone, Copy)]
enum Set {
    Holds(&'static str, u64),
    Count(&'static str, u64),
    Reached(&'static str, bool),
    Stuck(&'static str),
    Bare(&'static str),
    Cap(u64, u64),
}
use Set::{Bare, Cap, Count, Holds, Reached, Stuck};

/// Changes (a first step's before the builds are tracked), then this many
/// samples, then what each build's cgroup.freeze was written ("key=writes",
/// 1 freezes, 0 thaws).
type Step = (&'static [Set], u32, &'static str);

const LF: &[(&str, u64)] = &[("lead", 0), ("f", 1)];
const S: u64 = 1_000_000;
const MORE_THAN_THE_HOST: u64 = 1 << 40;

/// Builds (key, start), each running 4 threads on 4 CPUs, and steps.
const SCENARIOS: &[(&[(&str, u64)], &[Step])] = &[
    // The oldest leads, ties by key, whatever order a restarted worker
    // adopts builds in (tracked in the order adopted, "late" once led).
    (
        &[("late", 200), ("tie-b", 100), ("tie-a", 100)],
        &[(&[], 40, "late=1 tie-b=1 tie-a=")],
    ),
    // A build whose agent sent no handle is never frozen: the worker
    // opens no cgroup.freeze itself.
    (LF, &[(&[Bare("f")], 60, "lead= f=")]),
    // One that memory has no room for is not frozen, and is thawed once
    // it has none.
    (
        LF,
        &[
            (&[Holds("f", MORE_THAN_THE_HOST)], 60, "lead= f="),
            (&[Holds("f", 0)], 60, "lead= f=1"),
            (&[Holds("f", MORE_THAN_THE_HOST)], 1, "lead= f=10"),
        ],
    ),
    // Freezes one sample asks for are summed: of two followers under one
    // cap with headroom for one, only the older.
    (
        &[("lead", 0), ("f1", 1), ("f2", 2)],
        &[(
            &[
                Cap(2 << 30, 1434 << 20),
                Holds("f1", 400 << 20),
                Holds("f2", 400 << 20),
            ],
            60,
            "lead= f1=1 f2=",
        )],
    ),
    // The bound is the cgroup's own count over the last 30 s: arriving
    // with 6 s of it, a build is frozen, thawed at the sample its count
    // reaches 10 s though the leader still fills every CPU, and frozen
    // again only once those 10 s cannot all lie in the window: 24 s on.
    (
        LF,
        &[
            (&[Count("f", 6 * S)], 26, "lead= f=1"),
            (&[Count("f", 9_800_000)], 1, "lead= f=1"),
            (&[Count("f", 10 * S)], 1, "lead= f=10"),
            (&[], 93, "lead= f=10"),
            (&[], 1, "lead= f=101"),
        ],
    ),
    // A freeze not reached within FREEZE_TIMEOUT (25 samples) is undone,
    // and not asked again however long the leader keeps every CPU busy.
    (
        LF,
        &[
            (&[Reached("f", false)], 26, "lead= f=1"),
            (&[], 24, "lead= f=1"),
            (&[], 1, "lead= f=10"),
            (&[], 100, "lead= f=10"),
        ],
    ),
    // An adopted build found frozen (as root may leave it) is thawed
    // through its handle; without a handle it is left be.
    (
        LF,
        &[(&[Stuck("lead"), Stuck("f"), Bare("f")], 0, "lead=10 f=1")],
    ),
];

/// The arbiter as the worker runs it, a sample at a time: what it writes
/// to each build's cgroup.freeze.
#[cfg(target_os = "linux")]
#[test]
fn every_scenario_freezes_and_thaws_as_the_table_says() {
    for (row, &(builds, steps)) in SCENARIOS.iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let slice = dir.path().join("slice");
        fs::create_dir(&slice).unwrap();
        fs::write(slice.join("memory.max"), "max\n").unwrap();
        fs::write(dir.path().join("meminfo"), "MemAvailable: 104857600 kB\n").unwrap();
        let mut a = arbiter();
        Arc::get_mut(&mut a).unwrap().memory = memory::Roots::under(dir.path());
        let mut bare = Vec::new();
        for &(key, _) in builds {
            fs::write(busy_cgroup(&slice, key, 4).join("cgroup.freeze"), "").unwrap();
        }
        let mut now = Instant::now();
        for (step, &(sets, samples, want)) in steps.iter().enumerate() {
            for set in sets {
                let (dir, file, text) = match *set {
                    Holds(k, bytes) => (slice.join(k), "memory.current", format!("{bytes}")),
                    Count(k, us) => (
                        slice.join(k),
                        "cgroup.stat.local",
                        format!("frozen_usec {us}"),
                    ),
                    Reached(k, r) => (
                        slice.join(k),
                        "cgroup.events",
                        format!("frozen {}", u8::from(r)),
                    ),
                    Stuck(k) => (slice.join(k), "cgroup.freeze", "1".into()),
                    Bare(k) => {
                        bare.push(k);
                        continue;
                    }
                    Cap(max, current) => {
                        fs::write(slice.join("memory.max"), format!("{max}")).unwrap();
                        (slice.clone(), "memory.current", format!("{current}"))
                    }
                };
                fs::write(dir.join(file), text).unwrap();
            }
            for &(key, started) in builds.iter().filter(|_| step == 0) {
                let cg = slice.join(key);
                let open = || {
                    fs::OpenOptions::new()
                        .append(true)
                        .open(cg.join("cgroup.freeze"))
                };
                let freeze = (!bare.contains(&key)).then(|| open().unwrap());
                a.insert(key, key, started, Some(cg.clone()), freeze);
            }
            for _ in 0..samples {
                now += SAMPLE_EVERY;
                a.tick(now);
            }
            let written = |key| fs::read_to_string(slice.join(key).join("cgroup.freeze")).unwrap();
            let got: Vec<_> = builds
                .iter()
                .map(|b| format!("{}={}", b.0, written(b.0)))
                .collect();
            assert_eq!(got.join(" "), want, "scenario {row}, step {step}");
        }
    }
}
