//! The `pairs` phase: the CPU arbiter makes no pair of builds fail, on the
//! real kernel's freezer and memory accounting. Each row is a leader (the
//! older build) and a follower on a 4-core worker whose agents' slice is
//! capped and swaps to zram (nix/tests/phase-pairs.nix). What the arbiter
//! decides from what it reads is its unit tests' (worker/arbiter/memory.rs,
//! tests.rs; worker/build/agent_exec.rs for the clocks).

use crate::harness::{
    Node, count, ended, journal, more_than, submit, succeed, wait_until_succeeds,
};

const SLICE: &str = "/sys/fs/cgroup/system.slice/system-tribuchet\\x2dagent.slice";

struct Row {
    name: &'static str,
    /// The leader's and the follower's workloads (phase-pairs.nix).
    work: [&'static str; 2],
    /// worker.toml's build-timeout-secs and max-silent-time-secs, 0 for none.
    limits: (i64, i64),
    /// The follower's frozen_ms: at least, at most.
    frozen: (i64, i64),
}

const ROWS: [Row; 3] = [
    // Page cache filling the slice is headroom: the follower is frozen
    // (a rule that counted it as used froze nothing). The cache is written
    // once, before.
    Row {
        name: "cache",
        work: ["CL", "CF"],
        limits: (0, 0),
        frozen: (5000, i64::MAX),
    },
    // The follower's random bytes stay pinned (zswap refuses pages that
    // do not compress, and swap is no room): they do not fit, so it is not
    // frozen (frozen, it OOM-killed the leader at the merge base in 1 of 2
    // runs and stalled the slice in the other).
    Row {
        name: "A",
        work: ["AL", "AF"],
        limits: (0, 0),
        frozen: (0, 0),
    },
    // Only running time counts toward the build timeout and silence: the
    // follower, frozen about 10 s, passes 5 s inside its timeout.
    Row {
        name: "deadline",
        work: ["K", "D"],
        limits: (20, 8),
        frozen: (9000, i64::MAX),
    },
];

/// Restarts the worker with these limits.
fn limits((timeout, silent): (i64, i64)) {
    succeed(
        Node::Worker,
        &format!(
            "mkdir -p /run/arb && \
             {{ test -e /run/arb/base.toml || cp $(readlink -f /etc/tribuchet/worker.toml) /run/arb/base.toml; }} && \
             {{ printf 'build-timeout-secs = %d\\nmax-silent-time-secs = %d\\n' {} {silent}; \
                cat /run/arb/base.toml; }} > /run/arb/worker.toml && \
             cp --remove-destination /run/arb/worker.toml /etc/tribuchet/worker.toml",
            if timeout == 0 { 86_400 } else { timeout },
        ),
    );
    let registered = count(Node::Hub, "tribuchet-hub", "worker registered");
    succeed(Node::Worker, "systemctl restart tribuchet-worker");
    wait_until_succeeds(
        Node::Hub,
        &more_than("tribuchet-hub", "worker registered", registered),
        120,
    );
}

/// The slice's OOM kills so far.
fn oom_kills() -> i64 {
    succeed(
        Node::Worker,
        &format!("awk '$1 == \"oom_kill\" {{print $2}}' '{SLICE}/memory.events'"),
    )
    .trim()
    .parse()
    .unwrap()
}

/// Build `name`'s frozen_ms and wall_ms, as the hub's usage line has them.
fn usage(name: &str) -> (i64, i64) {
    let line = succeed(
        Node::Hub,
        &format!(
            "{} | grep 'build usage' | grep -F '\"{name}\"' | tail -n 1 || true",
            journal("tribuchet-hub")
        ),
    );
    let field = |key: &str| {
        line.split_whitespace()
            .find_map(|w| w.strip_prefix(key)?.strip_prefix('='))
            .and_then(|v| v.parse().ok())
            .unwrap_or(-1)
    };
    (field("frozen_ms"), field("wall_ms"))
}

/// Runs `row`; any problem is pushed onto `problems`.
fn run(row: &Row, problems: &mut Vec<String>) {
    limits(row.limits);
    let kills = oom_kills();
    let names = row.work.map(|w| format!("{}-{w}", row.name));
    for (name, work) in names.iter().zip(row.work) {
        let started = count(Node::Worker, "tribuchet-worker", "builder started on agent");
        submit(
            name,
            &format!(
                "nix-build /etc/tt/pair.nix --argstr name {name} --argstr work {work} --no-out-link"
            ),
        );
        // Started, and a second on: the next build's start is later.
        wait_until_succeeds(
            Node::Worker,
            &more_than("tribuchet-worker", "builder started on agent", started),
            120,
        );
        succeed(Node::Hub, "sleep 1");
    }
    succeed(Node::Hub, "sleep 7");
    let rcs = names.each_ref().map(|n| ended(n, 300));
    let killed = oom_kills() - kills;
    let (frozen, wall) = usage(&names[1]);
    eprintln!(
        "{}: rc {rcs:?}, {killed} OOM kills, follower frozen {frozen} ms of {wall} ms",
        row.name
    );
    let mut problem = |ok: bool, what: &str| {
        if !ok {
            problems.push(format!("{}: {what}", row.name));
        }
    };
    problem(
        rcs == ["0", "0"] && killed == 0,
        "a build failed or was OOM-killed",
    );
    problem(
        (row.frozen.0..=row.frozen.1).contains(&frozen),
        "the follower's frozen time",
    );
    let (timeout, silent) = row.limits;
    problem(
        timeout == 0 || (wall > timeout * 1000 && wall - frozen < timeout * 1000),
        "past its timeout by the wall clock but not by running time",
    );
    problem(
        silent == 0 || frozen > silent * 1000,
        "frozen past its silence",
    );
}

/// Each row, every problem reported at the end.
#[test]
fn rows() {
    // the slice's cgroup, whose OOM kills are read, exists once it runs
    succeed(
        Node::Worker,
        "systemctl start 'system-tribuchet\\x2dagent.slice'",
    );
    // Page cache charged to the slice: a unit in it writes and reads back
    // more than the slice's cap, then exits.
    succeed(
        Node::Worker,
        "systemd-run --wait --collect --quiet --slice='system-tribuchet\\x2dagent.slice' \
         --unit=arb-fill bash -lc 'dd if=/dev/zero of=/var/tmp/fill bs=1M count=2600 status=none \
         && sync && cat /var/tmp/fill > /dev/null'",
    );
    let mut problems = Vec::new();
    for row in &ROWS {
        run(row, &mut problems);
    }
    assert!(problems.is_empty(), "{problems:#?}");
}
