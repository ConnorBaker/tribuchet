//! The `pairs` phase: the CPU arbiter makes no pair of builds fail, on the
//! real kernel's freezer and memory accounting. Each row is a leader (the
//! older build) and a follower on a 4-core worker whose agents' slice is
//! capped and swaps to zram (nix/tests/phase-pairs.nix). What the arbiter
//! decides from what it reads is its unit tests' (worker/arbiter/memory.rs,
//! tests.rs; worker/build/agent_exec.rs for the clocks).

use crate::harness::{
    Node, count, ended, journal, more_than, submit, succeed, succeeds, wait_until_succeeds,
};

const SLICE: &str = "/sys/fs/cgroup/system.slice/system-tribuchet\\x2dagent.slice";

struct Row {
    name: &'static str,
    /// The leader's and the follower's workloads (phase-pairs.nix).
    work: [&'static str; 2],
    /// worker.toml's build-timeout-secs and max-silent-time-secs, 0 for none.
    limits: (i64, i64),
    /// The follower's frozen_ms: at least, at most. Where at least one is
    /// asked for, the leader keeps every CPU until the follower's cgroup
    /// has counted that long in one freeze, then is stopped.
    frozen: (i64, i64),
}

const ROWS: [Row; 2] = [
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
    // follower, frozen until the leader ends (9 s by its cgroup's count,
    // however late the freeze), passes 5 s inside its timeout.
    Row {
        name: "deadline",
        work: ["K", "D"],
        limits: (30, 8),
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
             {{ printf 'build-timeout-secs = %d\\nmax-silent-time-secs = %d\\n' {timeout} {silent}; \
                cat /run/arb/base.toml; }} > /run/arb/worker.toml && \
             cp --remove-destination /run/arb/worker.toml /etc/tribuchet/worker.toml"
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

/// The value of `key`=VALUE in a log line, unquoted.
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|w| w.strip_prefix(key)?.strip_prefix('='))
        .map(|v| v.trim_matches('"'))
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
    let n = |key| field(&line, key).and_then(|v| v.parse().ok()).unwrap_or(-1);
    (n("frozen_ms"), n("wall_ms"))
}

/// The id and the build dir of the build last started on the worker.
fn last_started() -> (String, String) {
    let line = succeed(
        Node::Worker,
        &format!(
            "{} | grep 'builder started on agent' | tail -n 1",
            journal("tribuchet-worker")
        ),
    );
    let f = |key| field(&line, key).unwrap_or_default().to_owned();
    (f("id"), f("scratch"))
}

/// Waits until build `id`'s cgroup has counted `ms` frozen, by its own
/// frozen_usec, with no thaw since its freeze was asked: false if it is
/// thawed first or not frozen that long within two minutes.
fn held_frozen(id: &str, ms: i64) -> bool {
    let wait = format!(
        "u=0; until [ \"$u\" -ge {us} ]; do \
           [ \"$u\" -gt 0 ] && [ \"$(cat \"$0/cgroup.freeze\")\" = 0 ] && exit 1; sleep 0.1; \
           while read -r k v; do [ \"$k\" = frozen_usec ] && u=$v; done < \"$0/cgroup.stat.local\"; \
         done; [ \"$(cat \"$0/cgroup.freeze\")\" = 1 ]",
        us = ms * 1000
    );
    let cmd = format!("timeout 120 bash -c '{wait}' '{SLICE}'/*/build-{id}");
    succeeds(Node::Worker, &cmd)
}

/// Runs `row`; any problem is pushed onto `problems`.
fn run(row: &Row, problems: &mut Vec<String>) {
    limits(row.limits);
    let kills = oom_kills();
    let names = row.work.map(|w| format!("{}-{w}", row.name));
    // Each build first alone, as a no-op (N): the hub admits a pair by
    // the memory history expects, and both must fit the slice to co-run,
    // the arbiter's subject. Their own keys, so learned, expect almost
    // none, whatever another row taught their class. That relies on the
    // no-op's peak (its shell's) not being 0: a run measured as 0 teaches
    // nothing (Run::of), which the check of each dispatch below catches.
    for name in &names {
        let usage = count(Node::Hub, "tribuchet-hub", "build usage");
        succeed(
            Node::Hub,
            &format!(
                "nix-build /etc/tt/pair.nix --argstr name {name} --argstr work N --no-out-link"
            ),
        );
        let learned = more_than("tribuchet-hub", "build usage", usage);
        wait_until_succeeds(Node::Hub, &learned, 120);
    }
    let mut started = Vec::new();
    for (name, work) in names.iter().zip(row.work) {
        let before = count(Node::Worker, "tribuchet-worker", "builder started on agent");
        submit(
            name,
            &format!(
                "nix-build /etc/tt/pair.nix --argstr name {name} --argstr work {work} --no-out-link"
            ),
        );
        // Started, and a second on: the next build's start is later.
        wait_until_succeeds(
            Node::Worker,
            &more_than("tribuchet-worker", "builder started on agent", before),
            120,
        );
        started.push(last_started());
        succeed(Node::Hub, "sleep 1");
    }
    // How long the follower is frozen is the leader's to hold, not the
    // wall clock's: the arbiter freezes it some seconds into its run
    // (worker/arbiter/policy.rs), how many depending on the host's load.
    let held = row.frozen.0 == 0 || held_frozen(&started[1].0, row.frozen.0);
    // Stopping the leader only shortens the row: held is its verdict. A
    // leader that has already ended (at its own deadline) has no build dir.
    if row.frozen.0 > 0 {
        succeeds(Node::Worker, &format!("touch '{}/stop'", started[0].1));
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
        held && (row.frozen.0..=row.frozen.1).contains(&frozen),
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
    for name in &names {
        let line = succeed(
            Node::Hub,
            &format!(
                "{} | grep 'dispatching build' | grep -F 'drv=\"{name}\"' | tail -n 1 || true",
                journal("tribuchet-hub")
            ),
        );
        problem(
            line.contains("learned=\"key\""),
            "dispatched not by its key's own runs (the no-op taught none)",
        );
    }
}

/// Each row, every problem reported at the end.
#[test]
fn rows() {
    // the slice's cgroup, whose OOM kills are read, exists once it runs
    succeed(
        Node::Worker,
        "systemctl start 'system-tribuchet\\x2dagent.slice'",
    );
    let mut problems = Vec::new();
    for row in &ROWS {
        run(row, &mut problems);
    }
    assert!(problems.is_empty(), "{problems:#?}");
}
