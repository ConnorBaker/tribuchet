//! The `arbiter` phase: the CPU arbiter on the real kernel's freezer, on a
//! 4-core worker (nix/tests/phase-arbiter.nix). No build is marked: every
//! one is subject to it. The oldest parallel build leads while it fills
//! every CPU, and the younger parallel ones freeze, 10 s of any 30 s at most;
//! only a build's own agent, and root, may write its cgroup.freeze; no
//! freeze outlives the worker that made it; a client's timeout counts only
//! its build's running time. Which build the arbiter freezes and when, from
//! what it samples, is its unit tests' (worker/arbiter/policy.rs, bound.rs,
//! tests.rs).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::harness::{
    Node, count, ended, fetch, more_than, run, submit, succeed, wait_until_succeeds,
};

const AGENTS: &str = "/sys/fs/cgroup/system.slice/system-tribuchet\\x2dagent.slice";

/// Asks for arb-`name` as a client of its own, `client` if given, else
/// `nix-build`, submitted as the unit arb-`name`.
fn launch(name: &str, client: Option<&str>) {
    let client = client.unwrap_or("nix-build");
    submit(
        &format!("arb-{name}"),
        &format!("{client} /etc/tt/arb-{name}.nix --no-out-link"),
    );
}

/// Waits up to `secs` for `cmd` to succeed on `node`, or notes `what`.
fn within(problems: &mut Vec<String>, node: Node, cmd: &str, secs: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while run(node, cmd).code != 0 {
        if Instant::now() >= deadline {
            problems.push(what.to_owned());
            return;
        }
        succeed(node, "sleep 1");
    }
}

fn build_cgroups() -> HashSet<String> {
    succeed(
        Node::Worker,
        &format!("ls -d '{AGENTS}'/*/build-* 2>/dev/null || true"),
    )
    .split_whitespace()
    .map(str::to_owned)
    .collect()
}

/// Launches `name` and returns the build cgroup the worker made for it.
fn launch_seen(name: &str, client: Option<&str>) -> String {
    let before = build_cgroups();
    launch(name, client);
    wait_until_succeeds(
        Node::Worker,
        &format!(
            "test $(ls -d '{AGENTS}'/*/build-* 2>/dev/null | wc -l) -gt {}",
            before.len()
        ),
        120,
    );
    let new: Vec<String> = build_cgroups().difference(&before).cloned().collect();
    assert_eq!(new.len(), 1, "{name}: new build cgroups {new:?}");
    // The next build's start, in whole seconds, falls past this one's.
    succeed(Node::Worker, "sleep 1");
    new[0].clone()
}

fn wait_frozen(cgroup: &str) {
    wait_until_succeeds(
        Node::Worker,
        &format!("grep -qx 'frozen 1' '{cgroup}/cgroup.events'"),
        60,
    );
}

/// Whether build `name` ended with 0 and its output says ok.
fn ok(name: &str) -> bool {
    let out = format!("cat $(tail -n 1 /tmp/arb-{name}.out) 2>/dev/null || true");
    ended(&format!("arb-{name}"), 240) == "0" && succeed(Node::Hub, &out).trim() == "ok"
}

/// The monitor's samples (nix/tests/arb-monitor.py) of each NAME=CGROUP,
/// every 20 ms until they are gone or `secs` have passed.
fn monitor(file: &str, secs: u64, cgroups: &[(&str, &str)]) {
    let args: Vec<String> = cgroups.iter().map(|(n, c)| format!("'{n}={c}'")).collect();
    succeed(
        Node::Worker,
        &format!(
            "systemd-run --unit=arb-monitor-{secs} bash -lc 'python3 /etc/tt/arb-monitor.py {secs} {} \
             > {file}'",
            args.join(" ").replace('\'', "'\\''")
        ),
    );
}

/// Its rows, once the monitor has ended.
fn rows(file: &str, secs: u64) -> Vec<Value> {
    wait_until_succeeds(
        Node::Worker,
        &format!("! systemctl is-active -q arb-monitor-{secs}"),
        60,
    );
    let text = fetch(Node::Worker, file);
    eprintln!("{file}: {} bytes", text.len());
    text.lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

/// `name`'s samples: (time, frozen, runnable threads, frozen_usec).
fn samples(rows: &[Value], name: &str) -> Vec<(f64, bool, u64, i64)> {
    rows.iter()
        .filter_map(|r| {
            let s = r.get(name).filter(|s| !s.is_null())?;
            Some((
                r["t"].as_f64()?,
                s["frozen"].as_bool()?,
                s["R"].as_u64()?,
                s["us"].as_i64()?,
            ))
        })
        .collect()
}

/// Who may write build F's cgroup.freeze: its agent, the file's owner,
/// and root. Neither the worker nor L's agent nor F's build can open it
/// to write, nor F's build L's; the worker holds a write-only handle on
/// it, from the agent. Each probe writes 0, freezing nothing. First of the
/// phase, so that no other test's failure leaves it without a slot.
#[test]
fn a_builds_freeze_is_its_agents_to_write() {
    let l: &str = &launch_seen("P1", None);
    let f: &str = &launch_seen("P2", None);
    let uid_of = |path: &str| {
        succeed(Node::Worker, &format!("stat -c %u {path}"))
            .trim()
            .to_owned()
    };
    let pid = |unit: &str| format!("/proc/$(systemctl show -P MainPID '{unit}')");
    let agent = |cgroup: &str| uid_of(&pid(cgroup.rsplit('/').nth(1).unwrap()));
    let (worker, f_agent, l_agent) = (uid_of(&pid("tribuchet-worker")), agent(f), agent(l));
    let f_build = uid_of(&format!("'{f}'"));
    let uids = [&worker, &f_agent, &l_agent, &f_build];
    assert_eq!(
        uids.iter().collect::<HashSet<_>>().len(),
        4,
        "worker, F's and L's agents, F's build: {uids:?}"
    );
    let write = |who: &str, cgroup: &str| {
        run(
            Node::Worker,
            &format!(
                "setpriv --reuid={who} --regid={who} --clear-groups \
                 sh -c 'echo 0 > \"{cgroup}/cgroup.freeze\"'"
            ),
        )
    };
    for (who, uid, (whose, cgroup)) in [
        ("the worker", &worker, ("F's", f)),
        ("L's agent", &l_agent, ("F's", f)),
        ("F's build", &f_build, ("F's", f)),
        ("F's build", &f_build, ("L's", l)),
    ] {
        let o = write(uid, cgroup);
        assert!(
            o.code != 0 && o.combined().contains("Permission denied"),
            "{who} opened {whose} cgroup.freeze to write: {} {}",
            o.code,
            o.combined()
        );
    }
    let owner = uid_of(&format!("'{f}/cgroup.freeze'"));
    assert_eq!(owner, f_agent, "the owner of F's cgroup.freeze");
    // The handle: the worker's fds on F's cgroup.freeze, all write-only.
    let flags = succeed(
        Node::Worker,
        &format!(
            "p={}; for fd in $p/fd/*; do [ \"$(readlink $fd)\" = \"{f}/cgroup.freeze\" ] \
             && awk '/^flags/ {{print $2}}' $p/fdinfo/${{fd##*/}}; done; true",
            pid("tribuchet-worker")
        ),
    );
    let modes: Vec<i64> = flags
        .split_whitespace()
        .filter_map(|o| i64::from_str_radix(o, 8).ok())
        .map(|o| o & 3)
        .collect();
    assert!(
        !modes.is_empty() && modes.iter().all(|&m| m == 1),
        "the worker's fds on F's cgroup.freeze, by access mode: {modes:?}"
    );
    let o = write(&l_agent, l);
    assert_eq!(o.code, 0, "L's agent could not write L's: {}", o.combined());
    assert!(ok("P1") && ok("P2"), "P1 or P2 did not succeed");
}

/// L (oldest) spins 4 threads, then 1; F spins 4; S spins 1; C is F again.
/// F and C freeze while L fills every CPU; L and S never freeze. Once two
/// idle builds take the last slot and wait for one, C, frozen and its
/// client gone, is given up for them (hub/state/clientless.rs) and its
/// cgroup removed; the others succeed.
#[test]
fn leader_freezes_followers() {
    let names = ["L", "F", "S", "C"];
    let cg: Vec<String> = names.iter().map(|n| launch_seen(n, None)).collect();
    let watched: Vec<(&str, &str)> = names
        .iter()
        .copied()
        .zip(cg.iter().map(String::as_str))
        .collect();
    monitor("/tmp/monitor1.jsonl", 150, &watched);
    let queued = count(Node::Hub, "tribuchet-hub", "queueing build");

    wait_frozen(&cg[3]);
    launch_seen("Q1", None);
    launch("Q2", None);
    wait_until_succeeds(
        Node::Hub,
        &more_than("tribuchet-hub", "queueing build", queued + 1),
        60,
    );
    succeed(Node::Hub, "kill $(cat /tmp/arb-C.pid)");
    wait_until_succeeds(Node::Worker, &format!("test ! -e '{}'", cg[3]), 60);

    let done: Vec<bool> = ["L", "F", "S", "Q1", "Q2"].map(ok).to_vec();
    let rows = rows("/tmp/monitor1.jsonl", 150);
    let seen: Vec<_> = names.iter().map(|n| samples(&rows, n)).collect();
    let ever: Vec<bool> = seen.iter().map(|s| s.iter().any(|x| x.1)).collect();
    let most: Vec<i64> = seen
        .iter()
        .map(|s| s.iter().map(|x| x.3).max().unwrap_or(-1))
        .collect();
    assert_eq!(
        (done, ever, [most[0], most[2]]),
        (vec![true; 5], vec![false, true, false, true], [0, 0]),
        "L F S Q1 Q2 succeeded, L F S C ever frozen, L's and S's frozen µs"
    );
}

/// No freeze outlives the worker that made it, however it ends: F, frozen
/// behind L, is thawed once the worker is SIGKILLed and kept down, by its
/// agent as the lease ends (the unit's ExecStopPost cleared), and, its
/// agent stopped, by that ExecStopPost as root. Frozen again as root, F is
/// thawed by the next worker, through the handle its agent sends on
/// adoption, and both succeed under it; it holds F until its agent
/// answers (worker/resume).
#[test]
fn no_freeze_outlives_its_worker() {
    let dropin = "/run/systemd/system/tribuchet-worker.service.d";
    let mut problems = Vec::new();
    for (l, f, agent_stopped) in [("L2", "F2", false), ("L3", "F3", true)] {
        launch_seen(l, None);
        let cg = launch_seen(f, None);
        wait_frozen(&cg);
        let agent = cg.rsplit('/').nth(1).unwrap();
        let signal = |s: &str| format!("systemctl kill --kill-whom=main --signal={s} '{agent}'");
        let (stop, clear, thawer) = if agent_stopped {
            (signal("SIGSTOP"), "", "the worker unit's ExecStopPost")
        } else {
            ("true".to_owned(), "ExecStopPost=\\n", "its agent")
        };
        succeed(
            Node::Worker,
            &format!(
                "{stop} && mkdir -p {dropin} \
                 && printf '[Service]\\nRestart=no\\n{clear}' > {dropin}/down.conf \
                 && systemctl daemon-reload \
                 && systemctl kill --kill-whom=main --signal=SIGKILL tribuchet-worker"
            ),
        );
        let post = succeed(
            Node::Worker,
            "systemctl show -P ExecStopPost tribuchet-worker",
        );
        assert_eq!(
            post.trim().is_empty(),
            !agent_stopped,
            "ExecStopPost: {post}"
        );
        within(
            &mut problems,
            Node::Worker,
            &format!("grep -qx 'frozen 0' '{cg}/cgroup.events'"),
            30,
            &format!("{f} not thawed by {thawer}"),
        );
        wait_until_succeeds(
            Node::Worker,
            "test \"$(systemctl show -P ActiveState tribuchet-worker)\" = failed",
            60,
        );
        // Frozen again as root, as root may leave it: only the next worker
        // thaws it, through the handle its agent sends on adoption.
        succeed(Node::Worker, &format!("echo 1 > '{cg}/cgroup.freeze'"));
        wait_frozen(&cg);
        succeed(
            Node::Worker,
            &format!(
                "rm {dropin}/down.conf && systemctl daemon-reload \
                 && systemctl reset-failed tribuchet-worker && systemctl start tribuchet-worker \
                 && {}",
                signal("SIGCONT")
            ),
        );
        let thawed = format!("until grep -qx 'frozen 0' '{cg}/cgroup.events'; do sleep 0.1; done");
        let next = run(Node::Worker, &format!("timeout 60 bash -c \"{thawed}\"")).code == 0;
        assert!(next, "{f} not thawed by the next worker");
        for n in [l, f] {
            if !ok(n) {
                problems.push(format!("{n} did not succeed after {thawer} thawed {f}"));
            }
        }
    }
    assert!(problems.is_empty(), "{problems:#?}");
}

/// L4 fills every CPU for 42 s while F4 (4 spinners, 60 s of wall time)
/// runs under a client that gives up after 80 s and asks for a timeout of
/// 55 s, which F4's frozen time does not count toward: F4 is frozen no
/// more than 10 s of any 30 s by its cgroup's count, thawed at the bound
/// though L4 still fills every CPU, frozen again, thawed within a sample
/// of L4 leaving slack, and finishes in time.
#[test]
fn the_bound() {
    let l4 = launch_seen("L4", None);
    let f4 = launch_seen("F4", Some("timeout 80 nix-build --option timeout 55"));
    monitor("/tmp/monitor4.jsonl", 200, &[("L4", &l4), ("F4", &f4)]);
    let done = [ok("L4"), ok("F4")];
    let rows = rows("/tmp/monitor4.jsonl", 200);
    let (l, f) = (samples(&rows, "L4"), samples(&rows, "F4"));
    // The most F4's cgroup counted in any 30 s: the bound, a sample and the call.
    let mut j = 0;
    let mut window = -1;
    for s in &f {
        while j + 1 < f.len() && f[j + 1].0 <= s.0 - 30.0 {
            j += 1;
        }
        window = window.max(s.3 - f[j].3);
    }
    // L4's runnable threads at F4's first thaw after a freeze.
    let first = f.iter().position(|s| s.1);
    let thawed = first.and_then(|i| f[i..].iter().find(|s| !s.1));
    let l4_then = thawed.and_then(|t| l.iter().rfind(|s| s.0 <= t.0).map(|s| s.2));
    let freezes = f.windows(2).filter(|w| w[1].1 && !w[0].1).count();
    // L4's last drop below 4 runnable threads: F4 then, and its thaw after.
    let slack = (0..l.len())
        .find(|&i| l[i..].iter().all(|s| s.2 < 4))
        .map(|i| l[i].0);
    let at_slack = slack.and_then(|t| f.iter().rfind(|s| s.0 < t).map(|s| s.1));
    let latency = slack.and_then(|t| f.iter().find(|s| s.0 >= t && !s.1).map(|s| s.0 - t));
    assert_eq!(
        (
            done,
            (9_500_000..=10_500_000).contains(&window),
            l4_then.is_some_and(|r| r >= 4),
            freezes >= 2,
            at_slack,
            latency.is_some_and(|d| d <= 0.4),
        ),
        ([true, true], true, true, true, Some(true), true),
        "L4 and F4 succeeded; F4 frozen {window} µs in a 30 s window; L4 at F4's first thaw \
         {l4_then:?}; F4 frozen {freezes} times, {at_slack:?} at L4's slack, thawed {latency:?} s after"
    );
}

/// A client's timeout and max-silent-time are its build's, counted by the
/// worker: T and U (one spinner, silent, 30 s), asked for with a timeout
/// and a max-silent-time of 5 s, are killed by the worker, not by Nix,
/// whose own messages differ.
#[test]
fn a_clients_limits_are_the_workers_to_count() {
    for (name, limit, why) in [
        ("T", "timeout", "build timed out"),
        ("U", "max-silent-time", "build produced no output for 5s"),
    ] {
        launch(name, Some(&format!("nix-build --option {limit} 5")));
        let killed = !ok(name);
        let out = succeed(Node::Hub, &format!("cat /tmp/arb-{name}.out"));
        let told = out.contains(&format!("tribuchet worker error: {why}"));
        assert!(killed && told, "{name}: {out}");
    }
}
