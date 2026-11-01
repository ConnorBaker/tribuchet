//! The `held` phase (nix/tests/phase-held.nix): a worker restarted while a
//! running build's agent is busy in a long Finish or Cleanup, or stopped
//! from outside Tribuchet. The test makes the work long by stopping the
//! userns helper the agent waits on (SIGSTOP from the test, never the agent
//! itself): the agent runs, as in a long make-readable or remove. A build
//! whose agent does not answer Adopt within AGENT_ANSWER (7 s) is held until
//! it does, its client told it waits; an agent busy in Finish answers.

use std::thread;
use std::time::{Duration, Instant};

use crate::harness::{
    Node, count, ended, more_than, run, submit, succeed, wait_until_succeeds, write_file,
};

const AGENTS: &str = "/sys/fs/cgroup/system.slice/system-tribuchet\\x2dagent.slice";
const WORKER: &str = "tribuchet-worker";
const HELD: &str = "holding the build until it does";
const ADOPTED: &str = "adopted running build";
const FINISHED: &str = "adopted finished build awaiting delivery";
const NOTE: &str = "does not answer; waiting for it";
const DOWN: &str = "not leasing builds to it";
const JOINED: &str = "build agents joined";

/// Stops (SIGSTOP) the agent's userns helper doing `$1` as soon as it runs,
/// and records its pids. The pattern is split so this script's own command
/// line never matches it.
const CATCH: &str = r#"op=$1
pat="__agent""_fs [^ ]* $op "
until pkill -STOP -f "$pat"; do :; done
pgrep -f "$pat" > /tmp/caught-$op"#;

fn launch(name: &str) {
    submit(
        &format!("held-{name}"),
        &format!("nix-build /etc/tt/held-{name}.nix --no-out-link"),
    );
}

fn seen(pattern: &str) -> i64 {
    count(Node::Worker, WORKER, pattern)
}

/// Whether the worker logs `pattern` more than `n` times within `secs`.
fn seen_within(pattern: &str, n: i64, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while seen(pattern) <= n {
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_secs(1));
    }
    true
}

/// Whether build `name` ended with 0 and its output is complete.
fn ok(name: &str) -> bool {
    let out = format!("test -e $(tail -n 1 /tmp/held-{name}.out)/ok");
    ended(&format!("held-{name}"), 300) == "0" && run(Node::Hub, &out).code == 0
}

fn told(name: &str) -> bool {
    run(Node::Hub, &format!("grep -q '{NOTE}' /tmp/held-{name}.out")).code == 0
}

fn catch(op: &str) {
    write_file(Node::Worker, "/tmp/catch.sh", CATCH);
    succeed(
        Node::Worker,
        &format!("systemd-run --unit=catch-{op} bash -l /tmp/catch.sh {op}"),
    );
}

/// Waits for `catch(op)` to have stopped a helper, and shows the agents'
/// processes: the helper stopped (T), its agent not.
fn caught(op: &str) {
    wait_until_succeeds(
        Node::Worker,
        &format!("! systemctl is-active -q catch-{op} && test -s /tmp/caught-{op}"),
        180,
    );
    let ps = succeed(
        Node::Worker,
        "ps -eo pid,ppid,stat,args | grep -E '[t]ribuchet agent|[_]_agent_fs' | cut -c1-160",
    );
    eprintln!("[held] {op} helper stopped:\n{ps}");
}

fn release(op: &str) {
    succeed(Node::Worker, &format!("kill -CONT $(cat /tmp/caught-{op})"));
}

fn restart() -> Instant {
    succeed(Node::Worker, &format!("systemctl restart {WORKER}"));
    Instant::now()
}

fn show(name: &str) {
    let lines = succeed(
        Node::Worker,
        &format!(
            "journalctl -a -o short-precise -u {WORKER} | sed 's/\\x1b\\[[0-9;]*m//g' \
             | grep -E 'holding|adopted|answered at last|not leasing|joined|\
             handover|registered|re-adopting' \
             | cut -c1-400 || true"
        ),
    );
    let out = succeed(Node::Hub, &format!("cut -c1-300 /tmp/held-{name}.out"));
    eprintln!("[held] {name}: worker journal:\n{lines}\n[held] {name}: client:\n{out}");
}

/// A worker restarted while F's agent is in Finish, its make-readable
/// helper stopped: the agent still answers Adopt, the Finish on a thread
/// of its own, so F is adopted at once, held never and its client told
/// nothing; F succeeds once the helper goes on.
#[test]
fn a_busy_finish() {
    let (held0, adopted0) = (seen(HELD), seen(ADOPTED));
    catch("make-readable");
    launch("F");
    caught("make-readable");
    let t = restart();
    let either = format!(
        "{} || {}",
        more_than(WORKER, HELD, held0),
        more_than(WORKER, ADOPTED, adopted0)
    );
    wait_until_succeeds(Node::Worker, &either, 60);
    eprintln!(
        "[held] F: held or adopted {:?} after the restart",
        t.elapsed()
    );
    // Long past AGENT_ANSWER and two retries, the helper still stopped.
    succeed(Node::Worker, "sleep 15");
    let while_busy = (seen(HELD) > held0, seen(ADOPTED) > adopted0, told("F"));
    // Stopped, not gone: no kill sweep of another call killed it.
    let alive = run(Node::Worker, "kill -0 $(cat /tmp/caught-make-readable)").code == 0;
    assert!(alive, "a kill sweep killed Finish's helper");
    release("make-readable");
    wait_until_succeeds(Node::Worker, &more_than(WORKER, ADOPTED, adopted0), 60);
    eprintln!("[held] F: adopted {:?} after the restart", t.elapsed());
    let done = ok("F");
    show("F");
    assert_eq!(
        (while_busy, done),
        ((false, true, false), true),
        "F (held, adopted, client told) while its agent was busy in Finish; F succeeded"
    );
}

/// A worker restarted while S's agent is stopped (SIGSTOP from outside):
/// S is held, its client told, adopted once the agent is continued.
#[test]
fn b_stopped_agent() {
    let (held0, adopted0) = (seen(HELD), seen(ADOPTED));
    let builds = format!("find '{AGENTS}' -mindepth 2 -maxdepth 2 -name 'build-*'");
    succeed(Node::Worker, &format!("{builds} > /tmp/builds-before"));
    launch("S");
    // S's build cgroup: the one not there before it was launched.
    let new = format!("{builds} | grep -vxF -f /tmp/builds-before");
    wait_until_succeeds(Node::Worker, &new, 120);
    let cg = succeed(Node::Worker, &new);
    let agent = cg
        .lines()
        .next()
        .unwrap()
        .rsplit('/')
        .nth(1)
        .unwrap()
        .to_owned();
    let signal = |s: &str| format!("systemctl kill --kill-whom=main --signal={s} '{agent}'");
    succeed(Node::Worker, &signal("SIGSTOP"));
    let t = restart();
    wait_until_succeeds(Node::Worker, &more_than(WORKER, HELD, held0), 60);
    eprintln!("[held] S: held {:?} after the restart", t.elapsed());
    succeed(Node::Worker, "sleep 15");
    let while_stopped = (seen(ADOPTED) > adopted0, told("S"));
    succeed(Node::Worker, &signal("SIGCONT"));
    wait_until_succeeds(Node::Worker, &more_than(WORKER, ADOPTED, adopted0), 60);
    eprintln!("[held] S: adopted {:?} after the restart", t.elapsed());
    let done = ok("S");
    show("S");
    assert_eq!(
        (while_stopped, done),
        ((false, true), true),
        "S (adopted, client told) while its agent was stopped; S succeeded"
    );
}

/// A worker restarted while C's agent is in Cleanup, its remove helper
/// stopped: C's result is on disk (persist_finished) before Cleanup, so the
/// next worker delivers it, holding nothing, while the agent still cleans.
/// The agent, unanswering, is left out of the pool, and rejoins once the
/// helper goes on and the Cleanup ends.
#[test]
fn c_busy_cleanup() {
    let (held0, finished0) = (seen(HELD), seen(FINISHED));
    let (down0, joined0) = (seen(DOWN), seen(JOINED));
    catch("remove");
    launch("C");
    caught("remove");
    let t = restart();
    wait_until_succeeds(Node::Worker, &more_than(WORKER, FINISHED, finished0), 60);
    eprintln!("[held] C: '{FINISHED}' {:?} after the restart", t.elapsed());
    let done = ok("C");
    eprintln!("[held] C: client done {:?} after the restart", t.elapsed());
    let held = seen(HELD) > held0;
    let down = seen(DOWN) > down0;
    release("remove");
    // The rescan probes agents left out every 10 s.
    let rejoined = seen_within(JOINED, joined0, 60);
    show("C");
    assert_eq!(
        (held, done, down, rejoined),
        (false, true, true, true),
        "C held; C succeeded with its agent still in Cleanup; the agent left out, then rejoined"
    );
}
