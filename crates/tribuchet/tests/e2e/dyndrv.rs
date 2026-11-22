//! The `dyndrv` phase (nix/tests/phase-dyndrv.nix): dynamic-derivation
//! producers (builder-rpc-v0) built through Tribuchet, the hub's Nix being
//! the farm's. In order, in one test, since each step sets the workers up
//! for the next (DESIGN.md, "Dynamic-derivation producers": R1, DD8a, OQ1):
//!
//! - Step 8: both workers advertise builder-rpc-v0. Under a scripted load,
//!   a producer whose build.json carries drvPath and one whose does not are
//!   placed alike, and only the first has a row in the record's drv_path
//!   table. Where the worker chosen is not the hub's own, attach's fetch of
//!   the listener is refused, "no such run on the local worker" (DD12):
//!   the run expires, a loss in transport (DD10), never counted against
//!   the run, and is requeued; with that worker stopped, it runs on the
//!   hub's and exits 0.
//! - Step 1: a producer builds on the hub's worker, its output submitted
//!   to the client's store.
//! - Step 9: a producer's run is lost after it reached the client: the hub
//!   sends no second assignment (DD8a) and answers 222; the client's Nix
//!   rebuilds it itself, without a duplicate submission, and its build
//!   exits 0.
//! - Step 9b (OQ1, decided (c)): a producer whose client's Nix cannot
//!   build it itself is refused by attach before anything is submitted.
//! - DD1b: a fixed-output producer. The probe first: the hub's own Nix,
//!   stock, builds one itself, sandboxed (no external builder); then the
//!   same through Tribuchet, which must answer as stock Nix did.

use crate::harness::{
    Node, count, ended, journal, run, submit, succeed, wait_journal, wait_until_succeeds,
};

const HUB: &str = "tribuchet-hub";

/// The hub's dispatch lines for the build named `drv`.
fn dispatches(drv: &str) -> Vec<String> {
    let lines = succeed(
        Node::Hub,
        &format!(
            "{} | grep 'dispatching build' | grep -F 'drv=\"{drv}\"' || true",
            journal(HUB)
        ),
    );
    lines.lines().map(str::to_owned).collect()
}

/// The worker a dispatch line names.
fn worker_of(line: &str) -> String {
    let worker = line.split("worker=\"").nth(1).expect("no worker field");
    worker.split('"').next().unwrap().to_owned()
}

/// A producer named `name`, built in the background as unit `name`.
fn produce(name: &str, args: &str) {
    submit(
        name,
        &format!("nix-build /etc/tt/producer.nix --argstr name {name} {args} --no-out-link"),
    );
}

/// The capacity a worker's session declares when idle (both workers here
/// are alike): the first `capacity declared` line's.
fn capacity() -> usize {
    let line = succeed(
        Node::Hub,
        &format!(
            "{} | awk '/capacity declared/ && !seen {{ print; seen = 1 }}'",
            journal(HUB)
        ),
    );
    let field = line.split("capacity=").nth(1).expect("no capacity field");
    field.split(' ').next().unwrap().parse().unwrap()
}

/// Fill the capacity of the worker that serves builds now with builds
/// sleeping `secs`, each named `name-N`; returns once all are dispatched.
fn load(name: &str, secs: u32) {
    let slots = capacity();
    for n in 0..slots {
        submit(
            &format!("{name}-{n}"),
            &format!(
                "nix-build /etc/tt/load.nix --argstr name {name}-{n} --argstr seconds {secs} --no-out-link"
            ),
        );
    }
    for n in 0..slots {
        let drv = format!("{name}-{n}");
        wait_until_succeeds(
            Node::Hub,
            &format!(
                "{} | grep 'dispatching build' | grep -F 'drv=\"{drv}\"' > /dev/null",
                journal(HUB)
            ),
            120,
        );
    }
}

/// The record's drv_path rows naming a build of `drv`.
fn drv_path_rows(drv: &str) -> i64 {
    let q = format!("SELECT count(*) FROM drv_path WHERE drv_path LIKE '%-{drv}.drv'");
    let n = succeed(
        Node::Hub,
        &format!("sqlite3 -readonly /var/lib/tribuchet/record.sqlite \"{q}\""),
    );
    n.trim().parse().unwrap()
}

fn start_hub_worker() {
    succeed(
        Node::Hub,
        "install -d /etc/tribuchet/tls \
         && install -m 0644 /root/ca/worker.crt /root/ca/ca.crt /etc/tribuchet/tls/ \
         && install -m 0600 /root/ca/worker.key /etc/tribuchet/tls/ \
         && systemctl start tribuchet-worker",
    );
}

/// The sessions that have declared their capacity so far.
fn sessions_declared() -> String {
    format!(
        "{} | grep 'capacity declared' | grep -o 'session=[0-9]*' | sort -u | wc -l",
        journal(HUB)
    )
}

/// Both workers' sessions declared their capacity (their agents idle).
fn both_declared() {
    wait_until_succeeds(
        Node::Hub,
        &format!("[ $({}) -ge 2 ]", sessions_declared()),
        60,
    );
}

/// The first dispatch line of the build named `drv`, once there is one.
fn first_dispatch(drv: &str) -> String {
    let pattern = format!("dispatching build.*drv=\"{drv}\"");
    wait_journal(Node::Hub, HUB, &pattern, 120);
    dispatches(drv).swap_remove(0)
}

/// What the hub logs as it requeues a producer whose listener reached no
/// attach (Error::HandoffExpired).
const EXPIRED: &str = "requeueing build: no attach fetched the producer";

/// Step 8: under `case`'s load, placed on `on`; the producer with drvPath
/// and the one without are placed alike, and only the first is recorded
/// with its drvPath. Placed on the worker VM, neither listener reaches an
/// attach: each run expires, a loss in transport (DD10), and is requeued;
/// with that worker stopped, each runs on the hub's once its load ended.
fn placed_alike(case: &str, on: &str) {
    let with = format!("p8-{case}-with");
    let path = succeed(
        Node::Hub,
        &format!("nix-instantiate /etc/tt/producer.nix --argstr name {with}"),
    );
    succeed(
        Node::Hub,
        &format!("echo -n '{}' > /run/tt-drvpath", path.trim()),
    );
    let expired = count(Node::Hub, HUB, EXPIRED);
    produce(&with, "");
    let placed_with = worker_of(&first_dispatch(&with));
    if on == "hub" {
        ended(&with, 300);
    }
    succeed(Node::Hub, "rm -f /run/tt-drvpath");
    let without = format!("p8-{case}-without");
    produce(&without, "");
    let placed = [placed_with, worker_of(&first_dispatch(&without))];
    println!("[dyndrv] step 8 {case}: placed {placed:?}");
    if on != "hub" {
        let more = format!(
            "[ $({} | grep -c '{EXPIRED}') -ge {} ]",
            journal(HUB),
            expired + 2
        );
        wait_until_succeeds(Node::Hub, &more, 300);
        succeed(Node::Worker, "systemctl stop tribuchet-worker");
    }
    let rcs = [&with, &without].map(|p| ended(p, 900));
    println!("[dyndrv] step 8 {case}: exit {rcs:?}");
    for p in [&with, &without] {
        let out = succeed(Node::Hub, &format!("cat /tmp/{p}.out"));
        let local = can_build_locally(p);
        println!("[dyndrv] step 8 {p}: canBuildLocally {local:?}\n{out}");
    }
    assert_eq!(placed[0], placed[1], "{case}: placed differently");
    assert_eq!(placed[0], on, "{case}: placed elsewhere");
    assert_eq!(rcs, ["0", "0"], "{case}: exits");
    assert_eq!(
        [drv_path_rows(&with), drv_path_rows(&without)],
        [1, 0],
        "{case}: drv_path recorded"
    );
    if on != "hub" {
        let out = succeed(Node::Hub, &format!("cat /tmp/{with}.out"));
        assert!(out.contains("no such run on the local worker"), "{out}");
        let lost = count(Node::Hub, HUB, "build lost without a resume");
        assert_eq!(lost, 0, "{case}: an expiry counted against the run");
        declared_after(|| drop(succeed(Node::Worker, "systemctl start tribuchet-worker")));
    }
}

/// Run `start`, and wait until the session it opens declared its capacity.
fn declared_after(start: impl FnOnce()) {
    let before = succeed(Node::Hub, &sessions_declared());
    start();
    let more = format!("[ $({}) -gt {} ]", sessions_declared(), before.trim());
    wait_until_succeeds(Node::Hub, &more, 60);
}

fn step8() {
    start_hub_worker();
    both_declared();
    // The worker VM's agents busy: the producers go to the hub's worker.
    succeed(Node::Hub, "systemctl stop tribuchet-worker");
    load("load-a", 120);
    declared_after(start_hub_worker);
    placed_alike("a", "hub");
    // The worker VM's agents free again (its worker adopted load-a's runs
    // across the restart): both producers of b find them free.
    for n in 0..capacity() {
        assert_eq!(ended(&format!("load-a-{n}"), 300), "0", "load-a-{n}");
    }
    // The hub's agents busy while both producers of b are first
    // dispatched: they go to the worker VM's, and reach no attach.
    succeed(Node::Worker, "systemctl stop tribuchet-worker");
    load("load-b", 240);
    declared_after(|| drop(succeed(Node::Worker, "systemctl start tribuchet-worker")));
    placed_alike("b", "worker");
    for n in 0..capacity() {
        assert_eq!(ended(&format!("load-b-{n}"), 900), "0", "load-b-{n}");
    }
}

fn step1() {
    produce("p1", "");
    assert_eq!(
        ended("p1", 300),
        "0",
        "{}",
        succeed(Node::Hub, "cat /tmp/p1.out")
    );
    let out = succeed(Node::Hub, "tail -n 1 /tmp/p1.out");
    succeed(
        Node::Hub,
        &format!("grep -q 'made by a producer: p1' {}/made", out.trim()),
    );
    assert_eq!(dispatches("p1").len(), 1, "p1 dispatched more than once");
    assert_eq!(worker_of(&dispatches("p1")[0]), "hub");
}

/// The build.json the hub's attach got for `drv`, its canBuildLocally.
fn can_build_locally(drv: &str) -> String {
    succeed(
        Node::Hub,
        &format!(
            "{{ grep -l '\"{drv}\"' /var/lib/tt-buildjson/* \
             | xargs -r grep -ho '\"canBuildLocally\": *[a-z]*' | sort -u; }} || true"
        ),
    )
    .trim()
    .replace("\"canBuildLocally\":", "")
    .replace(' ', "")
}

fn step9() {
    produce("p9", "--arg other true --arg seconds 60");
    wait_journal(Node::Hub, HUB, "producer.s run reached its client", 300);
    assert_eq!(can_build_locally("p9"), "true", "step 9's client");
    // Lose the run: the hub's worker goes, its agent and the run with it,
    // and the worker comes back without it.
    succeed(Node::Hub, "systemctl stop tribuchet-worker");
    let agents = succeed(
        Node::Hub,
        "systemctl list-units --state=active --no-legend 'tribuchet-agent@*.service' | awk '{print $1}'",
    );
    for agent in agents.split_whitespace() {
        succeed(Node::Hub, &format!("systemctl kill -s KILL {agent}"));
    }
    start_hub_worker();
    let rc = ended("p9", 600);
    let log = succeed(Node::Hub, "cat /tmp/p9.out");
    println!("[dyndrv] step 9: exit {rc}\n{log}");
    // First, so a hub answering success instead of 222 fails here.
    // The realisation registered is the local build's (DD8a).
    // After the 222, the client's Nix starts p9 a second time, itself: two
    // "building" lines for p9.drv, no "tribuchet: building on" after the
    // second.
    let starts: Vec<usize> = log
        .match_indices("building '")
        .filter(|(i, _)| log[*i..].lines().next().unwrap_or("").contains("-p9.drv'"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(starts.len(), 2, "p9 not rebuilt by the client: {log}");
    assert!(
        !log[starts[1]..].contains("tribuchet: building on"),
        "p9's second build went to Tribuchet: {log}"
    );
    assert_eq!(rc, "0", "the client's build failed");
    assert_eq!(dispatches("p9").len(), 1, "a second assignment of p9");
    assert!(
        count(
            Node::Hub,
            HUB,
            "relayed producer.s run is lost; no fresh run"
        ) > 0
    );
    assert!(count(Node::Hub, HUB, "hub cannot build it; declining") > 0);
    assert!(
        !log.contains("Attempted to submit duplicate output"),
        "{log}"
    );
    let outs = succeed(
        Node::Hub,
        "nix-build /etc/tt/producer.nix --argstr name p9 --arg other true --arg seconds 60 \
         --no-out-link -A out -A other",
    );
    for out in outs.split_whitespace() {
        succeed(Node::Hub, &format!("nix-store --check-validity {out}"));
    }
}

fn step9b() {
    // Requires a feature the hub's Nix lacks (its system-features keep
    // NixOS' defaults, big-parallel among them): its build.json says
    // canBuildLocally false.
    let refused = run(
        Node::Hub,
        "nix-build /etc/tt/producer.nix --argstr name p9b --arg features '[\"tt-elsewhere\"]' \
         --no-out-link 2>&1",
    );
    println!(
        "[dyndrv] step 9b: exit {}\n{}",
        refused.code,
        refused.combined()
    );
    assert_ne!(refused.code, 0, "the producer was built");
    assert_eq!(can_build_locally("p9b"), "false", "step 9b's client");
    assert!(
        refused
            .combined()
            .contains("refuses a dynamic-derivation producer"),
        "{}",
        refused.combined()
    );
    let queued = count(Node::Hub, HUB, "queueing build.*p9b");
    assert_eq!(queued, 0, "the refused producer reached the hub");
    assert!(
        dispatches("p9b").is_empty(),
        "the refused producer was dispatched"
    );
}

/// The NAR hash of the object a fixed-output producer named `name`
/// submits (producer.nix names it `name`-object).
fn submitted_hash(name: &str) -> String {
    let dir = format!("/root/d1b/{name}-object");
    succeed(
        Node::Hub,
        &format!("mkdir -p {dir} && echo 'made by a producer: {name}-object' > {dir}/made"),
    );
    let hash = succeed(
        Node::Hub,
        &format!("nix --extra-experimental-features nix-command hash path {dir}"),
    );
    hash.trim().to_owned()
}

fn d1b() {
    let probe = "fod-probe";
    let cmd = format!(
        "timeout 300 nix-build /etc/tt/producer.nix --argstr name {probe} --argstr hash {} \
         --option external-builders '[]' --no-out-link 2>&1",
        submitted_hash(probe)
    );
    let stock = run(Node::Hub, &cmd);
    println!(
        "[dyndrv] DD1b probe: {cmd}\n[dyndrv] DD1b probe exit {}\n{}",
        stock.code,
        stock.combined()
    );
    assert!(
        dispatches(probe).is_empty(),
        "the probe went through Tribuchet"
    );
    // Bounded as the probe is: a hang is exit 124 on both sides.
    let fod = "fod-producer";
    submit(
        fod,
        &format!(
            "timeout 300 nix-build /etc/tt/producer.nix --argstr name {fod} --argstr hash {} --no-out-link",
            submitted_hash(fod)
        ),
    );
    let rc = ended(fod, 330);
    let log = succeed(Node::Hub, &format!("cat /tmp/{fod}.out"));
    println!("[dyndrv] DD1b through Tribuchet: exit {rc}\n{log}");
    assert_eq!(
        rc,
        stock.code.to_string(),
        "Tribuchet answered unlike stock Nix"
    );
    if rc == "0" {
        let lines = dispatches(fod);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("tribuchet-network"), "{}", lines[0]);
    }
}

/// What the hub's Nix decides canBuildLocally from (OQ1), as the daemon
/// sees it.
fn hub_nix_settings() {
    let shown = succeed(
        Node::Hub,
        "for s in system-features max-jobs experimental-features; do \
         echo \"$s = $(nix --extra-experimental-features nix-command config show $s)\"; done",
    );
    println!("[dyndrv] the hub's Nix:\n{shown}");
}

#[test]
fn producers_through_tribuchet() {
    hub_nix_settings();
    step8();
    // From here only the hub's worker serves producers.
    succeed(Node::Worker, "systemctl stop tribuchet-worker");
    step1();
    step9();
    step9b();
    d1b();
}
