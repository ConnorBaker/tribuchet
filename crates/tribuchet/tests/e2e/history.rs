//! The `history` phase (nix/tests/phase-history.nix): what builds of a
//! pname used steers the next one, whatever it declares, from a history
//! seeded from the hub's journal as DESIGN.md says: into a side file
//! while the hub serves, then swapped in. Two workers, the worker VM's
//! and one on the hub VM, three agents on two cores each. Once a `hot`
//! build (both cores busy) and a `nap` (no core) have run, the history is
//! seeded and swapped in; a `hot` build then runs on one worker, two naps
//! go to the other, which that hot build leaves less full, and a second
//! hot build follows them there, the naps using no core. Without
//! history, or without its file, every build is the prior one core: the
//! second nap ties the two workers, and goes to either, and wherever it
//! went the second hot build fails the test.

use crate::harness::{Node, ended, journal, submit, succeed, wait_until_succeeds};

/// The hub's dispatch line for the build named `drv`, once there is one.
fn dispatch_line(drv: &str) -> String {
    let line = format!(
        "{} | grep 'dispatching build' | grep -F 'drv=\"{drv}\" '",
        journal("tribuchet-hub")
    );
    wait_until_succeeds(Node::Hub, &line, 60);
    succeed(Node::Hub, &line)
}

/// The worker a dispatch line names.
fn worker(line: &str) -> String {
    let worker = line.split("worker=\"").nth(1).expect("no worker field");
    worker.split('"').next().unwrap().to_owned()
}

/// Wait until both workers' sessions with the running hub have declared
/// their free slots: in its journal since it last started.
fn both_declared() {
    let declared = "[ $(journalctl -a _SYSTEMD_INVOCATION_ID=$(systemctl show -p InvocationID \
                    --value tribuchet-hub) | sed 's/\\x1b\\[[0-9;]*m//g' | grep 'capacity declared' \
                    | grep -o 'session=[0-9]*' | sort -u | wc -l) -ge 2 ]";
    wait_until_succeeds(Node::Hub, declared, 120);
}

/// The history file, where hub.toml's [placement] history puts it
/// (nix/tests/phase-history.nix), and the side file seeded beside it, of
/// the same stem.
const FILE: &str = "/var/lib/tt-history/placement-history.json";
const SIDE: &str = "/var/lib/tt-history/placement-history.seeded";

/// Seed as seed-history's --help says: while the hub serves, seed SIDE
/// from its journal, which reaches seed-history 10 s late, and meanwhile
/// have the hub place nap-0b (a nap: its pname ends at the 0); then run
/// the help's one swap command, LIVE and SIDE filled in. The hub a
/// worker's connection starts loads SIDE's keys; what the stopped hub
/// saved is moved aside.
fn seed_and_swap() {
    let seed = format!(
        "(sleep 10; journalctl -u tribuchet-hub -o cat) \
         | tribuchet seed-history --history {SIDE} > /tmp/seed.out 2>&1; echo $? > /tmp/seed.rc"
    );
    succeed(
        Node::Hub,
        &format!("systemd-run --unit=seed bash -lc '{seed}'"),
    );
    launch("nap", "nap-0b", 5);
    dispatch_line("nap-0b");
    let out = || succeed(Node::Hub, "cat /tmp/seed.out");
    assert_eq!(ended("seed", 120), "0", "{}", out());
    assert_eq!(ended("nap-0b", 120), "0", "nap-0b, placed while seeding");
    let seeded = succeed(Node::Hub, &format!("sha256sum < {SIDE}"));
    let swap = succeed(
        Node::Hub,
        &format!(
            "tribuchet seed-history --help | sed -n '/^  systemctl stop/,/^  systemctl start/p' \
             | sed 's|LIVE|{FILE}|g; s|SIDE|{SIDE}|g' | tee /tmp/swap.sh"
        ),
    );
    assert_eq!(swap.lines().count(), 3, "the help's swap command: {swap}");
    let cursor = succeed(
        Node::Hub,
        "journalctl -n 1 --show-cursor | sed -n 's/^-- cursor: //p'",
    );
    succeed(Node::Hub, "sh /tmp/swap.sh");
    // systemd stops the hub before its socket (the module's requires and
    // after), so the socket listens through the hub's last save.
    let stops = succeed(
        Node::Hub,
        &format!(
            "journalctl -o cat _PID=1 --after-cursor='{}' \
             | grep -E '^(Stopped|Closed) tribuchet-hub\\.(service|socket)'",
            cursor.trim()
        ),
    );
    let closed_after = stops.contains("\nClosed tribuchet-hub.socket");
    assert!(
        stops.starts_with("Stopped tribuchet-hub.service") && closed_after,
        "{stops}"
    );
    let saved = succeed(Node::Hub, &format!("(cat {FILE}.[0-9]* || true) | wc -c"));
    assert_ne!(saved.trim(), "0", "the hub saved no history as it stopped");
    wait_until_succeeds(Node::Hub, "systemctl is-active tribuchet-hub.service", 60);
    let current = "journalctl -a _SYSTEMD_INVOCATION_ID=$(systemctl show -p InvocationID --value tribuchet-hub)";
    wait_until_succeeds(
        Node::Hub,
        &format!("{current} | grep -q 'placement history loaded'"),
        60,
    );
    let log = succeed(Node::Hub, current);
    assert!(
        log.contains(&format!("path={FILE} keys=2 ")),
        "the hub did not load the seeded keys: {log}"
    );
    let live = succeed(Node::Hub, &format!("sha256sum < {FILE}"));
    assert_eq!(live, seeded, "the hub's file is not the one seeded");
    // With SIDE not a file, here a directory, the swap is refused before
    // anything moves: LIVE stays a file, with no new backup, and the
    // socket is started again.
    let refused = succeed(
        Node::Hub,
        &format!("mkdir {SIDE}.dir && sed 's|{SIDE}|{SIDE}.dir|g' /tmp/swap.sh | sh 2>&1"),
    );
    assert!(refused.contains("NOT SWAPPED"), "{refused}");
    succeed(
        Node::Hub,
        &format!(
            "test -f {FILE} && test $(ls -d {FILE}.[0-9]* | wc -l) = 1 \
             && systemctl is-active tribuchet-hub.socket"
        ),
    );
    wait_until_succeeds(Node::Hub, "systemctl is-active tribuchet-hub.service", 60);
    // With a backup of LIVE of this second and the next 30 already there,
    // the swap is refused: no backup overwritten, LIVE and SIDE kept. A
    // stop longer than 30 s would fail this, never pass it wrongly.
    succeed(
        Node::Hub,
        &format!(
            "echo side > {SIDE} && t=$(date +%s) \
             && for i in $(seq 0 30); do echo kept > {FILE}.$((t + i)); done"
        ),
    );
    let refused = succeed(Node::Hub, "sh /tmp/swap.sh 2>&1");
    assert!(refused.contains("NOT SWAPPED"), "{refused}");
    succeed(
        Node::Hub,
        &format!(
            "test $(grep -lx kept {FILE}.[0-9]* | wc -l) = 31 && grep -qx side {SIDE} \
             && ! grep -qx side {FILE} && systemctl is-active tribuchet-hub.socket"
        ),
    );
    wait_until_succeeds(Node::Hub, "systemctl is-active tribuchet-hub.service", 60);
}

/// Submit build `name` of /etc/tt/`kind`.nix, running `secs` seconds.
fn launch(kind: &str, name: &str, secs: u32) {
    submit(
        name,
        &format!(
            "nix-build /etc/tt/{kind}.nix --argstr name {name} --arg secs {secs} --no-out-link"
        ),
    );
}

// Its name has no `build_`, which the builds phase selects.
#[test]
fn a_learned_hot_one_goes_where_the_naps_are() {
    succeed(
        Node::Hub,
        "install -d /etc/tribuchet/tls \
         && install -m 0644 /root/ca/worker.crt /root/ca/ca.crt /etc/tribuchet/tls/ \
         && install -m 0600 /root/ca/worker.key /etc/tribuchet/tls/ \
         && systemctl start tribuchet-worker",
    );
    both_declared();
    // What a hot build and a nap use, from one of each in the journal.
    launch("hot", "hot-0", 10);
    launch("nap", "nap-0", 10);
    assert_eq!([ended("hot-0", 120), ended("nap-0", 120)], ["0", "0"]);
    seed_and_swap();
    both_declared();
    // 120 s builds: the placements below take seconds, and each counts
    // the builds before it only while they run.
    launch("hot", "hot-1", 120);
    let first = dispatch_line("hot-1");
    let busy = worker(&first);
    for nap in ["nap-1a", "nap-1b"] {
        launch("nap", nap, 120);
        let line = dispatch_line(nap);
        println!("[history] {nap}: {line}");
        assert_ne!(worker(&line), busy, "{nap} went to the hot build's worker");
    }
    launch("hot", "hot-2", 120);
    let line = dispatch_line("hot-2");
    println!("[history] hot-2: {line}");
    assert_ne!(worker(&line), busy, "both hot builds on one worker");
    for name in ["hot-1", "nap-1a", "nap-1b", "hot-2"] {
        assert_eq!(ended(name, 300), "0", "{name}");
    }
    // Last, so that a hub learning nothing fails on where builds went.
    assert!(first.contains(r#"learned="key""#), "{first}");
    // The size the worker registered: its 2 cores, not its 3 agents.
    assert!(first.contains(" cores=2.0"), "{first}");
}
