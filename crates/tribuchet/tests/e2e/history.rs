//! The `history` phase (nix/tests/phase-history.nix): what builds of a
//! pname used steers the next one, whatever it declares, from the hub's
//! build record across a restart: learned before it,
//! folded from the record after. Two workers, the worker VM's and one on
//! the hub VM, three agents on two cores each. Once a `hot` build (both
//! cores busy) and a `nap` (no core) have run, the hub is restarted,
//! then killed; a `hot` build then runs on one worker, two naps go to the
//! other, which that hot build leaves less full, and a second hot build
//! follows them there, the naps using no core. Without history, or
//! without its record, every build is the prior one core: the second nap
//! ties the two workers, and goes to either, and wherever it went the
//! second hot build fails the test.

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

/// The record, where hub.toml's [placement] record puts it
/// (nix/tests/phase-history.nix).
const RECORD: &str = "/var/lib/tt-history/record.sqlite";

/// Wait for the hub to be running again, then for the fold of its
/// record in its journal since it started; that line.
fn folded() -> String {
    wait_until_succeeds(Node::Hub, "systemctl is-active tribuchet-hub.service", 60);
    let current = "journalctl -a _SYSTEMD_INVOCATION_ID=$(systemctl show -p InvocationID \
                   --value tribuchet-hub) | sed 's/\\x1b\\[[0-9;]*m//g' | grep 'build record folded'";
    wait_until_succeeds(Node::Hub, current, 60);
    succeed(Node::Hub, current)
}

/// Restart the hub, which commits its record as it stops, and the
/// next folds both runs; then kill it, and the next folds them
/// still: a committed row survives a kill.
fn restart() {
    succeed(Node::Hub, "systemctl restart tribuchet-hub.service");
    let line = folded();
    assert!(
        line.contains(&format!("path={RECORD} rows=2 keys=2 ")),
        "{line}"
    );
    succeed(Node::Hub, "systemctl kill -s KILL tribuchet-hub.service");
    wait_until_succeeds(
        Node::Hub,
        "[ $(systemctl show -p NRestarts --value tribuchet-hub.service) -ge 1 ]",
        60,
    );
    let line = folded();
    assert!(
        line.contains(&format!("path={RECORD} rows=2 keys=2 ")),
        "{line}"
    );
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
    restart();
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
