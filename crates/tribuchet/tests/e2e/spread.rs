//! The `spread` phase (nix/tests/phase-spread.nix): a heavy build, which
//! declares big-parallel, submitted while another runs goes to the other
//! worker, which has idle agents. Two workers, the worker VM's and one on
//! the hub VM, each with two agents.

use crate::harness::{Node, ended, journal, submit, succeed, wait_until_succeeds};

/// Rounds of: submit `a`; once it is dispatched, submit `b`.
const ROUNDS: usize = 3;

/// The hub's dispatch line for the build named `drv`.
fn dispatch_line(drv: &str) -> String {
    succeed(
        Node::Hub,
        &format!(
            "{} | grep 'dispatching build' | grep -F 'drv=\"{drv}\"'",
            journal("tribuchet-hub")
        ),
    )
}

/// The worker a dispatch line names.
fn worker(line: &str) -> String {
    let worker = line.split("worker=\"").nth(1).expect("no worker field");
    worker.split('"').next().unwrap().to_owned()
}

fn launch(name: &str) {
    submit(
        name,
        &format!("nix-build /etc/tt/heavy.nix --argstr name {name} --no-out-link"),
    );
}

// Its name has no `build_`, which the builds phase selects.
#[test]
fn a_second_heavy_goes_to_the_other_worker() {
    succeed(
        Node::Hub,
        "install -d /etc/tribuchet/tls \
         && install -m 0644 /root/ca/worker.crt /root/ca/ca.crt /etc/tribuchet/tls/ \
         && install -m 0600 /root/ca/worker.key /etc/tribuchet/tls/ \
         && systemctl start tribuchet-worker",
    );
    // Both sessions have declared their free slots, so the hub knows
    // the other worker's idle agents when a is dispatched.
    let declared = format!(
        "[ $({} | grep 'capacity declared' | grep -o 'session=[0-9]*' | sort -u | wc -l) -ge 2 ]",
        journal("tribuchet-hub")
    );
    wait_until_succeeds(Node::Hub, &declared, 60);
    for round in 0..ROUNDS {
        let [a, b] = ["a", "b"].map(|n| format!("spread-{round}-{n}"));
        launch(&a);
        let dispatched = format!(
            "{} | grep 'dispatching build' | grep -qF 'drv=\"{a}\"'",
            journal("tribuchet-hub")
        );
        wait_until_succeeds(Node::Hub, &dispatched, 60);
        let first = dispatch_line(&a);
        assert!(
            first.contains("heavy=true") && first.contains("idle_elsewhere=1"),
            "round {round}: {first}"
        );
        launch(&b);
        assert_eq!(
            [ended(&a, 120), ended(&b, 120)],
            ["0", "0"],
            "round {round}"
        );
        let to = [worker(&first), worker(&dispatch_line(&b))];
        println!("[spread] round {round}: {to:?}");
        assert_ne!(
            to[0], to[1],
            "round {round}: both heavy builds on one worker"
        );
    }
}
