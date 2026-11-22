//! The `spread` phase (nix/tests/phase-spread.nix): a heavy big-parallel
//! build submitted while another of its stem runs goes to the other
//! worker, idle and less full with it, by what the earlier rounds taught.
//! Round 0's two are unseen stems (one would hold `b` for its key), at a
//! prior larger than either worker: `b` waits for the largest to empty
//! and is taken with what `a` taught their prefix, on either worker. Two
//! workers, the worker VM's and the hub VM's, each with two agents.

use crate::harness::{Node, ended, journal, submit, succeed, wait_journal, wait_until_succeeds};

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
/// Round 0 assumes the hub VM's worker has a little more memory than
/// the worker VM's: an empty worker admits a build larger than every
/// worker only if it is the largest (`Room::admits`), so `b` waits for
/// `a`'s worker and learns from `a`. Equal, the idle one would take `b`
/// first and `learned="prefix"` would fail with admission right.
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
        let [a, b] = ["a", "b"].map(|n| match round {
            0 => format!("spread-{n}-0"),
            // One stem, `spread-a`: a version-like part is not in it.
            _ => format!("spread-a-{round}{n}"),
        });
        launch(&a);
        wait_journal(
            Node::Hub,
            "tribuchet-hub",
            &format!("dispatching build.*drv=\"{a}\""),
            60,
        );
        let first = dispatch_line(&a);
        assert!(
            first.contains(r#"features="big-parallel""#) && first.contains("idle_elsewhere=1"),
            "round {round}: {first}"
        );
        launch(&b);
        assert_eq!(
            [ended(&a, 120), ended(&b, 120)],
            ["0", "0"],
            "round {round}"
        );
        let second = dispatch_line(&b);
        let to = [worker(&first), worker(&second)];
        println!("[spread] round {round}: {to:?}");
        if round == 0 {
            assert!(second.contains(r#"learned="prefix""#), "{second}");
            continue;
        }
        assert_ne!(
            to[0], to[1],
            "round {round}: both heavy builds on one worker"
        );
    }
}
