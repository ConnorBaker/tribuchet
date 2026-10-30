//! The `daemon` phase: what the hub does when Nix's real daemon refuses
//! the hub (hub/relay/daemon.rs). Each build has an input only the hub
//! has, so the worker asks for it and the hub queries its daemon for it.
//! Which failure fails a build and which is waited out, by what the hub
//! saw, is the hub's unit tests' (hub/relay/daemon/tests.rs); these give
//! them the real daemon's side: its journal's refusal of the hub's user, a
//! close before the handshake when it cannot fork, and what its journal
//! says of an OOM kill in its unit. Each case sets its daemon up and puts
//! it back; they share the hub's daemon, so they run one at a time.

use crate::harness::{
    Node, assert_journal, bash, count, ended, run, submit, succeed, wait_until_succeeds, write_file,
};

/// Asks for tt-`name` as root on the store itself (the daemon may refuse
/// root), submitted as the unit `name`.
fn ask(name: &str) {
    let expr = format!(
        r#"derivation {{
  name = "tt-{name}";
  system = "x86_64-linux";
  builder = builtins.storePath "{bash}" + "/bin/bash";
  args = [ "-c" "read -r x < ${{builtins.toFile "input-{name}" "{name}"}}; echo $x > $out" ];
}}"#,
        bash = bash(),
    );
    write_file(Node::Hub, &format!("/root/{name}.nix"), &expr);
    submit(
        name,
        &format!("NIX_REMOTE=local nix-build /root/{name}.nix --no-out-link"),
    );
}

/// Its exit status and output, once it ended.
fn outcome(name: &str, secs: u64) -> (String, String) {
    let rc = ended(name, secs);
    (rc, succeed(Node::Hub, &format!("cat /tmp/{name}.out")))
}

/// A daemon that refuses the hub's user, root (allowed-users and
/// trusted-users without it), closes each new connection before the
/// handshake: the build fails, told what the daemon's journal says. Once
/// root is let in, the next build succeeds: each build judges its own.
/// The daemon's workers go too (KillMode=process keeps them across a
/// restart): one left from an earlier case serves the hub's pooled
/// connection as before, and the build succeeded through it.
#[test]
fn refused_user() {
    let dropin = "/run/systemd/system/nix-daemon.service.d";
    succeed(
        Node::Hub,
        &format!(
            "mkdir -p /run/refuse {dropin} && \
             printf 'include /etc/nix/nix.conf\\nallowed-users = nobody\\ntrusted-users =\\n' \
               > /run/refuse/nix.conf && \
             printf '[Service]\\nEnvironment=NIX_CONF_DIR=/run/refuse\\n' > {dropin}/refuse.conf && \
             systemctl daemon-reload && {{ systemctl kill --signal=SIGKILL nix-daemon.service || true; }} \
             && systemctl restart nix-daemon"
        ),
    );
    ask("refused");
    let (rc, log) = outcome("refused", 300);
    assert_ne!(rc, "0", "refused succeeded: {log}");
    assert!(
        log.contains("does not let the hub's user root connect, as its journal says"),
        "{log}"
    );
    assert_journal(
        Node::Hub,
        "nix-daemon",
        "is not allowed to connect to the Nix daemon",
    );

    succeed(
        Node::Hub,
        &format!(
            "rm {dropin}/refuse.conf && systemctl daemon-reload && systemctl restart nix-daemon"
        ),
    );
    ask("fixed");
    let (rc, log) = outcome("fixed", 180);
    assert_eq!(rc, "0", "{log}");
}

/// A daemon that cannot fork a worker (its unit's TasksMax 1) closes each
/// new connection alike, its journal naming no refusal of the hub's user,
/// as the hub says: the build waits and succeeds once the daemon can fork.
#[test]
fn forkless() {
    // Socket-activated: started by a query, so that it has made the
    // threads it starts with (TasksMax counts them) before the cap.
    succeed(
        Node::Hub,
        "NIX_REMOTE=daemon nix-store -q --hash $(readlink -f /run/current-system) && \
         systemctl set-property --runtime nix-daemon.service TasksMax=1",
    );
    // Its workers, the hub's pooled connections' among them, killed (a
    // worker's SIGTERM may be blocked): each new connection needs a fork.
    let pid: u32 = succeed(
        Node::Hub,
        "systemctl show -p MainPID --value nix-daemon.service",
    )
    .trim()
    .parse()
    .unwrap();
    assert!(pid > 1, "nix-daemon's main pid {pid}");
    succeed(Node::Hub, &format!("pkill -KILL -P {pid} || true"));
    let unrefused = "names no refusal of the hub.s user root: waiting";
    let waits = count(Node::Hub, "tribuchet-hub", unrefused);
    ask("forkless");
    succeed(Node::Hub, "sleep 170");
    assert_ne!(
        run(Node::Hub, "test -e /tmp/forkless.rc").code,
        0,
        "ended while no worker could fork: {}",
        succeed(Node::Hub, "cat /tmp/forkless.out")
    );
    assert_journal(Node::Hub, "nix-daemon", "unable to fork");
    let told = count(Node::Hub, "tribuchet-hub", unrefused) - waits;
    assert!(told > 0, "the hub told of no unrefused close");
    succeed(
        Node::Hub,
        "systemctl set-property --runtime nix-daemon.service TasksMax=infinity",
    );
    let (rc, log) = outcome("forkless", 300);
    assert_eq!(rc, "0", "{log}");
}

/// What systemd writes to nix-daemon's journal when the kernel OOM-kills in
/// its unit, read as the hub reads it: the first line of OOM in
/// hub/relay/daemon/tests.rs, where a drop then waits and evidence.rs reads
/// no crash, and no daemon worker that crashed. The hog runs at OOM score 1000.
#[test]
fn oom_kill_in_the_daemons_unit() {
    let since = succeed(Node::Hub, "date +%s");
    succeed(
        Node::Hub,
        "sysctl -w vm.panic_on_oom=0 && systemctl start nix-daemon.service && \
         systemctl set-property --runtime nix-daemon.service MemoryMax=64M",
    );
    run(
        Node::Hub,
        "bash -c 'echo $$ > /sys/fs/cgroup/system.slice/nix-daemon.service/cgroup.procs \
         && echo 1000 > /proc/self/oom_score_adj && head -c 256M /dev/zero | tail -n 1 >/dev/null'",
    );
    let read = format!(
        "journalctl --quiet --no-pager --output=cat --unit nix-daemon.service --since=@{}",
        since.trim()
    );
    let oom = "nix-daemon.service: The kernel OOM killer killed some processes in this unit.";
    wait_until_succeeds(
        Node::Hub,
        &format!("[ $({read} | grep -cxF '{oom}') -gt 0 ]"),
        30,
    );
    let journal = succeed(
        Node::Hub,
        &format!("systemctl set-property --runtime nix-daemon.service MemoryMax=infinity; {read}"),
    );
    assert!(!journal.contains(" crashed"), "{journal}");
}
