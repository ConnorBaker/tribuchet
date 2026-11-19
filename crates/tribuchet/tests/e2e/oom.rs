//! An agent runs at OOM score -1000 and its builds at 0, which they
//! cannot lower, so the kernel never picks an agent (nix/worker-units.nix,
//! worker/sandbox.rs's unprotect_from_oom).

use crate::harness::{Node, assert_journal, build_grep, succeed};

#[test]
fn build_oom_score() {
    let agent = succeed(
        Node::Worker,
        "systemctl show -P OOMScoreAdjust tribuchet-agent@1.service",
    );
    assert_eq!(agent.trim(), "-1000", "the agent's OOMScoreAdjust");
    build_grep("/etc/tt/oomscore.nix", "adj=0 lower=refused");
    // under an agent, not by the hub's own daemon, whose builds start at 0
    // too
    assert_journal(
        Node::Hub,
        "tribuchet-hub",
        r#"build usage.*drv="tt-oomscore".*worker="worker" exit_code=0"#,
    );
}
