//! A build gets the system-call answers a stock nix-daemon's build gets
//! in the same VM: Nix's filter (extended attributes ENOTSUP,
//! setuid/setgid mode changes EPERM, the i386 and x32 ABIs allowed) and
//! nothing narrower from the agent's unit (systemd's RestrictSUIDSGID
//! fails every openat2 ENOSYS).

use crate::harness::{Node, succeed};

#[test]
fn build_syscalls() {
    let answers = |node, variant| {
        succeed(
            node,
            &format!(
                // a failed build fails here, with its log, not as empty answers
                "out=$(nix-build /etc/tt/syscalls.nix --argstr variant {variant} --no-out-link) && cat \"$out\""
            ),
        )
    };
    let stock = answers(Node::Worker, "stock");
    for line in [
        "openat2 ok",
        "setxattr EOPNOTSUPP",
        "listxattr EOPNOTSUPP",
        "chmod-setuid EPERM",
        "i386-getpid ok",
    ] {
        assert!(stock.lines().any(|l| l == line), "{line} not in:\n{stock}");
    }
    assert_eq!(answers(Node::Hub, "tribuchet"), stock);
}
