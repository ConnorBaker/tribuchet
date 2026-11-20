//! Who asked for a build, as the hub logs it.

use crate::harness::{Node, assert_journal, succeed, write_echo_deriv};

#[test]
fn build_names_its_client() {
    // attach names the nix-daemon connection that runs it, by the hub's
    // boot, the connection's pid and its start time, and the hub logs it.
    write_echo_deriv("/root/client.nix", "tt-client", "tribuchet-client", "named");
    succeed(Node::Hub, "nix-build /root/client.nix --no-out-link");
    let boot = succeed(Node::Hub, "cat /proc/sys/kernel/random/boot_id");
    let owner = format!(
        "drv=\"tt-client\" owner=\"{}/[0-9]\\+/[0-9]\\+\"",
        boot.trim()
    );
    assert_journal(Node::Hub, "tribuchet-hub", &owner);
}
