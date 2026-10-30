//! `build_fod`'s IPv6 steps: the FOD sandbox gets IPv6 exactly when the
//! worker has an IPv6 default route its connects can use (presto-pasta's
//! `netdev::host_has_ipv6_default_route`, whose gives_ipv6 tests in
//! src/netdev.rs at eb64bdc, the pin, are the table of route states), on a
//! worker without one and on one with one. Each build of
//! /etc/tt/fod-ipv6.nix (nix/tests/fod-ipv6.nix) fails with its checks'
//! verdicts.

use crate::harness::{Node, build_grep, succeed};

/// The steps, after `build_fod`'s others: they need the hub's fodsrv, and
/// they change the worker's routes.
pub fn states() {
    let build = |state: &str| {
        build_grep(
            &format!("/etc/tt/fod-ipv6.nix --argstr state {state}"),
            "hello-fod",
        );
    };
    // 4) Link-local IPv6 only on the worker: the sandbox gets no IPv6, an
    //    IPv6 connect fails at once instead of after the guest's SYN
    //    retransmits, and IPv4 still works.
    build("unrouted");

    // 5) With a default route and a global address on its interface (on
    //    the hub's /64) the sandbox gets IPv6, and the worker's failed
    //    connects reach it as errors at once: one fails at connect
    //    (unreachable route), one when neighbor discovery behind the
    //    on-link default route gives up.
    let routed = |verb: &str| {
        format!(
            "ip -6 route {verb} default dev eth1 && \
             ip -6 route {verb} unreachable 2001:db8:ffff::/48 && \
             ip -6 addr {verb} 2001:db8:1::2/64 dev eth1 nodad"
        )
    };
    succeed(Node::Worker, &routed("add"));
    build("routed");
    succeed(Node::Worker, &routed("del").replace(" nodad", ""));
}
