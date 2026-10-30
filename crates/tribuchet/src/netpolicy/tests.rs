//! Tests of the `[fod-network]` flow policy.

use presto_pasta::{Config, FlowDst};

use super::*;

/// Loopback and unspecified destinations, plain and IPv4-mapped.
const HOST_LOCAL: [&str; 8] = [
    "127.0.0.1",
    "127.1.2.3",
    "0.0.0.0",
    "::1",
    "::",
    "::ffff:127.0.0.1",
    "::ffff:127.255.255.254",
    "::ffff:0.0.0.0",
];

/// The worker's own addresses, and their networks' prefix lengths, in
/// the rows that give it some.
const OWN: [(&str, u8); 3] = [
    ("198.51.100.7", 24),
    ("2001:db8::5", 64),
    ("192.168.1.2", 24),
];

fn own() -> Own {
    OWN.iter().map(|(a, p)| (a.parse().unwrap(), *p)).collect()
}

/// A policy of `default` and `rules`: (action, protocol, destination,
/// ports), as worker.toml writes them.
fn policy(default: &str, rules: &[(&str, &str, &str, &[&str])]) -> NetPolicy {
    let mut toml = format!("default = \"{default}\"\n");
    for (action, proto, dst, ports) in rules {
        toml += &format!(
            "[[rules]]\naction = \"{action}\"\nproto = \"{proto}\"\ndst = \"{dst}\"\nports = {ports:?}\n"
        );
    }
    toml::from_str(&toml).unwrap()
}

/// The policies the table decides with.
fn policies() -> [(&'static str, NetPolicy); 7] {
    [
        // a worker without `[fod-network]`
        ("default", toml::from_str("").unwrap()),
        (
            "allow-host",
            policy(
                "deny",
                &[
                    ("allow", "any", "127.0.0.0/8", &[]),
                    ("allow", "any", "::1", &[]),
                    ("allow", "any", "any", &[]),
                ],
            ),
        ),
        (
            "lan",
            policy("allow", &[("allow", "any", "192.168.1.0/24", &["8765"])]),
        ),
        (
            "private",
            policy("allow", &[("allow", "any", "private", &[])]),
        ),
        (
            "first-match",
            policy(
                "deny",
                &[
                    ("allow", "tcp", "192.0.2.0/24", &["443", "8000-8999"]),
                    ("deny", "any", "192.0.2.7", &[]),
                ],
            ),
        ),
        (
            "keywords",
            policy(
                "allow",
                &[
                    ("deny", "any", "private", &[]),
                    ("deny", "tcp", "2001:db8::/32", &["25"]),
                ],
            ),
        ),
        (
            "tcp-443",
            policy("deny", &[("allow", "tcp", "any", &["443"])]),
        ),
    ]
}

/// Every flow's verdict: (policy, the worker's own addresses given, IP
/// protocol, destination, port, allowed). Past these rows, every
/// loopback or unspecified destination is refused under every policy.
const ROWS: &[(&str, bool, u8, &str, u16, bool)] = &[
    // Public destinations alone, by default; also IPv4-mapped.
    ("default", false, 6, "93.184.216.34", 443, true),
    ("default", false, 6, "2001:4860:4860::8888", 443, true),
    ("default", false, 6, "::ffff:93.184.216.34", 443, true),
    // Not the LAN, a tailnet (Tailscale's CGNAT and ULA ranges), or a
    // cloud metadata service (EC2's IPv4 and IPv6 endpoints).
    ("default", false, 6, "10.1.2.3", 443, false),
    ("default", false, 6, "192.168.1.1", 443, false),
    ("default", false, 6, "100.64.0.1", 443, false),
    ("default", false, 6, "100.100.100.100", 443, false),
    ("default", false, 6, "fd7a:115c:a1e0::1", 443, false),
    ("default", false, 6, "169.254.169.254", 443, false),
    ("default", false, 6, "fd00:ec2::254", 443, false),
    ("default", false, 6, "::ffff:169.254.169.254", 443, false),
    ("default", false, 6, "fe80::1", 443, false),
    // A rule allowing loopback reaches nothing; "any" reaches the rest.
    ("allow-host", false, 6, "93.184.216.34", 8080, true),
    // The worker's own addresses are the worker itself whatever a rule
    // says; another host's are not.
    ("allow-host", true, 6, "198.51.100.7", 50051, false),
    ("allow-host", true, 6, "2001:db8::5", 50051, false),
    ("allow-host", true, 6, "::ffff:198.51.100.7", 50051, false),
    ("allow-host", true, 6, "192.168.1.2", 50051, false),
    ("allow-host", true, 6, "198.51.100.8", 50051, true),
    ("allow-host", true, 6, "2001:db8::6", 50051, true),
    // A host on one of the worker's networks is not public, a LAN
    // peer's or router's global IPv6 included: a rule must allow it.
    ("default", true, 6, "198.51.100.8", 443, false),
    ("default", true, 6, "2001:db8::6", 443, false),
    ("default", true, 6, "198.51.101.8", 443, true),
    ("default", true, 6, "2001:db8:1::6", 443, true),
    ("keywords", true, 6, "2001:db8::6", 443, false),
    // A rule, and only a rule, opens a non-public destination.
    ("lan", false, 6, "192.168.1.1", 8765, true),
    ("lan", false, 6, "192.168.1.1", 8766, false),
    ("lan", false, 6, "192.168.2.1", 8765, false),
    ("private", false, 6, "100.64.0.1", 7437, true),
    // The first rule matching decides; a flow it misses by port,
    // protocol or address falls through to the next, then to `default`.
    ("first-match", false, 6, "192.0.2.7", 443, true),
    ("first-match", false, 6, "192.0.2.7", 8500, true),
    ("first-match", false, 6, "192.0.2.7", 80, false),
    ("first-match", false, 17, "192.0.2.7", 443, false),
    ("first-match", false, 6, "192.0.3.1", 443, false),
    // "private" and IPv6 CIDRs; IPv4-mapped addresses classify by the
    // embedded IPv4.
    ("keywords", false, 6, "10.1.2.3", 80, false),
    ("keywords", false, 6, "fd00::1", 80, false),
    ("keywords", false, 6, "::ffff:10.1.2.3", 80, false),
    ("keywords", false, 6, "::ffff:93.184.216.34", 80, true),
    ("keywords", false, 6, "2001:db8::5", 25, false),
    ("keywords", false, 6, "2001:db8::5", 443, true),
    ("keywords", false, 6, "93.184.216.34", 80, true),
    ("keywords", false, 6, "2001:4860:4860::8888", 80, true),
    // The flow's protocol and port count.
    ("tcp-443", false, 6, "93.184.216.34", 443, true),
    ("tcp-443", false, 6, "2001:4860:4860::8888", 443, true),
    ("tcp-443", false, 6, "93.184.216.34", 80, false),
    ("tcp-443", false, 17, "93.184.216.34", 443, false),
];

#[test]
fn every_flow_is_decided_as_the_table_says() {
    let policies = policies();
    let named = |name: &str| &policies.iter().find(|(n, _)| *n == name).unwrap().1;
    let own = own();
    for &(name, with_own, proto, ip, port, allowed) in ROWS {
        let own = if with_own { &own[..] } else { &[] };
        let got = named(name).allows(own, proto, ip.parse().unwrap(), port);
        assert_eq!(got, allowed, "{name} {proto} {ip}:{port}");
    }
    for (name, p) in &policies {
        for ip in HOST_LOCAL {
            for proto in [1, 6, 17, 58] {
                let got = p.allows(&own, proto, ip.parse().unwrap(), 53);
                assert!(!got, "{name} {proto} {ip}");
            }
        }
    }
}

/// The filter as the worker hands it to presto-pasta, which reads the
/// worker's addresses per flow (the build sandbox this runs in has
/// loopback up): every row of the table above with them, the host-local
/// destinations refused with the addresses read, and no flow where they
/// cannot be read.
#[test]
fn flow_filter_is_the_policy_with_the_workers_own_addresses() {
    let read = own_addresses().unwrap();
    assert!(
        read.contains(&("127.0.0.1".parse().unwrap(), 8)),
        "{read:?}"
    );
    let dst = |ip: &str, proto, port| FlowDst {
        proto,
        ip: ip.parse().unwrap(),
        port,
    };
    let policies = policies();
    let named = |name: &str| policies.iter().find(|(n, _)| *n == name).unwrap().1.clone();
    for &(name, with_own, proto, ip, port, allowed) in ROWS {
        let own = if with_own { own() } else { Own::new() };
        let filter = named(name).flow_filter(move || Some(own.clone()));
        assert_eq!(
            filter(&dst(ip, proto, port)),
            allowed,
            "{name} {proto} {ip}:{port}"
        );
    }
    for (name, p) in &policies {
        let filter = p.clone().flow_filter(own_addresses);
        for ip in HOST_LOCAL {
            for (proto, port) in [(6, 22), (17, 53), (1, 0), (58, 0)] {
                assert!(!filter(&dst(ip, proto, port)), "{name} {ip} {proto}");
            }
        }
    }
    let filter = NetPolicy::default().flow_filter(|| None);
    assert!(!filter(&dst("93.184.216.34", 6, 443)), "addresses unread");
    // DNS to the gateway never reaches the filter: presto-pasta
    // forwards it to the host resolver first. The filter itself
    // would refuse the IPv4 gateway, a link-local address.
    let gateway = Config::default().gateway4.to_string();
    let filter = NetPolicy::default().flow_filter(|| Some(own()));
    assert!(!filter(&dst(&gateway, 17, 53)));
}

#[test]
fn bad_values_are_rejected() {
    for toml in [
        r#"[[rules]]
           action = "deny"
           dst = "not-an-ip""#,
        r#"[[rules]]
           action = "deny"
           dst = "10.0.0.0/33""#,
        r#"[[rules]]
           action = "deny"
           dst = "any"
           ports = ["80-70"]"#,
        // no dst: it once meant "any", opening ports to the LAN too
        r#"[[rules]]
           action = "allow"
           ports = ["80", "443"]"#,
    ] {
        assert!(toml::from_str::<NetPolicy>(toml).is_err(), "{toml}");
    }
}
