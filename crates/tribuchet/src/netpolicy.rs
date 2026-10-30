//! Flow policy for the fixed-output build network (`[fod-network]` in
//! worker.toml): an ordered rule list matched against the destination
//! of each new outbound flow, first match wins, plus a default action
//! for public destinations. A non-public one (`Dst::Private`) that no
//! rule matches is refused: a fetch has no business with the worker's
//! LAN, its tailnet or a cloud metadata service unless a rule says so.
//! Public is globally routable and on none of the networks the worker's
//! interfaces are on, so a LAN peer's or router's global IPv6 is not.
//!
//! Only destinations are matched (the sandbox is the only source), and
//! rules are IP-based on purpose: hostname rules would be enforced on
//! whatever the name resolves to at connect time and are trivially
//! bypassed by a build resolving names itself. DNS forwarding to the
//! host resolver is handled separately by presto-pasta and is not
//! affected by these rules.
//!
//! The worker itself is refused before any rule: loopback, unspecified
//! and every address on its interfaces, which reach its own services
//! as loopback does. presto-pasta's sockets live in the worker's
//! network namespace, and its own loopback default applies only to a
//! datapath given no filter; the worker always gives it
//! [`NetPolicy::flow_filter`].

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
#[cfg(target_os = "linux")]
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// `[fod-network]` section of worker.toml.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct NetPolicy {
    /// Action when no rule matches a public destination.
    #[serde(default)]
    pub default: Action,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Proto {
    #[default]
    Any,
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Rule {
    pub action: Action,
    #[serde(default)]
    pub proto: Proto,
    /// Destination match, required: "any", "private", a single IP, or
    /// CIDR ("192.0.2.0/24", "2001:db8::/32"). A rule that opens ports
    /// opens them to the LAN, the tailnet and metadata services too only
    /// where it says "any". Loopback and unspecified destinations are
    /// refused before any rule; an allow rule matching them has no effect.
    pub dst: Dst,
    /// Destination ports: single ("443") or range ("8000-8999");
    /// empty means any port.
    #[serde(default)]
    pub ports: Vec<PortRange>,
}

/// The worker's interface addresses, each with its network's prefix
/// length.
pub type Own = Vec<(IpAddr, u8)>;

impl NetPolicy {
    /// Decide a new flow to `ip:port` with IP protocol number `proto`
    /// from a worker whose interfaces carry `own`. The worker itself
    /// (loopback, unspecified, its own addresses) is refused whatever the
    /// rules say; another non-public destination only a rule allows.
    #[cfg(target_os = "linux")]
    fn allows(&self, own: &[(IpAddr, u8)], proto: u8, ip: IpAddr, port: u16) -> bool {
        let ip = ip.to_canonical();
        if is_host_local(ip) || own.iter().any(|(a, _)| a.to_canonical() == ip) {
            return false;
        }
        match self.rules.iter().find(|r| r.matches(own, proto, ip, port)) {
            Some(r) => r.action == Action::Allow,
            None => self.default == Action::Allow && is_public(own, ip),
        }
    }

    /// The flow filter the worker hands presto-pasta for a sandbox, with
    /// `own` reading the worker's addresses ([`own_addresses`]) per flow:
    /// SLAAC and DHCP change them while a build runs. Unreadable, every
    /// flow is refused. presto-pasta calls it for every new guest flow
    /// except DNS to its gateway, which it forwards to the host resolver
    /// itself.
    #[cfg(target_os = "linux")]
    pub fn flow_filter(
        self,
        own: impl Fn() -> Option<Own> + Send + Sync + 'static,
    ) -> presto_pasta::FlowFilter {
        Arc::new(move |d: &presto_pasta::FlowDst| {
            own().is_some_and(|own| self.allows(&own, d.proto, d.ip, d.port))
        })
    }
}

/// Addresses on the worker's interfaces, in its network namespace, with
/// their netmasks' prefix lengths (the address alone without one).
#[cfg(target_os = "linux")]
pub fn own_addresses() -> Option<Own> {
    let ifs = nix::ifaddrs::getifaddrs().ok()?;
    let v4 = |s: &nix::sys::socket::SockaddrStorage| Some(IpAddr::V4(s.as_sockaddr_in()?.ip()));
    let v6 = |s: &nix::sys::socket::SockaddrStorage| Some(IpAddr::V6(s.as_sockaddr_in6()?.ip()));
    let bits = |ip: IpAddr| match ip {
        IpAddr::V4(m) => u32::from(m).count_ones(),
        IpAddr::V6(m) => u128::from(m).count_ones(),
    };
    Some(
        ifs.filter_map(|i| {
            let a = i.address.as_ref()?;
            let ip = v4(a).or_else(|| v6(a))?;
            let mask = i.netmask.as_ref().and_then(|m| v4(m).or_else(|| v6(m)));
            let prefix = mask.map_or(if ip.is_ipv4() { 32 } else { 128 }, bits);
            Some((ip, u8::try_from(prefix).ok()?))
        })
        .collect(),
    )
}

/// Loopback or unspecified, also IPv4-mapped: a host socket connecting
/// there reaches the worker's loopback (Linux connects an unspecified
/// destination over loopback).
#[cfg(target_os = "linux")]
fn is_host_local(ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    ip.is_loopback() || ip.is_unspecified()
}

#[cfg(target_os = "linux")]
impl Rule {
    fn matches(&self, own: &[(IpAddr, u8)], proto: u8, ip: IpAddr, port: u16) -> bool {
        let proto_ok = match self.proto {
            Proto::Any => true,
            Proto::Tcp => proto == 6,
            Proto::Udp => proto == 17,
        };
        proto_ok
            && self.dst.matches(own, ip)
            && (self.ports.is_empty() || self.ports.iter().any(|r| r.contains(port)))
    }
}

/// Destination address matcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dst {
    Any,
    /// Non-public destinations: loopback, RFC 1918, link-local, ULA,
    /// CGNAT, multicast, ...
    Private,
    Cidr {
        ip: IpAddr,
        prefix: u8,
    },
}

#[cfg(target_os = "linux")]
impl Dst {
    fn matches(self, own: &[(IpAddr, u8)], ip: IpAddr) -> bool {
        // ::ffff:a.b.c.d is a.b.c.d for `private` and IPv4 CIDRs.
        let ip = ip.to_canonical();
        match self {
            Self::Any => true,
            Self::Private => !is_public(own, ip),
            Self::Cidr { ip: net, prefix } => cidr_contains(net, prefix, ip),
        }
    }
}

/// Globally routable (presto-pasta's classification) and on none of the
/// networks of the worker's addresses `own`.
#[cfg(target_os = "linux")]
fn is_public(own: &[(IpAddr, u8)], ip: IpAddr) -> bool {
    let flow = presto_pasta::FlowDst {
        proto: 0,
        ip,
        port: 0,
    };
    flow.is_public()
        && !own
            .iter()
            .any(|&(net, prefix)| cidr_contains(net.to_canonical(), prefix, ip))
}

#[cfg(target_os = "linux")]
fn cidr_contains(net: IpAddr, prefix: u8, ip: IpAddr) -> bool {
    fn masked(ip: u128, bits: u32, prefix: u8) -> u128 {
        let shift = bits - u32::from(prefix);
        // prefix == 0 would shift by the full width; that means "match all".
        if shift >= bits { 0 } else { ip >> shift }
    }
    match (net, ip) {
        (IpAddr::V4(net), IpAddr::V4(ip)) => {
            masked(u32::from(net).into(), 32, prefix) == masked(u32::from(ip).into(), 32, prefix)
        }
        (IpAddr::V6(net), IpAddr::V6(ip)) => {
            masked(net.into(), 128, prefix) == masked(ip.into(), 128, prefix)
        }
        _ => false,
    }
}

impl FromStr for Dst {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => return Ok(Self::Any),
            "private" => return Ok(Self::Private),
            _ => {}
        }
        let (addr, prefix) = match s.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (s, None),
        };
        let ip: IpAddr = addr
            .parse()
            .map_err(|e| format!("bad destination {s:?}: {e}"))?;
        let max = if ip.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            None => max,
            Some(p) => match p.parse::<u8>() {
                Ok(p) if p <= max => p,
                _ => return Err(format!("bad prefix length in {s:?}")),
            },
        };
        Ok(Self::Cidr { ip, prefix })
    }
}

impl fmt::Display for Dst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => f.write_str("any"),
            Self::Private => f.write_str("private"),
            Self::Cidr { ip, prefix } => write!(f, "{ip}/{prefix}"),
        }
    }
}

/// Inclusive destination port range, written as "443" or "8000-8999".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortRange {
    start: u16,
    end: u16,
}

#[cfg(target_os = "linux")]
impl PortRange {
    fn contains(self, port: u16) -> bool {
        (self.start..=self.end).contains(&port)
    }
}

impl FromStr for PortRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (start, end) = match s.split_once('-') {
            Some((a, b)) => (a, b),
            None => (s, s),
        };
        let parse = |p: &str| {
            p.trim()
                .parse::<u16>()
                .map_err(|e| format!("bad port range {s:?}: {e}"))
        };
        let (start, end) = (parse(start)?, parse(end)?);
        if start > end {
            return Err(format!("bad port range {s:?}: start > end"));
        }
        Ok(Self { start, end })
    }
}

impl fmt::Display for PortRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

// TOML representation of Dst and PortRange is their string form.
macro_rules! serde_via_str {
    ($ty:ty, $expecting:literal) => {
        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                s.parse().map_err(|e| {
                    serde::de::Error::invalid_value(
                        serde::de::Unexpected::Str(&s),
                        &format!("{}: {e}", $expecting).as_str(),
                    )
                })
            }
        }
    };
}
serde_via_str!(Dst, "a destination matcher");
serde_via_str!(PortRange, "a port or port range");

#[cfg(all(test, target_os = "linux"))]
mod tests;
