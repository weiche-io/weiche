//! route raw TCP, UDP and TLS passthrough traffic to weighted backends.
use super::RefError;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

/// socket namespace and Kubernetes Service port protocol.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum Transport {
    /// connection-oriented TCP sockets.
    #[default]
    Tcp,
    /// datagram-oriented UDP sockets.
    Udp,
}
impl Transport {
    /// choose the socket protocol required by a listener.
    pub fn for_protocol(protocol: &str) -> Self {
        if protocol == "UDP" { Self::Udp } else { Self::Tcp }
    }
    /// return the Kubernetes transport protocol.
    pub fn protocol(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
}

/// invalid targets retain their share of connection or packet rejections.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRoute {
    /// weighted target lists and resolution errors.
    pub backends: Vec<(u32, Result<Vec<SocketAddr>, RefError>)>,
}
impl StreamRoute {
    /// enumerate addresses still reachable through resolved, positive-weight backends.
    pub fn targets(&self) -> impl Iterator<Item = SocketAddr> {
        self.backends
            .iter()
            .filter(|(weight, _)| *weight > 0)
            .filter_map(|(_, target)| target.as_ref().ok())
            .flatten()
            .copied()
    }

    /// sample a weighted backend and one of its ready addresses.
    pub fn select(&self) -> Option<SocketAddr> {
        let (_, target) = weighted_by(&self.backends, |(weight, _)| *weight)?;
        fastrand::choice(target.as_ref().ok()?).copied()
    }
}

/// keep the listener identity separate from the target set when endpoints change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamListener<R = Arc<StreamRoute>> {
    /// owning gateway identity.
    pub gateway_uid: String,
    /// resolved route in the snapshot or its live watch on the worker.
    pub route: R,
}

/// highest-precedence attached route for each original route hostname.
pub type SniRoutes = BTreeMap<String, Arc<StreamRoute>>;

/// virtual listeners sharing a TLS passthrough socket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSocket {
    /// owner of the binding across live updates.
    pub gateway_uid: String,
    /// listener hostname to its attached routes, including listeners with no routes.
    pub hosts: BTreeMap<String, SniRoutes>,
}

impl TlsSocket {
    /// select within the most specific listener even when it has no usable backend.
    pub fn select(&self, hostname: &str) -> Option<SocketAddr> {
        let hostname = hostname.to_ascii_lowercase();
        let listener = lookup(&self.hosts, &hostname)?;
        lookup(listener, &hostname)?.select()
    }
}

/// try the exact name, then each wildcard suffix from longest to shortest, then catch-all.
fn lookup<'a, T>(hosts: &'a BTreeMap<String, T>, hostname: &str) -> Option<&'a T> {
    hosts.get(hostname).or_else(|| {
        hostname
            .match_indices('.')
            .find_map(|(dot, _)| hosts.get(&format!("*{}", &hostname[dot..])))
            .or_else(|| hosts.get(""))
    })
}

/// sample a weighted reference while retaining its protocol-specific metadata.
pub fn weighted_by<T>(targets: &[T], weight: impl Fn(&T) -> u32) -> Option<&T> {
    if let [target] = targets {
        return (weight(target) > 0).then_some(target);
    }
    let total: u64 = targets.iter().map(|target| u64::from(weight(target))).sum();
    if total == 0 {
        return None;
    }
    let mut slot = fastrand::u64(0..total);
    for target in targets {
        let share = u64::from(weight(target));
        if slot < share {
            return Some(target);
        }
        slot -= share;
    }
    None
}
