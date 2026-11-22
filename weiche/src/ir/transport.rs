//! raw transport routing shared by TCP connections and UDP flows.
use super::RefError;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

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
        let endpoints = weighted(&self.backends)?.as_ref().ok()?;
        fastrand::choice(endpoints).copied()
    }
}

/// keep the listener identity separate from the target set when endpoints change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamListener {
    /// owning gateway identity.
    pub gateway_uid: String,
    /// resolved route currently serving new flows.
    pub route: Arc<StreamRoute>,
}

/// sample a target with its configured weight, including targets that failed resolution.
pub fn weighted<T>(targets: &[(u32, T)]) -> Option<&T> {
    weighted_by(targets, |(weight, _)| *weight).map(|(_, target)| target)
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
