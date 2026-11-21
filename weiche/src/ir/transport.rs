//! raw transport routing shared by TCP connections and UDP flows.
use super::RefError;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

/// socket namespace and Kubernetes Service port protocol.
#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum Transport {
    Tcp,
    Udp,
}
impl Transport {
    pub fn for_protocol(protocol: &str) -> Self {
        if protocol == "UDP" { Self::Udp } else { Self::Tcp }
    }
    pub fn protocol(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
    pub fn route_kind(self) -> &'static str {
        match self {
            Self::Tcp => "TCPRoute",
            Self::Udp => "UDPRoute",
        }
    }
}

/// invalid targets retain their share of connection or packet rejections.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRoute {
    pub backends: Vec<(u32, Result<Vec<SocketAddr>, RefError>)>,
}
impl StreamRoute {
    pub fn select(&self) -> Option<SocketAddr> {
        let endpoints = weighted(&self.backends)?.as_ref().ok()?;
        fastrand::choice(endpoints).copied()
    }
}

/// keep the listener identity separate from the target set when endpoints change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamListener {
    pub gateway_uid: String,
    pub route: Arc<StreamRoute>,
}

/// sample a target with its configured weight, including targets that failed resolution.
pub fn weighted<T>(targets: &[(u32, T)]) -> Option<&T> {
    if let [(weight, target)] = targets {
        return (*weight > 0).then_some(target);
    }
    let total: u64 = targets.iter().map(|(w, _)| u64::from(*w)).sum();
    if total == 0 {
        return None;
    }
    let mut slot = fastrand::u64(0..total);
    for (weight, target) in targets {
        if slot < u64::from(*weight) {
            return Some(target);
        }
        slot -= u64::from(*weight);
    }
    None
}
