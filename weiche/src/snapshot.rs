//! immutable data-plane input, independent of Kubernetes objects and worker state.
use super::{
    cluster::{ClusterId, ClusterSpec},
    router::Router,
};
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForwardingPolicy {
    /// includes pool admission, TCP connect and response headers; excludes body streaming.
    pub response_header_timeout: Duration,
}

impl Default for ForwardingPolicy {
    fn default() -> Self {
        Self {
            response_header_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GatewaySnapshot {
    pub listener: Option<SocketAddr>,
    pub router: Router,
    pub clusters: BTreeMap<ClusterId, ClusterSpec>,
    pub forwarding: ForwardingPolicy,
}

pub type Revision = usize;

#[derive(Debug)]
pub struct PublishedSnapshot {
    // publication is not an all-worker commit; workers acknowledge their local switches.
    pub revision: Revision,
    pub gateway: GatewaySnapshot,
}
