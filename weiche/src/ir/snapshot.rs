//! immutable data-plane input, independent of Kubernetes objects and worker state.
use super::{Route, ServerTls};
use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForwardingPolicy {
    /// includes pool admission, TCP connect and response headers; excludes body streaming.
    pub response_header_timeout: Duration,
}

impl Default for ForwardingPolicy {
    fn default() -> Self {
        Self { response_header_timeout: Duration::from_secs(10) }
    }
}

/// socket and transport identity applied together by each worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listener {
    pub address: SocketAddr,
    /// present when the listener terminates HTTPS.
    pub tls: Option<Arc<ServerTls>>,
}

/// configuration produced by one compilation of the watched resources.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GatewaySnapshot {
    pub listener: Option<Listener>,
    pub routes: Vec<Route>,
    pub forwarding: ForwardingPolicy,
}

impl GatewaySnapshot {
    /// address reported by socket binding and readiness checks.
    pub fn address(&self) -> Option<SocketAddr> {
        self.listener.as_ref().map(|listener| listener.address)
    }
}

pub type Revision = usize;

#[derive(Debug)]
pub struct PublishedSnapshot {
    // publication is not an all-worker commit; workers acknowledge their local switches.
    pub revision: Revision,
    pub gateway: GatewaySnapshot,
}
