//! immutable data-plane input, independent of Kubernetes objects and worker state.
use super::{Route, ServerTls};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listener {
    pub address: SocketAddr,
    /// advertised port may differ from the local port behind a Kubernetes Service.
    pub port: u16,
    /// prevents established connections from following a reallocated socket to another Gateway.
    pub gateway_uid: String,
    /// ordered from most to least specific SNI hostname.
    pub tls: Vec<Arc<ServerTls>>,
}

/// logical hostname listeners sharing one transport socket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketRoutes {
    pub listener: Listener,
    pub hosts: BTreeMap<String, Vec<Route>>,
}

/// all sockets published together at a resource revision.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewaySnapshot {
    pub sockets: BTreeMap<SocketAddr, SocketRoutes>,
    pub streams: BTreeMap<(super::Transport, SocketAddr), super::StreamListener>,
    pub forwarding: ForwardingPolicy,
}

pub type Revision = usize;

#[derive(Debug)]
pub struct PublishedSnapshot {
    // publication is not an all-worker commit; workers acknowledge their local switches.
    pub revision: Revision,
    pub gateway: GatewaySnapshot,
}
