//! immutable data-plane input, independent of Kubernetes objects and worker state.
use super::{Route, ServerTls};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use std::{net::SocketAddr, time::Duration};

/// default upstream deadlines for the gateway.
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

/// identity shared by the socket, its live connections and the active router.
#[derive(Debug, PartialEq, Eq)]
pub struct Binding {
    /// local socket address including the IPv4 or IPv6 bind address.
    pub address: SocketAddr,
    /// advertised port of the listener.
    pub port: u16,
    /// identity of the gateway that accepted the connection.
    pub gateway_uid: String,
    /// whether the listener terminates TLS.
    pub tls: bool,
}

/// listener configuration carried in the gateway snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listener {
    /// local socket address including the IPv4 or IPv6 bind address.
    pub address: SocketAddr,
    /// advertised port may differ from the local port behind a Kubernetes Service.
    pub port: u16,
    /// prevents established connections from following a reallocated socket to another Gateway.
    pub gateway_uid: String,
    /// ordered from most to least specific SNI hostname.
    pub tls: Vec<Arc<ServerTls>>,
}

impl Listener {
    /// certificate updates apply to new handshakes on the same binding.
    pub fn binding(&self) -> Binding {
        Binding {
            address: self.address,
            port: self.port,
            gateway_uid: self.gateway_uid.clone(),
            tls: !self.tls.is_empty(),
        }
    }
}

/// resolved HTTP routes sharing one transport socket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketRoutes {
    /// transport identity and TLS settings on the shared socket.
    pub listener: Listener,
    /// attached rules with their effective hostname intersections.
    pub routes: Vec<Route>,
    /// listener hostnames reserved on the socket, including those with no attached rules.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hostnames: Vec<String>,
}

/// all sockets published together at a resource revision.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewaySnapshot {
    /// web listeners indexed by local address.
    pub sockets: BTreeMap<SocketAddr, SocketRoutes>,
    /// raw listeners keyed by transport and local address.
    pub streams: BTreeMap<(super::Transport, SocketAddr), super::StreamListener>,
    /// default upstream forwarding budgets.
    pub forwarding: ForwardingPolicy,
    /// passthrough sockets indexed by IPv4 or IPv6 bind address.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tls: BTreeMap<SocketAddr, Arc<super::TlsSocket>>,
}

/// sequence number of a published configuration.
pub type Revision = usize;

/// gateway configuration and revision distributed to workers.
#[derive(Debug)]
pub struct PublishedSnapshot {
    /// revision acknowledged by each worker after switching configurations.
    pub revision: Revision,
    /// configuration installed at this revision.
    pub gateway: GatewaySnapshot,
}
