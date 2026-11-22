//! select a TLS listener and its backend using the original SNI hostname.
use super::StreamRoute;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

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
