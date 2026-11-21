//! protocol filters share endpoint selection, but not their routing predicates.
pub mod http;
pub mod tcp;

use super::super::ir::{BackendTls, ClusterSpec, Protocol};
use std::{cell::Cell, net::SocketAddr, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointKey {
    pub address: SocketAddr,
    pub protocol: Protocol,
    pub tls: Option<Arc<BackendTls>>,
}

/// worker-local round-robin state shared by routes with the same membership and protocol.
pub struct Cluster {
    pub spec: Arc<ClusterSpec>,
    cursor: Cell<usize>,
}

impl Cluster {
    pub fn new(spec: Arc<ClusterSpec>) -> Self {
        Self { spec, cursor: Cell::new(0) }
    }

    pub fn select(&self) -> Option<EndpointKey> {
        let endpoints = &self.spec.endpoints;
        if endpoints.is_empty() {
            return None;
        }
        let index = self.cursor.get();
        self.cursor.set(index.wrapping_add(1));
        Some(EndpointKey {
            address: endpoints[index % endpoints.len()],
            protocol: self.spec.protocol,
            tls: self.spec.tls.clone(),
        })
    }
}
