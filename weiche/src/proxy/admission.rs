//! select worker-local endpoints and bound downstream connection admission.
use super::super::ir::{BackendTls, Binding, ClusterSpec, Protocol};
use std::{
    cell::Cell,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// identity of a pooled upstream transport.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointKey {
    /// backend socket address.
    pub address: SocketAddr,
    /// application protocol expected by the backend.
    pub protocol: Protocol,
    /// optional server identity and trust configuration.
    pub tls: Option<Arc<BackendTls>>,
}

/// worker-local round-robin state shared by routes with the same membership and protocol.
pub struct Cluster {
    /// resolved endpoints and transport policy.
    pub spec: Arc<ClusterSpec>,
    /// next position in the round-robin rotation.
    cursor: Cell<usize>,
}

impl Cluster {
    /// start endpoint selection at the first member.
    pub fn new(spec: Arc<ClusterSpec>) -> Self {
        Self { spec, cursor: Cell::new(0) }
    }

    /// advance the rotation and select a member when available.
    pub fn select(&self) -> Option<EndpointKey> {
        let endpoints = &self.spec.endpoints;
        if endpoints.is_empty() {
            return None;
        }
        let index = self.cursor.get();
        self.cursor.set(index.wrapping_add(1));
        Some(self.endpoint(endpoints[index % endpoints.len()]))
    }

    /// identify a member with the protocol and TLS settings used to pool its connections.
    pub fn endpoint(&self, address: SocketAddr) -> EndpointKey {
        EndpointKey {
            address,
            protocol: self.spec.protocol,
            tls: self.spec.tls.clone(),
        }
    }
}

/// target attached to a configured listener, before any protocol-specific matching.
pub struct Route<T> {
    /// listener identity that must match the accepted connection.
    pub binding: Binding,
    /// routing table for that listener.
    pub target: T,
}

impl<T> Route<T> {
    /// keep existing connections bound to their original socket and TLS identity.
    pub fn find(&self, binding: &Binding) -> Option<&T> {
        (&self.binding == binding).then_some(&self.target)
    }
}

/// global cap shared by the TCP listeners of all workers.
pub struct Admission {
    /// number of outstanding connection permits.
    live: AtomicUsize,
    /// maximum number of concurrent permits.
    limit: usize,
}

impl Admission {
    /// start a shared admission budget with no outstanding permits.
    pub fn new(limit: usize) -> Self {
        Self { live: AtomicUsize::new(0), limit }
    }
    /// admit a connection for the lifetime of its permit.
    pub fn acquire(self: &Arc<Self>) -> Option<Permit> {
        self.live
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .ok()
            .map(|_| Permit(self.clone()))
    }
}

/// returns the connection slot when dropped, including on cancellation.
pub struct Permit(
    /// shared budget credited when this permit drops.
    Arc<Admission>,
);
impl Drop for Permit {
    /// return the connection or flow reservation to the global budget.
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}
