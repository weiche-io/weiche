//! downstream listener transports and per-worker application reports.
mod passthrough;
pub mod quic;
mod relay;
pub mod tcp;
pub mod udp;

use super::super::ir::{Revision, Transport};
use std::{collections::BTreeSet, net::SocketAddr, sync::Mutex};
use tokio::sync::watch;

/// sockets successfully applied at one worker revision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStatus {
    /// snapshot revision installed by this worker.
    pub revision: Revision,
    /// transport and address pairs the worker successfully bound.
    pub bound: BTreeSet<(Transport, SocketAddr)>,
}
/// latest binding reports from the local workers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerGroupState {
    /// one report per configured worker.
    pub workers: Vec<WorkerStatus>,
}
impl WorkerGroupState {
    /// check that every worker bound a socket at the requested revision.
    pub fn programmed(
        &self,
        revision: Revision,
        address: (Transport, SocketAddr),
    ) -> bool {
        !self.workers.is_empty()
            && self
                .workers
                .iter()
                .all(|w| w.revision == revision && w.bound.contains(&address))
    }

    /// check that every worker is currently serving at least one bound listener.
    pub fn serving(&self) -> bool {
        !self.workers.is_empty() && self.workers.iter().all(|w| !w.bound.is_empty())
    }
}
/// publish consistent binding reports for the worker group.
pub struct ListenerGroup {
    /// ordered UDP bindings when HTTP/3 shares a worker group with raw UDP.
    datagrams: quic::Groups,
    /// latest report at each worker index.
    workers: Mutex<Vec<WorkerStatus>>,
    /// channel carrying combined reports to the readiness monitor.
    state: watch::Sender<WorkerGroupState>,
}
impl ListenerGroup {
    /// allocate a status slot for each configured worker.
    pub fn new(count: usize, state: watch::Sender<WorkerGroupState>) -> Self {
        Self {
            datagrams: quic::Groups::default(),
            workers: Mutex::new(vec![WorkerStatus::default(); count]),
            state,
        }
    }
    /// publish a fresh group report when a worker changes state.
    pub fn report(&self, id: usize, status: WorkerStatus) {
        let mut workers = self.workers.lock().unwrap();
        if workers[id] != status {
            workers[id] = status;
            self.state.send_replace(WorkerGroupState { workers: workers.clone() });
        }
    }
}

/// create a reusable worker socket in the requested address family.
fn bind_socket(
    address: SocketAddr,
    kind: socket2::Type,
    protocol: socket2::Protocol,
) -> std::io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        kind,
        Some(protocol),
    )?;
    if address.is_ipv6() && address.ip().is_unspecified() {
        socket.set_only_v6(false)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&address.into())?;
    Ok(socket)
}
