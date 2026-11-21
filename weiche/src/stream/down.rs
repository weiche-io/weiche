//! downstream listener transports and per-worker application reports.
mod http;
#[cfg(feature = "quic")]
pub mod quic;
pub mod tcp;
mod tls;
pub mod udp;

use super::super::ir::Revision;
use std::{collections::BTreeSet, net::SocketAddr, sync::Mutex};
use tokio::sync::watch;

/// sockets successfully applied at one worker revision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStatus {
    pub revision: Revision,
    pub bound: BTreeSet<SocketAddr>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerGroupState {
    pub workers: Vec<WorkerStatus>,
}
impl WorkerGroupState {
    pub fn programmed(&self, revision: Revision, address: SocketAddr) -> bool {
        !self.workers.is_empty()
            && self
                .workers
                .iter()
                .all(|w| w.revision == revision && w.bound.contains(&address))
    }
}
pub struct ListenerGroup {
    workers: Mutex<Vec<WorkerStatus>>,
    state: watch::Sender<WorkerGroupState>,
}
impl ListenerGroup {
    pub fn new(count: usize, state: watch::Sender<WorkerGroupState>) -> Self {
        Self {
            workers: Mutex::new(vec![WorkerStatus::default(); count]),
            state,
        }
    }
    pub fn report(&self, id: usize, status: WorkerStatus) {
        let mut workers = self.workers.lock().unwrap();
        if workers[id] != status {
            workers[id] = status;
            self.state.send_replace(WorkerGroupState { workers: workers.clone() });
        }
    }
}

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
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&address.into())?;
    Ok(socket)
}
