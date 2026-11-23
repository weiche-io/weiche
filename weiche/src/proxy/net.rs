//! downstream listener transports and per-worker application reports.
mod cid;
mod group;
pub(super) mod h3;
pub mod quic;
pub mod tcp;
pub mod udp;

use super::super::ir::{Revision, Transport};
use compio::net::TcpListener;
use std::{collections::BTreeSet, io, net::SocketAddr};
use tokio::sync::watch;

/// sockets successfully applied at one worker revision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStatus {
    /// snapshot revision installed by this worker.
    pub revision: Revision,
    /// transport and address pairs the worker successfully bound.
    pub bound: BTreeSet<(Transport, SocketAddr)>,
}
/// latest binding reports indexed by worker.
pub type WorkerGroupState = Vec<WorkerStatus>;

/// publish consistent binding reports for the worker group.
pub struct ListenerGroup {
    /// ordered UDP bindings when HTTP/3 shares a worker group with raw UDP.
    pub datagrams: group::Groups,
    /// number of slots to publish with the first changed worker report.
    count: usize,
    /// channel carrying combined reports to the readiness monitor.
    state: watch::Sender<WorkerGroupState>,
}
impl ListenerGroup {
    /// start an empty watch and reserve one report index for each configured worker.
    pub fn new(count: usize) -> (Self, watch::Receiver<WorkerGroupState>) {
        let (state, reports) = watch::channel(Vec::new());
        (Self { datagrams: group::Groups::default(), count, state }, reports)
    }

    /// publish a fresh group report when a worker changes state.
    pub fn report(&self, id: usize, status: WorkerStatus) {
        self.state.send_if_modified(|workers| {
            if workers.is_empty() {
                if status == WorkerStatus::default() {
                    return false;
                }
                workers.resize(self.count, WorkerStatus::default());
            }
            if workers[id] == status {
                return false;
            }
            workers[id] = status;
            true
        });
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

/// bind a reusable listener on the worker's runtime.
pub fn listen_tcp(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, socket2::Protocol::TCP)?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}
