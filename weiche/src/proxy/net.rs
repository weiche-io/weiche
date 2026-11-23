//! downstream listener transports and per-worker application reports.
mod cid;
mod group;
pub(super) mod h3;
pub mod quic;
pub mod tcp;
pub mod udp;

use super::{
    super::ir::{Revision, Transport},
    config::{RuntimeConfig, SocketBuffers, TcpConfig},
};
use compio::net::{TcpListener, UdpSocket};
use group::DatagramMode;
use socket2::{Domain, Protocol, Socket, Type};
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
    datagrams: group::Groups,
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

    /// reserve a raw UDP socket in the ordered group when HTTP/3 can share its address.
    pub fn bind_udp(
        &self,
        address: SocketAddr,
        id: usize,
        config: &RuntimeConfig,
    ) -> io::Result<BoundUdp> {
        let buffers = config.udp.buffers;
        if config.quic.enabled {
            let (socket, reader) = self.datagrams.acquire(
                address,
                id,
                self.count,
                DatagramMode::Udp,
                buffers,
            )?;
            Ok(BoundUdp { socket, reader: Some(reader) })
        } else {
            let socket = bind_socket(address, Type::DGRAM, Protocol::UDP, buffers, id)?;
            Ok(BoundUdp {
                socket: UdpSocket::from_std(socket.into())?,
                reader: None,
            })
        }
    }

    /// keep the ordered QUIC slot reserved through endpoint shutdown.
    fn bind_quic(
        &self,
        address: SocketAddr,
        id: usize,
        buffers: SocketBuffers,
    ) -> io::Result<(UdpSocket, group::Reader)> {
        self.datagrams
            .acquire(address, id, self.count, DatagramMode::Quic, buffers)
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

/// a downstream UDP socket with its optional reuseport reservation.
pub struct BoundUdp {
    socket: UdpSocket,
    reader: Option<group::Reader>,
}

impl BoundUdp {
    /// wait for outstanding I/O to release the socket before retiring its slot.
    async fn close(self) -> io::Result<()> {
        let result = self.socket.close().await;
        drop(self.reader);
        result
    }
}

/// grow a buffer and request a fresh socket if the cap would shrink its default.
fn size_buffer(
    target: &mut Option<usize>,
    clamped: &mut Option<usize>,
    get: impl Fn() -> io::Result<usize>,
    set: impl FnOnce(usize) -> io::Result<()>,
) -> io::Result<bool> {
    let Some(size) = *target else { return Ok(true) };
    // linux reports twice the requested capacity to account for packet metadata.
    let expected = size.saturating_mul(2);
    let current = get()?;
    *clamped = None;
    if current >= expected {
        return Ok(true);
    }
    set(size)?;
    let effective = get()?;
    if effective < current {
        *target = None;
        *clamped = Some(current);
        return Ok(false);
    }
    *clamped = (effective < expected).then_some(effective);
    Ok(true)
}

/// create a reusable worker socket in the requested address family.
fn bind_socket(
    address: SocketAddr,
    kind: Type,
    protocol: Protocol,
    buffers: SocketBuffers,
    worker_id: usize,
) -> io::Result<Socket> {
    let mut targets = buffers;
    let mut clamped = SocketBuffers::default();
    let socket = loop {
        let socket = Socket::new(Domain::for_address(address), kind, Some(protocol))?;
        let recv = size_buffer(
            &mut targets.recv,
            &mut clamped.recv,
            || socket.recv_buffer_size(),
            |size| socket.set_recv_buffer_size(size),
        )?;
        let send = size_buffer(
            &mut targets.send,
            &mut clamped.send,
            || socket.send_buffer_size(),
            |size| socket.set_send_buffer_size(size),
        )?;
        if recv && send {
            break socket;
        }
    };
    if address.is_ipv6() && address.ip().is_unspecified() {
        socket.set_only_v6(false)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&address.into())?;
    if worker_id == 0 && (clamped.recv.is_some() || clamped.send.is_some()) {
        tracing::info!(%address, ?protocol, ?buffers, ?clamped, "listener socket buffers limited by kernel");
    }
    Ok(socket)
}

/// bind a reusable listener on the worker's runtime.
pub fn listen_tcp(
    address: SocketAddr,
    tcp: &TcpConfig,
    worker_id: usize,
) -> io::Result<TcpListener> {
    let socket =
        bind_socket(address, Type::STREAM, Protocol::TCP, tcp.buffers, worker_id)?;
    socket.listen(tcp.backlog)?;
    TcpListener::from_std(socket.into())
}
