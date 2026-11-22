//! proxy UDP datagrams with a connected upstream socket per downstream peer.
use super::{
    super::{
        super::ir::StreamRoute,
        admission::{Admission, Permit},
        worker::{Task, Worker},
    },
    bind_socket,
};
use compio::{BufResult, net::UdpSocket, runtime::time::timeout};
use futures::{
    FutureExt, StreamExt,
    future::{AbortHandle, abortable},
    stream::FuturesUnordered,
};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
/// receive buffer capacity for a full datagram.
const MAX_PACKET: usize = 65535;

/// bind a reusable datagram socket on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<UdpSocket> {
    let socket = bind_socket(address, socket2::Type::DGRAM, socket2::Protocol::UDP)?;
    UdpSocket::from_std(socket.into())
}

/// bind a raw listener and reserve its group slot through socket close when sharing UDP with QUIC.
pub fn bind(
    worker: &Worker,
    address: SocketAddr,
    routes: watch::Receiver<Arc<StreamRoute>>,
) -> io::Result<Task> {
    let idle = worker.runtime.upstream_idle_timeout;
    let admission = worker.admission.clone();
    if !worker.runtime.http3 {
        return Ok(serve(listen(address)?, routes, admission, idle).boxed_local());
    }
    let (socket, reader) = worker.group.datagrams.acquire(
        address,
        worker.id,
        worker.runtime.workers.get(),
        false,
    )?;
    Ok(async move {
        let result = serve(socket.clone(), routes, admission, idle).await;
        socket.close().await?;
        drop(reader);
        result
    }
    .boxed_local())
}

/// upstream socket and idle accounting for one downstream peer.
struct Flow {
    /// selected backend for this flow.
    target: SocketAddr,
    /// connected socket for sending and receiving backend datagrams.
    upstream: UdpSocket,
    /// time of the last successful send in either direction.
    active: Cell<Instant>,
    /// connection slot held for the lifetime of the flow.
    _permit: Permit,
}

impl Flow {
    /// bind in the backend address family and connect the flow.
    async fn connect(target: SocketAddr, permit: Permit) -> io::Result<Self> {
        let ip = if target.is_ipv4() {
            Ipv4Addr::UNSPECIFIED.into()
        } else {
            Ipv6Addr::UNSPECIFIED.into()
        };
        let upstream = UdpSocket::bind(SocketAddr::new(ip, 0)).await?;
        upstream.connect(target).await?;
        let active = Cell::new(Instant::now());
        Ok(Self { target, upstream, active, _permit: permit })
    }
    /// receive independently of the client's next packet, including multi-datagram replies.
    async fn replies(
        self: Rc<Self>,
        socket: &UdpSocket,
        peer: SocketAddr,
        idle: Duration,
    ) -> SocketAddr {
        let mut receiving =
            std::pin::pin!(self.upstream.recv(Vec::with_capacity(MAX_PACKET)));
        loop {
            let remaining = idle.saturating_sub(self.active.get().elapsed());
            if remaining.is_zero() {
                break;
            }
            // the pending compio operation owns its buffer across idle checks.
            let Ok(BufResult(received, mut packet)) =
                timeout(remaining, &mut receiving).await
            else {
                continue;
            };
            let Ok(len) = received else { break };
            packet.truncate(len);
            let BufResult(sent, mut packet) = socket.send_to(packet, peer).await;
            if sent.is_err() {
                break;
            }
            self.active.set(Instant::now());
            packet.clear();
            receiving.set(self.upstream.recv(packet));
        }
        peer
    }
}

/// serve UDP flows for one bound datagram socket until routes close.
pub async fn serve(
    socket: UdpSocket,
    mut routes: watch::Receiver<Arc<StreamRoute>>,
    admission: Arc<Admission>,
    idle: Duration,
) -> io::Result<()> {
    let mut flows = HashMap::<SocketAddr, (Rc<Flow>, AbortHandle)>::new();
    let mut replies = FuturesUnordered::new();
    let mut receiving = std::pin::pin!(socket.recv_from(Vec::with_capacity(MAX_PACKET)));
    loop {
        let BufResult(received, mut packet) = tokio::select! {
            biased;
            changed = routes.changed() => {
                if changed.is_err() { return Ok(()) }
                // a new route may revoke a backend or its ReferenceGrant.
                let targets: HashSet<_> = routes.borrow_and_update().targets().collect();
                flows.retain(|_, (flow, abort)| {
                    let keep = targets.contains(&flow.target);
                    if !keep { abort.abort(); }
                    keep
                });
                continue;
            }
            Some(result) = replies.next(), if !replies.is_empty() => {
                if let Ok(peer) = result {
                    flows.remove(&peer);
                }
                continue;
            }
            packet = &mut receiving => packet,
        };
        let (len, peer) = received?;
        packet.truncate(len);
        // invalid backends drop their weighted share of every peer's packets.
        // packets admitted to an established flow keep its upstream socket.
        let target = routes.borrow().select();
        if let Some(target) = target {
            if let std::collections::hash_map::Entry::Vacant(entry) = flows.entry(peer)
                && let Some(permit) = admission.acquire()
                && let Ok(flow) = Flow::connect(target, permit).await
            {
                let flow = Rc::new(flow);
                let (reply, abort) = abortable(flow.clone().replies(&socket, peer, idle));
                replies.push(reply);
                entry.insert((flow, abort));
            }
            if let Some((flow, _)) = flows.get(&peer)
                && match routes.has_changed() {
                    Ok(false) => true,
                    // connecting a new flow can yield while the route changes.
                    Ok(true) => {
                        routes.borrow().targets().any(|target| target == flow.target)
                    }
                    Err(_) => false,
                }
            {
                let BufResult(sent, buf) = flow.upstream.send(packet).await;
                if sent.is_ok() {
                    flow.active.set(Instant::now());
                }
                packet = buf;
            }
        }
        packet.clear();
        receiving.set(socket.recv_from(packet));
    }
}
