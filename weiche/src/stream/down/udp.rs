//! proxy UDP datagrams with a connected upstream socket per downstream peer.
use super::{
    super::{
        super::ir::StreamRoute,
        core::tcp::{Admission, Permit},
    },
    bind_socket,
};
use compio::{net::UdpSocket, runtime::time::timeout};
use futures::{
    StreamExt,
    future::{AbortHandle, abortable},
    stream::FuturesUnordered,
};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
const MAX_PACKET: usize = 65535;

/// bind a reusable datagram socket on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<UdpSocket> {
    let socket = bind_socket(address, socket2::Type::DGRAM, socket2::Protocol::UDP)?;
    UdpSocket::from_std(socket.into())
}

struct Flow {
    target: SocketAddr,
    upstream: UdpSocket,
    active: Cell<Instant>,
    _permit: Permit,
}

impl Flow {
    async fn connect(target: SocketAddr, permit: Permit) -> io::Result<Self> {
        let ip = if target.is_ipv4() {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        } else {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        };
        let upstream = UdpSocket::bind(SocketAddr::new(ip, 0)).await?;
        upstream.connect(target).await?;
        Ok(Self {
            target,
            upstream,
            active: Cell::new(Instant::now()),
            _permit: permit,
        })
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
            let Ok(compio::BufResult(received, mut packet)) =
                timeout(remaining, &mut receiving).await
            else {
                continue;
            };
            let Ok(len) = received else { break };
            packet.truncate(len);
            let compio::BufResult(sent, mut packet) = socket.send_to(packet, peer).await;
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
        let (received, mut packet) = tokio::select! {
            biased;
            changed = routes.changed() => {
                if changed.is_err() { return Ok(()) }
                // a new route may revoke a backend or its ReferenceGrant.
                let targets: HashSet<_> = routes.borrow_and_update().targets().collect();
                flows.retain(|_, (flow, abort)| {
                    if targets.contains(&flow.target) {
                        true
                    } else {
                        abort.abort();
                        false
                    }
                });
                continue;
            }
            Some(result) = replies.next(), if !replies.is_empty() => {
                if let Ok(peer) = result {
                    flows.remove(&peer);
                }
                continue;
            }
            compio::BufResult(received, packet) = &mut receiving => (received, packet),
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
                let compio::BufResult(sent, buf) = flow.upstream.send(packet).await;
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
