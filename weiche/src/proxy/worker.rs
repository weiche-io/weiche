//! reconcile a snapshot against the listeners owned by one compio runtime.
use super::{
    super::ir::{
        Binding, GatewaySnapshot, Listener, PublishedSnapshot, Revision, ServerTls,
        StreamRoute, TlsSocket, Transport,
    },
    admission::Admission,
    config::{Reader, RuntimeConfig},
    http::{ProxyService, server::Server},
    net::{ListenerGroup, WorkerStatus, quic, tcp, udp},
    stopped,
};
use compio::runtime::time::sleep;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use std::{collections::BTreeMap, io, net::SocketAddr, rc::Rc, sync::Arc};
use tokio::sync::watch;

/// accept or datagram loop driven on the worker that owns its socket.
pub type Task = LocalBoxFuture<'static, io::Result<()>>;

/// identity and reloadable certificate table of a bound HTTP socket.
pub struct HttpListener {
    /// immutable identity shared with accepted connections and the QUIC endpoint.
    pub binding: Rc<Binding>,
    /// latest certificate table for new handshakes on this socket.
    pub certs: watch::Sender<Rc<[Arc<ServerTls>]>>,
}

impl HttpListener {
    /// reload certificates in place if the existing binding can serve this listener.
    fn reload(&self, listener: &Listener) -> bool {
        if listener.binding() != *self.binding {
            return false;
        }
        self.certs.send_if_modified(|current| {
            if current.as_ref() == listener.tls.as_slice() {
                return false;
            }
            *current = Rc::from(listener.tls.as_slice());
            true
        });
        true
    }
}

/// owner and live forwarding table of a raw TCP or UDP socket.
struct StreamListener {
    /// binding owner retained across route changes.
    gateway_uid: String,
    /// current targets for admitted connections or datagrams.
    routes: watch::Sender<Arc<StreamRoute>>,
}

/// resources shared by listener tasks on one compio runtime.
pub struct Worker {
    /// index in the reuseport group and worker status table.
    pub id: usize,
    /// immutable timeouts, worker count and HTTP/3 policy.
    pub runtime: Arc<RuntimeConfig>,
    /// connection and flow budget shared with the other workers.
    pub admission: Arc<Admission>,
    /// datagram reservations and status reports for the group.
    pub group: Arc<ListenerGroup>,
    /// worker-local HTTP service shared by TCP and QUIC listeners.
    pub server: Rc<Server>,
}

/// active bindings and their serving tasks on one worker.
#[derive(Default)]
struct Listeners {
    /// last revision reconciled, including snapshots still waiting for a socket.
    revision: Option<Revision>,
    /// active QUIC handles that own the HTTP/3 advertisements.
    quic: BTreeMap<SocketAddr, quic::Listener>,
    /// bound HTTP sockets retained across route and certificate updates.
    http: BTreeMap<SocketAddr, HttpListener>,
    /// gateway identity and current forwarding table of each raw socket.
    streams: BTreeMap<(Transport, SocketAddr), StreamListener>,
    /// live SNI routing tables of TLS passthrough sockets.
    tls: BTreeMap<SocketAddr, watch::Sender<Arc<TlsSocket>>>,
    /// accept loops and draining connections for current and retired bindings.
    serving: FuturesUnordered<Task>,
}

impl Listeners {
    /// apply routes and report actual bindings after retiring obsolete socket owners.
    fn reconcile(
        &mut self,
        worker: &Worker,
        snapshot: Arc<PublishedSnapshot>,
    ) -> anyhow::Result<bool> {
        worker.server.service.apply(&snapshot)?;
        let gateway = &snapshot.gateway;
        self.http.retain(|address, http| {
            gateway
                .sockets
                .get(address)
                .is_some_and(|socket| http.reload(&socket.listener))
        });
        for (&address, socket) in &gateway.sockets {
            if self.http.contains_key(&address) {
                continue;
            }
            match worker.bind_http(address, &socket.listener) {
                Ok((listener, task)) => {
                    self.serving.push(task);
                    self.http.insert(address, listener);
                }
                Err(error) => tracing::warn!(%address, %error, "listener bind failed"),
            }
        }
        let changed = self.revision.replace(snapshot.revision) != Some(snapshot.revision);
        let retry_quic =
            worker.runtime.http3 && self.reconcile_http3(worker, gateway, changed);
        // release obsolete stream routes before binding replacement passthrough sockets.
        self.streams.retain(|key, stream| {
            gateway
                .streams
                .get(key)
                .is_some_and(|listener| listener.gateway_uid == stream.gateway_uid)
        });
        self.reconcile_tls(worker, gateway);
        self.reconcile_streams(worker, gateway);
        worker.group.report(worker.id, self.status(snapshot.revision));
        Ok(retry_quic
            || self.http.len() != gateway.sockets.len()
            || self.tls.len() != gateway.tls.len()
            || self.streams.len() != gateway.streams.len())
    }

    /// follow terminating HTTP bindings unless an explicit UDP route owns the address.
    fn reconcile_http3(
        &mut self,
        worker: &Worker,
        gateway: &GatewaySnapshot,
        changed: bool,
    ) -> bool {
        self.quic.retain(|addr, listener| {
            self.http
                .get(addr)
                .is_some_and(|http| http.binding == listener.binding)
                && !gateway.streams.contains_key(&(Transport::Udp, *addr))
        });
        let mut retry = false;
        for (&address, http) in &self.http {
            if !http.binding.tls {
                continue;
            }
            if gateway.streams.contains_key(&(Transport::Udp, address)) {
                if changed && worker.id == 0 {
                    tracing::warn!(%address, "explicit udp listener takes precedence over http/3");
                }
                continue;
            }
            if let Some(listener) = self.quic.get(&address) {
                if changed {
                    listener.certificates.update(&http.certs.borrow());
                }
                continue;
            }
            match quic::Listener::bind(worker, address, http) {
                Ok((listener, task)) => {
                    self.serving.push(task);
                    self.quic.insert(address, listener);
                }
                Err(error) => {
                    retry = true;
                    if error.kind() != io::ErrorKind::WouldBlock {
                        tracing::warn!(%address, %error, "http/3 listener bind failed");
                    }
                }
            }
        }
        retry
    }

    /// preserve passthrough sockets while their owning gateway stays the same.
    fn reconcile_tls(&mut self, worker: &Worker, gateway: &GatewaySnapshot) {
        self.tls.retain(|addr, tx| {
            gateway
                .tls
                .get(addr)
                .is_some_and(|s| s.gateway_uid == tx.borrow().gateway_uid)
        });
        for (&address, socket) in &gateway.tls {
            if let Some(tx) = self.tls.get(&address) {
                update(tx, socket);
                continue;
            }
            let (tx, rx) = watch::channel(socket.clone());
            match tcp::bind(worker, address, rx, tcp::passthrough) {
                Ok(task) => {
                    self.serving.push(task);
                    self.tls.insert(address, tx);
                }
                Err(error) => {
                    tracing::warn!(%address, %error, "tls listener bind failed")
                }
            }
        }
    }

    /// update forwarding tables in place and bind missing raw TCP and UDP sockets.
    fn reconcile_streams(&mut self, worker: &Worker, gateway: &GatewaySnapshot) {
        for (&key, listener) in &gateway.streams {
            if let Some(stream) = self.streams.get(&key) {
                update(&stream.routes, &listener.route);
                continue;
            }
            let (tx, rx) = watch::channel(listener.route.clone());
            let (transport, address) = key;
            let task = match transport {
                Transport::Tcp => tcp::bind(worker, address, rx, tcp::forward),
                Transport::Udp => udp::bind(worker, address, rx),
            };
            match task {
                Ok(task) => {
                    self.serving.push(task);
                    self.streams.insert(
                        key,
                        StreamListener {
                            gateway_uid: listener.gateway_uid.clone(),
                            routes: tx,
                        },
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    tracing::warn!(%address, ?transport, %error, "transport listener bind failed")
                }
            }
        }
    }

    /// report only sockets this worker has successfully bound at the current revision.
    fn status(&self, revision: Revision) -> WorkerStatus {
        WorkerStatus {
            revision,
            bound: self
                .http
                .keys()
                .chain(self.tls.keys())
                .map(|a| (Transport::Tcp, *a))
                .chain(self.quic.keys().map(|a| (Transport::Udp, *a)))
                .chain(self.streams.keys().copied())
                .collect(),
        }
    }

    /// withdraw advertisements and routes, then drain before clearing status and closing upstreams.
    async fn drain(mut self, worker: &Worker) {
        self.quic.clear();
        self.http.clear();
        self.streams.clear();
        self.tls.clear();
        while self.serving.next().await.is_some() {}
        worker.group.report(worker.id, WorkerStatus::default());
        worker.server.service.close();
    }
}

impl Worker {
    /// bind an HTTP socket with a certificate watch for new handshakes.
    fn bind_http(
        &self,
        address: SocketAddr,
        listener: &Listener,
    ) -> io::Result<(HttpListener, Task)> {
        let socket = tcp::listen(address)?;
        let binding = Rc::new(listener.binding());
        let (certs, rx) = watch::channel(Rc::from(listener.tls.as_slice()));
        let task = self.server.clone().accept(
            socket,
            binding.clone(),
            self.admission.clone(),
            rx,
            self.runtime.worker_drain_timeout,
        );
        Ok((HttpListener { binding, certs }, task.boxed_local()))
    }
}

/// notify an existing socket only when its forwarding table changes.
fn update<T: PartialEq>(tx: &watch::Sender<Arc<T>>, desired: &Arc<T>) {
    tx.send_if_modified(|current| {
        if current == desired {
            return false;
        }
        *current = desired.clone();
        true
    });
}

/// drive reconciliation, serving tasks and retry timers until this worker is stopped.
pub async fn run(
    id: usize,
    mut config: Reader,
    runtime: Arc<RuntimeConfig>,
    admission: Arc<Admission>,
    group: Arc<ListenerGroup>,
    stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let retry_interval = runtime.listener_retry_interval;
    let snapshot = config.borrow_and_update().clone();
    let service = ProxyService::new(snapshot, &runtime)?;
    let server = Rc::new(Server::new(service, &runtime));
    let worker = Worker { id, runtime, admission, group, server };
    let mut listeners = Listeners::default();
    let stopping = stopped(stop);
    tokio::pin!(stopping);
    loop {
        let snapshot = config.borrow_and_update().clone();
        let retry = listeners.reconcile(&worker, snapshot)?;
        tokio::select! {
            biased;
            _ = &mut stopping => break,
            change = config.changed() => if change.is_err() { break },
            Some(result) = listeners.serving.next(), if !listeners.serving.is_empty() => result?,
            _ = sleep(retry_interval), if retry => {}
        }
    }
    listeners.drain(&worker).await;
    Ok(())
}
