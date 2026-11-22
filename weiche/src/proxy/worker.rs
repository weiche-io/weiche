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
type Task = LocalBoxFuture<'static, io::Result<()>>;

/// identity and reloadable certificate table of a bound HTTP socket.
struct HttpListener {
    /// immutable identity shared with accepted connections and the QUIC endpoint.
    binding: Rc<Binding>,
    /// latest certificate table for new handshakes on this socket.
    certs: watch::Sender<Rc<[Arc<ServerTls>]>>,
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

/// active bindings, their serving tasks and the resources shared by this worker.
struct Listeners {
    /// index in the reuseport group and worker status table.
    id: usize,
    /// immutable timeouts, worker count and HTTP/3 policy.
    runtime: Arc<RuntimeConfig>,
    /// connection and flow budget shared with the other workers.
    admission: Arc<Admission>,
    /// datagram reservations and status reports for the group.
    group: Arc<ListenerGroup>,
    /// worker-local HTTP service shared by TCP and QUIC listeners.
    server: Rc<Server>,
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
    /// create the worker's routing service before opening any sockets.
    fn new(
        id: usize,
        snapshot: Arc<PublishedSnapshot>,
        runtime: Arc<RuntimeConfig>,
        admission: Arc<Admission>,
        group: Arc<ListenerGroup>,
    ) -> anyhow::Result<Self> {
        let service = ProxyService::new(snapshot, &runtime)?;
        let server = Rc::new(Server::new(service, &runtime));
        Ok(Self {
            id,
            runtime,
            admission,
            group,
            server,
            revision: None,
            quic: BTreeMap::new(),
            http: BTreeMap::new(),
            streams: BTreeMap::new(),
            tls: BTreeMap::new(),
            serving: FuturesUnordered::new(),
        })
    }

    /// apply routes, report actual bindings and request a retry for any missing socket.
    fn reconcile(&mut self, snapshot: Arc<PublishedSnapshot>) -> anyhow::Result<bool> {
        self.server.service.apply(snapshot.clone())?;
        let gateway = &snapshot.gateway;
        self.reconcile_http(gateway);
        let changed = self.revision.replace(snapshot.revision) != Some(snapshot.revision);
        let retry_quic = self.runtime.http3 && self.reconcile_http3(gateway, changed);
        // release obsolete stream routes before binding replacement passthrough sockets.
        self.streams.retain(|key, stream| {
            gateway
                .streams
                .get(key)
                .is_some_and(|listener| listener.gateway_uid == stream.gateway_uid)
        });
        self.reconcile_tls(gateway);
        self.reconcile_streams(gateway);
        self.group.report(self.id, self.status(snapshot.revision));
        Ok(retry_quic
            || self.http.len() != gateway.sockets.len()
            || self.tls.len() != gateway.tls.len()
            || self.streams.len() != gateway.streams.len())
    }

    /// retain matching bindings and start an accept loop for each newly bound HTTP socket.
    fn reconcile_http(&mut self, gateway: &GatewaySnapshot) {
        self.http.retain(|address, http| {
            gateway
                .sockets
                .get(address)
                .is_some_and(|socket| http.reload(&socket.listener))
        });
        for (&address, routes) in &gateway.sockets {
            if self.http.contains_key(&address) {
                continue;
            }
            let socket = match tcp::listen(address) {
                Ok(socket) => socket,
                Err(error) => {
                    tracing::warn!(%address, %error, "listener bind failed");
                    continue;
                }
            };
            let binding = Rc::new(routes.listener.binding());
            let (certs, rx) = watch::channel(Rc::from(routes.listener.tls.as_slice()));
            self.serving.push(
                self.server
                    .clone()
                    .accept(
                        socket,
                        binding.clone(),
                        self.admission.clone(),
                        rx,
                        self.runtime.worker_drain_timeout,
                    )
                    .boxed_local(),
            );
            self.http.insert(address, HttpListener { binding, certs });
        }
    }

    /// follow terminating HTTP bindings unless an explicit UDP route owns the address.
    fn reconcile_http3(&mut self, gateway: &GatewaySnapshot, changed: bool) -> bool {
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
                if changed && self.id == 0 {
                    tracing::warn!(%address, "explicit udp listener takes precedence over http/3");
                }
                continue;
            }
            if let Some(listener) = self.quic.get(&address) {
                if changed {
                    listener.update(&http.certs.borrow());
                }
                continue;
            }
            match self.bind_http3(address, http) {
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

    /// reserve this worker's UDP slot and keep it until the QUIC endpoint shuts down.
    fn bind_http3(
        &self,
        address: SocketAddr,
        http: &HttpListener,
    ) -> io::Result<(quic::Listener, Task)> {
        let (socket, reader) = self.group.datagrams.acquire(
            address,
            self.id,
            self.runtime.workers.get(),
            true,
        )?;
        let (listener, endpoint) = quic::Listener::bind(
            socket,
            self.server.service.clone(),
            http.binding.clone(),
            &http.certs.borrow(),
            self.id,
            &self.runtime,
        )?;
        let task = listener.serve(
            endpoint,
            self.admission.clone(),
            self.runtime.clone(),
            reader,
        );
        Ok((listener, task.boxed_local()))
    }

    /// preserve passthrough sockets while their owning gateway stays the same.
    fn reconcile_tls(&mut self, gateway: &GatewaySnapshot) {
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
            match tcp::listen(address) {
                Ok(listener) => {
                    let (tx, rx) = watch::channel(socket.clone());
                    self.serving.push(
                        tcp::serve(
                            listener,
                            rx,
                            self.admission.clone(),
                            self.runtime.clone(),
                            tcp::passthrough,
                        )
                        .boxed_local(),
                    );
                    self.tls.insert(address, tx);
                }
                Err(error) => {
                    tracing::warn!(%address, %error, "tls listener bind failed")
                }
            }
        }
    }

    /// update forwarding tables in place and bind missing raw TCP and UDP sockets.
    fn reconcile_streams(&mut self, gateway: &GatewaySnapshot) {
        for (&key, listener) in &gateway.streams {
            if let Some(stream) = self.streams.get(&key) {
                update(&stream.routes, &listener.route);
                continue;
            }
            let (tx, rx) = watch::channel(listener.route.clone());
            let (transport, address) = key;
            match self.bind_stream(transport, address, rx) {
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

    /// bind a raw listener and return the task that owns its socket.
    fn bind_stream(
        &self,
        transport: Transport,
        address: SocketAddr,
        routes: watch::Receiver<Arc<StreamRoute>>,
    ) -> io::Result<Task> {
        let admission = self.admission.clone();
        match transport {
            Transport::Tcp => {
                let listener = tcp::listen(address)?;
                let task = tcp::serve(
                    listener,
                    routes,
                    admission,
                    self.runtime.clone(),
                    tcp::forward,
                );
                Ok(task.boxed_local())
            }
            Transport::Udp => udp::bind(
                address,
                self.id,
                &self.group.datagrams,
                &self.runtime,
                routes,
                admission,
            ),
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
    async fn drain(mut self) {
        self.quic.clear();
        self.http.clear();
        self.streams.clear();
        self.tls.clear();
        while self.serving.next().await.is_some() {}
        self.group.report(self.id, WorkerStatus::default());
        self.server.service.close();
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
    let mut listeners = Listeners::new(id, config.snapshot(), runtime, admission, group)?;
    let stopping = stopped(stop);
    tokio::pin!(stopping);
    loop {
        let retry = listeners.reconcile(config.snapshot())?;
        tokio::select! {
            biased;
            _ = &mut stopping => break,
            change = config.changed() => if change.is_err() { break },
            Some(result) = listeners.serving.next(), if !listeners.serving.is_empty() => result?,
            _ = sleep(retry_interval), if retry => {}
        }
    }
    listeners.drain().await;
    Ok(())
}
