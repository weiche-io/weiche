//! reconcile a snapshot against the listeners owned by one compio runtime.
use super::{
    super::ir::{
        Binding, GatewaySnapshot, Listener, PublishedSnapshot, Revision, ServerTls,
        StreamListener, StreamRoute, TlsSocket, Transport,
    },
    admission::Admission,
    config::{Reader, RuntimeConfig},
    http::{ProxyService, server::Server},
    net::{ListenerGroup, WorkerStatus, quic, tcp, udp},
    stopped,
};
use compio::runtime::time::sleep;
use futures::{StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
};
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
        let certs = listener.tls.as_slice();
        if self.certs.borrow().as_ref() != certs {
            self.certs.send_replace(Rc::from(certs));
        }
        true
    }
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
    pub server: Server,
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
    streams: BTreeMap<
        (Transport, SocketAddr),
        StreamListener<watch::Sender<Arc<StreamRoute>>>,
    >,
    /// live SNI routing tables of TLS passthrough sockets.
    tls: BTreeMap<SocketAddr, watch::Sender<Arc<TlsSocket>>>,
    /// accept loops and draining connections for current and retired bindings.
    serving: FuturesUnordered<Task>,
}

impl Listeners {
    /// apply routes and report actual bindings after retiring obsolete socket owners.
    fn reconcile(
        &mut self,
        worker: &Rc<Worker>,
        snapshot: Arc<PublishedSnapshot>,
    ) -> anyhow::Result<bool> {
        worker.server.service.apply(&snapshot)?;
        let gateway = &snapshot.gateway;
        retain(&mut self.http, &gateway.sockets, |http, socket| {
            http.reload(&socket.listener)
        });
        let retry_http = bind_listeners(
            &self.serving,
            &mut self.http,
            &gateway.sockets,
            |_, _| {},
            |address, socket| worker.bind_http(address, &socket.listener),
            |address, error| tracing::warn!(%address, %error, "listener bind failed"),
        );

        let changed = self.revision.replace(snapshot.revision) != Some(snapshot.revision);
        let retry_quic =
            worker.runtime.http3 && self.reconcile_http3(worker, gateway, changed);
        // release obsolete stream routes before binding replacement passthrough sockets.
        retain(&mut self.streams, &gateway.streams, |active, next| {
            active.gateway_uid == next.gateway_uid
        });
        retain(&mut self.tls, &gateway.tls, |tx, next| {
            tx.borrow().gateway_uid == next.gateway_uid
        });
        let retry_tls = bind_listeners(
            &self.serving,
            &mut self.tls,
            &gateway.tls,
            update,
            |address, socket| {
                let listener = tcp::listen(address)?;
                let (tx, rx) = watch::channel(socket.clone());
                Ok((tx, tcp::serve(worker, listener, rx, tcp::passthrough)))
            },
            |address, error| tracing::warn!(%address, %error, "tls listener bind failed"),
        );
        let retry_streams = bind_listeners(
            &self.serving,
            &mut self.streams,
            &gateway.streams,
            |active, next| update(&active.route, &next.route),
            |(transport, address), next| {
                let (route, rx) = watch::channel(next.route.clone());
                let task = match transport {
                    Transport::Tcp => {
                        tcp::serve(worker, tcp::listen(address)?, rx, tcp::forward)
                    }
                    Transport::Udp => udp::bind(worker, address, rx)?,
                };
                Ok((
                    StreamListener { gateway_uid: next.gateway_uid.clone(), route },
                    task,
                ))
            },
            |(transport, address), error| {
                if error.kind() != io::ErrorKind::WouldBlock {
                    tracing::warn!(%address, ?transport, %error, "transport listener bind failed");
                }
            },
        );
        worker.group.report(worker.id, self.status(snapshot.revision));
        Ok(retry_http || retry_quic || retry_tls || retry_streams)
    }

    /// follow terminating HTTP bindings unless an explicit UDP route owns the address.
    fn reconcile_http3(
        &mut self,
        worker: &Rc<Worker>,
        gateway: &GatewaySnapshot,
        changed: bool,
    ) -> bool {
        self.quic.retain(|addr, listener| {
            self.http
                .get(addr)
                .is_some_and(|http| http.binding == listener.binding)
                && !gateway.streams.contains_key(&(Transport::Udp, *addr))
        });
        let desired = self.http.iter().filter(|&(&address, http)| {
            if !http.binding.tls {
                return false;
            }
            let explicit = gateway.streams.contains_key(&(Transport::Udp, address));
            if explicit && changed && worker.id == 0 {
                tracing::warn!(%address, "explicit udp listener takes precedence over http/3");
            }
            !explicit
        });
        bind_listeners(
            &self.serving,
            &mut self.quic,
            desired,
            |listener, http| {
                if changed {
                    listener.certificates.update(&http.certs.borrow());
                }
            },
            |address, http| quic::Listener::bind(worker, address, http),
            |address, error| {
                if error.kind() != io::ErrorKind::WouldBlock {
                    tracing::warn!(%address, %error, "http/3 listener bind failed");
                }
            },
        )
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

/// release missing or incompatible owners before opening any replacement sockets.
fn retain<K: Ord, L, D>(
    live: &mut BTreeMap<K, L>,
    desired: &BTreeMap<K, D>,
    keep: impl Fn(&L, &D) -> bool,
) {
    live.retain(|key, listener| {
        desired.get(key).is_some_and(|next| keep(listener, next))
    });
}

/// update retained sockets in key order, register successful binds and flag failures for retry.
fn bind_listeners<'a, K: Ord + Copy + 'a, L, D: 'a>(
    serving: &FuturesUnordered<Task>,
    live: &mut BTreeMap<K, L>,
    desired: impl IntoIterator<Item = (&'a K, &'a D)>,
    update: impl Fn(&L, &D),
    bind: impl Fn(K, &D) -> io::Result<(L, Task)>,
    warn: impl Fn(K, io::Error),
) -> bool {
    let mut retry = false;
    for (&key, next) in desired {
        match live.entry(key) {
            Entry::Occupied(entry) => update(entry.get(), next),
            Entry::Vacant(entry) => match bind(key, next) {
                Ok((listener, task)) => {
                    serving.push(task);
                    entry.insert(listener);
                }
                Err(error) => {
                    retry = true;
                    warn(key, error);
                }
            },
        }
    }
    retry
}

/// notify an existing socket only when its forwarding table changes.
fn update<T: PartialEq>(tx: &watch::Sender<Arc<T>>, desired: &Arc<T>) {
    if *tx.borrow() != *desired {
        tx.send_replace(desired.clone());
    }
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
    let server = Server::new(service, &runtime);
    let worker = Rc::new(Worker { id, runtime, admission, group, server });
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
