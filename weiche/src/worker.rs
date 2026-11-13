//! one runtime's listeners, services and active HTTP connections.
use super::{
    connector::io, listener::ListenerGroup, proxy::ProxyService, routing,
    runtime_config::RuntimeConfig,
};
use compio::{
    net::TcpStream,
    runtime::{self, time::timeout},
};
use hyper::service::service_fn;
use std::{
    cell::Cell,
    mem::DropGuard,
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, watch};

pub struct Admission {
    live: AtomicUsize,
    limit: usize,
}

impl Admission {
    pub fn new(limit: usize) -> Self {
        Self {
            live: AtomicUsize::new(0),
            limit,
        }
    }
    fn acquire(self: &Arc<Self>) -> Option<Permit> {
        self.live
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .ok()
            .map(|_| Permit(self.clone()))
    }
}

struct Permit(Arc<Admission>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Connections {
    active: Cell<usize>,
    changed: Notify,
}

pub struct WorkerState {
    id: usize,
    config: routing::Reader,
    runtime: Arc<RuntimeConfig>,
    service: Rc<ProxyService>,
    connections: Rc<Connections>,
}

impl WorkerState {
    /// called on the owning runtime for every worker, including worker zero.
    pub async fn start(
        id: usize,
        config: routing::Reader,
        runtime: Arc<RuntimeConfig>,
        admission: Arc<Admission>,
        listeners: Arc<ListenerGroup>,
        stop: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let snapshot = config.snapshot();
        let revision = snapshot.revision;
        let service = ProxyService::new(snapshot, &runtime)
            .inspect_err(|error| listeners.failed(id, revision, error))?;
        let worker = Self {
            id,
            config,
            runtime,
            service: Rc::new(service),
            connections: Rc::new(Connections::default()),
        };
        listeners.applied(id, revision);
        worker.run(admission, listeners, stop).await
    }

    fn refresh(&self, listeners: &ListenerGroup) {
        let snapshot = self.config.snapshot();
        let revision = snapshot.revision;
        match self.service.apply(snapshot) {
            Ok(revision) => {
                listeners.applied(self.id, revision);
                tracing::debug!(worker = self.id, revision, "applied gateway snapshot");
            }
            Err(error) => {
                listeners.failed(self.id, revision, &error);
                tracing::error!(worker = self.id, revision, %error, "failed to prepare gateway snapshot");
            }
        }
    }

    fn serve(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Permit,
        shutdown: watch::Receiver<bool>,
    ) {
        // the 2026-11-12 benchmark showed ~40 ms header/body gaps from Nagle/delayed ACKs.
        let stream = match stream.set_nodelay(true).and_then(|()| io(stream)) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(%peer, %error, "failed to prepare HTTP socket");
                return;
            }
        };
        tracing::debug!(worker = self.id, %peer, "serving HTTP connection");
        self.connections
            .active
            .set(self.connections.active.get() + 1);
        // capture the guard before spawning so cancellation before the first poll releases it.
        let guard = DropGuard::new(
            (self.connections.clone(), permit),
            |(connections, _permit)| {
                connections.active.set(connections.active.get() - 1);
                connections.changed.notify_one();
            },
        );
        let service = self.service.clone();
        let max_headers = self.runtime.http_max_headers;
        let max_buffer = self.runtime.http_max_buffer;
        let drain_timeout = self.runtime.connection_drain_timeout;
        runtime::spawn(async move {
            let _guard = guard;
            if *shutdown.borrow() { return; }
            let service = service_fn(move |request| {
                let service = service.clone();
                async move { service.call(request).await }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .max_headers(max_headers).max_buf_size(max_buffer).serve_connection(stream, service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => {
                    if let Err(error) = result { tracing::debug!(%peer, %error, "http connection ended"); }
                }
                _ = stopped(shutdown) => {
                    connection.as_mut().graceful_shutdown();
                    let _ = timeout(drain_timeout, connection).await;
                }
            }
        }).detach();
    }

    async fn drain(&self) {
        // in-flight requests can finish acquiring upstream connections during grace.
        let drained = timeout(self.runtime.worker_drain_timeout, async {
            while self.connections.active.get() > 0 {
                self.connections.changed.notified().await;
            }
        })
        .await;
        self.service.close();
        if drained.is_err() {
            tracing::warn!(worker = self.id, "worker drain deadline exceeded");
        }
    }

    async fn run(
        mut self,
        admission: Arc<Admission>,
        listeners: Arc<ListenerGroup>,
        stop: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let shutdown = stop.subscribe();
        let stopping = stopped(shutdown.clone());
        tokio::pin!(stopping);
        let mut listener = None;
        let mut bound = None;
        let mut requested = None;
        let result = async {
            'listen: loop {
                if *shutdown.borrow() { break; }
                let desired = self.service.listener();
                if desired != bound || desired != requested {
                    requested = desired;
                    listener = None;
                    bound = None;
                    listeners.clear_listener(self.id);
                    if let Some(address) = desired {
                        match listeners.bind(self.id, address) {
                            Ok(socket) => {
                                tracing::info!(worker = self.id, address = %socket.local_addr()?, "compio reuseport listener ready");
                                listener = Some(socket);
                                bound = Some(address);
                            }
                            Err(error) => tracing::error!(worker = self.id, %address, %error, "http listener bind failed; retrying"),
                        }
                    }
                }
                let accepted = async { listener.as_ref().unwrap().accept().await };
                tokio::pin!(accepted);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut stopping => break 'listen,
                        changed = self.config.changed() => {
                            if changed.is_err() {
                                (&mut stopping).await;
                                break 'listen;
                            }
                            self.refresh(&listeners);
                            let desired = self.service.listener();
                            if desired != bound || desired != requested { continue 'listen; }
                            // route updates must not cancel this pending native accept.
                        }
                        _ = runtime::time::sleep(self.runtime.listener_retry_interval), if listener.is_none() => continue 'listen,
                        result = &mut accepted, if listener.is_some() => {
                            let (stream, peer) = result?;
                            if let Some(permit) = admission.acquire() {
                                self.serve(stream, peer, permit, shutdown.clone());
                            }
                            continue 'listen;
                        }
                    }
                }
            }
            Ok(())
        }.await;
        drop(listener);
        listeners.stopped(self.id);
        if result.is_err() {
            stop.send_replace(true);
        }
        self.drain().await;
        result
    }
}

pub async fn stopped(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}
