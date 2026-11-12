//! streaming HTTP/1.1 proxy with one compio runtime per data-plane worker.
use crate::{
    config::RoutingTable,
    routing,
    upstream::{Pool, io},
};
use anyhow::Context;
use bytes::Bytes;
use compio::{
    net::{TcpListener, TcpStream},
    runtime::{self, time::timeout},
};
use futures::{StreamExt, stream::FuturesUnordered};
use http::{HeaderMap, Request, Response, StatusCode, Uri, Version, header};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::{oneshot, watch};

// response leases stay on the compio worker and must not cross threads.
type Body = Pin<Box<dyn hyper::body::Body<Data = Bytes, Error = hyper::Error>>>;
const MAX_CONNECTIONS: usize = 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListenerState {
    pub bound: Option<SocketAddr>,
    pub error: Option<String>,
}

struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ConnectionGuard {
    active: Rc<Cell<usize>>,
    _permit: Permit,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active.set(self.active.get() - 1);
    }
}

struct Worker {
    id: usize,
    config: routing::Reader,
    snapshot: Rc<RefCell<Arc<RoutingTable>>>,
    cursor: Rc<Cell<usize>>,
    active: Rc<Cell<usize>>,
    pool: Rc<Pool>,
}

impl Worker {
    fn new(id: usize, config: routing::Reader) -> Self {
        // the 2026-11-12 benchmark exhausted ephemeral ports with one upstream
        // connection per request; each worker keeps its own reusable pool.
        let pool = Pool::new();
        let snapshot = config.snapshot();
        pool.configure(&snapshot);
        Self {
            id,
            config,
            snapshot: Rc::new(RefCell::new(snapshot)),
            cursor: Rc::new(Cell::new(0)),
            active: Rc::new(Cell::new(0)),
            pool,
        }
    }

    fn refresh(&self) {
        let snapshot = self.config.snapshot();
        self.pool.configure(&snapshot);
        *self.snapshot.borrow_mut() = snapshot;
    }

    fn serve(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Permit,
        shutdown: watch::Receiver<bool>,
    ) {
        // the 2026-11-12 kind benchmark showed ~40 ms gaps between response
        // headers and bodies; disable Nagle before HTTP to avoid delayed ACKs.
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%peer, %error, "failed to enable TCP_NODELAY");
            return;
        }
        tracing::debug!(worker = self.id, %peer, "serving HTTP connection");
        self.active.set(self.active.get() + 1);
        let guard = ConnectionGuard {
            active: self.active.clone(),
            _permit: permit,
        };
        let config = self.snapshot.clone();
        let cursor = self.cursor.clone();
        let pool = self.pool.clone();
        runtime::spawn(async move {
            let _guard = guard;
            if *shutdown.borrow() { return; }
            let service = service_fn(move |request| proxy(request, config.clone(), cursor.clone(), pool.clone()));
            let connection = hyper::server::conn::http1::Builder::new()
                .max_headers(100).max_buf_size(32 * 1024).serve_connection(io(stream), service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => {
                    if let Err(error) = result { tracing::debug!(%peer, %error, "HTTP connection ended"); }
                },
                _ = stopped(shutdown) => {
                    connection.as_mut().graceful_shutdown();
                    let _ = timeout(Duration::from_secs(5), connection).await;
                }
            }
        }).detach();
    }

    async fn drain(&self) {
        self.pool.close();
        let _ = timeout(Duration::from_secs(6), async {
            while self.active.get() > 0 {
                runtime::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
    }

    async fn run(
        mut self,
        total: Arc<AtomicUsize>,
        listeners: Arc<Listeners>,
        stop: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let shutdown = stop.subscribe();
        let stopped = stopped(shutdown.clone());
        tokio::pin!(stopped);
        let mut listener = None;
        let mut bound = None;
        let mut requested = None;
        let result = async {
            'listen: loop {
                if *shutdown.borrow() { break; }
                let desired = self.snapshot.borrow().bind;
                if desired != bound || desired != requested {
                    requested = desired;
                    listener = None;
                    bound = None;
                    listeners.clear(self.id);
                    if let Some(address) = desired {
                        match listeners.bind(self.id, address) {
                            Ok(socket) => {
                                tracing::info!(worker = self.id, address = %socket.local_addr()?, "compio reuseport listener ready");
                                listener = Some(socket);
                                bound = Some(address);
                            }
                            Err(error) => tracing::error!(worker = self.id, %address, %error, "HTTP listener bind failed; retrying"),
                        }
                    }
                }
                let accepted = async { listener.as_ref().unwrap().accept().await };
                tokio::pin!(accepted);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut stopped => break 'listen,
                        changed = self.config.changed() => {
                            if changed.is_err() {
                                // the supervisor also observes publisher closure and stops all workers.
                                (&mut stopped).await;
                                break 'listen;
                            }
                            self.refresh();
                            let desired = self.snapshot.borrow().bind;
                            if desired != bound || desired != requested { continue 'listen; }
                            // keep this native accept future alive across route-only updates.
                        },
                        _ = runtime::time::sleep(Duration::from_millis(250)), if listener.is_none() => continue 'listen,
                        result = &mut accepted, if listener.is_some() => {
                            let (stream, peer) = result?;
                            if total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < MAX_CONNECTIONS).then_some(n + 1)).is_ok() {
                                self.serve(stream, peer, Permit(total.clone()), shutdown.clone());
                            }
                            continue 'listen;
                        },
                    }
                }
            }
            Ok(())
        }.await;
        drop(listener);
        listeners.clear(self.id);
        if result.is_err() {
            stop.send_replace(true);
        }
        self.drain().await;
        result
    }
}

pub(crate) async fn stopped(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}

#[derive(Clone, Default)]
struct Binding {
    requested: Option<SocketAddr>,
    state: ListenerState,
}

// this lock is used only when listeners bind or close, never to accept connections.
// it also lets port-zero listeners join the same kernel-selected port.
struct Listeners {
    bindings: Mutex<Vec<Binding>>,
    config: routing::Reader,
    state: watch::Sender<ListenerState>,
}

impl Listeners {
    fn bind(&self, worker: usize, requested: SocketAddr) -> std::io::Result<TcpListener> {
        let mut bindings = self.bindings.lock().unwrap();
        let address = bindings
            .iter()
            .find(|binding| binding.requested == Some(requested) && binding.state.bound.is_some())
            .and_then(|binding| binding.state.bound)
            .unwrap_or(requested);
        let result = listen(address).and_then(|socket| Ok((socket.local_addr()?, socket)));
        let state = match &result {
            Ok((address, _)) => ListenerState {
                bound: Some(*address),
                error: None,
            },
            Err(error) => ListenerState {
                bound: None,
                error: Some(error.to_string()),
            },
        };
        bindings[worker] = Binding {
            requested: Some(requested),
            state,
        };
        self.publish(&bindings);
        result.map(|(_, socket)| socket)
    }

    fn clear(&self, worker: usize) {
        let mut bindings = self.bindings.lock().unwrap();
        bindings[worker] = Binding::default();
        self.publish(&bindings);
    }

    fn publish(&self, bindings: &[Binding]) {
        let desired = self.config.snapshot().bind;
        let error = bindings.iter().enumerate().find_map(|(id, binding)| {
            binding
                .state
                .error
                .as_ref()
                .map(|error| format!("worker {id}: {error}"))
        });
        let bound = bindings[0].state.bound.filter(|address| {
            desired.is_some()
                && error.is_none()
                && bindings.iter().all(|binding| {
                    binding.requested == desired && binding.state.bound == Some(*address)
                })
        });
        // advertise readiness only when every worker has joined the same listener group.
        let state = ListenerState { bound, error };
        self.state.send_if_modified(|old| {
            if *old == state {
                false
            } else {
                *old = state;
                true
            }
        });
    }
}

fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(target_os = "linux")]
    socket.set_reuse_port(true)?;
    #[cfg(not(target_os = "linux"))]
    return Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reuseport workers require Linux",
    ));
    socket.bind(&address.into())?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}

/// each worker owns a native listener; the kernel distributes new connections.
pub async fn run(
    mut config: routing::Reader,
    state: watch::Sender<ListenerState>,
    shutdown: watch::Receiver<bool>,
    workers: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(workers > 0, "at least one data-plane worker is required");
    let listeners = Arc::new(Listeners {
        bindings: Mutex::new(vec![Binding::default(); workers]),
        config: config.clone(),
        state: state.clone(),
    });
    let total = Arc::new(AtomicUsize::new(0));
    let (stop, stopped_rx) = watch::channel(false);
    let local = Worker::new(0, config.clone()).run(total.clone(), listeners.clone(), stop.clone());
    tokio::pin!(local);
    let mut local_done = false;
    let mut threads = Vec::new();
    let mut completed = FuturesUnordered::new();
    let mut result = async {
        let mut ready = Vec::new();
        for id in 1..workers {
            let config = config.clone();
            let stop = stop.clone();
            let total = total.clone();
            let listeners = listeners.clone();
            let (ready_tx, ready_rx) = oneshot::channel();
            let (done_tx, done_rx) = oneshot::channel();
            threads.push(thread::Builder::new().name(format!("weiche-worker-{id}")).spawn(move || {
                let result = (|| -> anyhow::Result<()> {
                    let runtime = match runtime::Runtime::new() {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return Ok(());
                        }
                    };
                    tracing::info!(worker = id, driver = ?runtime.driver_type(), "starting compio worker");
                    runtime.block_on(async {
                        let worker = Worker::new(id, config);
                        let _ = ready_tx.send(Ok(()));
                        worker.run(total, listeners, stop).await
                    })
                })();
                let _ = done_tx.send(result);
            }).context("spawn data-plane worker")?);
            ready.push((id, ready_rx));
            completed.push(done_rx);
        }
        for (id, ready) in ready {
            ready.await.with_context(|| format!("worker {id} failed during startup"))?
                .with_context(|| format!("create compio runtime for worker {id}"))?;
        }
        tokio::select! {
            biased;
            _ = stopped(shutdown) => Ok(()),
            _ = stopped(stopped_rx) => Ok(()),
            _ = async { while config.changed().await.is_ok() {} } => Ok(()),
            result = &mut local => { local_done = true; result },
            result = completed.next(), if !completed.is_empty() => {
                result.unwrap().context("worker panicked")??;
                anyhow::bail!("data-plane worker stopped unexpectedly");
            },
        }
    }.await;
    stop.send_replace(true);
    if !local_done {
        let local_result = local.await;
        if result.is_ok() {
            result = local_result;
        }
    }
    while let Some(done) = completed.next().await {
        let done = done.context("worker panicked").and_then(|result| result);
        if result.is_ok() {
            result = done;
        }
    }
    for thread in threads {
        if thread.join().is_err() && result.is_ok() {
            result = Err(anyhow::anyhow!("data-plane worker panicked"));
        }
    }
    state.send_replace(ListenerState::default());
    result
}

async fn proxy(
    mut request: Request<Incoming>,
    config: Rc<RefCell<Arc<RoutingTable>>>,
    cursor: Rc<Cell<usize>>,
    pool: Rc<Pool>,
) -> Result<Response<Body>, Infallible> {
    if request.method() == http::Method::CONNECT || request.headers().contains_key(header::UPGRADE)
    {
        return Ok(reply(
            StatusCode::NOT_IMPLEMENTED,
            "HTTP upgrades and CONNECT are unsupported\n",
        ));
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<http::uri::Authority>().ok())
        .map(|h| h.host().to_ascii_lowercase());
    let Some(host) = host else {
        return Ok(reply(
            StatusCode::BAD_REQUEST,
            "a valid Host header is required\n",
        ));
    };
    // only configuration notifications touch the A/B locks and shared Arc counts.
    // borrow this worker's snapshot until backend selection, never across I/O.
    let endpoint = {
        let snapshot = config.borrow();
        let Some(route) = snapshot.find(
            &host,
            request.uri().path(),
            request.method(),
            request.headers(),
        ) else {
            return Ok(reply(StatusCode::NOT_FOUND, "no matching HTTPRoute\n"));
        };
        if route.invalid_backend {
            return Ok(reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                "HTTPRoute has an invalid backend reference\n",
            ));
        }
        if route.endpoints.is_empty() {
            return Ok(reply(
                StatusCode::SERVICE_UNAVAILABLE,
                "Service has no ready endpoints\n",
            ));
        }
        let index = cursor.get();
        cursor.set(index.wrapping_add(1));
        route.endpoints[index % route.endpoints.len()]
    };
    // Preserve Host, path, query, method and streaming body. Strip proxy connection metadata.
    strip_hop_headers(request.headers_mut());
    // Framing is regenerated by hyper from Incoming's size hint and frames.
    *request.version_mut() = Version::HTTP_11;
    let path = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    *request.uri_mut() = path.parse::<Uri>().unwrap();
    let response = timeout(Duration::from_secs(10), pool.send(endpoint, request)).await;
    match response {
        Ok(Ok(response)) => {
            let (mut parts, body) = response.into_parts();
            strip_hop_headers(&mut parts.headers);
            Ok(Response::from_parts(parts, Box::pin(body) as Body))
        }
        Ok(Err(error)) => {
            tracing::warn!(%endpoint, %error, "HTTP backend request failed");
            Ok(reply(StatusCode::BAD_GATEWAY, "backend request failed\n"))
        }
        Err(_) => Ok(reply(
            StatusCode::GATEWAY_TIMEOUT,
            "backend response headers timed out\n",
        )),
    }
}

pub fn strip_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|s| s.trim().parse::<header::HeaderName>().ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn reply(status: StatusCode, text: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Box::pin(
            Full::new(Bytes::from_static(text.as_bytes()))
                .map_err(|never| -> hyper::Error { match never {} }),
        ) as Body)
        .unwrap()
}
