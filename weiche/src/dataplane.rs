//! streaming HTTP/1.1 proxy with one compio runtime per data-plane worker.
use crate::{
    config::RoutingTable,
    routing,
    upstream::{Pool, io},
};
use anyhow::Context;
use bytes::Bytes;
use compio::{
    net::TcpStream,
    runtime::{self, fd::PollFd, time::timeout},
};
use futures::{StreamExt, stream::FuturesUnordered};
use http::{HeaderMap, Request, Response, StatusCode, Uri, Version, header};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn};
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    future::poll_fn,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

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

// only an unattached, owned socket crosses threads; no compio I/O is in flight.
struct Accepted {
    socket: std::net::TcpStream,
    peer: SocketAddr,
    permit: Permit,
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

    fn serve(&self, accepted: Accepted, shutdown: watch::Receiver<bool>) {
        let Accepted {
            socket,
            peer,
            permit,
        } = accepted;
        let stream = match TcpStream::from_std(socket) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(worker = self.id, %peer, %error, "failed to attach client socket");
                return;
            }
        };
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

    async fn receive(
        mut self,
        mut incoming: mpsc::Receiver<Accepted>,
        shutdown: watch::Receiver<bool>,
    ) {
        let stop = stopped(shutdown.clone());
        tokio::pin!(stop);
        loop {
            tokio::select! {
                biased;
                _ = &mut stop => break,
                changed = self.config.changed() => {
                    if changed.is_err() {
                        // the supervisor owns shutdown, including control-thread exit.
                        stopped(shutdown.clone()).await;
                        break;
                    }
                    self.refresh();
                },
                accepted = incoming.recv() => match accepted {
                    Some(accepted) => {
                        self.serve(accepted, shutdown.clone());
                        yield_to_connections().await;
                    },
                    None => break,
                },
            }
        }
        drop(incoming);
        self.drain().await;
    }
}

pub(crate) async fn stopped(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}

async fn yield_to_connections() {
    // a continuously ready accept/receive loop must still let HTTP tasks run.
    let mut yielded = false;
    poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

fn listen(address: SocketAddr) -> std::io::Result<PollFd<std::net::TcpListener>> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    PollFd::new(socket.into())
}

async fn accept(
    listener: &PollFd<std::net::TcpListener>,
) -> std::io::Result<(std::net::TcpStream, SocketAddr)> {
    loop {
        match listener.accept() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                // the 2026-11-13 single-core comparison exposed extra accept
                // attempts: compio polls its root future on every I/O turn.
                // keep waiting here instead of retrying the syscall each poll.
                listener.accept_ready().await?;
            }
            result => return result,
        }
    }
}

/// worker zero accepts and distributes sockets in round-robin order, including itself.
pub async fn run(
    config: routing::Reader,
    state: watch::Sender<ListenerState>,
    shutdown: watch::Receiver<bool>,
    workers: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(workers > 0, "at least one data-plane worker is required");
    let mut local = Worker::new(0, config.clone());
    let (stop, stopped_rx) = watch::channel(false);
    let mut threads = Vec::new();
    let mut senders = Vec::new();
    let mut completed = FuturesUnordered::new();
    let total = Arc::new(AtomicUsize::new(0));
    let mut result = async {
        let mut ready = Vec::new();
        for id in 1..workers {
            let config = config.clone();
            let shutdown = stopped_rx.clone();
            let (tx, rx) = mpsc::channel(MAX_CONNECTIONS);
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
                        worker.receive(rx, shutdown).await;
                    });
                    Ok(())
                })();
                let _ = done_tx.send(result);
            }).context("spawn data-plane worker")?);
            senders.push(tx);
            ready.push((id, ready_rx));
            completed.push(done_rx);
        }
        for (id, ready) in ready {
            ready.await.with_context(|| format!("worker {id} failed during startup"))?
                .with_context(|| format!("create compio runtime for worker {id}"))?;
        }
        let mut listener = None;
        let mut bound = None;
        let mut next = 0;
        let shutdown = stopped(shutdown);
        tokio::pin!(shutdown);
        loop {
            let desired = local.snapshot.borrow().bind;
            if desired.is_none() && *state.borrow() != ListenerState::default() {
                state.send_replace(ListenerState::default());
            }
            if desired != bound {
                listener = None;
                bound = None;
                if let Some(address) = desired {
                    match listen(address) {
                        Ok(socket) => {
                            let actual = socket.local_addr()?;
                            listener = Some(socket);
                            bound = Some(address);
                            state.send_replace(ListenerState { bound: Some(actual), error: None });
                            tracing::info!(address = %actual, workers, "compio HTTP listener ready");
                        },
                        Err(error) => {
                            tracing::error!(%address, %error, "HTTP listener bind failed; retrying");
                            state.send_replace(ListenerState { bound: None, error: Some(error.to_string()) });
                        },
                    }
                } else {
                    state.send_replace(ListenerState::default());
                }
            }
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                result = completed.next(), if !completed.is_empty() => {
                    result.unwrap().context("worker panicked")??;
                    anyhow::bail!("data-plane worker stopped unexpectedly");
                },
                changed = local.config.changed() => {
                    if changed.is_err() { break; }
                    local.refresh();
                },
                _ = runtime::time::sleep(Duration::from_millis(250)), if listener.is_none() => {},
                // readiness has no accepted socket to lose if configuration wakes us.
                accepted = async { accept(listener.as_ref().unwrap()).await }, if listener.is_some() => {
                    let (socket, peer) = accepted?;
                    if total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < MAX_CONNECTIONS).then_some(n + 1)).is_err() {
                        yield_to_connections().await;
                        continue;
                    }
                    let accepted = Accepted { socket, peer, permit: Permit(total.clone()) };
                    let target = next;
                    next = (next + 1) % workers;
                    if target == 0 {
                        local.serve(accepted, stopped_rx.clone());
                    } else {
                        senders[target - 1].try_send(accepted).map_err(|_| anyhow::anyhow!("worker queue unavailable"))?;
                    }
                    yield_to_connections().await;
                },
            }
        }
        Ok(())
    }.await;
    stop.send_replace(true);
    drop(senders);
    // all workers drain concurrently, including worker zero on the main runtime.
    local.drain().await;
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
