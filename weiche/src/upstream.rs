//! bounded HTTP/1 connection reuse on the compio worker.
use crate::config::RoutingTable;
use bytes::Bytes;
use compio::{io::compat::AsyncStream, net::TcpStream, runtime};
use http::{Request, Response};
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use hyper_util::rt::TokioIo;
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

// match the downstream connection bound; opening, leased and idle sockets
// share this budget, so slow bodies cannot grow an unbounded upstream pool.
const MAX_CONNECTIONS: usize = 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

type Io = TokioIo<Compat<Pin<Box<AsyncStream<TcpStream>>>>>;
pub(crate) fn io(stream: TcpStream) -> Io {
    TokioIo::new(Box::pin(AsyncStream::new(stream)).compat())
}

#[derive(Default)]
struct Budget {
    live: Cell<usize>,
    changed: Notify,
}

struct Slot(Rc<Budget>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.live.set(self.0.live.get() - 1);
        self.0.changed.notify_one();
    }
}

struct Connection {
    sender: SendRequest<Incoming>,
    // dropping the lease on cancellation also cancels the compio driver.
    _driver: runtime::JoinHandle<()>,
    _slot: Slot,
}

impl Connection {
    async fn open(address: SocketAddr, slot: Slot) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (sender, connection) = hyper::client::conn::http1::handshake(io(stream)).await?;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream connection ended");
            }
        });
        Ok(Self {
            sender,
            _driver: driver,
            _slot: slot,
        })
    }
}

struct Idle {
    connection: Connection,
    since: Instant,
}

struct Endpoint {
    enabled: Cell<bool>,
    closed: Notify,
    idle: RefCell<Vec<Idle>>,
    budget: Rc<Budget>,
}

impl Endpoint {
    fn close(&self) {
        self.enabled.set(false);
        self.idle.borrow_mut().clear();
        self.closed.notify_waiters();
    }

    fn put(&self, connection: Connection) {
        if self.enabled.get() && connection.sender.is_ready() {
            self.idle.borrow_mut().push(Idle {
                connection,
                since: Instant::now(),
            });
            self.budget.changed.notify_one();
        }
    }

    fn prune(&self) {
        self.idle.borrow_mut().retain(|idle| {
            idle.since.elapsed() < IDLE_TIMEOUT && !idle.connection.sender.is_closed()
        });
    }
}

struct State {
    config: Arc<RoutingTable>,
    allowed: HashSet<SocketAddr>,
    endpoints: HashMap<SocketAddr, Rc<Endpoint>>,
}

pub(crate) struct Pool {
    state: RefCell<State>,
    budget: Rc<Budget>,
    closed: Cell<bool>,
    reaper: RefCell<Option<runtime::JoinHandle<()>>>,
}

impl Pool {
    pub(crate) fn new() -> Rc<Self> {
        let pool = Rc::new(Self {
            state: RefCell::new(State {
                config: Arc::new(RoutingTable::default()),
                allowed: HashSet::new(),
                endpoints: HashMap::new(),
            }),
            budget: Rc::new(Budget::default()),
            closed: Cell::new(false),
            reaper: RefCell::new(None),
        });
        let weak = Rc::downgrade(&pool);
        *pool.reaper.borrow_mut() = Some(runtime::spawn(async move {
            loop {
                runtime::time::sleep(Duration::from_secs(1)).await;
                let Some(pool) = weak.upgrade() else { break };
                pool.state.borrow_mut().endpoints.retain(|_, endpoint| {
                    endpoint.prune();
                    Rc::strong_count(endpoint) > 1 || !endpoint.idle.borrow().is_empty()
                });
            }
        }));
        pool
    }

    pub(crate) fn configure(&self, config: &Arc<RoutingTable>) {
        let mut state = self.state.borrow_mut();
        if self.closed.get() || Arc::ptr_eq(&state.config, config) {
            return;
        }
        let allowed: HashSet<_> = config
            .routes
            .iter()
            .filter(|route| !route.invalid_backend && config.bind.is_some())
            .flat_map(|route| route.endpoints.iter().copied())
            .collect();
        state.endpoints.retain(|address, endpoint| {
            if allowed.contains(address) {
                true
            } else {
                // old leases keep this disabled generation even if the address
                // is subsequently added again by another routing snapshot.
                endpoint.close();
                false
            }
        });
        state.allowed = allowed;
        state.config = config.clone();
        self.budget.changed.notify_waiters();
    }

    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.reaper.borrow_mut().take();
        for endpoint in self.state.borrow().endpoints.values() {
            endpoint.close();
        }
        self.budget.changed.notify_waiters();
    }

    fn evict_idle(&self) {
        // reclaim the oldest idle socket when a different backend needs space.
        let state = self.state.borrow();
        let oldest = state
            .endpoints
            .values()
            .filter_map(|endpoint| {
                endpoint
                    .idle
                    .borrow()
                    .first()
                    .map(|idle| (idle.since, endpoint))
            })
            .min_by_key(|(since, _)| *since);
        if let Some((_, endpoint)) = oldest {
            endpoint.idle.borrow_mut().remove(0);
        }
    }

    async fn acquire(&self, address: SocketAddr, reuse: bool) -> anyhow::Result<Lease> {
        let endpoint = {
            let mut state = self.state.borrow_mut();
            anyhow::ensure!(!self.closed.get(), "upstream pool is closed");
            anyhow::ensure!(state.allowed.contains(&address), "backend was removed");
            state
                .endpoints
                .entry(address)
                .or_insert_with(|| {
                    Rc::new(Endpoint {
                        enabled: Cell::new(true),
                        closed: Notify::new(),
                        idle: RefCell::new(Vec::new()),
                        budget: self.budget.clone(),
                    })
                })
                .clone()
        };
        loop {
            anyhow::ensure!(
                !self.closed.get() && endpoint.enabled.get(),
                "backend was removed or pool closed"
            );
            let idle = if reuse {
                endpoint.idle.borrow_mut().pop()
            } else {
                None
            };
            if let Some(mut idle) = idle {
                if idle.since.elapsed() >= IDLE_TIMEOUT || idle.connection.sender.is_closed() {
                    continue;
                }
                if idle.connection.sender.ready().await.is_ok() {
                    return Ok(Lease {
                        connection: idle.connection,
                        endpoint,
                        reused: true,
                    });
                }
                continue;
            }
            if self.budget.live.get() == MAX_CONNECTIONS {
                self.evict_idle();
            }
            if self.budget.live.get() < MAX_CONNECTIONS {
                self.budget.live.set(self.budget.live.get() + 1);
                let connection = Connection::open(address, Slot(self.budget.clone())).await?;
                return Ok(Lease {
                    connection,
                    endpoint,
                    reused: false,
                });
            }
            // register, then recheck: an idle return retains its slot and is
            // just as important as a slot release for a capacity waiter.
            let changed = self.budget.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.closed.get()
                || !endpoint.enabled.get()
                || self.budget.live.get() < MAX_CONNECTIONS
                || self
                    .state
                    .borrow()
                    .endpoints
                    .values()
                    .any(|entry| !entry.idle.borrow().is_empty())
            {
                continue;
            }
            changed.await;
        }
    }

    pub(crate) async fn send(
        &self,
        address: SocketAddr,
        request: Request<Incoming>,
    ) -> anyhow::Result<Response<PooledBody>> {
        let mut lease = self.acquire(address, true).await?;
        let response = match lease.connection.sender.try_send_request(request).await {
            Ok(response) => response,
            Err(mut error) => {
                // retry once on a fresh socket only when hyper returns an
                // untouched request; never replay a possibly transmitted POST.
                let request = if lease.reused {
                    error.take_message()
                } else {
                    None
                };
                let Some(request) = request else {
                    return Err(error.into_error().into());
                };
                drop(lease);
                lease = self.acquire(address, false).await?;
                lease.connection.sender.send_request(request).await?
            }
        };
        Ok(response.map(|body| PooledBody::new(body, lease)))
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.close();
    }
}

struct Lease {
    connection: Connection,
    endpoint: Rc<Endpoint>,
    reused: bool,
}

impl Lease {
    fn recycle(self) {
        if !self.endpoint.enabled.get() || self.connection.sender.is_closed() {
            return;
        }
        if self.connection.sender.is_ready() {
            self.endpoint.put(self.connection);
        } else {
            // response EOF alone does not prove that an upload has finished or
            // that hyper has completed HTTP framing. wait before publishing idle.
            runtime::spawn(async move {
                if !self.endpoint.enabled.get() {
                    return;
                }
                let mut connection = self.connection;
                // a slow upload still owns its slot; closing the endpoint must
                // cancel this wait without imposing a new request body timeout.
                tokio::select! {
                    ready = connection.sender.ready() => {
                        if ready.is_ok() {
                            self.endpoint.put(connection);
                        }
                    },
                    _ = self.endpoint.closed.notified() => {},
                }
            })
            .detach();
        }
    }
}

pub(crate) struct PooledBody {
    body: Incoming,
    lease: Option<Lease>,
}

impl PooledBody {
    fn new(body: Incoming, lease: Lease) -> Self {
        let mut body = Self {
            body,
            lease: Some(lease),
        };
        // HEAD, 204 and empty bodies may never be polled by the downstream.
        if hyper::body::Body::is_end_stream(&body.body) {
            body.complete();
        }
        body
    }

    fn complete(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.recycle();
        }
    }
}

impl hyper::body::Body for PooledBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Err(_))) => {
                self.lease.take();
            }
            Poll::Ready(None) => self.complete(),
            Poll::Ready(Some(Ok(frame))) if frame.is_trailers() || self.body.is_end_stream() => {
                self.complete()
            }
            _ => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}
