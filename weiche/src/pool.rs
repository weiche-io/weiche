//! bounded HTTP/1 connection reuse on the compio worker.
use super::connector::{Connected, Connector};
use bytes::Bytes;
use compio::runtime;
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

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
    transport: Connected,
    _slot: Slot,
}

struct Idle {
    connection: Connection,
    since: Instant,
}

pub struct Endpoint {
    pub address: SocketAddr,
    idle_timeout: Duration,
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
        if self.enabled.get() && connection.transport.sender.is_ready() {
            self.idle.borrow_mut().push(Idle {
                connection,
                since: Instant::now(),
            });
            self.budget.changed.notify_one();
        }
    }

    fn prune(&self) {
        self.idle.borrow_mut().retain(|idle| {
            idle.since.elapsed() < self.idle_timeout
                && !idle.connection.transport.sender.is_closed()
        });
    }
}

pub struct ConnectionPool {
    endpoints: RefCell<HashMap<SocketAddr, Rc<Endpoint>>>,
    limit: usize,
    idle_timeout: Duration,
    connector: Connector,
    budget: Rc<Budget>,
    closed: Cell<bool>,
    reaper: RefCell<Option<runtime::JoinHandle<()>>>,
}

impl ConnectionPool {
    pub fn new(limit: usize, idle_timeout: Duration) -> Rc<Self> {
        let pool = Rc::new(Self {
            endpoints: RefCell::new(HashMap::new()),
            limit,
            idle_timeout,
            connector: Connector,
            budget: Rc::new(Budget::default()),
            closed: Cell::new(false),
            reaper: RefCell::new(None),
        });
        let weak = Rc::downgrade(&pool);
        *pool.reaper.borrow_mut() = Some(runtime::spawn(async move {
            loop {
                runtime::time::sleep(Duration::from_secs(1)).await;
                let Some(pool) = weak.upgrade() else { break };
                for endpoint in pool.endpoints.borrow().values() {
                    endpoint.prune();
                }
            }
        }));
        pool
    }

    /// membership is supplied by the cluster manager; no routing knowledge lives here.
    pub fn reconcile(&self, allowed: &HashSet<SocketAddr>) {
        let mut endpoints = self.endpoints.borrow_mut();
        endpoints.retain(|address, endpoint| {
            if allowed.contains(address) {
                true
            } else {
                // leases retain this disabled generation across address removal/re-addition.
                endpoint.close();
                false
            }
        });
        for &address in allowed {
            endpoints.entry(address).or_insert_with(|| {
                Rc::new(Endpoint {
                    address,
                    idle_timeout: self.idle_timeout,
                    enabled: Cell::new(true),
                    closed: Notify::new(),
                    idle: RefCell::new(Vec::new()),
                    budget: self.budget.clone(),
                })
            });
        }
        self.budget.changed.notify_waiters();
    }

    pub fn endpoint(&self, address: SocketAddr) -> Option<Rc<Endpoint>> {
        self.endpoints.borrow().get(&address).cloned()
    }

    pub fn close(&self) {
        self.closed.set(true);
        self.reaper.borrow_mut().take();
        for endpoint in self.endpoints.borrow().values() {
            endpoint.close();
        }
        self.budget.changed.notify_waiters();
    }

    fn evict_idle(&self) {
        // reclaim the oldest idle socket when a different backend needs space.
        let endpoints = self.endpoints.borrow();
        let oldest = endpoints
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

    pub async fn acquire(&self, endpoint: Rc<Endpoint>, reuse: bool) -> anyhow::Result<Lease> {
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
                if idle.since.elapsed() >= self.idle_timeout
                    || idle.connection.transport.sender.is_closed()
                {
                    continue;
                }
                if idle.connection.transport.sender.ready().await.is_ok() && endpoint.enabled.get()
                {
                    return Ok(Lease {
                        connection: idle.connection,
                        endpoint,
                        reused: true,
                    });
                }
                continue;
            }
            if self.budget.live.get() == self.limit {
                self.evict_idle();
            }
            if self.budget.live.get() < self.limit {
                self.budget.live.set(self.budget.live.get() + 1);
                let slot = Slot(self.budget.clone());
                let transport = tokio::select! {
                    result = self.connector.connect(endpoint.address) => result?,
                    _ = endpoint.closed.notified() => anyhow::bail!("backend was removed while connecting"),
                };
                anyhow::ensure!(
                    endpoint.enabled.get() && !self.closed.get(),
                    "backend was removed while connecting"
                );
                let connection = Connection {
                    transport,
                    _slot: slot,
                };
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
                || self.budget.live.get() < self.limit
                || self
                    .endpoints
                    .borrow()
                    .values()
                    .any(|entry| !entry.idle.borrow().is_empty())
            {
                continue;
            }
            changed.await;
        }
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        self.close();
    }
}

pub struct Lease {
    connection: Connection,
    endpoint: Rc<Endpoint>,
    reused: bool,
}

impl Lease {
    pub fn sender(&mut self) -> &mut SendRequest<Incoming> {
        &mut self.connection.transport.sender
    }
    pub fn reused(&self) -> bool {
        self.reused
    }
    pub fn into_body(self, body: Incoming) -> PooledBody {
        PooledBody::new(body, self)
    }

    fn recycle(self) {
        if !self.endpoint.enabled.get() || self.connection.transport.sender.is_closed() {
            return;
        }
        if self.connection.transport.sender.is_ready() {
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
                    ready = connection.transport.sender.ready() => {
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

pub struct PooledBody {
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
