//! bounded physical connections, HTTP/1 leases and multiplexed HTTP/2 streams.
use super::{
    cluster::{EndpointKey, Protocol},
    connector::{Connected, Connector},
    rtconf::RuntimeConfig,
};
use bytes::Bytes;
use compio::runtime;
use hyper::{
    body::Incoming,
    client::conn::{http1::SendRequest, http2},
};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    mem::DropGuard,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[derive(Default)]
struct Budget {
    live: Cell<usize>,
    changed: Rc<Notify>,
}

struct Slot(Rc<Budget>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.live.set(self.0.live.get() - 1);
        self.0.changed.notify_waiters();
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
    pub key: EndpointKey,
    idle_timeout: Duration,
    enabled: Cell<bool>,
    closed: Notify,
    idle: RefCell<Vec<Idle>>,
    http2: RefCell<Option<Rc<Http2Connection>>>,
    connecting: Cell<bool>,
    budget: Rc<Budget>,
}

impl Endpoint {
    fn close(&self) {
        self.enabled.set(false);
        self.idle.borrow_mut().clear();
        self.http2.borrow_mut().take();
        self.closed.notify_waiters();
    }

    fn put(&self, connection: Connection) {
        if self.enabled.get() && connection.transport.sender.is_ready() {
            self.idle.borrow_mut().push(Idle {
                connection,
                since: Instant::now(),
            });
            self.budget.changed.notify_waiters();
        }
    }

    fn prune(&self) {
        self.idle.borrow_mut().retain(|idle| {
            idle.since.elapsed() < self.idle_timeout
                && !idle.connection.transport.sender.is_closed()
        });
        let mut http2 = self.http2.borrow_mut();
        if http2.as_ref().is_some_and(|connection| {
            connection.transport.sender.is_closed()
                || connection
                    .idle_since()
                    .is_some_and(|since| since.elapsed() >= self.idle_timeout)
        }) {
            http2.take();
        }
    }

    pub fn retire_http2(&self, lease: &Http2Lease) {
        let mut current = self.http2.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|connection| Rc::ptr_eq(connection, &lease.connection))
        {
            current.take();
            self.budget.changed.notify_waiters();
        }
    }

    fn idle_since(&self) -> Option<Instant> {
        let http1 = self.idle.borrow().first().map(|idle| idle.since);
        let http2 = self
            .http2
            .borrow()
            .as_ref()
            .and_then(|connection| connection.idle_since());
        http1.into_iter().chain(http2).min()
    }

    fn evict_idle(&self) {
        if self.key.protocol == Protocol::Http2 {
            self.http2.borrow_mut().take();
        } else {
            self.idle.borrow_mut().remove(0);
        }
    }
}

pub struct ConnectionPool {
    endpoints: RefCell<HashMap<EndpointKey, Rc<Endpoint>>>,
    limit: usize,
    max_streams: usize,
    idle_timeout: Duration,
    connector: Connector,
    budget: Rc<Budget>,
    closed: Cell<bool>,
    reaper: RefCell<Option<runtime::JoinHandle<()>>>,
}

impl ConnectionPool {
    pub fn new(config: &RuntimeConfig) -> Rc<Self> {
        let pool = Rc::new(Self {
            endpoints: RefCell::new(HashMap::new()),
            limit: config.upstream_connections_per_worker.get(),
            max_streams: config.http2_max_concurrent_streams as usize,
            idle_timeout: config.upstream_idle_timeout,
            connector: Connector {
                max_buffer: config.http_max_buffer,
            },
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
    pub fn reconcile(&self, allowed: &HashSet<EndpointKey>) {
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
        for &key in allowed {
            endpoints.entry(key).or_insert_with(|| {
                Rc::new(Endpoint {
                    key,
                    idle_timeout: self.idle_timeout,
                    enabled: Cell::new(true),
                    closed: Notify::new(),
                    idle: RefCell::new(Vec::new()),
                    http2: RefCell::new(None),
                    connecting: Cell::new(false),
                    budget: self.budget.clone(),
                })
            });
        }
        self.budget.changed.notify_waiters();
    }

    pub fn endpoint(&self, key: EndpointKey) -> Option<Rc<Endpoint>> {
        self.endpoints.borrow().get(&key).cloned()
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
            .filter_map(|endpoint| endpoint.idle_since().map(|since| (since, endpoint)))
            .min_by_key(|(since, _)| *since);
        if let Some((_, endpoint)) = oldest {
            endpoint.evict_idle();
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
                    result = self.connector.connect(endpoint.key.address) => result?,
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
                    .any(|entry| entry.idle_since().is_some())
            {
                continue;
            }
            changed.await;
        }
    }

    pub async fn acquire_http2(&self, endpoint: &Rc<Endpoint>) -> anyhow::Result<Http2Lease> {
        loop {
            let changed = self.budget.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            anyhow::ensure!(
                !self.closed.get() && endpoint.enabled.get(),
                "backend was removed or pool closed"
            );
            endpoint.prune();
            let connection = endpoint.http2.borrow().clone();
            if let Some(connection) = connection {
                if connection.activity.active.get() < self.max_streams {
                    return Ok(connection.lease());
                }
            } else if !endpoint.connecting.get() {
                if self.budget.live.get() == self.limit {
                    self.evict_idle();
                }
                if self.budget.live.get() < self.limit {
                    // one dial per endpoint, even when many streams arrive together.
                    endpoint.connecting.set(true);
                    let _connecting = DropGuard::new(endpoint, |endpoint| {
                        endpoint.connecting.set(false);
                        endpoint.budget.changed.notify_waiters();
                    });
                    self.budget.live.set(self.budget.live.get() + 1);
                    let slot = Slot(self.budget.clone());
                    let transport = tokio::select! {
                        result = self.connector.connect_http2(endpoint.key.address) => result?,
                        _ = endpoint.closed.notified() => anyhow::bail!("backend was removed while connecting"),
                    };
                    anyhow::ensure!(
                        endpoint.enabled.get() && !self.closed.get(),
                        "backend was removed while connecting"
                    );
                    let connection = Rc::new(Http2Connection {
                        transport,
                        _slot: slot,
                        activity: Rc::new(Activity {
                            active: Cell::new(0),
                            idle_since: Cell::new(Instant::now()),
                            changed: self.budget.changed.clone(),
                        }),
                    });
                    *endpoint.http2.borrow_mut() = Some(connection.clone());
                    return Ok(connection.lease());
                }
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
        PooledBody::new(body, ResponseLease::Http1(self))
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

struct Http2Connection {
    transport: Connected<http2::SendRequest<Upload>>,
    _slot: Slot,
    activity: Rc<Activity>,
}

impl Http2Connection {
    fn idle_since(&self) -> Option<Instant> {
        (self.activity.active.get() == 0).then(|| self.activity.idle_since.get())
    }

    fn lease(self: &Rc<Self>) -> Http2Lease {
        self.activity.active.set(self.activity.active.get() + 1);
        Http2Lease {
            connection: self.clone(),
            stream: Rc::new(Stream(self.activity.clone())),
        }
    }
}

struct Activity {
    active: Cell<usize>,
    idle_since: Cell<Instant>,
    changed: Rc<Notify>,
}

struct Stream(Rc<Activity>);

impl Drop for Stream {
    fn drop(&mut self) {
        self.0.active.set(self.0.active.get() - 1);
        self.0.idle_since.set(Instant::now());
        self.0.changed.notify_waiters();
    }
}

pub struct Http2Lease {
    connection: Rc<Http2Connection>,
    stream: Rc<Stream>,
}

impl Http2Lease {
    pub fn sender(&self) -> http2::SendRequest<Upload> {
        self.connection.transport.sender.clone()
    }

    pub fn upload(&self, body: Incoming) -> Upload {
        Upload {
            body,
            _stream: self.stream.clone(),
        }
    }

    pub fn into_body(self, body: Incoming) -> PooledBody {
        PooledBody::new(body, ResponseLease::Http2(self))
    }
}

// count a stream until both directions finish. the upload retains only activity,
// not the connection driver, so the body cannot form a reference cycle with it.
pub struct Upload {
    body: Incoming,
    _stream: Rc<Stream>,
}

impl Upload {
    pub fn into_inner(self) -> Incoming {
        self.body
    }
}

impl hyper::body::Body for Upload {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

enum ResponseLease {
    Http1(Lease),
    Http2(Http2Lease),
}

pub struct PooledBody {
    body: Incoming,
    lease: Option<ResponseLease>,
}

impl PooledBody {
    fn new(body: Incoming, lease: ResponseLease) -> Self {
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
        match self.lease.take() {
            Some(ResponseLease::Http1(lease)) => lease.recycle(),
            Some(ResponseLease::Http2(lease)) => drop(lease),
            None => {}
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
