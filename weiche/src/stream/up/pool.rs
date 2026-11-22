//! worker-local connection capacity, endpoint generations and response leases.
mod http1;
mod http2;

use super::{
    super::{
        super::{ir::Protocol, rtconf::RuntimeConfig},
        core::EndpointKey,
    },
    connector::Connector,
};
use bytes::Bytes;
use compio::runtime;
use http1::Idle;
pub use http1::Lease;
use http2::{Http2Connection, Http2Lease};
use hyper::body::Incoming;
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, futures::Notified};

/// worker-wide connection slot counter and admission notifications.
#[derive(Default)]
struct Budget {
    /// number of slots currently reserved.
    live: Cell<usize>,
    /// wake admissions when capacity or stream availability changes.
    changed: Notify,
}

/// exclusive reservation released when the transport is dropped.
struct Slot {
    /// shared socket capacity on this worker.
    budget: Rc<Budget>,
}
impl Drop for Slot {
    fn drop(&mut self) {
        self.budget.live.set(self.budget.live.get() - 1);
        self.budget.changed.notify_waiters();
    }
}

/// one generation of a resolved upstream endpoint.
pub struct Endpoint {
    /// immutable address, protocol and TLS identity.
    pub key: EndpointKey,
    /// whether new requests may use this generation.
    enabled: Cell<bool>,
    /// wake waiters when the endpoint is retired.
    closed: Notify,
    /// idle reusable HTTP/1 connections.
    idle: RefCell<Vec<Idle>>,
    /// multiplexed HTTP/2 transports.
    http2: RefCell<Vec<Rc<Http2Connection>>>,
    /// whether an HTTP/2 handshake is already in progress.
    connecting: Cell<bool>,
    /// shared socket capacity on this worker.
    budget: Rc<Budget>,
}

impl Endpoint {
    /// retire this pool or endpoint and wake pending requests.
    fn close(&self) {
        self.enabled.set(false);
        self.idle.borrow_mut().clear();
        self.http2.borrow_mut().clear();
        self.closed.notify_waiters();
    }

    /// evict closed and expired idle transports.
    fn prune(&self, timeout: Duration) {
        self.idle.borrow_mut().retain(|idle| idle.reusable(timeout));
        self.http2
            .borrow_mut()
            .retain(|connection| !connection.expired(timeout));
    }

    /// find the oldest idle socket and its index in the protocol's connection list.
    fn oldest_idle(&self) -> Option<(Instant, usize)> {
        match self.key.protocol {
            Protocol::Http1 => self.idle.borrow().first().map(|idle| (idle.since, 0)),
            Protocol::Http2 => self
                .http2
                .borrow()
                .iter()
                .enumerate()
                .filter_map(|(index, connection)| {
                    connection.idle_since().map(|since| (since, index))
                })
                .min(),
        }
    }

    /// remove the selected idle connection.
    fn evict_idle(&self, index: usize) {
        match self.key.protocol {
            Protocol::Http1 => {
                self.idle.borrow_mut().remove(index);
            }
            Protocol::Http2 => {
                self.http2.borrow_mut().swap_remove(index);
            }
        }
    }
}

/// bounded worker-local pool of active endpoint generations.
pub struct ConnectionPool {
    /// current connection state keyed by endpoint identity.
    endpoints: RefCell<HashMap<EndpointKey, Rc<Endpoint>>>,
    /// maximum simultaneous socket reservations.
    limit: usize,
    /// maximum active streams per HTTP/2 connection.
    max_streams: usize,
    /// maximum idle age before a connection expires.
    idle_timeout: Duration,
    /// handshake limits used for fresh transports.
    connector: Connector,
    /// shared socket capacity on this worker.
    budget: Rc<Budget>,
    /// whether the worker has stopped accepting new connections.
    closed: Cell<bool>,
    /// task expiring idle transports.
    reaper: RefCell<Option<runtime::JoinHandle<()>>>,
}

impl ConnectionPool {
    /// initialize socket budgets and schedule idle eviction.
    pub fn new(config: &RuntimeConfig) -> Rc<Self> {
        let pool = Rc::new(Self {
            endpoints: RefCell::new(HashMap::new()),
            limit: config.upstream_connections_per_worker.get(),
            max_streams: config.http2_max_concurrent_streams as usize,
            idle_timeout: config.upstream_idle_timeout,
            connector: Connector { max_buffer: config.http_max_buffer },
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
                    endpoint.prune(pool.idle_timeout);
                }
            }
        }));
        pool
    }

    /// retain connections only for endpoints reachable through active routes.
    #[expect(
        clippy::mutable_key_type,
        reason = "TLS session caches do not participate in endpoint key equality or hashing"
    )]
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
        for key in allowed {
            endpoints.entry(key.clone()).or_insert_with(|| {
                Rc::new(Endpoint {
                    key: key.clone(),
                    enabled: Cell::new(true),
                    closed: Notify::new(),
                    idle: RefCell::new(Vec::new()),
                    http2: RefCell::new(Vec::new()),
                    connecting: Cell::new(false),
                    budget: self.budget.clone(),
                })
            });
        }
        self.budget.changed.notify_waiters();
    }

    /// look up the current generation for a selected address.
    pub fn endpoint(&self, key: EndpointKey) -> Option<Rc<Endpoint>> {
        self.endpoints.borrow().get(&key).cloned()
    }

    /// retire all endpoint generations.
    pub fn close(&self) {
        self.closed.set(true);
        self.reaper.borrow_mut().take();
        for endpoint in self.endpoints.borrow().values() {
            endpoint.close();
        }
        self.budget.changed.notify_waiters();
    }

    /// make room for a dial by removing the oldest idle socket.
    fn evict_idle(&self) {
        // reclaim the oldest idle socket when a different backend needs space.
        let endpoints = self.endpoints.borrow();
        let oldest = endpoints
            .values()
            .filter_map(|endpoint| {
                let (since, index) = endpoint.oldest_idle()?;
                Some((since, endpoint, index))
            })
            .min_by_key(|(since, _, _)| *since);
        if let Some((_, endpoint, index)) = oldest {
            endpoint.evict_idle(index);
        }
    }

    /// subscribe to capacity changes while the endpoint is still usable.
    fn watch_capacity(&self, endpoint: &Endpoint) -> anyhow::Result<Notified<'_>> {
        anyhow::ensure!(
            !self.closed.get() && endpoint.enabled.get(),
            "backend was removed or pool closed"
        );
        // notify_waiters covers changes after creation, even before the first poll.
        Ok(self.budget.changed.notified())
    }

    /// reserve a free slot after evicting an idle socket if necessary.
    fn try_reserve(&self) -> Option<Slot> {
        if self.budget.live.get() == self.limit {
            self.evict_idle();
        }
        let live = self.budget.live.get();
        if live >= self.limit {
            return None;
        }
        self.budget.live.set(live + 1);
        Some(Slot { budget: self.budget.clone() })
    }

    /// cancel a handshake if its endpoint is removed.
    async fn dial<T>(
        &self,
        endpoint: &Endpoint,
        handshake: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let transport = tokio::select! {
            result = handshake => result?,
            _ = endpoint.closed.notified() => anyhow::bail!("backend was removed while connecting"),
        };
        anyhow::ensure!(
            endpoint.enabled.get() && !self.closed.get(),
            "backend was removed while connecting"
        );
        Ok(transport)
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        self.close();
    }
}

/// transport-specific ownership retained by an ordinary response.
enum ResponseLease {
    /// exclusive HTTP/1 transport.
    Http1(Lease),
    /// one stream on a shared HTTP/2 transport.
    Http2(Http2Lease),
}

/// upstream body retaining capacity until framing completes.
pub struct PooledBody {
    /// incoming body carrying data and trailers.
    body: Incoming,
    /// live transport guard until EOF or error.
    lease: Option<ResponseLease>,
}

impl PooledBody {
    /// upgraded transports hand their connection lease to the tunnel.
    pub fn upgraded(body: Incoming) -> Self {
        Self { body, lease: None }
    }

    /// attach the lease and release it immediately for empty bodies.
    fn new(body: Incoming, lease: ResponseLease) -> Self {
        let mut body = Self { body, lease: Some(lease) };
        // responses to HEAD, status 204 and empty bodies may never be polled by the downstream.
        if hyper::body::Body::is_end_stream(&body.body) {
            body.complete();
        }
        body
    }

    /// return capacity after the final data or trailer frame.
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
            Poll::Ready(Some(Ok(frame)))
                if frame.is_trailers() || self.body.is_end_stream() =>
            {
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
