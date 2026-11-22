//! a shared HTTP/2 connection and its active request and response streams.
use super::{
    super::{super::http::Body, connector::Connected, deadline::TimedBody},
    ConnectionPool, Endpoint, PooledBody, ResponseLease, Slot,
};
use bytes::Bytes;
use hyper::{body::Incoming, client::conn::http2};
use std::{
    cell::Cell,
    mem::DropGuard,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

impl Endpoint {
    /// remove a stale connection from future stream selection.
    pub fn retire_http2(&self, lease: &Http2Lease) {
        self.http2
            .borrow_mut()
            .retain(|connection| !Rc::ptr_eq(connection, &lease.connection));
        self.budget.changed.notify_waiters();
    }
}

impl ConnectionPool {
    /// acquire stream capacity or open a new HTTP/2 transport.
    pub async fn acquire_http2(
        &self,
        endpoint: &Rc<Endpoint>,
    ) -> anyhow::Result<Http2Lease> {
        loop {
            let changed = self.watch_capacity(endpoint)?;
            endpoint.prune(self.idle_timeout);
            if let Some(connection) = endpoint
                .http2
                .borrow()
                .iter()
                .find(|connection| connection.active.get() < self.max_streams)
            {
                return Ok(connection.lease());
            }
            if !endpoint.connecting.get()
                && let Some(slot) = self.try_reserve()
            {
                // one dial per endpoint, even when many streams arrive together.
                endpoint.connecting.set(true);
                let _connecting = DropGuard::new(endpoint, |endpoint| {
                    endpoint.connecting.set(false);
                    endpoint.budget.changed.notify_waiters();
                });
                let transport = self
                    .dial(endpoint, self.connector.connect_http2(&endpoint.key))
                    .await?;
                let connection = Rc::new(Http2Connection {
                    transport,
                    slot,
                    active: Cell::new(0),
                    idle_since: Cell::new(Instant::now()),
                });
                endpoint.http2.borrow_mut().push(connection.clone());
                return Ok(connection.lease());
            }
            changed.await;
        }
    }
}

/// shared HTTP/2 upstream connection and its active stream counter.
pub struct Http2Connection {
    /// multiplexed sender and its worker driver.
    transport: Connected<http2::SendRequest<TimedBody<Upload>>>,
    /// reservation for the underlying connection.
    slot: Slot,
    /// streams with either upload or response still active.
    active: Cell<usize>,
    /// last time the final stream completed.
    idle_since: Cell<Instant>,
}

impl Http2Connection {
    /// check whether the connection closed or sat idle past timeout.
    pub fn expired(&self, timeout: Duration) -> bool {
        self.transport.sender.is_closed()
            || self.idle_since().is_some_and(|since| since.elapsed() >= timeout)
    }

    /// return when the last active stream finished, or None while streams are active.
    pub fn idle_since(&self) -> Option<Instant> {
        (self.active.get() == 0).then(|| self.idle_since.get())
    }

    /// reserve a stream slot across both halves.
    fn lease(self: &Rc<Self>) -> Http2Lease {
        self.active.set(self.active.get() + 1);
        Http2Lease {
            connection: self.clone(),
            stream: Rc::new(Stream(Rc::downgrade(self))),
        }
    }
}

/// upload and response share this guard. the upload must not keep its driver alive.
struct Stream(Weak<Http2Connection>);

impl Drop for Stream {
    fn drop(&mut self) {
        if let Some(connection) = self.0.upgrade() {
            let remaining = connection.active.get() - 1;
            connection.active.set(remaining);
            if remaining == 0 {
                connection.idle_since.set(Instant::now());
            }
            connection.slot.budget.changed.notify_waiters();
        }
    }
}

/// ownership of an active response stream.
pub struct Http2Lease {
    /// shared connection on which the request travels.
    connection: Rc<Http2Connection>,
    /// stream accounting shared with the upload.
    stream: Rc<Stream>,
}

impl Http2Lease {
    /// clone the request sender for this stream.
    pub fn sender(&self) -> http2::SendRequest<TimedBody<Upload>> {
        self.connection.transport.sender.clone()
    }

    /// attach stream accounting before arming the upload deadline.
    pub fn upload(&self, body: Body) -> Upload {
        Upload { body, _stream: self.stream.clone() }
    }

    /// retain the HTTP/2 lease during response streaming.
    pub fn into_body(self, body: Incoming) -> PooledBody {
        PooledBody::new(body, ResponseLease::Http2(self))
    }
}

/// request body retaining stream capacity until upload completion.
pub struct Upload {
    /// original streaming upload from the downstream request.
    body: Body,
    /// shared stream accounting guard.
    _stream: Rc<Stream>,
}

impl Upload {
    /// recover a body returned before dispatch for a fresh attempt.
    pub fn into_inner(self) -> Body {
        self.body
    }
}

impl hyper::body::Body for Upload {
    type Data = Bytes;
    type Error = std::io::Error;

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
