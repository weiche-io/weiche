//! a shared HTTP/2 connection and its active request and response streams.
use super::{
    super::http::RequestBody,
    connector::Connected,
    deadline::TimedBody,
    pool::{ConnectionPool, Endpoint, Slot},
};
use hyper::client::conn::http2;
use std::{
    cell::Cell,
    mem::DropGuard,
    rc::{Rc, Weak},
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
    transport: Connected<http2::SendRequest<Upload>>,
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
pub struct Stream(
    /// weak reference used to return stream capacity on completion.
    Weak<Http2Connection>,
);

impl Drop for Stream {
    /// release stream capacity and record when the connection becomes idle.
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
    pub fn sender(&self) -> http2::SendRequest<Upload> {
        self.connection.transport.sender.clone()
    }

    /// retain stream capacity with the upload until it finishes or its deadline expires.
    pub fn upload(&self, body: RequestBody, end: Option<Instant>) -> Upload {
        TimedBody::guarded(body, self.stream.clone(), end)
    }
}

/// the upload holds its share of the stream reservation independently of the response.
type Upload = TimedBody<RequestBody, Rc<Stream>>;
