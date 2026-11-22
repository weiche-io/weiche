//! exclusive HTTP/1 leases and idle connection reuse.
use super::{
    super::http::RequestBody,
    connector::Connected,
    deadline::TimedBody,
    pool::{ConnectionPool, Endpoint, PooledBody, ResponseLease, Slot},
};
use compio::runtime;
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use std::{
    rc::Rc,
    time::{Duration, Instant},
};

/// a transport and its exclusive connection slot.
struct Connection {
    /// upstream HTTP/1 sender and its driver.
    transport: Connected,
    /// reserved worker capacity.
    _slot: Slot,
}

/// idle HTTP/1 connection waiting for reuse or expiration.
pub struct Idle {
    /// exclusive upstream connection.
    connection: Connection,
    /// instant at which the connection became idle.
    pub since: Instant,
}

impl Idle {
    /// check whether this idle connection is still open and within the idle timeout.
    pub fn reusable(&self, timeout: Duration) -> bool {
        self.since.elapsed() < timeout && !self.connection.transport.sender.is_closed()
    }
}

impl Endpoint {
    /// publish a reusable connection and notify waiting requests.
    fn put(&self, connection: Connection) {
        if self.enabled.get() && connection.transport.sender.is_ready() {
            self.idle
                .borrow_mut()
                .push(Idle { connection, since: Instant::now() });
            self.budget.changed.notify_waiters();
        }
    }
}

impl ConnectionPool {
    /// reuse an idle socket when permitted or dial within the worker budget.
    pub async fn acquire(
        &self,
        endpoint: Rc<Endpoint>,
        reuse: bool,
    ) -> anyhow::Result<Lease> {
        loop {
            let changed = self.watch_capacity(&endpoint)?;
            let idle = if reuse { endpoint.idle.borrow_mut().pop() } else { None };
            if let Some(mut idle) = idle {
                if idle.reusable(self.idle_timeout)
                    && idle.connection.transport.sender.ready().await.is_ok()
                    && endpoint.enabled.get()
                {
                    return Ok(Lease {
                        connection: idle.connection,
                        endpoint,
                        reused: true,
                    });
                }
                continue;
            }
            if let Some(slot) = self.try_reserve() {
                let transport =
                    self.dial(&endpoint, self.connector.connect(&endpoint.key)).await?;
                let connection = Connection { transport, _slot: slot };
                return Ok(Lease { connection, endpoint, reused: false });
            }
            changed.await;
        }
    }
}

/// exclusive ownership of an HTTP/1 connection.
pub struct Lease {
    /// exclusive upstream connection.
    connection: Connection,
    /// endpoint generation to which this connection belongs.
    endpoint: Rc<Endpoint>,
    /// whether this request acquired an idle socket.
    reused: bool,
}

impl Lease {
    /// borrow the sender for dispatch.
    pub fn sender(&mut self) -> &mut SendRequest<TimedBody<RequestBody>> {
        &mut self.connection.transport.sender
    }
    /// report whether a stale idle socket can justify a retry.
    pub fn reused(&self) -> bool {
        self.reused
    }
    /// retain this lease until response framing completes.
    pub fn into_body(self, body: Incoming) -> PooledBody {
        PooledBody::new(body, ResponseLease::Http1(self))
    }

    /// return an idle HTTP/1 connection to its endpoint pool once framing completes.
    pub fn recycle(self) {
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
