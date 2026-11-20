//! exclusive HTTP/1 leases and idle connection reuse.
use super::{
    super::connector::Connected, ConnectionPool, Endpoint, PooledBody, ResponseLease,
    Slot,
};
use compio::runtime;
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use std::{
    rc::Rc,
    time::{Duration, Instant},
};

struct Connection {
    transport: Connected,
    _slot: Slot,
}

pub(super) struct Idle {
    connection: Connection,
    pub(super) since: Instant,
}

impl Idle {
    pub(super) fn reusable(&self, timeout: Duration) -> bool {
        self.since.elapsed() < timeout && !self.connection.transport.sender.is_closed()
    }
}

impl Endpoint {
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
                let transport = self
                    .dial(&endpoint, self.connector.connect(endpoint.key.address))
                    .await?;
                let connection = Connection { transport, _slot: slot };
                return Ok(Lease { connection, endpoint, reused: false });
            }
            changed.await;
        }
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

    pub(super) fn recycle(self) {
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
