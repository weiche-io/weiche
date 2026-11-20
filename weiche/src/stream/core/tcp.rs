//! TCP admission and listener selection; neither filter inspects application bytes.
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// target attached to a configured listener, before any protocol-specific matching.
pub struct Route<T> {
    pub listener: SocketAddr,
    pub target: T,
}

impl<T> Route<T> {
    /// match the accepting listener's configured address, including wildcard binds.
    pub fn find(&self, listener: SocketAddr) -> Option<&T> {
        (self.listener == listener).then_some(&self.target)
    }
}

/// global cap shared by the TCP listeners of all workers.
pub struct Admission {
    live: AtomicUsize,
    limit: usize,
}

impl Admission {
    pub fn new(limit: usize) -> Self {
        Self { live: AtomicUsize::new(0), limit }
    }
    /// admit a connection for the lifetime of its permit.
    pub fn acquire(self: &Arc<Self>) -> Option<Permit> {
        self.live
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .ok()
            .map(|_| Permit(self.clone()))
    }
}

/// returns the connection slot when dropped, including on cancellation.
pub struct Permit(Arc<Admission>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}
