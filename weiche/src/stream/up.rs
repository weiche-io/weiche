//! upstream HTTP transports, pooling and request dispatch.
mod connector;
mod pool;
mod tls;
mod upstream;

pub use pool::{ConnectionPool, Endpoint};
pub use upstream::UpstreamClient;
