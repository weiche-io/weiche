//! upstream HTTP transports, pooling and request dispatch.
mod connector;
mod pool;
mod upstream;

pub use connector::io;
pub use pool::{ConnectionPool, Endpoint};
pub use upstream::UpstreamClient;
