//! upstream HTTP transports, pooling and request dispatch.
mod connector;
mod deadline;
mod pool;
mod tls;
mod upgrade;
mod upstream;

pub use deadline::Deadlines;
pub use pool::{ConnectionPool, Endpoint};
pub use upgrade::{Tunnel, Upgrade};
pub use upstream::UpstreamClient;
