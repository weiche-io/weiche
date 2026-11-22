//! upstream HTTP transports, pooling and request dispatch.
mod connector;
mod deadline;
mod http1;
mod http2;
mod pool;
mod upgrade;
mod upstream;

pub use deadline::{Deadlines, ResponseDeadline};
pub use pool::ConnectionPool;
pub use upgrade::{Tunnel, Upgrade};
pub use upstream::UpstreamClient;
