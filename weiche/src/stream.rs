//! worker-local data plane: listeners, route filters and upstream connections.
pub mod core;
mod dataplane;
pub mod r#in;
pub mod out;
mod worker;

pub use dataplane::WorkerGroup;
pub use worker::stopped;
