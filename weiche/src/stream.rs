//! worker-local data plane: listeners, route filters and upstream connections.
pub mod core;
mod dataplane;
pub mod down;
pub mod up;

pub use dataplane::WorkerGroup;

pub async fn stopped(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}
