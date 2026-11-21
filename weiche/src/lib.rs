//! gateway configuration, control plane and worker-local data plane.

#[cfg(not(target_os = "linux"))]
compile_error!("weiche supports only Linux");

pub mod control;
pub mod gateway;
pub mod ir;
pub mod routing;
pub mod rtconf;
pub mod stream;

pub mod proxy;

/// wait for Kubernetes termination or a foreground interrupt.
pub async fn shutdown() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = terminate.recv() => {},
        _ = interrupt.recv() => {},
    }
    Ok(())
}
