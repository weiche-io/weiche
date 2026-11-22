//! worker-local data plane: listeners, route filters and upstream connections.
pub mod config;
pub mod core;
mod dataplane;
pub mod down;
mod io;
pub mod up;
mod watch;

use super::ir::GatewaySnapshot;
use anyhow::Context;
use config::RuntimeConfig;
use dataplane::WorkerGroup;
pub use dataplane::runtime;
use down::WorkerGroupState;
use std::thread;
use tokio::sync::oneshot;
pub use watch::ready;

/// wait for process shutdown or the shutdown sender to close.
pub async fn stopped(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}

/// start the watch thread and workers under the same shutdown signal.
pub async fn run(name: String, runtime: RuntimeConfig) -> anyhow::Result<()> {
    let (config_tx, config_rx) = config::channel(GatewaySnapshot::default());
    let (listener_tx, listener_rx) =
        tokio::sync::watch::channel(WorkerGroupState::default());
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let control_stop = stopped.clone();
    let (done_tx, mut done_rx) = oneshot::channel();
    let http3 = runtime.http3;
    let group = WorkerGroup::new(runtime, config_rx, listener_tx)?;
    let control = thread::Builder::new()
        .name("weiche-proxy-watch".into())
        .spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("create control-plane Tokio runtime")?;
                runtime.block_on(watch::run(
                    name,
                    http3,
                    config_tx,
                    listener_rx,
                    control_stop,
                ))
            })();
            let _ = done_tx.send(result);
        })
        .context("spawn control-plane thread")?;
    let data = group.run(stopped);
    tokio::pin!(data);
    let (mut result, data_done, control_done) = tokio::select! {
        result = &mut data => (result, true, false),
        result = &mut done_rx => (
            result.context("control thread panicked").and_then(|r| r),
            false,
            true,
        ),
    };
    stop.send_replace(true);
    if !data_done {
        result = result.and(data.await);
    }
    if !control_done {
        let control_result =
            done_rx.await.context("control thread panicked").and_then(|r| r);
        result = result.and(control_result);
    }
    if control.join().is_err() {
        result = result.and(Err(anyhow::anyhow!("control thread panicked")));
    }
    result
}
