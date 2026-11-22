//! supervise compio workers and a dedicated Tokio control thread.
use super::{
    ir::GatewaySnapshot,
    proxy, routing,
    rtconf::RuntimeConfig,
    stream::{WorkerGroup, down::WorkerGroupState},
};
use anyhow::Context;
use std::thread;
use tokio::sync::{oneshot, watch};

/// start the watch thread and workers under the same shutdown signal.
pub async fn run(name: String, runtime: RuntimeConfig) -> anyhow::Result<()> {
    let (config_tx, config_rx) = routing::channel(GatewaySnapshot::default());
    let (listener_tx, listener_rx) = watch::channel(WorkerGroupState::default());
    let (stop, stopped) = watch::channel(false);
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
                runtime.block_on(proxy::run(
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
