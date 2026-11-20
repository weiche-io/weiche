//! supervise compio workers and a dedicated Tokio control thread.
use super::{
    control::{self, Selection},
    ir::GatewaySnapshot,
    routing,
    rtconf::RuntimeConfig,
    stream::{WorkerGroup, r#in::WorkerGroupState},
};
use anyhow::Context;
use std::thread;
use tokio::sync::{oneshot, watch};

pub async fn run(selection: Selection, runtime: RuntimeConfig) -> anyhow::Result<()> {
    let (config_tx, config_rx) = routing::channel(GatewaySnapshot::default());
    let (listener_tx, listener_rx) = watch::channel(WorkerGroupState::default());
    let (stop, stopped) = watch::channel(false);
    let control_stop = stopped.clone();
    let (done_tx, mut done_rx) = oneshot::channel();
    let group = WorkerGroup::new(runtime, config_rx, listener_tx)?;
    let control = thread::Builder::new()
        .name("weiche-control".into())
        .spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("create control-plane Tokio runtime")?;
                runtime.block_on(control::run(
                    selection,
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
    let mut data_done = false;
    let mut control_done = false;
    let mut result = tokio::select! {
        result = &mut data => { data_done = true; result },
        result = &mut done_rx => {
            control_done = true;
            result.context("control thread panicked").and_then(|result| result)
        },
    };
    stop.send_replace(true);
    if !data_done {
        let data_result = data.await;
        if result.is_ok() {
            result = data_result;
        }
    }
    if !control_done {
        let control_result = done_rx
            .await
            .context("control thread panicked")
            .and_then(|result| result);
        if result.is_ok() {
            result = control_result;
        }
    }
    if control.join().is_err() && result.is_ok() {
        result = Err(anyhow::anyhow!("control thread panicked"));
    }
    result
}
