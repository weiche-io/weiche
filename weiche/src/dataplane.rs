//! supervise a group of compio workers; worker zero runs on the caller's runtime.
use super::{
    listener::{ListenerGroup, WorkerGroupState},
    routing,
    rtconf::RuntimeConfig,
    worker::{Admission, WorkerState, stopped},
};
use anyhow::Context;
use futures::{StreamExt, stream::FuturesUnordered};
use std::{sync::Arc, thread};
use tokio::sync::{oneshot, watch};

pub struct WorkerGroup {
    runtime: Arc<RuntimeConfig>,
    config: routing::Reader,
    state: watch::Sender<WorkerGroupState>,
}

impl WorkerGroup {
    pub fn new(
        runtime: RuntimeConfig,
        config: routing::Reader,
        state: watch::Sender<WorkerGroupState>,
    ) -> anyhow::Result<Self> {
        runtime.validate()?;
        Ok(Self {
            runtime: Arc::new(runtime),
            config,
            state,
        })
    }

    pub async fn run(self, shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        let Self {
            runtime,
            mut config,
            state,
        } = self;
        let listeners = Arc::new(ListenerGroup::new(
            runtime.workers.get(),
            config.clone(),
            state.clone(),
        ));
        let admission = Arc::new(Admission::new(runtime.downstream_connections_global.get()));
        let (stop, stopped_rx) = watch::channel(false);
        let local = WorkerState::start(
            0,
            config.clone(),
            runtime.clone(),
            admission.clone(),
            listeners.clone(),
            stop.clone(),
        );
        tokio::pin!(local);
        let mut local_done = false;
        let mut threads = Vec::new();
        let mut completed = FuturesUnordered::new();
        let mut result = async {
            for id in 1..runtime.workers.get() {
                let config = config.clone();
                let runtime_config = runtime.clone();
                let stop = stop.clone();
                let admission = admission.clone();
                let listeners = listeners.clone();
                let (done_tx, done_rx) = oneshot::channel();
                threads.push(thread::Builder::new().name(format!("weiche-worker-{id}")).spawn(move || {
                    let result = (|| -> anyhow::Result<()> {
                        let runtime = compio::runtime::Runtime::new().context("create worker compio runtime")?;
                        tracing::info!(worker = id, driver = ?runtime.driver_type(), "starting compio worker");
                        runtime.block_on(WorkerState::start(id, config, runtime_config, admission, listeners, stop))
                    })().with_context(|| format!("worker {id}"));
                    let _ = done_tx.send(result);
                }).context("spawn data-plane worker")?);
                completed.push(done_rx);
            }
            tokio::select! {
                biased;
                _ = stopped(shutdown) => Ok(()),
                _ = stopped(stopped_rx) => Ok(()),
                _ = async { while config.changed().await.is_ok() {} } => Ok(()),
                result = &mut local => { local_done = true; result },
                result = completed.next(), if !completed.is_empty() => {
                    result.unwrap().context("worker panicked")??;
                    anyhow::bail!("data-plane worker stopped unexpectedly");
                }
            }
        }.await;
        stop.send_replace(true);
        if !local_done {
            let local_result = local.await;
            if result.is_ok() {
                result = local_result;
            }
        }
        while let Some(done) = completed.next().await {
            let done = done.context("worker panicked").and_then(|result| result);
            if result.is_ok() {
                result = done;
            }
        }
        for thread in threads {
            if thread.join().is_err() && result.is_ok() {
                result = Err(anyhow::anyhow!("data-plane worker panicked"));
            }
        }
        state.send_replace(WorkerGroupState::default());
        result
    }
}
