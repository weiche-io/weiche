//! supervise the gateway watch and its worker-local compio data planes.
pub mod admission;
pub mod config;
pub mod http;
mod io;
pub mod net;
mod up;
mod watch;
mod worker;

use super::ir::GatewaySnapshot;
use admission::Admission;
use anyhow::Context;
use compio::{driver::ProactorBuilder, runtime::Runtime};
use config::RuntimeConfig;
use net::ListenerGroup;
use std::{mem::DropGuard, sync::Arc, thread};
use tokio::sync::{
    oneshot,
    watch::{Receiver, Sender, channel},
};
pub use watch::ready;

/// build a thread-local compio io_uring runtime tuned for one worker thread.
pub fn runtime() -> std::io::Result<Runtime> {
    let mut proactor = ProactorBuilder::new();
    proactor
        .capacity(4096)
        .single_issuer(true)
        .defer_taskrun(true)
        .coop_taskrun(true);
    match Runtime::builder().with_proactor(proactor).build() {
        Ok(rt) => Ok(rt),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
            tracing::warn!(%error, "tuned io_uring flags unsupported, falling back to defaults");
            Runtime::new()
        }
        Err(error) => Err(error),
    }
}

/// wait for process shutdown or the shutdown sender to close.
pub async fn stopped(mut shutdown: Receiver<bool>) {
    if !*shutdown.borrow_and_update() {
        let _ = shutdown.changed().await;
    }
}

/// completion signal and result of one supervised OS thread.
struct ProxyThread {
    /// wakes the caller's runtime when the thread exits or unwinds.
    finished: oneshot::Receiver<()>,
    /// retains the result or panic until the supervisor joins.
    handle: thread::JoinHandle<anyhow::Result<()>>,
}

/// threads that share a shutdown signal with the worker on the caller's runtime.
struct Threads {
    /// stopping any member wakes the entire group.
    stop: Sender<bool>,
    /// join every started thread, including after a partial startup failure.
    handles: Vec<ProxyThread>,
}

impl Threads {
    /// signal the group on return or unwind and retain the result for the supervisor.
    fn spawn(
        &mut self,
        name: String,
        run: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let stop = self.stop.clone();
        let (done, finished) = oneshot::channel();
        let handle = thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let _on_exit = DropGuard::new((stop, done), |(stop, done)| {
                    stop.send_replace(true);
                    let _ = done.send(());
                });
                run()
            })
            .with_context(|| format!("spawn {name}"))?;
        self.handles.push(ProxyThread { finished, handle });
        Ok(())
    }

    /// request shutdown, join all threads and report each failure with its origin.
    async fn join(mut self, local: anyhow::Result<()>) -> anyhow::Result<()> {
        self.stop.send_replace(true);
        let mut errors: Vec<_> = local.err().into_iter().collect();
        for ProxyThread { finished, handle } in self.handles.drain(..) {
            // keep driving worker zero's runtime while other workers drain.
            let _ = finished.await;
            let name = handle.thread().name().unwrap_or("proxy thread").to_owned();
            let result = handle
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("thread panicked")));
            if let Err(error) = result.context(name) {
                errors.push(error);
            }
        }
        match errors.len() {
            0 => Ok(()),
            1 => Err(errors.pop().unwrap()),
            _ => anyhow::bail!(
                "{}",
                errors
                    .iter()
                    .map(|error| format!("{error:#}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        }
    }
}

impl Drop for Threads {
    /// wake the watch and workers if the caller drops the supervisor future.
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// start the watch on Tokio, run worker zero locally and join the other compio workers.
pub async fn run(name: String, policy: RuntimeConfig) -> anyhow::Result<()> {
    let policy = Arc::new(policy);
    let (writer, reader) = config::channel(GatewaySnapshot::default());
    let (listeners, state) = ListenerGroup::new(policy.workers.get());
    let (stop, shutdown) = channel(false);
    let mut threads = Threads { stop, handles: Vec::new() };
    let http3 = policy.quic.enabled;
    let watch_stop = shutdown.clone();
    threads.spawn("weiche-proxy-watch".into(), move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create control-plane Tokio runtime")?
            .block_on(watch::run(name, http3, writer, state, watch_stop))
    })?;

    let listeners = Arc::new(listeners);
    let admission = Arc::new(Admission::new(policy.downstream_connections_global.get()));
    for id in 1..policy.workers.get() {
        // construct the worker-local future on its owning runtime.
        let config = reader.clone();
        let policy = policy.clone();
        let admission = admission.clone();
        let listeners = listeners.clone();
        let stop = shutdown.clone();
        let started = threads.spawn(format!("weiche-worker-{id}"), move || {
            let io = runtime().context("create worker compio runtime")?;
            tracing::info!(worker = id, driver = ?io.driver_type(), "starting compio worker");
            io.block_on(worker::run(id, config, policy, admission, listeners, stop))
        });
        if started.is_err() {
            return threads.join(started).await;
        }
    }
    let result =
        worker::run(0, reader, policy, admission, listeners.clone(), shutdown).await;
    threads.join(result.context("worker 0")).await
}
