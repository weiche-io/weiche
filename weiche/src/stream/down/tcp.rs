//! downstream TCP listeners for HTTP/1 and HTTP/2.
use super::{
    super::{
        super::{routing, rtconf::RuntimeConfig},
        core::{http::ProxyService, tcp::Admission},
        stopped,
    },
    ListenerGroup, bind_socket,
    http::Server,
};
use compio::{
    net::TcpListener,
    runtime::{
        self,
        time::{sleep, timeout},
    },
};
use std::{future::pending, io, net::SocketAddr, pin::pin, rc::Rc, sync::Arc};
use tokio::sync::watch;

/// bind a reusable listener on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, socket2::Protocol::TCP)?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}

/// run one listener on its owning compio runtime.
pub(in super::super) async fn run(
    id: usize,
    mut config: routing::Reader,
    runtime: Arc<RuntimeConfig>,
    admission: Arc<Admission>,
    listeners: Arc<ListenerGroup>,
    stop: watch::Sender<bool>,
) -> anyhow::Result<()> {
    let snapshot = config.snapshot();
    let revision = snapshot.revision;
    let service = ProxyService::new(snapshot, &runtime)
        .inspect_err(|error| listeners.failed(id, revision, error))?;
    let server = Rc::new(Server::new(service, &runtime));
    listeners.applied(id, revision);

    let (connections, _) = watch::channel(false);
    let stopping = stopped(stop.subscribe());
    tokio::pin!(stopping);
    let result = 'bind: loop {
        if *stop.borrow() {
            break Ok(());
        }
        let requested = server.service.listener().map(|l| l.address);
        listeners.clear_listener(id);
        let binding = requested.and_then(|address| match listeners.bind(id, address) {
            Ok(socket) => Some(socket),
            Err(error) => {
                tracing::error!(worker = id, %address, %error, "http listener bind failed; retrying");
                None
            }
        });
        let retry = requested.is_some() && binding.is_none();
        // keep the native accept pending across route-only snapshot updates.
        let accept = async || {
            let Some(socket) = &binding else { return pending().await };
            socket.accept().await
        };
        let mut incoming = pin!(accept());
        loop {
            tokio::select! {
                biased;
                _ = &mut stopping => break 'bind Ok(()),
                changed = config.changed() => {
                    if changed.is_err() {
                        (&mut stopping).await;
                        break 'bind Ok(());
                    }
                    let snapshot = config.snapshot();
                    let revision = snapshot.revision;
                    let previous = server.service.listener();
                    match server.service.apply(snapshot) {
                        Ok(revision) => {
                            if previous != server.service.listener() {
                                connections.send_replace(false);
                            }
                            listeners.applied(id, revision);
                            tracing::debug!(worker = id, revision, "applied gateway snapshot");
                        }
                        Err(error) => {
                            listeners.failed(id, revision, &error);
                            tracing::error!(worker = id, revision, %error, "failed to prepare gateway snapshot");
                        }
                    }
                    if retry || server.service.listener().map(|l| l.address) != requested { continue 'bind; }
                }
                _ = sleep(runtime.listener_retry_interval), if retry => continue 'bind,
                accepted = &mut incoming => {
                    let (stream, peer) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => break 'bind Err(error.into()),
                    };
                    if let Some(listener) = server.service.listener() && let Some(permit) = admission.acquire() {
                        tracing::debug!(worker = id, %peer, "serving HTTP connection");
                        let task = server.clone().serve(listener, stream, peer, permit, connections.subscribe());
                        runtime::spawn(task).detach();
                    }
                    incoming.set(accept());
                }
            }
        }
    };
    listeners.stopped(id);
    if result.is_err() {
        stop.send_replace(true);
    }
    // each connection holds a receiver until its HTTP drain completes.
    connections.send_replace(true);
    if timeout(runtime.worker_drain_timeout, connections.closed())
        .await
        .is_err()
    {
        tracing::warn!(worker = id, "worker drain deadline exceeded");
    }
    server.service.close();
    result
}
