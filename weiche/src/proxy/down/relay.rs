//! forward opaque TCP connections, retaining half-close semantics in both directions.
use super::super::{super::ir::StreamRoute, config::RuntimeConfig, core::tcp::Admission};
use compio::{
    io::{AsyncWriteExt, copy},
    net::{TcpListener, TcpStream},
    runtime::time::timeout,
};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::watch;

/// admit and drain raw TCP and TLS connections using the current route watch.
pub async fn serve<T>(
    socket: TcpListener,
    mut routes: watch::Receiver<T>,
    admission: Arc<Admission>,
    config: Arc<RuntimeConfig>,
    forward: impl AsyncFn(TcpStream, watch::Receiver<T>, &RuntimeConfig) -> io::Result<()>,
) -> io::Result<()> {
    let mut connections = FuturesUnordered::new();
    let result = {
        // completion-based accept must keep its future even when another stream finishes.
        let mut accepting = std::pin::pin!(socket.accept());
        loop {
            let (client, _) = tokio::select! {
                biased;
                changed = routes.changed() => {
                    if changed.is_err() { break Ok(()) }
                    continue;
                }
                _ = connections.next(), if !connections.is_empty() => continue,
                accepted = &mut accepting => match accepted {
                    Ok(pair) => pair,
                    Err(error) => break Err(error),
                },
            };
            accepting.set(socket.accept());
            if let Some(permit) = admission.acquire() {
                let connection = forward(client, routes.clone(), &config);
                connections.push(async move {
                    let _permit = permit;
                    let _ = connection.await;
                });
            }
        }
    };
    drop(socket);
    let _ = timeout(config.connection_drain_timeout, async {
        while connections.next().await.is_some() {}
    })
    .await;
    result
}

/// select the backend for an opaque TCP stream before opening its upstream connection.
pub async fn tcp(
    client: TcpStream,
    routes: watch::Receiver<Arc<StreamRoute>>,
    config: &RuntimeConfig,
) -> io::Result<()> {
    let Some(address) = routes.borrow().select() else { return Ok(()) };
    forward(client, address, config.tcp_connect_timeout, Vec::new()).await
}

/// send any consumed prefix verbatim before relaying both halves of the stream.
pub async fn forward(
    client: TcpStream,
    address: SocketAddr,
    deadline: Duration,
    prefix: Vec<u8>,
) -> io::Result<()> {
    let mut upstream = timeout(deadline, TcpStream::connect(address))
        .await
        .map_err(|_| io::ErrorKind::TimedOut)??;
    upstream.set_nodelay(true)?;
    if !prefix.is_empty() {
        upstream.write_all(prefix).await.0?;
    }
    let (mut cr, mut cw) = client.split();
    let (mut ur, mut uw) = upstream.split();
    // compio copy flushes and shuts down its writer at EOF while the reverse copy continues.
    futures::try_join!(copy(&mut cr, &mut uw), copy(&mut ur, &mut cw))?;
    Ok(())
}
