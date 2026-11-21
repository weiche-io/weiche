//! forward opaque TCP connections, retaining half-close semantics in both directions.
use super::super::{super::ir::StreamRoute, core::tcp::Admission};
use compio::{
    io::copy,
    net::{TcpListener, TcpStream},
    runtime::time::timeout,
};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::watch;

/// select a backend on accept. existing streams keep their peer across endpoint updates.
pub async fn serve(
    socket: TcpListener,
    mut routes: watch::Receiver<Arc<StreamRoute>>,
    admission: Arc<Admission>,
    connect_timeout: Duration,
    drain_timeout: Duration,
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
            if let Some(address) = routes.borrow().select()
                && let Some(permit) = admission.acquire()
            {
                connections.push(async move {
                    let _permit = permit;
                    let _ = relay(client, address, connect_timeout).await;
                });
            }
        }
    };
    drop(socket);
    let _ = timeout(drain_timeout, async { while connections.next().await.is_some() {} })
        .await;
    result
}

/// shut down each destination writer when its source reaches EOF.
async fn relay(
    client: TcpStream,
    address: SocketAddr,
    deadline: Duration,
) -> io::Result<()> {
    let upstream = timeout(deadline, TcpStream::connect(address))
        .await
        .map_err(|_| io::ErrorKind::TimedOut)??;
    let (mut cr, mut cw) = client.split();
    let (mut ur, mut uw) = upstream.split();
    // compio copy flushes and shuts down its writer at EOF while the reverse copy continues.
    futures::try_join!(copy(&mut cr, &mut uw), copy(&mut ur, &mut cw))?;
    Ok(())
}
