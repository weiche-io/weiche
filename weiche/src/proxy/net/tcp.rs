//! relay opaque TCP and TLS passthrough connections with bidirectional half-close.
use super::super::{
    super::ir::{StreamRoute, TlsSocket},
    admission::Admission,
    config::RuntimeConfig,
};
use compio::{
    BufResult,
    buf::{IntoInner, IoBufExt},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, copy},
    net::{TcpListener, TcpStream},
    runtime::time::timeout,
    tls::rustls::pki_types::DnsName,
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
pub async fn forward(
    client: TcpStream,
    routes: watch::Receiver<Arc<StreamRoute>>,
    config: &RuntimeConfig,
) -> io::Result<()> {
    let Some(address) = routes.borrow().select() else { return Ok(()) };
    relay(client, address, config.tcp_connect_timeout, Vec::new()).await
}

/// send any consumed prefix verbatim before relaying both halves of the stream.
async fn relay(
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

/// maximum bytes retained on the wire, including all handshake record headers.
const MAX_FLIGHT: usize = 16 * 1024 + 5;

/// inspect SNI under the handshake deadline, then select from the current listener table.
pub async fn passthrough(
    mut client: TcpStream,
    routes: watch::Receiver<Arc<TlsSocket>>,
    config: &RuntimeConfig,
) -> io::Result<()> {
    let (prefix, host) = timeout(config.tls_handshake_timeout, client_hello(&mut client))
        .await
        .map_err(|_| io::ErrorKind::TimedOut)??;
    if routes.has_changed().is_err() {
        return Ok(());
    }
    let Some(address) = routes.borrow().select(&host) else { return Ok(()) };
    relay(client, address, config.tcp_connect_timeout, prefix).await
}

/// assemble the first handshake across records while retaining every byte for replay.
async fn client_hello(reader: &mut impl AsyncRead) -> io::Result<(Vec<u8>, String)> {
    let invalid = io::ErrorKind::InvalidData;
    let mut wire = Vec::new();
    let mut hello = Vec::new();
    loop {
        let BufResult(read, header) = reader.read_exact([0; 5]).await;
        read?;
        let [22, 3, 0..=3, high, low] = header else { return Err(invalid.into()) };
        let size = usize::from(u16::from_be_bytes([high, low]));
        let start = wire.len() + header.len();
        let end = start + size;
        if size == 0 || end > MAX_FLIGHT {
            return Err(invalid.into());
        }
        wire.reserve(header.len() + size);
        wire.extend_from_slice(&header);
        let BufResult(read, record) = reader.read_exact(wire.slice(start..end)).await;
        read?;
        wire = record.into_inner();
        hello.extend_from_slice(&wire[start..end]);
        if hello[0] != 1 {
            return Err(invalid.into());
        }
        if let [_, a, b, c, body @ ..] = hello.as_slice() {
            let size = u32::from_be_bytes([0, *a, *b, *c]) as usize;
            if size + 4 > MAX_FLIGHT - 5 {
                return Err(invalid.into());
            }
            if let Some(body) = body.get(..size) {
                let host = server_name(body).ok_or(invalid)?;
                return Ok((wire, host.to_owned()));
            }
        }
    }
}

/// consume a two-byte length and its complete payload.
fn vector<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = u16::from_be_bytes(*bytes.split_off(..2)?.as_array()?);
    bytes.split_off(..usize::from(len))
}

/// extract the DNS host_name from a complete ClientHello, validating all vector boundaries.
fn server_name(mut body: &[u8]) -> Option<&str> {
    let [3, 0..=3] = body.split_off(..2)? else { return None };
    body.split_off(..32)?; // skip random.
    let session = usize::from(*body.split_off_first()?);
    if session > 32 {
        return None;
    }
    body.split_off(..session)?;
    let ciphers = vector(&mut body)?;
    if ciphers.is_empty() || !ciphers.len().is_multiple_of(2) {
        return None;
    }
    let compression = usize::from(*body.split_off_first()?);
    if compression == 0 {
        return None;
    }
    body.split_off(..compression)?;
    if body.is_empty() {
        return Some("");
    }
    let mut extensions = vector(&mut body)?;
    if !body.is_empty() {
        return None;
    }
    let mut hostname = None;
    while !extensions.is_empty() {
        let kind = extensions.split_off(..2)?;
        let mut data = vector(&mut extensions)?;
        if kind != [0, 0] {
            continue;
        }
        let mut names = vector(&mut data)?;
        if names.is_empty() || !data.is_empty() || hostname.is_some() {
            return None;
        }
        hostname = Some("");
        while !names.is_empty() {
            let kind = *names.split_off_first()?;
            let name = vector(&mut names)?;
            if kind == 0 {
                if hostname != Some("") {
                    return None;
                }
                let name = std::str::from_utf8(name).ok()?;
                DnsName::try_from(name).ok()?;
                hostname = Some(name);
            }
        }
    }
    Some(hostname.unwrap_or(""))
}
