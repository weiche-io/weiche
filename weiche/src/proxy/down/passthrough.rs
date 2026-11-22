//! inspect a bounded ClientHello and replay its original TLS records to the selected backend.
use super::{super::super::ir::TlsSocket, super::config::RuntimeConfig, relay};
use compio::{
    BufResult,
    buf::{IntoInner, IoBufExt},
    io::{AsyncRead, AsyncReadExt},
    net::TcpStream,
    runtime::time::timeout,
    tls::rustls::pki_types::DnsName,
};
use std::{io, sync::Arc};
use tokio::sync::watch;

/// maximum bytes retained on the wire, including all handshake record headers.
const MAX_FLIGHT: usize = 16 * 1024 + 5;

/// inspect SNI under the handshake deadline, then select from the current listener table.
pub async fn forward(
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
    relay::forward(client, address, config.tcp_connect_timeout, prefix).await
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
