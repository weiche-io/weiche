//! authenticate a backend using the SNI and trust anchors selected for its Service port.
use super::super::{
    super::ir::{BackendTls, Protocol},
    io::{Io, TlsIo},
};
use anyhow::{Context, ensure};
use compio::{net::TcpStream, tls::rustls::pki_types::ServerName};

/// require ALPN h2 before writing an HTTP/2 preface on a TLS connection.
pub async fn connect(
    stream: TcpStream,
    tls: &BackendTls,
    protocol: Protocol,
) -> anyhow::Result<Io> {
    let name = ServerName::try_from(tls.hostname.clone())
        .context("invalid backend TLS hostname")?;
    let stream = TlsIo::connect(stream, name, tls.config()).await?;
    ensure!(
        protocol != Protocol::Http2 || stream.negotiated_alpn() == Some(b"h2"),
        "backend did not negotiate h2"
    );
    Ok(Io::tls(stream))
}
