//! authenticate a backend using the SNI and trust anchors selected for its Service port.
use super::super::{
    super::ir::{BackendTls, Protocol},
    io::{Io, Socket},
};
use anyhow::ensure;
use compio::tls::TlsConnector;
use cyper_core::HyperStream;

/// require ALPN h2 before writing an HTTP/2 preface on a TLS connection.
pub async fn connect(
    socket: Socket,
    tls: &BackendTls,
    protocol: Protocol,
) -> anyhow::Result<Io> {
    let stream = TlsConnector::from(tls.config())
        .connect(&tls.hostname, socket)
        .await?;
    ensure!(
        protocol != Protocol::Http2 || stream.negotiated_alpn().as_deref() == Some(b"h2"),
        "backend did not negotiate h2"
    );
    Ok(HyperStream::new_tls(stream))
}
