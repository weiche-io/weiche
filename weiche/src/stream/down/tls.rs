//! select the terminating listener by SNI before releasing application data to HTTP.
use super::super::{
    super::ir::{ServerTls, hostname_matches},
    io::{AcceptedTls, TlsIo},
};
use compio::net::TcpStream;
use std::io;

/// a listener without a hostname also accepts clients that omit SNI.
pub async fn accept(
    stream: TcpStream,
    certs: &[std::sync::Arc<ServerTls>],
) -> io::Result<TlsIo> {
    let handshake = AcceptedTls::accept(stream).await?;
    let host = handshake
        .client_hello()
        .server_name()
        .unwrap_or("")
        .to_ascii_lowercase();
    let tls = certs
        .iter()
        .find(|tls| hostname_matches(&tls.hostname, &host))
        .ok_or_else(|| io::Error::other("SNI does not match a listener hostname"))?;
    handshake.into_stream(tls.config()).await
}
