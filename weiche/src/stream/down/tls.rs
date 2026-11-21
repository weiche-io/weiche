//! select the terminating listener by SNI before releasing application data to HTTP.
use super::super::{
    super::ir::{ServerTls, hostname_matches},
    io::{Io, Socket},
};
use compio::tls::LazyConfigAcceptor;
use cyper_core::HyperStream;
use std::io;

/// a listener without a hostname also accepts clients that omit SNI.
pub async fn accept(
    socket: Socket,
    certs: &[std::sync::Arc<ServerTls>],
) -> io::Result<Io> {
    let handshake = LazyConfigAcceptor::new(Default::default(), socket).await?;
    let host = handshake
        .client_hello()
        .server_name()
        .unwrap_or("")
        .to_ascii_lowercase();
    let tls = certs
        .iter()
        .find(|tls| hostname_matches(&tls.hostname, &host))
        .ok_or_else(|| io::Error::other("SNI does not match a listener hostname"))?;
    Ok(HyperStream::new_tls(handshake.into_stream(tls.config()).await?))
}
