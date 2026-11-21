//! select the terminating listener by SNI before releasing application data to HTTP.
use super::super::{
    super::ir::{ServerTls, hostname_matches},
    io::{Io, Socket},
};
use compio::tls::LazyConfigAcceptor;
use cyper_core::HyperStream;
use std::io;

/// a listener without a hostname also accepts clients that omit SNI.
pub async fn accept(socket: Socket, tls: &ServerTls) -> io::Result<Io> {
    let handshake = LazyConfigAcceptor::new(Default::default(), socket).await?;
    if !tls.hostname.is_empty()
        && !handshake
            .client_hello()
            .server_name()
            .is_some_and(|sni| hostname_matches(&tls.hostname, &sni.to_ascii_lowercase()))
    {
        return Err(io::Error::other("SNI does not match listener hostname"));
    }
    Ok(HyperStream::new_tls(handshake.into_stream(tls.config()).await?))
}
