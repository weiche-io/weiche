//! QUIC listener transport; the caller supplies its TLS and transport configuration.
use super::udp;
use compio::quic::{Endpoint, EndpointConfig, ServerConfig};
use std::{io, net::SocketAddr};

/// create a server endpoint, without enabling HTTP/3 or advertising a Gateway listener.
pub fn listen(address: SocketAddr, config: ServerConfig) -> io::Result<Endpoint> {
    Endpoint::new(udp::listen(address)?, EndpointConfig::default(), Some(config), None)
}
