//! datagram listener transport; binding does not enable a Gateway UDPRoute.
use super::bind_socket;
use compio::net::UdpSocket;
use std::{io, net::SocketAddr};

/// bind a reusable datagram socket on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<UdpSocket> {
    let socket = bind_socket(address, socket2::Type::DGRAM, socket2::Protocol::UDP)?;
    UdpSocket::from_std(socket.into())
}
