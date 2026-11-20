//! downstream TCP listeners for plaintext HTTP/1 and HTTP/2.
use super::bind_socket;
use compio::net::TcpListener;
use std::{io, net::SocketAddr};

/// bind a reusable listener on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, socket2::Protocol::TCP)?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}
