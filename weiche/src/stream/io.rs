//! adapt nonblocking compio sockets for the HTTP and TLS drivers.
use compio::{net::TcpStream, runtime::fd::PollFd};
use cyper_core::HyperStream;

pub type Socket = PollFd<socket2::Socket>;
pub type Io = HyperStream<Socket>;

/// into_poll_fd retains the original blocking mode; both drivers poll borrowed buffers.
pub fn socket(stream: TcpStream) -> std::io::Result<Socket> {
    stream.set_nodelay(true)?;
    let socket = stream.into_poll_fd()?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}
