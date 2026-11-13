//! plaintext HTTP/1 transport creation on the owning compio runtime.
use compio::{net::TcpStream, runtime};
use cyper_core::HyperStream;
use hyper::{body::Incoming, client::conn::http1::SendRequest};
use std::net::SocketAddr;

pub type Io = HyperStream<runtime::fd::PollFd<socket2::Socket>>;

pub fn io(stream: TcpStream) -> std::io::Result<Io> {
    let socket = stream.into_poll_fd()?;
    // into_poll_fd does not set O_NONBLOCK; HyperStream polls borrowed buffers.
    socket.set_nonblocking(true)?;
    Ok(HyperStream::new_plain(socket))
}

pub struct Connected {
    pub sender: SendRequest<Incoming>,
    // cancelling a lease also cancels its driver on the owning runtime.
    pub driver: runtime::JoinHandle<()>,
}

pub struct Connector;

impl Connector {
    pub async fn connect(&self, address: SocketAddr) -> anyhow::Result<Connected> {
        let stream = TcpStream::connect(address).await?;
        // the 2026-11-12 kind benchmark exposed delayed ACK/Nagle stalls.
        stream.set_nodelay(true)?;
        let (sender, connection) = hyper::client::conn::http1::handshake(io(stream)?).await?;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream connection ended");
            }
        });
        Ok(Connected { sender, driver })
    }
}
