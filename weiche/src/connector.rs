//! plaintext HTTP transport creation on the owning compio runtime.
use bytes::Bytes;
use compio::{net::TcpStream, runtime};
use cyper_core::{CompioExecutor, CompioTimer, HyperStream};
use hyper::{
    body::{Body, Incoming},
    client::conn::{http1, http2},
};
use std::net::SocketAddr;

pub type Io = HyperStream<runtime::fd::PollFd<socket2::Socket>>;

pub fn io(stream: TcpStream) -> std::io::Result<Io> {
    let socket = stream.into_poll_fd()?;
    // into_poll_fd does not set O_NONBLOCK; HyperStream polls borrowed buffers.
    socket.set_nonblocking(true)?;
    Ok(HyperStream::new_plain(socket))
}

pub struct Connected<S = http1::SendRequest<Incoming>> {
    pub sender: S,
    // cancelling a lease also cancels its driver on the owning runtime.
    pub driver: runtime::JoinHandle<()>,
}

pub struct Connector {
    pub max_buffer: usize,
}

impl Connector {
    pub async fn connect(&self, address: SocketAddr) -> anyhow::Result<Connected> {
        let (sender, connection) = http1::handshake(self.open(address).await?).await?;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream connection ended");
            }
        });
        Ok(Connected { sender, driver })
    }

    pub async fn connect_http2<B>(
        &self,
        address: SocketAddr,
    ) -> anyhow::Result<Connected<http2::SendRequest<B>>>
    where
        B: Body<Data = Bytes, Error = hyper::Error> + Unpin + 'static,
    {
        let (sender, connection) = http2::Builder::new(CompioExecutor)
            .timer(CompioTimer)
            .max_send_buf_size(self.max_buffer)
            .max_header_list_size(self.max_buffer as u32)
            .handshake(self.open(address).await?)
            .await?;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream HTTP/2 connection ended");
            }
        });
        Ok(Connected { sender, driver })
    }

    async fn open(&self, address: SocketAddr) -> std::io::Result<Io> {
        let stream = TcpStream::connect(address).await?;
        // the 2026-11-12 kind benchmark exposed delayed ACK/Nagle stalls.
        stream.set_nodelay(true)?;
        io(stream)
    }
}
