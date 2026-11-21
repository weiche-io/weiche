//! adapt compio sockets for worker-local HTTP and TLS connections.
mod plain;

pub use plain::PlainIo;

use compio::{net::TcpStream, runtime::fd::PollFd};
use cyper_core::HyperStream;
use hyper::rt::{Read, ReadBufCursor, Write};
use std::{
    borrow::Cow,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

/// nonblocking readiness socket used by the TLS driver.
pub type Socket = PollFd<socket2::Socket>;

/// worker-local HTTP transport covering plain TCP and TLS connections.
pub enum Io {
    Plain(PlainIo),
    Tls(Box<HyperStream<Socket>>),
}

impl Io {
    /// build an uninitialized, readiness-cached plain HTTP socket.
    pub fn plain(stream: TcpStream) -> io::Result<Self> {
        Ok(Self::Plain(PlainIo::new(stream)?))
    }

    /// return the negotiated ALPN protocol when running over TLS.
    pub fn negotiated_alpn(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Plain(_) => None,
            Self::Tls(stream) => stream.negotiated_alpn(),
        }
    }
}

/// prepare a nonblocking PollFd socket for TLS handshakes.
pub fn socket(stream: TcpStream) -> io::Result<Socket> {
    stream.set_nodelay(true)?;
    stream.into_poll_fd()
}

impl Read for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
        }
    }
}

impl Write for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
        }
    }
}
