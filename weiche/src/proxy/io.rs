//! adapt compio sockets for worker-local HTTP and TLS connections.
mod handshake;
mod plain;
mod tls;
mod transport;

pub use handshake::AcceptedTls;
pub use plain::PlainIo;
pub use tls::TlsIo;

use compio::net::TcpStream;
use hyper::rt::{Read, ReadBufCursor, Write};
use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

/// worker-local HTTP transport covering plain TCP and io_uring TLS connections.
pub struct Io(
    /// transport restricted to the thread that created it.
    send_wrapper::SendWrapper<Transport>,
);

/// hyper erases upgraded transports behind a Send bound. all polling and destruction
/// stays on the owning compio worker, enforced by the wrapper's thread check.
enum Transport {
    /// cleartext socket adapter.
    Plain(PlainIo),
    /// encrypted socket adapter.
    Tls(Box<TlsIo>),
}

impl Io {
    /// build an uninitialized, readiness-cached plain HTTP socket.
    pub fn plain(stream: TcpStream) -> io::Result<Self> {
        Ok(Self(send_wrapper::SendWrapper::new(Transport::Plain(PlainIo::new(stream)?))))
    }

    /// wrap a completed io_uring TLS transport.
    pub fn tls(stream: TlsIo) -> Self {
        Self(send_wrapper::SendWrapper::new(Transport::Tls(Box::new(stream))))
    }
}

impl Read for Io {
    /// read from the transport selected for this connection.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self.get_mut().0 {
            Transport::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Transport::Tls(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
        }
    }
}

impl Write for Io {
    /// write to the active plain or encrypted stream.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self.get_mut().0 {
            Transport::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Transport::Tls(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
        }
    }

    /// preserve vectored writes through either adapter.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self.get_mut().0 {
            Transport::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Transport::Tls(stream) => {
                Pin::new(&mut **stream).poll_write_vectored(cx, bufs)
            }
        }
    }

    /// report the active adapter's vectored write support.
    fn is_write_vectored(&self) -> bool {
        match &*self.0 {
            Transport::Plain(stream) => stream.is_write_vectored(),
            Transport::Tls(stream) => stream.is_write_vectored(),
        }
    }

    /// flush pending writes on the active transport.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self.get_mut().0 {
            Transport::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Transport::Tls(stream) => Pin::new(&mut **stream).poll_flush(cx),
        }
    }

    /// let the transport complete its write-side shutdown.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self.get_mut().0 {
            Transport::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Transport::Tls(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
        }
    }
}
