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
use send_wrapper::SendWrapper;
use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

/// worker-local HTTP transport covering plain TCP and io_uring TLS connections.
pub struct Io(
    /// transport restricted to the thread that created it.
    SendWrapper<Transport>,
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
        Ok(Self(SendWrapper::new(Transport::Plain(PlainIo::new(stream)?))))
    }

    /// wrap a completed io_uring TLS transport.
    pub fn tls(stream: TlsIo) -> Self {
        Self(SendWrapper::new(Transport::Tls(Box::new(stream))))
    }
}

/// dispatch hyper operations to the concrete adapter after checking thread ownership.
macro_rules! transport {
    ($method:ident($($arg:ident: $ty:ty),*) -> $result:ty) => {
        #[doc = concat!("forward `", stringify!($method), "` to the pinned socket adapter.")]
        fn $method(self: Pin<&mut Self>, cx: &mut Context<'_>, $($arg: $ty),*)
            -> Poll<io::Result<$result>>
        {
            transport!(&mut *self.get_mut().0, |stream| Pin::new(stream).$method(cx, $($arg),*))
        }
    };
    ($io:expr, |$stream:ident| $call:expr) => {
        match $io {
            Transport::Plain($stream) => $call,
            Transport::Tls($stream) => $call,
        }
    };
}

impl Read for Io {
    transport!(poll_read(buf: ReadBufCursor<'_>) -> ());
}

impl Write for Io {
    transport!(poll_write(buf: &[u8]) -> usize);
    transport!(poll_write_vectored(bufs: &[IoSlice<'_>]) -> usize);
    transport!(poll_flush() -> ());
    transport!(poll_shutdown() -> ());

    /// report vectored write support on the owning worker.
    fn is_write_vectored(&self) -> bool {
        transport!(&*self.0, |stream| stream.is_write_vectored())
    }
}
