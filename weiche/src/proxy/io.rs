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

/// forward a hyper poll method to the pinned transport on its owning worker.
macro_rules! poll_transport {
    ($(#[$doc:meta])* fn $method:ident($($arg:ident: $ty:ty),*) -> $result:ty) => {
        $(#[$doc])*
        fn $method(self: Pin<&mut Self>, cx: &mut Context<'_>, $($arg: $ty),*)
            -> Poll<io::Result<$result>>
        {
            match &mut *self.get_mut().0 {
                Transport::Plain(io) => Pin::new(io).$method(cx, $($arg),*),
                Transport::Tls(io) => Pin::new(&mut **io).$method(cx, $($arg),*),
            }
        }
    };
}

impl Read for Io {
    poll_transport!(
        /// read from the active transport.
        fn poll_read(buf: ReadBufCursor<'_>) -> ()
    );
}

impl Write for Io {
    poll_transport!(
        /// write to the active transport.
        fn poll_write(buf: &[u8]) -> usize
    );
    poll_transport!(
        /// preserve vectored writes through either adapter.
        fn poll_write_vectored(bufs: &[IoSlice<'_>]) -> usize
    );
    poll_transport!(
        /// flush pending writes on the active transport.
        fn poll_flush() -> ()
    );
    poll_transport!(
        /// let the transport complete its write-side shutdown.
        fn poll_shutdown() -> ()
    );

    /// report the active adapter's vectored write support.
    fn is_write_vectored(&self) -> bool {
        match &*self.0 {
            Transport::Plain(stream) => stream.is_write_vectored(),
            Transport::Tls(stream) => stream.is_write_vectored(),
        }
    }
}
