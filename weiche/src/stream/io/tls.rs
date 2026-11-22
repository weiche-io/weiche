//! adapt rustls plaintext to hyper while compio owns the ciphertext in flight.
mod handshake;
mod transport;

use compio::{
    BufResult,
    driver::{SharedFd, ToSharedFd, op::ShutdownSocket},
    runtime::{Submit, submit},
    tls::rustls::{self, Connection},
};
pub use handshake::AcceptedTls;
use hyper::rt::{Read, ReadBufCursor, Write as HyperWrite};
use socket2::Socket;
use std::{
    io::{self, BufRead, IoSlice, Write},
    net::Shutdown,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use transport::Transport;

fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// worker-local TLS stream with immediate h2 reads and writes and io_uring fallback.
pub struct TlsIo {
    transport: Transport,
    conn: Connection,
    closing: bool,
    shutdown: Option<Submit<ShutdownSocket<SharedFd<Socket>>>>,
    closed: bool,
    read_error: Option<Arc<io::Error>>,
}

impl TlsIo {
    /// return the ALPN protocol negotiated during the TLS handshake.
    pub fn negotiated_alpn(&self) -> Option<&[u8]> {
        self.conn.alpn_protocol()
    }

    /// feed one record fragment to rustls, reporting EOF or close_notify to the handshake.
    fn poll_packet(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        ready!(self.transport.poll_input(cx))?;
        let n = self.conn.read_tls(&mut self.transport.input)?;
        let state = self.conn.process_new_packets().map_err(tls_error)?;
        Poll::Ready(Ok(n != 0 && !state.peer_has_closed()))
    }

    /// drive pending writes from either half without making reads wait on backpressure.
    fn poll_output(
        &mut self,
        cx: &mut Context<'_>,
        from_read: bool,
    ) -> Poll<io::Result<()>> {
        self.transport.poll_send(cx, from_read, |buf| {
            while self.conn.wants_write() {
                self.conn.write_tls(buf)?;
            }
            Ok(())
        })
    }

    /// allow rustls to buffer the next records while the previous send is in flight.
    fn write_plaintext(
        &mut self,
        cx: &mut Context<'_>,
        mut write: impl FnMut(&mut rustls::Writer<'_>) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        if self.closing {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.transport.check_send()?;
        let mut n = write(&mut self.conn.writer())?;
        if n == 0 {
            // rustls has reached its record buffer limit. make room before accepting more.
            let sent = self.poll_output(cx, false)?;
            n = write(&mut self.conn.writer())?;
            if n == 0 {
                ready!(sent);
            }
        } else if n >= 32 * 1024 {
            // submit a large batch now. report late send failures on the next poll.
            let _ = self.poll_output(cx, false);
        }
        Poll::Ready(Ok(n))
    }
}

/// initialize only the prefix of hyper's buffer that contains decrypted bytes.
fn copy_plaintext(conn: &mut Connection, buf: &mut ReadBufCursor<'_>) -> io::Result<()> {
    let mut reader = conn.reader();
    let chunk = reader.fill_buf()?;
    let n = chunk.len().min(buf.remaining());
    buf.put_slice(&chunk[..n]);
    reader.consume(n);
    Ok(())
}

impl Read for TlsIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            let sent = self.poll_output(cx, true);
            if let Some(error) = &self.read_error {
                return Poll::Ready(Err(io::Error::new(error.kind(), error.clone())));
            }
            match copy_plaintext(&mut self.conn, &mut buf) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
            if let Poll::Ready(Err(error)) = sent {
                return Poll::Ready(Err(error));
            }
            if let Err(error) = ready!(self.poll_packet(cx)) {
                self.read_error = Some(Arc::new(error));
            }
        }
    }
}

impl HyperWrite for TlsIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.write_plaintext(cx, |writer| writer.write(buf))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        self.write_plaintext(cx, |writer| writer.write_vectored(bufs))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_output(cx, false)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if !self.closing {
            self.conn.send_close_notify();
            self.closing = true;
        }
        ready!(self.poll_output(cx, false))?;
        let this = &mut *self;
        let shutdown = this.shutdown.get_or_insert_with(|| {
            submit(ShutdownSocket::new(
                this.transport.socket.to_shared_fd(),
                Shutdown::Write,
            ))
        });
        let BufResult(result, _) = ready!(Pin::new(shutdown).poll(cx));
        this.shutdown = None;
        if let Err(error) = result
            && error.kind() != io::ErrorKind::NotConnected
        {
            return Poll::Ready(Err(error));
        }
        this.closed = true;
        Poll::Ready(Ok(()))
    }
}
