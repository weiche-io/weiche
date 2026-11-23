//! adapt rustls plaintext to hyper while compio owns the ciphertext in flight.
use super::transport::Transport;

use compio::{
    BufResult,
    driver::{SharedFd, ToSharedFd, op::ShutdownSocket},
    runtime::{Submit, submit},
    tls::rustls::{self, Connection},
};
use hyper::rt::{Read, ReadBufCursor, Write as HyperWrite};
use socket2::Socket;
use std::{
    future::poll_fn,
    io::{self, BufRead, IoSlice, Write},
    net::Shutdown,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

/// convert a rustls protocol failure to an invalid-data socket error.
pub fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// worker-local TLS stream with immediate h2 reads and writes and io_uring fallback.
pub struct TlsIo {
    /// buffered ciphertext and the underlying compio socket.
    transport: Transport,
    /// rustls session owning decrypted application data.
    conn: Connection,
    /// whether close_notify has been queued.
    closing: bool,
    /// in-flight shutdown of the socket write half.
    shutdown: Option<Submit<ShutdownSocket<SharedFd<Socket>>>>,
    /// whether the write half has finished shutting down.
    closed: bool,
    /// terminal record error preserved across subsequent reads.
    read_error: Option<Arc<io::Error>>,
}

impl TlsIo {
    /// send the final flight, or a queued fatal alert, before handing the socket back.
    pub async fn handshake(transport: Transport, conn: Connection) -> io::Result<Self> {
        let mut stream = Self {
            transport,
            conn,
            closing: false,
            shutdown: None,
            closed: false,
            read_error: None,
        };
        let result: io::Result<()> = async {
            stream.conn.process_new_packets().map_err(tls_error)?;
            while stream.conn.is_handshaking() {
                poll_fn(|cx| stream.poll_output(cx, false)).await?;
                if !poll_fn(|cx| stream.poll_packet(cx)).await?
                    && stream.conn.is_handshaking()
                {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            Ok(())
        }
        .await;
        let sent = poll_fn(|cx| stream.poll_output(cx, false)).await;
        result?;
        sent?;
        // let multiplexed h2 streams consume ready data and flush it in the same poll.
        stream.transport.try_immediate_io = stream.negotiated_alpn() == Some(b"h2");
        Ok(stream)
    }

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
    /// deliver buffered plaintext before driving the next record read.
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
    /// queue a plaintext slice and drive any pending ciphertext write.
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

    /// advertise support for vectored plaintext writes.
    fn is_write_vectored(&self) -> bool {
        true
    }

    /// pass the caller's slices to the rustls plaintext writer.
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

    /// flush queued records to the socket.
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_output(cx, false)
    }

    /// flush close_notify before shutting down the socket write half.
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
