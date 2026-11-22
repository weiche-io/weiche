//! adapt rustls plaintext to hyper while compio owns the ciphertext in flight.
mod handshake;
mod transport;

use compio::{
    driver::{SharedFd, op::ShutdownSocket},
    runtime::Submit,
    tls::rustls::{self, Connection},
};
pub use handshake::AcceptedTls;
use socket2::Socket;
use std::{
    io,
    sync::Arc,
    task::{Context, Poll, ready},
};
use transport::Transport;

fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// worker-local TLS stream for hyper driven by io_uring RECV and SEND completions.
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
}
