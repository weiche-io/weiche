//! borrowed-buffer HTTP I/O on the compio runtime.
// the 2026-11-13 diagnostic found an 8 KiB read staging buffer and two write
// copies in the AsyncStream -> Compat -> TokioIo path. read directly into
// Hyper's buffer and preserve its header/body vectors instead.
// only readiness operations outlive a poll: Hyper does not transfer ownership
// of these slices, so submitting them to completion I/O or SEND_ZC is unsafe.
// this avoids adapter copies; recv/sendmsg still copy across the kernel boundary.
use compio::{net::TcpStream, runtime::fd::PollFd};
use hyper::rt::{Read, ReadBufCursor, Write};
use socket2::Socket;
use std::{
    io::{self, IoSlice},
    net::Shutdown,
    pin::Pin,
    task::{Context, Poll, ready},
};

pub(crate) struct Io(PollFd<Socket>);
impl Io {
    pub(crate) fn new(stream: TcpStream) -> io::Result<Self> {
        let socket = stream.into_poll_fd()?;
        // into_poll_fd does not make the socket nonblocking on Unix.
        socket.set_nonblocking(true)?;
        Ok(Self(socket))
    }
}
fn retry<T>(mut f: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match f() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}
impl Read for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = ready!(self.0.poll_read_with(cx, |socket| {
            // recv never uninitializes bytes and retains no buffer on return.
            retry(|| socket.recv(unsafe { buf.as_mut() }))
        }))?;
        // recv initialized exactly the returned prefix.
        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}
impl Write for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.0.poll_write_with(cx, |socket| {
            retry(|| socket.send_with_flags(buf, libc::MSG_NOSIGNAL))
        })
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        // skip empty prefixes so truncation always makes progress; Linux supports 1024 iovecs.
        let first = bufs.iter().position(|buf| !buf.is_empty()).unwrap();
        let bufs = &bufs[first..bufs.len().min(first + 1024)];
        self.0.poll_write_with(cx, |socket| {
            retry(|| socket.send_vectored_with_flags(bufs, libc::MSG_NOSIGNAL))
        })
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(retry(|| self.0.shutdown(Shutdown::Write)))
    }
}
