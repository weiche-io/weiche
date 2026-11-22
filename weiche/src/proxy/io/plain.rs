//! plain HTTP socket transport with uninitialized reads and readiness caching.
use compio::{
    BufResult,
    buf::IntoInner,
    driver::{
        SharedFd,
        op::{Interest, PollOnce, SendFlags},
    },
    net::TcpStream,
    runtime::{Submit, submit},
};
use hyper::rt::{Read, ReadBufCursor, Write};
use socket2::Socket;
use std::{
    future::Future,
    io::{self, IoSlice},
    net::Shutdown,
    pin::Pin,
    task::{Context, Poll, ready},
};

/// worker-local nonblocking TCP stream for hyper without zero-fill or thread checks.
pub struct PlainIo {
    /// nonblocking socket registered with this worker.
    socket: SharedFd<Socket>,
    /// pending poll for read readiness.
    read_wait: Option<Submit<PollOnce<SharedFd<Socket>>>>,
    /// pending poll for write readiness.
    write_wait: Option<Submit<PollOnce<SharedFd<Socket>>>>,
    /// whether a read can be attempted immediately.
    read_ready: bool,
    /// whether a write can be attempted immediately.
    write_ready: bool,
}

impl PlainIo {
    /// configure nodelay and nonblocking mode, taking ownership of the shared descriptor.
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        // into_poll_fd switches the descriptor to nonblocking mode once.
        let socket = stream.into_poll_fd()?.into_inner();
        Ok(Self {
            socket,
            read_wait: None,
            write_wait: None,
            read_ready: true,
            write_ready: true,
        })
    }

    /// retry interrupted operations and wait for readiness after backpressure.
    fn poll_io<R>(
        socket: &SharedFd<Socket>,
        wait: &mut Option<Submit<PollOnce<SharedFd<Socket>>>>,
        ready_flag: &mut bool,
        interest: Interest,
        cx: &mut Context<'_>,
        mut op: impl FnMut(&Socket) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        loop {
            if !*ready_flag {
                let pending = wait.get_or_insert_with(|| {
                    submit(PollOnce::new(socket.clone(), interest))
                });
                let BufResult(result, _) = ready!(Pin::new(pending).poll(cx));
                *wait = None;
                result?;
                *ready_flag = true;
            }
            match op(socket) {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    *ready_flag = false
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}

impl Read for PlainIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let cap = buf.remaining();
        if cap == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = ready!(Self::poll_io(
            &this.socket,
            &mut this.read_wait,
            &mut this.read_ready,
            Interest::Readable,
            cx,
            // safety: socket2::Socket::recv accepts MaybeUninit and initializes the returned prefix.
            |socket| socket.recv(unsafe { buf.as_mut() }),
        ))?;
        unsafe { buf.advance(n) };
        // a short read drains the Linux TCP receive queue; clear readiness to skip a blind EAGAIN.
        if n > 0 && n < cap {
            this.read_ready = false;
        }
        Poll::Ready(Ok(()))
    }
}

impl Write for PlainIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = ready!(Self::poll_io(
            &this.socket,
            &mut this.write_wait,
            &mut this.write_ready,
            Interest::Writable,
            cx,
            |socket| socket.send_with_flags(buf, SendFlags::NOSIGNAL.bits() as _),
        ))?;
        if n > 0 && n < buf.len() {
            this.write_ready = false;
        }
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        let n = ready!(Self::poll_io(
            &this.socket,
            &mut this.write_wait,
            &mut this.write_ready,
            Interest::Writable,
            cx,
            |socket| socket
                .send_vectored_with_flags(bufs, SendFlags::NOSIGNAL.bits() as _),
        ))?;
        if n > 0 && n < total {
            this.write_ready = false;
        }
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let result = match self.socket.shutdown(Shutdown::Write) {
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            result => result,
        };
        if result.is_ok() {
            self.write_wait = None;
        }
        Poll::Ready(result)
    }
}
