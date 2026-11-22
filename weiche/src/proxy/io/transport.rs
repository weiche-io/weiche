//! move owned ciphertext buffers between rustls and the worker's compio socket.
use compio::{
    BufResult,
    driver::op::{RecvFlags, SendFlags},
    io::{AsyncRead, AsyncWriteExt},
    net::TcpStream,
};
use futures::{
    future::LocalBoxFuture,
    task::{ArcWake, AtomicWaker, waker_ref},
};
use socket2::SockRef;
use std::{
    io::{self, Cursor},
    mem,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, ready},
};

/// fall back to an owned socket operation when the immediate attempt makes no progress.
fn immediate(result: io::Result<usize>) -> io::Result<Option<usize>> {
    use io::ErrorKind::{Interrupted, WouldBlock};

    match result {
        Ok(n) => Ok(Some(n)),
        Err(error) if matches!(error.kind(), WouldBlock | Interrupted) => Ok(None),
        Err(error) => Err(error),
    }
}

/// either half can drive the same send while the other is waiting on it.
#[derive(Default)]
struct SendWake {
    /// task polling the read half.
    reader: AtomicWaker,
    /// task polling the write half.
    writer: AtomicWaker,
}

impl ArcWake for SendWake {
    fn wake_by_ref(this: &Arc<Self>) {
        this.reader.wake();
        this.writer.wake();
    }
}

/// keep one receive and one send in flight with buffers owned until completion.
pub struct Transport {
    /// socket shared with outstanding compio operations.
    pub socket: Rc<TcpStream>,
    /// received records and the next unread byte.
    pub input: Cursor<Vec<u8>>,
    /// try socket I/O in the caller's poll after negotiating h2.
    pub try_immediate_io: bool,
    /// pending ciphertext available for the next send.
    output: Vec<u8>,
    /// outstanding read and its owned buffer.
    recv: Option<LocalBoxFuture<'static, BufResult<usize, Vec<u8>>>>,
    /// outstanding write and its owned buffer.
    send: Option<LocalBoxFuture<'static, BufResult<(), Vec<u8>>>>,
    /// first terminal write error.
    send_error: Option<Arc<io::Error>>,
    /// wake both halves when a shared send completes.
    wake: Arc<SendWake>,
}

impl Transport {
    /// configure the worker-local socket and reserve its reusable receive buffer.
    pub fn new(socket: TcpStream) -> io::Result<Self> {
        socket.set_nodelay(true)?;
        Ok(Self {
            socket: Rc::new(socket),
            input: Cursor::new(Vec::with_capacity(64 * 1024)),
            try_immediate_io: false,
            output: Vec::new(),
            recv: None,
            send: None,
            send_error: None,
            wake: Arc::default(),
        })
    }

    /// preserve unconsumed records, including bytes read ahead of the ClientHello.
    pub fn poll_input(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.input.position() < self.input.get_ref().len() as u64 {
            return Poll::Ready(Ok(()));
        }
        loop {
            let recv = match &mut self.recv {
                Some(recv) => recv,
                None => {
                    let buf = self.input.get_mut();
                    buf.clear();
                    if self.try_immediate_io {
                        let socket = SockRef::from(self.socket.as_ref());
                        let flags = RecvFlags::DONTWAIT.bits() as _;
                        if let Some(n) = immediate(
                            socket.recv_with_flags(buf.spare_capacity_mut(), flags),
                        )? {
                            // safety: recv initialized the first n bytes of the empty buffer.
                            unsafe { buf.set_len(n) };
                            self.input.set_position(0);
                            return Poll::Ready(Ok(()));
                        }
                    }
                    let buf = mem::take(buf);
                    let socket = self.socket.clone();
                    self.recv
                        .insert(Box::pin(async move { socket.as_ref().read(buf).await }))
                }
            };
            let BufResult(result, buf) = ready!(recv.as_mut().poll(cx));
            self.recv = None;
            self.input = Cursor::new(buf);
            match result {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return Poll::Ready(result.map(|_| ())),
            }
        }
    }

    /// report a previous send failure even if another task consumed its completion.
    pub fn check_send(&self) -> io::Result<()> {
        match &self.send_error {
            Some(error) => Err(io::Error::new(error.kind(), error.clone())),
            None => Ok(()),
        }
    }

    /// retain a terminal send error for the other half of the connection.
    fn fail_send(&mut self, error: io::Error) -> io::Error {
        let error = Arc::new(error);
        self.send_error = Some(error.clone());
        io::Error::new(error.kind(), error)
    }

    /// try once, leaving the unsent suffix for the completion-based writer.
    fn try_send(&mut self) -> io::Result<()> {
        let flags = SendFlags::DONTWAIT | SendFlags::NOSIGNAL;
        let socket = SockRef::from(self.socket.as_ref());
        match immediate(socket.send_with_flags(&self.output, flags.bits() as _)) {
            Ok(Some(0)) => Err(self.fail_send(io::ErrorKind::WriteZero.into())),
            Ok(Some(n)) => {
                self.output.drain(..n);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => Err(self.fail_send(error)),
        }
    }

    /// reuse one ciphertext buffer. rustls can queue the next batch while it is in flight.
    pub fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        from_read: bool,
        mut fill: impl FnMut(&mut Vec<u8>) -> io::Result<()>,
    ) -> Poll<io::Result<()>> {
        self.check_send()?;
        loop {
            let send = match &mut self.send {
                Some(send) => send,
                None => {
                    fill(&mut self.output)?;
                    if self.output.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    if self.try_immediate_io {
                        self.try_send()?;
                        if self.output.is_empty() {
                            continue;
                        }
                    }
                    let buf = mem::take(&mut self.output);
                    let socket = self.socket.clone();
                    self.send.insert(Box::pin(async move {
                        socket.as_ref().write_all(buf).await
                    }))
                }
            };
            let slot = if from_read { &self.wake.reader } else { &self.wake.writer };
            slot.register(cx.waker());
            let waker = waker_ref(&self.wake);
            let BufResult(result, mut buf) =
                ready!(send.as_mut().poll(&mut Context::from_waker(&waker)));
            slot.take();
            self.send = None;
            if let Err(error) = result {
                return Poll::Ready(Err(self.fail_send(error)));
            }
            buf.clear();
            self.output = buf;
        }
    }
}
