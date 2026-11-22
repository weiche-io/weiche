//! move owned ciphertext buffers between rustls and the worker's compio socket.
use compio::{
    BufResult,
    driver::op::SendFlags,
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

/// either half can drive the same send while the other is waiting on it.
#[derive(Default)]
struct SendWake {
    reader: AtomicWaker,
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
    pub socket: Rc<TcpStream>,
    pub input: Cursor<Vec<u8>>,
    /// small h2 flushes may complete in the caller's poll when TCP has room.
    pub try_small_writes: bool,
    output: Vec<u8>,
    recv: Option<LocalBoxFuture<'static, BufResult<usize, Vec<u8>>>>,
    send: Option<LocalBoxFuture<'static, BufResult<(), Vec<u8>>>>,
    send_error: Option<Arc<io::Error>>,
    wake: Arc<SendWake>,
}

impl Transport {
    /// configure the worker-local socket and reserve its reusable receive buffer.
    pub fn new(socket: TcpStream) -> io::Result<Self> {
        socket.set_nodelay(true)?;
        Ok(Self {
            socket: Rc::new(socket),
            input: Cursor::new(Vec::with_capacity(64 * 1024)),
            try_small_writes: false,
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
            let recv = self.recv.get_or_insert_with(|| {
                let mut buf = mem::take(self.input.get_mut());
                buf.clear();
                let socket = self.socket.clone();
                Box::pin(async move { socket.as_ref().read(buf).await })
            });
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

    fn fail_send(&mut self, error: io::Error) -> io::Error {
        let error = Arc::new(error);
        self.send_error = Some(error.clone());
        io::Error::new(error.kind(), error)
    }

    /// try once, leaving the unsent suffix for the completion-based writer.
    fn try_send(&mut self) -> io::Result<()> {
        let flags = SendFlags::DONTWAIT | SendFlags::NOSIGNAL;
        let socket = SockRef::from(self.socket.as_ref());
        match socket.send_with_flags(&self.output, flags.bits() as _) {
            Ok(0) => Err(self.fail_send(io::ErrorKind::WriteZero.into())),
            Ok(n) => {
                self.output.drain(..n);
                Ok(())
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(())
            }
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
                    if self.try_small_writes && self.output.len() <= 4096 {
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
