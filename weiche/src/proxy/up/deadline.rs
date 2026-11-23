//! absolute deadlines shared by admission, dispatch and body streaming.
use super::super::super::ir::Timeouts;
use compio::runtime::{self, time::sleep_until};
use hyper::body::{Body, Frame, SizeHint};
use std::{
    cell::RefCell,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

/// the request deadline survives retries while each attempt gets a fresh backend budget.
#[derive(Clone, Copy)]
pub struct Deadlines {
    /// absolute deadline for the whole exchange.
    request: Option<Instant>,
    /// duration measured from the beginning of each attempt.
    backend: Option<Duration>,
    /// legacy header-only deadline for routes without configured timeouts.
    headers: Option<Instant>,
}

impl Deadlines {
    /// preserve the original header-only default on unconfigured routes.
    pub fn new(start: Instant, timeouts: Timeouts, default: Duration) -> Self {
        Self {
            request: timeouts.request.filter(|d| !d.is_zero()).map(|d| start + d),
            backend: timeouts.backend_request.filter(|d| !d.is_zero()),
            headers: (timeouts == Timeouts::default()).then(|| start + default),
        }
    }

    /// limit a single attempt by the time remaining in the request.
    pub fn attempt(self) -> Option<Instant> {
        self.request
            .into_iter()
            .chain(self.backend.map(|d| Instant::now() + d))
            .min()
    }

    /// wait for response headers, giving an expired deadline precedence over a body error.
    pub async fn dispatch<T>(
        self,
        attempt: Option<Instant>,
        send: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let Some(end) = attempt.or(self.headers) else { return send.await };
        let expired = || anyhow::Error::from(io::Error::from(io::ErrorKind::TimedOut));
        if Instant::now() >= end {
            return Err(expired());
        }
        match runtime::time::timeout_at(end, send).await {
            Ok(result) if Instant::now() < end => result,
            _ => Err(expired()),
        }
    }
}

/// an HTTP body with optional cancellation independent of transport flow control.
pub enum TimedBody<B, G = ()> {
    /// original body and its lifetime guard without an active deadline.
    Direct(
        /// directly polled transport body.
        B,
        /// reservation released with the body.
        G,
    ),
    /// allow the worker to release a stalled upload or pool lease at its deadline.
    Limited {
        /// transport body, taken by the timer even if hyper has stopped polling.
        body: Rc<RefCell<Option<(B, G)>>>,
        /// absolute limit used when a frame and its timer become ready together.
        end: Instant,
        /// cancellable task that wakes the consumer on expiry.
        alarm: runtime::JoinHandle<()>,
    },
    /// final data, trailers, EOF or an error have been delivered.
    Done,
}

impl<B: Body + 'static> TimedBody<B> {
    /// arm the worker timer only for live bodies with a finite budget.
    pub fn new(body: B, deadline: Option<Instant>) -> Self {
        Self::guarded(body, (), deadline)
    }
}

impl<B: Body + 'static, G: 'static> TimedBody<B, G> {
    /// keep the guard with the body through completion, cancellation or an undispatched retry.
    pub fn guarded(body: B, guard: G, deadline: Option<Instant>) -> Self {
        let Some(end) = deadline.filter(|_| !body.is_end_stream()) else {
            return Self::Direct(body, guard);
        };
        let body = Rc::new(RefCell::new(Some((body, guard))));
        let pending = body.clone();
        let alarm = runtime::spawn(async move {
            sleep_until(end).await;
            pending.borrow_mut().take();
        });
        Self::Limited { body, end, alarm }
    }

    /// recover an undispatched upload unless its original attempt has already expired.
    pub fn into_inner(self) -> io::Result<B> {
        match self {
            Self::Direct(body, _) => Ok(body),
            Self::Limited { body, .. } => body
                .borrow_mut()
                .take()
                .map(|(body, _)| body)
                .ok_or(io::ErrorKind::TimedOut.into()),
            Self::Done => Err(io::ErrorKind::UnexpectedEof.into()),
        }
    }
}

impl<B, G: Unpin> Body for TimedBody<B, G>
where
    B: Body + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Data = B::Data;
    type Error = io::Error;

    /// enforce the deadline before delivering the next data or trailer frame.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, io::Error>>> {
        let frame = match &mut *self {
            Self::Done => return Poll::Ready(None),
            Self::Direct(body, _) => Pin::new(body).poll_frame(cx),
            Self::Limited { body, end, alarm } => {
                if Instant::now() >= *end || Pin::new(alarm).poll(cx).is_ready() {
                    *self = Self::Done;
                    return Poll::Ready(Some(Err(io::ErrorKind::TimedOut.into())));
                }
                let mut active = body.borrow_mut();
                let (inner, _) =
                    active.as_mut().expect("timer is pending on this worker");
                let frame = Pin::new(&mut *inner).poll_frame(cx);
                let finished = match &frame {
                    Poll::Ready(Some(Ok(f))) => f.is_trailers() || inner.is_end_stream(),
                    Poll::Ready(_) => true,
                    Poll::Pending => false,
                };
                drop(active);
                if finished {
                    *self = Self::Done;
                }
                frame
            }
        };
        frame.map(|frame| frame.map(|r| r.map_err(io::Error::other)))
    }

    /// report completion from the current storage state.
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Done => true,
            Self::Direct(body, _) => body.is_end_stream(),
            Self::Limited { body, .. } => {
                body.borrow().as_ref().is_some_and(|(body, _)| body.is_end_stream())
            }
        }
    }

    /// forward the available body's size estimate.
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Done => SizeHint::with_exact(0),
            Self::Direct(body, _) => body.size_hint(),
            Self::Limited { body, .. } => body
                .borrow()
                .as_ref()
                .map(|(body, _)| body.size_hint())
                .unwrap_or_default(),
        }
    }
}
