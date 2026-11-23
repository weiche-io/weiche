//! poll the HTTP/3 receive stream on demand for the upstream request body.
use super::fields::{self, ContentLength};
use bytes::{Buf, Bytes};
use compio::quic::{
    RecvStream,
    h3::{error::Code, server::RequestStream},
};
use hyper::body::{Body, Frame};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::sync::oneshot;

/// receive half of a request that still has frames to deliver.
struct Receiving {
    /// owns the peer's stream until completion or body cancellation.
    stream: RequestStream<RecvStream, Bytes>,
    /// present during DATA, then cleared while waiting for trailers and FIN.
    data: Option<ContentLength>,
    /// wake the response writer to reset a malformed message.
    reject: oneshot::Sender<()>,
}

impl Receiving {
    /// check the declared length before handing each DATA chunk to the backend.
    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Bytes>>> {
        let Some(length) = &mut self.data else { return Poll::Ready(Ok(None)) };
        let chunk = ready!(self.stream.poll_recv_data(cx)).map_err(io::Error::other)?;
        if let Some(mut chunk) = chunk {
            let size = chunk.remaining();
            length.consume(size)?;
            return Poll::Ready(Ok(Some(chunk.copy_to_bytes(size))));
        }
        length.finish()?;
        self.data = None;
        Poll::Ready(Ok(None))
    }
}

/// upstream-owned HTTP/3 upload, released after trailers, EOF or an error.
pub struct Upload {
    /// keep the QUIC stream out of the shared request body's inline storage.
    receiving: Option<Box<Receiving>>,
}

impl Upload {
    /// signal the response writer separately from the body error delivered to Hyper.
    pub fn new(
        stream: RequestStream<RecvStream, Bytes>,
        length: ContentLength,
    ) -> (Self, oneshot::Receiver<()>) {
        let (reject, malformed) = oneshot::channel();
        let receiving = Receiving { stream, data: Some(length), reject };
        (Self { receiving: Some(Box::new(receiving)) }, malformed)
    }
}

impl Body for Upload {
    type Data = Bytes;
    type Error = io::Error;

    /// poll one frame with the backend's waker and release the stream at its end.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        let Some(receiving) = &mut self.receiving else { return Poll::Ready(None) };
        let terminal = match ready!(receiving.poll_data(cx)) {
            Ok(Some(data)) => return Poll::Ready(Some(Ok(Frame::data(data)))),
            Ok(None) => ready!(receiving.stream.poll_recv_trailers(cx))
                .map_err(io::Error::other)
                .and_then(|trailers| trailers.map(fields::trailers).transpose()),
            Err(error) => Err(error),
        };
        let mut receiving = self.receiving.take().expect("active HTTP/3 upload");
        match terminal {
            Ok(trailers) => Poll::Ready(trailers.map(|h| Ok(Frame::trailers(h)))),
            Err(error) => {
                if error.kind() == io::ErrorKind::InvalidData {
                    receiving.stream.stop_sending(Code::H3_MESSAGE_ERROR);
                    let _ = receiving.reject.send(());
                }
                Poll::Ready(Some(Err(error)))
            }
        }
    }

    /// report completion after trailers, EOF or an error.
    fn is_end_stream(&self) -> bool {
        self.receiving.is_none()
    }
}
