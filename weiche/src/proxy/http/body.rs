//! adapt worker-local transport bodies for proxy dispatch.
use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

/// upload received from the downstream transport.
pub enum RequestBody {
    /// hyper owns framing for HTTP/1 and HTTP/2 connections.
    Incoming(
        /// incoming data and trailer frames.
        Incoming,
    ),
    /// adapt the HTTP/3 receive stream on demand.
    Stream(
        /// pinned worker-local stream of upload frames.
        Pin<Box<dyn Body<Data = Bytes, Error = io::Error>>>,
    ),
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = io::Error;

    /// poll the transport directly, preserving data and trailer frames.
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        match self.get_mut() {
            Self::Incoming(body) => {
                Pin::new(body).poll_frame(cx).map_err(io::Error::other)
            }
            Self::Stream(body) => body.as_mut().poll_frame(cx),
        }
    }

    /// report when the transport has finished the upload.
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Incoming(body) => body.is_end_stream(),
            Self::Stream(body) => body.is_end_stream(),
        }
    }

    /// expose the transport's size estimate for upstream framing.
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Incoming(body) => body.size_hint(),
            Self::Stream(body) => body.size_hint(),
        }
    }
}
