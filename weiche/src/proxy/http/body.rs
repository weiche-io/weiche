//! adapt worker-local transport bodies for proxy dispatch.
use super::super::up::{PooledBody, TimedBody};
use bytes::Bytes;
use http_body_util::Full;
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

/// downstream response with either a complete local payload or a leased upstream body.
pub enum ResponseBody {
    /// local response with a known length.
    Full(
        /// buffered bytes for the response.
        Full<Bytes>,
    ),
    /// response streamed from the selected backend.
    Upstream(
        /// leased body with its attempt deadline.
        TimedBody<PooledBody>,
    ),
}

impl ResponseBody {
    /// wrap a complete local payload for downstream framing.
    pub fn full(bytes: Bytes) -> Self {
        Self::Full(Full::new(bytes))
    }
}

/// forward both inline body variants and normalize the first variant's transport error.
macro_rules! forward_body {
    ($name:ident, $first:ident, $second:ident) => {
        impl Body for $name {
            type Data = Bytes;
            type Error = io::Error;

            /// preserve data and trailer frames from the active body.
            fn poll_frame(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
                match self.get_mut() {
                    Self::$first(body) => {
                        Pin::new(body).poll_frame(cx).map_err(io::Error::other)
                    }
                    Self::$second(body) => Pin::new(body).poll_frame(cx),
                }
            }

            /// report whether the active body has finished.
            fn is_end_stream(&self) -> bool {
                match self {
                    Self::$first(body) => body.is_end_stream(),
                    Self::$second(body) => body.is_end_stream(),
                }
            }

            /// preserve the active body's framing estimate.
            fn size_hint(&self) -> SizeHint {
                match self {
                    Self::$first(body) => body.size_hint(),
                    Self::$second(body) => body.size_hint(),
                }
            }
        }
    };
}

forward_body!(RequestBody, Incoming, Stream);
forward_body!(ResponseBody, Full, Upstream);
