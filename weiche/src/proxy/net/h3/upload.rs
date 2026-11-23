//! bridge a worker-local HTTP/3 receive stream to Hyper through a single frame slot.
use super::fields::{self, ContentLength};
use bytes::{Buf, Bytes};
use compio::{
    quic::{
        RecvStream,
        h3::{error::Code, server::RequestStream},
    },
    runtime,
};
use hyper::body::{Body, Frame};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

/// request frames and the task that produces them for the backend.
pub struct Upload {
    /// ordered DATA, optional trailers or a terminal read error.
    frames: mpsc::Receiver<io::Result<Frame<Bytes>>>,
    /// dropping the body cancels even a reader waiting for the peer's FIN.
    _reader: runtime::JoinHandle<()>,
}

impl Upload {
    /// tie the reader to the upload and signal the response writer on malformed input.
    pub(super) fn new(
        mut stream: RequestStream<RecvStream, Bytes>,
        length: ContentLength,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, frames) = mpsc::channel(1);
        let (reject, malformed) = oneshot::channel();
        let reader = runtime::spawn(async move {
            if let Err(error) = forward(&mut stream, length, &tx).await {
                if error.kind() == io::ErrorKind::InvalidData {
                    stream.stop_sending(Code::H3_MESSAGE_ERROR);
                    let _ = reject.send(());
                }
                let _ = tx.send(Err(error)).await;
            }
        });
        (Self { frames, _reader: reader }, malformed)
    }
}

impl Body for Upload {
    type Data = Bytes;
    type Error = io::Error;

    /// deliver frames in wire order until the reader closes the channel.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        self.frames.poll_recv(cx)
    }
}

/// reserve capacity before reading so a stalled backend stops advancing QUIC flow control.
async fn forward(
    stream: &mut RequestStream<RecvStream, Bytes>,
    mut length: ContentLength,
    tx: &mpsc::Sender<io::Result<Frame<Bytes>>>,
) -> io::Result<()> {
    let trailer_slot = loop {
        let Ok(slot) = tx.reserve().await else { return Ok(()) };
        let Some(mut data) = stream.recv_data().await.map_err(io::Error::other)? else {
            break slot;
        };
        let size = data.remaining();
        length.consume(size)?;
        slot.send(Ok(Frame::data(data.copy_to_bytes(size))));
    };

    length.finish()?;
    if let Some(trailers) = stream.recv_trailers().await.map_err(io::Error::other)? {
        trailer_slot.send(Ok(Frame::trailers(fields::trailers(trailers)?)));
    }
    Ok(())
}
