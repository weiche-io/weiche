//! adapt HTTP/3 request streams to the worker's shared HTTP service.
use super::super::{
    super::ir::Binding,
    admission::Permit,
    config::RuntimeConfig,
    http::{ProxyService, RequestBody, ResponseBody, wire::strip_hop_headers},
    stopped,
};
use bytes::{Buf, Bytes};
use compio::{
    quic::{
        Incoming,
        h3::{
            self,
            error::Code,
            server::{RequestResolver, RequestStream},
        },
    },
    runtime::time::{timeout, timeout_at},
};
use futures::{
    StreamExt, TryStreamExt,
    stream::{FuturesUnordered, try_unfold},
};
use http::{HeaderMap, Method, Response, StatusCode, header};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use std::{io, net::SocketAddr, rc::Rc};
use tokio::sync::{Notify, watch};

/// keep the permit through TLS negotiation, concurrent requests and graceful drain.
pub async fn serve(
    incoming: Incoming,
    service: &ProxyService,
    binding: &Binding,
    config: &RuntimeConfig,
    _permit: Permit,
    shutdown: watch::Receiver<bool>,
) {
    let stopping = stopped(shutdown);
    tokio::pin!(stopping);
    let handshake = async {
        let transport = incoming.await?;
        let conn = h3::server::builder()
            .max_field_section_size(config.http_max_buffer as u64)
            .build(transport.clone())
            .await?;
        Ok::<_, anyhow::Error>((transport, conn))
    };
    let (transport, mut conn) = tokio::select! {
        biased;
        _ = &mut stopping => return,
        result = timeout(config.tls_handshake_timeout, handshake) => match result {
            Ok(Ok(pair)) => pair,
            Ok(Err(error)) => { tracing::debug!(%error, "quic handshake failed"); return; }
            Err(_) => { tracing::debug!("quic handshake timed out"); return; }
        }
    };
    let mut requests = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut stopping => break,
            Some(()) = requests.next(), if !requests.is_empty() => {},
            result = conn.accept() => match result {
                Ok(Some(resolver)) => {
                    let peer = transport.remote_address();
                    requests.push(async move {
                        if let Err(error) = request(resolver, service, binding, peer).await {
                            tracing::debug!(%peer, %error, "http/3 stream ended");
                        }
                    });
                }
                Ok(None) => return,
                Err(error) => { tracing::debug!(%error, "http/3 connection ended"); return; }
            }
        }
    }
    let _ = timeout(config.connection_drain_timeout, async {
        conn.shutdown(0).await?;
        while requests.next().await.is_some() {}
        // a finished send stream can still have unacknowledged data on the wire.
        transport.closed().await;
        Ok::<_, h3::error::ConnectionError>(())
    })
    .await;
}

/// reject connection-specific fields before translating the request to another HTTP version.
fn valid_fields(headers: &HeaderMap) -> bool {
    !["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"]
        .iter()
        .any(|&name| headers.contains_key(name))
        && headers
            .get_all(header::TE)
            .iter()
            .all(|v| v.as_bytes().eq_ignore_ascii_case(b"trailers"))
}

/// validate the request fields and decode an optional decimal content length.
fn request_length(headers: &HeaderMap) -> Result<Option<u64>, Code> {
    if !valid_fields(headers) {
        return Err(Code::H3_MESSAGE_ERROR);
    }
    let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
    let Some(value) = values.next() else { return Ok(None) };
    if values.next().is_some() || !value.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(Code::H3_MESSAGE_ERROR);
    }
    value
        .to_str()
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Some)
        .ok_or(Code::H3_MESSAGE_ERROR)
}

/// stream data and trailers on demand so upstream flow control reaches the QUIC peer.
fn upload<S: h3::quic::RecvStream>(
    stream: RequestStream<S, Bytes>,
    length: Option<u64>,
    malformed: Rc<Notify>,
) -> impl hyper::body::Body<Data = Bytes, Error = io::Error> {
    let frames = try_unfold(Some((stream, length)), |state| async move {
        let Some((mut stream, mut remaining)) = state else { return Ok(None) };
        if let Some(mut data) = stream.recv_data().await.map_err(io::Error::other)? {
            let size = data.remaining();
            if let Some(left) = &mut remaining {
                *left = left.checked_sub(size as u64).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "excess HTTP/3 request data",
                    )
                })?;
            }
            return Ok(Some((
                Frame::data(data.copy_to_bytes(size)),
                Some((stream, remaining)),
            )));
        }
        if remaining.is_some_and(|left| left != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete HTTP/3 request body",
            ));
        }
        let trailers = stream.recv_trailers().await.map_err(io::Error::other)?;
        if trailers.as_ref().is_some_and(|h| {
            !valid_fields(h)
                || h.contains_key(header::CONTENT_LENGTH)
                || h.contains_key(header::HOST)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HTTP/3 trailers",
            ));
        }
        Ok(trailers.map(|h| (Frame::trailers(h), None)))
    });
    StreamBody::new(frames.inspect_err(move |error| {
        if error.kind() == io::ErrorKind::InvalidData {
            malformed.notify_one();
        }
    }))
}

/// bridge one request without serializing header resolution or buffering either body.
async fn request<C: h3::quic::Connection<Bytes> + 'static>(
    resolver: RequestResolver<C, Bytes>,
    service: &ProxyService,
    binding: &Binding,
    peer: SocketAddr,
) -> anyhow::Result<()>
where
    C::BidiStream: h3::quic::BidiStream<Bytes>,
{
    let (req, mut stream) = resolver.resolve_request().await?;
    let length = match request_length(req.headers()) {
        Ok(length) => length,
        Err(code) => {
            stream.stop_sending(code);
            stream.stop_stream(code);
            return Ok(());
        }
    };
    let head = req.method() == Method::HEAD;
    let (mut send, recv) = stream.split();
    let malformed = Rc::new(Notify::new());
    let req = req
        .map(|()| RequestBody::Stream(Box::pin(upload(recv, length, malformed.clone()))));
    let exchange = async {
        let reply = service.call(binding, peer, req).await;
        let write = respond(&mut send, reply.response, head);
        match reply.deadline {
            Some(end) => timeout_at(end, write).await?,
            None => write.await,
        }
    };
    let result = tokio::select! {
        biased;
        _ = malformed.notified() => {
            send.stop_stream(Code::H3_MESSAGE_ERROR);
            return Ok(());
        }
        result = exchange => result,
    };
    if result.is_err() {
        send.stop_stream(Code::H3_INTERNAL_ERROR);
    }
    result
}

/// flush the headers, data and optional trailers while respecting peer flow control.
async fn respond<S: h3::quic::SendStream<Bytes>>(
    send: &mut RequestStream<S, Bytes>,
    response: Response<ResponseBody>,
    head: bool,
) -> anyhow::Result<()> {
    let (mut parts, mut body) = response.into_parts();
    strip_hop_headers(&mut parts.headers);
    let has_body = !head
        && !parts.status.is_informational()
        && !matches!(parts.status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED);
    send.send_response(Response::from_parts(parts, ())).await?;
    if has_body {
        while let Some(frame) = body.frame().await {
            match frame?.into_data() {
                Ok(data) if data.is_empty() => continue,
                Ok(data) => send.send_data(data).await?,
                Err(frame) => {
                    if let Ok(mut trailers) = frame.into_trailers() {
                        strip_hop_headers(&mut trailers);
                        send.send_trailers(trailers).await?;
                        break;
                    }
                }
            }
        }
    }
    send.finish().await?;
    Ok(())
}
