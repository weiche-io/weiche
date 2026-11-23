//! adapt HTTP/3 request streams to the worker's shared HTTP service.
mod fields;
mod upload;

use super::super::{
    super::ir::Binding,
    http::{ProxyService, RequestBody, ResponseBody, wire::strip_hop_headers},
};
use bytes::Bytes;
use compio::{
    quic::{
        Connection,
        h3::{
            self,
            error::{Code, ConnectionError},
            server::{RequestResolver, RequestStream},
        },
    },
    runtime::time::{timeout, timeout_at},
};
use fields::ContentLength;
use futures::{StreamExt, TryFutureExt, stream::FuturesUnordered};
use http::{Method, Response, StatusCode};
use http_body_util::BodyExt;
use std::{net::SocketAddr, time::Duration};
pub use upload::Upload;

/// established HTTP/3 control streams and their underlying QUIC connection.
pub struct Session {
    /// accepts request streams and sends GOAWAY during drain.
    http: h3::server::Connection<Connection, Bytes>,
    /// supplies the current peer address and waits for outstanding sends at shutdown.
    transport: Connection,
}

impl Session {
    /// open HTTP/3 control streams on a negotiated QUIC connection.
    pub async fn new(
        transport: Connection,
        max_field_section_size: usize,
    ) -> Result<Self, ConnectionError> {
        let http = h3::server::builder()
            .max_field_section_size(max_field_section_size as u64)
            .build(transport.clone())
            .await?;
        Ok(Self { http, transport })
    }

    /// serve independent requests until shutdown, then drain them under one deadline.
    pub async fn serve(
        mut self,
        service: &ProxyService,
        binding: &Binding,
        mut stopping: impl Future<Output = ()> + Unpin,
        drain_timeout: Duration,
    ) {
        let mut requests = FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut stopping => break,
                Some(()) = requests.next(), if !requests.is_empty() => {},
                result = self.http.accept() => match result {
                    Ok(Some(resolver)) => {
                        let peer = self.transport.remote_address();
                        requests.push(request(resolver, service, binding, peer).unwrap_or_else(move |error| {
                            tracing::debug!(%peer, %error, "http/3 stream ended");
                        }));
                    }
                    Ok(None) => return,
                    Err(error) => { tracing::debug!(%error, "http/3 connection ended"); return; }
                }
            }
        }
        let _ = timeout(drain_timeout, async {
            self.http.shutdown(0).await?;
            while requests.next().await.is_some() {}
            // a finished send stream can still have unacknowledged data on the wire.
            self.transport.closed().await;
            Ok::<_, ConnectionError>(())
        })
        .await;
    }
}

/// resolve one request and forward its upload and response independently.
async fn request(
    resolver: RequestResolver<Connection, Bytes>,
    service: &ProxyService,
    binding: &Binding,
    peer: SocketAddr,
) -> anyhow::Result<()> {
    let (req, mut stream) = resolver.resolve_request().await?;
    let length = match ContentLength::parse(req.headers()) {
        Ok(length) => length,
        Err(code) => {
            stream.stop_sending(code);
            stream.stop_stream(code);
            return Ok(());
        }
    };
    let head = req.method() == Method::HEAD;
    let (mut send, recv) = stream.split();
    let (upload, malformed) = Upload::new(recv, length);
    let req = req.map(|()| RequestBody::Http3(upload));
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
        Ok(()) = malformed => {
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
    while has_body && let Some(frame) = body.frame().await {
        match frame?.into_data() {
            Ok(data) if !data.is_empty() => send.send_data(data).await?,
            Err(frame) => {
                let Ok(mut trailers) = frame.into_trailers() else { continue };
                strip_hop_headers(&mut trailers);
                send.send_trailers(trailers).await?;
                break;
            }
            _ => {}
        }
    }
    send.finish().await?;
    Ok(())
}
