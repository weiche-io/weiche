//! Streaming HTTP/1.1 proxy. All traffic sockets and timers run on compio.
use crate::config::RoutingTable;
use bytes::Bytes;
use compio::{
    io::compat::AsyncStream,
    net::{TcpListener, TcpStream},
    runtime::{self, time::timeout},
};
use http::{HeaderMap, Request, Response, StatusCode, Uri, Version, header};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{
    cell::Cell, convert::Infallible, net::SocketAddr, pin::Pin, rc::Rc, sync::Arc, time::Duration,
};
use tokio::sync::watch;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

type Body = UnsyncBoxBody<Bytes, hyper::Error>;
type Io = TokioIo<Compat<Pin<Box<AsyncStream<TcpStream>>>>>;
fn io(stream: TcpStream) -> Io {
    TokioIo::new(Box::pin(AsyncStream::new(stream)).compat())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListenerState {
    pub bound: Option<SocketAddr>,
    pub error: Option<String>,
}

pub async fn run(
    mut config: watch::Receiver<Arc<RoutingTable>>,
    state: watch::Sender<ListenerState>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let cursor = Rc::new(Cell::new(0usize));
    let active = Rc::new(Cell::new(0usize));
    let mut listener: Option<TcpListener> = None;
    let mut bound = None;
    loop {
        let desired = config.borrow_and_update().bind;
        if desired != bound {
            listener = None;
            bound = None;
            if let Some(address) = desired {
                match TcpListener::bind(address).await {
                    Ok(socket) => {
                        tracing::info!(%address, "compio HTTP listener ready");
                        let actual = socket.local_addr()?;
                        listener = Some(socket);
                        bound = Some(address);
                        state.send_replace(ListenerState {
                            bound: Some(actual),
                            error: None,
                        });
                    }
                    Err(error) => {
                        tracing::error!(%address, %error, "HTTP listener bind failed; retrying");
                        state.send_replace(ListenerState {
                            bound: None,
                            error: Some(error.to_string()),
                        });
                    }
                }
            } else {
                state.send_replace(ListenerState::default());
            }
        }
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = shutdown.changed() => break,
            changed = config.changed() => { if changed.is_err() { break; } },
            _ = runtime::time::sleep(Duration::from_millis(250)), if listener.is_none() => {},
            accepted = async { listener.as_ref().unwrap().accept().await }, if listener.is_some() => {
                let (stream, peer) = accepted?;
                // A small demo limit also bounds idle downstream connections.
                if active.get() >= 1024 { drop(stream); continue; }
                active.set(active.get() + 1);
                let config = config.clone(); let cursor = cursor.clone(); let active = active.clone();
                let mut shutdown = shutdown.clone();
                runtime::spawn(async move {
                    let service = service_fn(move |request| proxy(request, config.clone(), cursor.clone()));
                    let connection = hyper::server::conn::http1::Builder::new()
                        .max_headers(100).max_buf_size(32 * 1024).serve_connection(io(stream), service);
                    tokio::pin!(connection);
                    tokio::select! {
                        result = &mut connection => { if let Err(error) = result { tracing::debug!(%peer, %error, "HTTP connection ended"); } },
                        _ = shutdown.changed() => {
                            connection.as_mut().graceful_shutdown();
                            let _ = timeout(Duration::from_secs(5), connection).await;
                        }
                    }
                    active.set(active.get() - 1);
                }).detach();
            }
        }
    }
    drop(listener);
    // Connection tasks observe shutdown concurrently; cap the total drain time.
    let _ = timeout(Duration::from_secs(6), async {
        while active.get() > 0 {
            runtime::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    state.send_replace(ListenerState::default());
    Ok(())
}

async fn proxy(
    mut request: Request<Incoming>,
    config: watch::Receiver<Arc<RoutingTable>>,
    cursor: Rc<Cell<usize>>,
) -> Result<Response<Body>, Infallible> {
    if request.method() == http::Method::CONNECT || request.headers().contains_key(header::UPGRADE)
    {
        return Ok(reply(
            StatusCode::NOT_IMPLEMENTED,
            "HTTP upgrades and CONNECT are unsupported\n",
        ));
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<http::uri::Authority>().ok())
        .map(|h| h.host().to_ascii_lowercase());
    let Some(host) = host else {
        return Ok(reply(
            StatusCode::BAD_REQUEST,
            "a valid Host header is required\n",
        ));
    };
    let snapshot = config.borrow().clone();
    let Some(route) = snapshot.find(
        &host,
        request.uri().path(),
        request.method(),
        request.headers(),
    ) else {
        return Ok(reply(StatusCode::NOT_FOUND, "no matching HTTPRoute\n"));
    };
    if route.invalid_backend {
        return Ok(reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            "HTTPRoute has an invalid backend reference\n",
        ));
    }
    if route.endpoints.is_empty() {
        return Ok(reply(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service has no ready endpoints\n",
        ));
    }
    let index = cursor.get();
    cursor.set(index.wrapping_add(1));
    let endpoint = route.endpoints[index % route.endpoints.len()];
    // Preserve Host, path, query, method and streaming body. Strip proxy connection metadata.
    strip_hop_headers(request.headers_mut());
    // Framing is regenerated by hyper from Incoming's size hint and frames.
    *request.version_mut() = Version::HTTP_11;
    let path = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    *request.uri_mut() = path.parse::<Uri>().unwrap();
    let response = timeout(Duration::from_secs(10), async {
        let stream = TcpStream::connect(endpoint).await?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io(stream)).await?;
        // The response body owns a cancellation guard, so downstream cancellation
        // also drops the upstream driver (including an endless SSE response).
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "upstream connection ended");
            }
        });
        let response = sender.send_request(request).await?;
        Ok::<_, anyhow::Error>((response, driver))
    })
    .await;
    match response {
        Ok(Ok((response, driver))) => {
            let (mut parts, body) = response.into_parts();
            strip_hop_headers(&mut parts.headers);
            let body = DrivenBody {
                body,
                _driver: driver,
            };
            Ok(Response::from_parts(parts, body.boxed_unsync()))
        }
        Ok(Err(error)) => {
            tracing::warn!(%endpoint, %error, "HTTP backend request failed");
            Ok(reply(StatusCode::BAD_GATEWAY, "backend request failed\n"))
        }
        Err(_) => Ok(reply(
            StatusCode::GATEWAY_TIMEOUT,
            "backend response headers timed out\n",
        )),
    }
}

struct DrivenBody {
    body: Incoming,
    _driver: runtime::JoinHandle<()>,
}
impl hyper::body::Body for DrivenBody {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub fn strip_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|s| s.trim().parse::<header::HeaderName>().ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn reply(status: StatusCode, text: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(
            Full::new(Bytes::from_static(text.as_bytes()))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .unwrap()
}
