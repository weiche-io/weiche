//! worker-local service composition; keep-alive requests borrow the current view.
use super::{
    cluster::Clusters,
    pool::{ConnectionPool, Endpoint},
    router::RouteAction,
    runtime_config::RuntimeConfig,
    snapshot::{PublishedSnapshot, Revision},
    upstream::UpstreamClient,
};
use bytes::Bytes;
use compio::runtime::time::timeout;
use http::{HeaderMap, Request, Response, StatusCode, Uri, Version, header};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use std::{
    cell::RefCell, convert::Infallible, net::SocketAddr, pin::Pin, rc::Rc, sync::Arc,
    time::Duration,
};

// streaming response leases stay on the owning compio worker.
pub type Body = Pin<Box<dyn hyper::body::Body<Data = Bytes, Error = hyper::Error>>>;

struct WorkerView {
    snapshot: Arc<PublishedSnapshot>,
    clusters: Clusters,
}

impl WorkerView {
    fn prepare(snapshot: Arc<PublishedSnapshot>, previous: &Clusters) -> anyhow::Result<Self> {
        let gateway = &snapshot.gateway;
        anyhow::ensure!(
            !gateway.forwarding.response_header_timeout.is_zero(),
            "response header timeout must be positive"
        );
        for route in gateway.router.routes() {
            if let RouteAction::Forward(id) = &route.action {
                anyhow::ensure!(
                    gateway.clusters.contains_key(id),
                    "route references missing cluster {id:?}"
                );
            }
        }
        let clusters = Clusters::prepare(&gateway.clusters, previous);
        Ok(Self { snapshot, clusters })
    }
}

pub struct ProxyService {
    view: RefCell<WorkerView>,
    pool: Rc<ConnectionPool>,
    upstream: UpstreamClient,
}

impl ProxyService {
    pub fn new(snapshot: Arc<PublishedSnapshot>, runtime: &RuntimeConfig) -> anyhow::Result<Self> {
        let view = WorkerView::prepare(snapshot, &Clusters::default())?;
        // the 2026-11-12 benchmark exhausted ephemeral ports without connection reuse.
        let pool = ConnectionPool::new(
            runtime.upstream_connections_per_worker.get(),
            runtime.upstream_idle_timeout,
        );
        let service = Self {
            view: RefCell::new(view),
            upstream: UpstreamClient::new(pool.clone()),
            pool,
        };
        service.reconcile_pool(&service.view.borrow());
        Ok(service)
    }

    fn reconcile_pool(&self, view: &WorkerView) {
        let endpoints = if view.snapshot.gateway.listener.is_some() {
            view.clusters.endpoints()
        } else {
            Default::default()
        };
        self.pool.reconcile(&endpoints);
    }

    pub fn apply(&self, snapshot: Arc<PublishedSnapshot>) -> anyhow::Result<Revision> {
        if self.view.borrow().snapshot.revision == snapshot.revision {
            return Ok(snapshot.revision);
        }
        // preparation cannot mutate the live view or retire its connections.
        let next = WorkerView::prepare(snapshot, &self.view.borrow().clusters)?;
        let revision = next.snapshot.revision;
        // commit has no await or fallible operation; requests observe one complete local view.
        self.reconcile_pool(&next);
        *self.view.borrow_mut() = next;
        Ok(revision)
    }

    pub fn listener(&self) -> Option<SocketAddr> {
        self.view.borrow().snapshot.gateway.listener
    }
    pub fn close(&self) {
        self.pool.close();
    }

    fn select(&self, request: &Request<Incoming>) -> Result<(Rc<Endpoint>, Duration), ProxyError> {
        if request.method() == http::Method::CONNECT
            || request.headers().contains_key(header::UPGRADE)
        {
            return Err(ProxyError::UnsupportedUpgrade);
        }
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<http::uri::Authority>().ok())
            .map(|h| h.host().to_ascii_lowercase())
            .ok_or(ProxyError::InvalidHost)?;
        // no shared locks or Arc clones on requests; release this borrow before I/O.
        let view = self.view.borrow();
        if view.snapshot.gateway.listener.is_none() {
            return Err(ProxyError::NoRoute);
        }
        let action = view
            .snapshot
            .gateway
            .router
            .find(
                &host,
                request.uri().path(),
                request.method(),
                request.headers(),
            )
            .ok_or(ProxyError::NoRoute)?;
        let id = match action {
            RouteAction::Forward(id) => id,
            RouteAction::RejectInvalidBackend => return Err(ProxyError::InvalidBackend),
        };
        let address = view.clusters.select(id).ok_or(ProxyError::NoEndpoints)?;
        let endpoint = self.pool.endpoint(address).ok_or(ProxyError::NoEndpoints)?;
        Ok((
            endpoint,
            view.snapshot.gateway.forwarding.response_header_timeout,
        ))
    }

    pub async fn call(&self, mut request: Request<Incoming>) -> Result<Response<Body>, Infallible> {
        let (endpoint, deadline) = match self.select(&request) {
            Ok(selected) => selected,
            Err(error) => return Ok(error.response()),
        };
        strip_hop_headers(request.headers_mut());
        // preserve Host, method, path/query and streaming body; hyper regenerates framing.
        *request.version_mut() = Version::HTTP_11;
        let path = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        *request.uri_mut() = path.parse::<Uri>().unwrap();
        let address = endpoint.address;
        match timeout(deadline, self.upstream.send(endpoint, request)).await {
            Ok(Ok(response)) => {
                let (mut parts, body) = response.into_parts();
                strip_hop_headers(&mut parts.headers);
                Ok(Response::from_parts(parts, Box::pin(body) as Body))
            }
            Ok(Err(error)) => {
                tracing::warn!(%address, %error, "http backend request failed");
                Ok(ProxyError::Upstream.response())
            }
            Err(_) => Ok(ProxyError::HeaderTimeout.response()),
        }
    }
}

enum ProxyError {
    UnsupportedUpgrade,
    InvalidHost,
    NoRoute,
    InvalidBackend,
    NoEndpoints,
    Upstream,
    HeaderTimeout,
}

impl ProxyError {
    fn response(self) -> Response<Body> {
        let (status, text) = match self {
            Self::UnsupportedUpgrade => (
                StatusCode::NOT_IMPLEMENTED,
                "HTTP upgrades and CONNECT are unsupported\n",
            ),
            Self::InvalidHost => (StatusCode::BAD_REQUEST, "a valid Host header is required\n"),
            Self::NoRoute => (StatusCode::NOT_FOUND, "no matching HTTPRoute\n"),
            Self::InvalidBackend => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "HTTPRoute has an invalid backend reference\n",
            ),
            Self::NoEndpoints => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Service has no ready endpoints\n",
            ),
            Self::Upstream => (StatusCode::BAD_GATEWAY, "backend request failed\n"),
            Self::HeaderTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "backend response headers timed out\n",
            ),
        };
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Box::pin(
                Full::new(Bytes::from_static(text.as_bytes()))
                    .map_err(|never| -> hyper::Error { match never {} }),
            ) as Body)
            .unwrap()
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
