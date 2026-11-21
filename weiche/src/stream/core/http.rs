//! worker-local service composition; keep-alive requests borrow the current view.
mod router;

use super::{
    super::{
        super::{
            ir::{
                HeaderChange, Listener, Protocol, PublishedSnapshot, Revision, Route,
                RouteAction,
            },
            rtconf::RuntimeConfig,
        },
        up::{ConnectionPool, Endpoint, UpstreamClient},
    },
    Cluster, EndpointKey, tcp,
};
use bytes::Bytes;
use compio::runtime::time::timeout;
use http::{
    HeaderMap, Request, Response, StatusCode, Uri, Version, header, uri::Authority,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use router::Router;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

// streaming response leases stay on the owning compio worker.
pub type Body = Pin<Box<dyn hyper::body::Body<Data = Bytes, Error = hyper::Error>>>;

struct WorkerView {
    revision: Revision,
    deadline: Duration,
    route: Option<tcp::Route<Router>>,
}

impl WorkerView {
    #[expect(
        clippy::mutable_key_type,
        reason = "TLS configuration identity hashes only immutable certificate and protocol data"
    )]
    fn prepare(
        snapshot: &PublishedSnapshot,
        previous: Option<&tcp::Route<Router>>,
    ) -> anyhow::Result<(Self, HashSet<EndpointKey>)> {
        let gateway = &snapshot.gateway;
        anyhow::ensure!(
            !gateway.forwarding.response_header_timeout.is_zero(),
            "response header timeout must be positive"
        );
        let mut clusters: HashMap<_, _> = previous
            .into_iter()
            .flat_map(|route| route.target.routes())
            .flat_map(|route| &route.action.backends)
            .filter_map(|(_, target)| target.as_ref().ok())
            .map(|c| (c.spec.clone(), c.clone()))
            .collect();
        let mut endpoints = HashSet::new();
        let router = Router::compile(gateway.routes.iter().map(|route| {
            let backends = route
                .action
                .backends
                .iter()
                .map(|(weight, target)| {
                    (
                        *weight,
                        target
                            .as_ref()
                            .map(|spec| {
                                endpoints.extend(spec.endpoints.iter().map(|&address| {
                                    EndpointKey {
                                        address,
                                        protocol: spec.protocol,
                                        tls: spec.tls.clone(),
                                    }
                                }));
                                clusters
                                    .entry(spec.clone())
                                    .or_insert_with(|| {
                                        Rc::new(Cluster::new(spec.clone()))
                                    })
                                    .clone()
                            })
                            .map_err(|e| *e),
                    )
                })
                .collect();
            let action = RouteAction {
                backends,
                headers: route.action.headers.clone(),
                redirect: route.action.redirect.clone(),
            };
            Route {
                hostname: route.hostname.clone(),
                http: route.http.clone(),
                action,
            }
        }));
        Ok((
            Self {
                revision: snapshot.revision,
                deadline: gateway.forwarding.response_header_timeout,
                route: gateway
                    .listener
                    .clone()
                    .map(|listener| tcp::Route { listener, target: router }),
            },
            endpoints,
        ))
    }
}

pub struct ProxyService {
    view: RefCell<WorkerView>,
    pool: Rc<ConnectionPool>,
    upstream: UpstreamClient,
}

impl ProxyService {
    pub fn new(
        snapshot: Arc<PublishedSnapshot>,
        runtime: &RuntimeConfig,
    ) -> anyhow::Result<Self> {
        let (view, endpoints) = WorkerView::prepare(&snapshot, None)?;
        // the 2026-11-12 benchmark exhausted ephemeral ports without connection reuse.
        let pool = ConnectionPool::new(runtime);
        pool.reconcile(&endpoints);
        Ok(Self {
            view: RefCell::new(view),
            upstream: UpstreamClient::new(pool.clone()),
            pool,
        })
    }

    pub fn apply(&self, snapshot: Arc<PublishedSnapshot>) -> anyhow::Result<Revision> {
        if self.view.borrow().revision == snapshot.revision {
            return Ok(snapshot.revision);
        }
        // preparation cannot mutate the live view or retire its connections.
        let (next, endpoints) =
            WorkerView::prepare(&snapshot, self.view.borrow().route.as_ref())?;
        // commit has no await or fallible operation; requests observe one complete local view.
        self.pool.reconcile(&endpoints);
        *self.view.borrow_mut() = next;
        Ok(snapshot.revision)
    }

    /// capture the complete listener at TCP accept, before the first handshake poll.
    pub fn listener(&self) -> Option<Listener> {
        self.view.borrow().route.as_ref().map(|route| route.listener.clone())
    }

    pub fn close(&self) {
        self.pool.close();
    }

    fn select(
        &self,
        listener: &Listener,
        request: &mut Request<Incoming>,
        authority: &Authority,
    ) -> Result<(Rc<Endpoint>, Duration), ProxyError> {
        if request.method() == http::Method::CONNECT
            || request.headers().contains_key(header::UPGRADE)
        {
            return Err(ProxyError::UnsupportedUpgrade);
        }
        let host = authority.host().to_ascii_lowercase();
        // release the worker-local routing view before starting I/O.
        let view = self.view.borrow();
        let router = view
            .route
            .as_ref()
            .and_then(|route| route.find(listener))
            .ok_or(ProxyError::NoRoute)?;
        let action = router
            .find(&host, request.uri().path(), request.method(), request.headers())
            .ok_or(ProxyError::NoRoute)?;
        if let Some((status, hostname)) = &action.redirect {
            let scheme = if listener.tls.is_some() { "https" } else { "http" };
            let host = hostname.as_deref().unwrap_or_else(|| authority.host());
            let port = listener.address.port();
            let suffix = if (scheme == "http" && port == 80)
                || (scheme == "https" && port == 443)
            {
                String::new()
            } else {
                format!(":{port}")
            };
            let path = request.uri().path_and_query().map_or("/", |p| p.as_str());
            return Err(ProxyError::Redirect(
                *status,
                format!("{scheme}://{host}{suffix}{path}"),
            ));
        }
        for change in &action.headers {
            match change {
                HeaderChange::Remove(k) => {
                    request.headers_mut().remove(k);
                }
                HeaderChange::Add(k, v) => {
                    request.headers_mut().append(k, v.clone());
                }
                HeaderChange::Set(k, v) => {
                    request.headers_mut().insert(k, v.clone());
                }
            }
        }
        let target = match action.backends.as_slice() {
            [(w, target)] if *w > 0 => Some(target),
            weights => {
                let total: u64 = weights.iter().map(|(w, _)| u64::from(*w)).sum();
                if total == 0 {
                    return Err(ProxyError::InvalidBackend);
                }
                let mut slot = fastrand::u64(0..total);
                weights.iter().find_map(|(w, target)| {
                    if slot < u64::from(*w) {
                        Some(target)
                    } else {
                        slot -= u64::from(*w);
                        None
                    }
                })
            }
        };
        let cluster = target
            .and_then(|t| t.as_ref().ok())
            .ok_or(ProxyError::InvalidBackend)?;
        let address = cluster.select().ok_or(ProxyError::NoEndpoints)?;
        let endpoint = self.pool.endpoint(address).ok_or(ProxyError::NoEndpoints)?;
        Ok((endpoint, view.deadline))
    }

    /// routing and forwarding failures become HTTP responses rather than service errors.
    pub async fn call(
        &self,
        listener: &Listener,
        mut request: Request<Incoming>,
    ) -> Result<Response<Body>, !> {
        let authority = match normalize_authority(&mut request) {
            Ok(authority) => authority,
            Err(error) => return Ok(error.response()),
        };
        let (endpoint, deadline) = match self.select(listener, &mut request, &authority) {
            Ok(selected) => selected,
            Err(error) => return Ok(error.response()),
        };
        let downstream_version = request.version();
        prepare_request(
            &mut request,
            authority,
            endpoint.key.protocol,
            listener.tls.is_some(),
        );
        let address = endpoint.key.address;
        match timeout(deadline, self.upstream.send(endpoint, request)).await {
            Ok(Ok(response)) => {
                let (mut parts, body) = response.into_parts();
                strip_hop_headers(&mut parts.headers);
                parts.version = downstream_version;
                if downstream_version != Version::HTTP_2
                    && parts.headers.contains_key(header::TRAILER)
                {
                    parts.headers.remove(header::CONTENT_LENGTH);
                }
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
    Redirect(StatusCode, String),
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
            Self::Redirect(status, location) => {
                return Response::builder()
                    .status(status)
                    .header(header::LOCATION, location)
                    .body(Box::pin(
                        Full::new(Bytes::new())
                            .map_err(|never| -> hyper::Error { never }),
                    ) as Body)
                    .unwrap();
            }
            Self::UnsupportedUpgrade => (
                StatusCode::NOT_IMPLEMENTED,
                "HTTP upgrades and CONNECT are unsupported\n",
            ),
            Self::InvalidHost => (
                StatusCode::BAD_REQUEST,
                "a valid, consistent Host or :authority is required\n",
            ),
            Self::NoRoute => (StatusCode::NOT_FOUND, "no matching HTTPRoute\n"),
            Self::InvalidBackend => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "HTTPRoute has an invalid backend reference\n",
            ),
            Self::NoEndpoints => {
                (StatusCode::SERVICE_UNAVAILABLE, "Service has no ready endpoints\n")
            }
            Self::Upstream => (StatusCode::BAD_GATEWAY, "backend request failed\n"),
            Self::HeaderTimeout => {
                (StatusCode::GATEWAY_TIMEOUT, "backend response headers timed out\n")
            }
        };
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Box::pin(
                Full::new(Bytes::from_static(text.as_bytes()))
                    .map_err(|never| -> hyper::Error { never }),
            ) as Body)
            .unwrap()
    }
}

fn normalize_authority(request: &mut Request<Incoming>) -> Result<Authority, ProxyError> {
    let mut hosts = request.headers().get_all(header::HOST).iter();
    let host = hosts
        .next()
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|host| host.parse::<Authority>().ok())
                .ok_or(ProxyError::InvalidHost)
        })
        .transpose()?;
    if hosts.next().is_some() {
        return Err(ProxyError::InvalidHost);
    }
    let authority = match (request.uri().authority(), host) {
        (Some(authority), host) => {
            if host.as_ref().is_some_and(|host| host != authority) {
                return Err(ProxyError::InvalidHost);
            }
            authority.clone()
        }
        (None, Some(host)) => host,
        (None, None) => return Err(ProxyError::InvalidHost),
    };
    if authority.host().is_empty() || authority.as_str().contains('@') {
        return Err(ProxyError::InvalidHost);
    }
    // expose the same Host field to HTTPRoute header matches for both protocols.
    request
        .headers_mut()
        .entry(header::HOST)
        .or_insert_with(|| authority.as_str().parse().unwrap());
    Ok(authority)
}

fn prepare_request(
    request: &mut Request<Incoming>,
    authority: Authority,
    protocol: Protocol,
    downstream_tls: bool,
) {
    let trailers = request
        .headers()
        .get_all(header::TE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case("trailers"));
    strip_hop_headers(request.headers_mut());
    if trailers {
        request
            .headers_mut()
            .insert(header::TE, http::HeaderValue::from_static("trailers"));
    }
    // the route and both wire formats use the same authority; never substitute the backend IP.
    request
        .headers_mut()
        .insert(header::HOST, authority.as_str().parse().unwrap());
    let mut uri = request.uri().clone().into_parts();
    uri.path_and_query.get_or_insert_with(|| "/".parse().unwrap());
    match protocol {
        Protocol::Http1 => {
            *request.version_mut() = Version::HTTP_11;
            uri.scheme = None;
            uri.authority = None;
            if trailers {
                request
                    .headers_mut()
                    .insert(header::CONNECTION, http::HeaderValue::from_static("TE"));
            }
            if request.headers().contains_key(header::TRAILER) {
                request.headers_mut().remove(header::CONTENT_LENGTH);
                request.headers_mut().insert(
                    header::TRANSFER_ENCODING,
                    http::HeaderValue::from_static("chunked"),
                );
            }
        }
        Protocol::Http2 => {
            *request.version_mut() = Version::HTTP_2;
            // encode authority once; some HTTP/2 servers reject a duplicate Host field.
            request.headers_mut().remove(header::HOST);
            uri.scheme.get_or_insert(if downstream_tls {
                http::uri::Scheme::HTTPS
            } else {
                http::uri::Scheme::HTTP
            });
            uri.authority = Some(authority);
        }
    }
    *request.uri_mut() = Uri::from_parts(uri).unwrap();
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
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
