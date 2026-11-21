//! worker-local service composition; keep-alive requests borrow the current view.
mod router;

use super::{
    super::{
        super::{
            ir::{
                Binding, HeaderChange, Protocol, PublishedSnapshot, Revision, Route,
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
    HeaderMap, HeaderValue, Request, Response, StatusCode, Uri, Version, header,
    uri::Authority,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use router::Router;
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
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
    routes: BTreeMap<SocketAddr, tcp::Route<Router>>,
}

impl WorkerView {
    #[expect(
        clippy::mutable_key_type,
        reason = "TLS configuration identity hashes only immutable certificate and protocol data"
    )]
    fn prepare(
        snapshot: &PublishedSnapshot,
        previous: Option<&WorkerView>,
    ) -> anyhow::Result<(Self, HashSet<EndpointKey>)> {
        let gateway = &snapshot.gateway;
        anyhow::ensure!(
            !gateway.forwarding.response_header_timeout.is_zero(),
            "response header timeout must be positive"
        );
        let mut clusters: HashMap<_, _> = previous
            .into_iter()
            .flat_map(|view| view.routes.values())
            .flat_map(|socket| socket.target.routes())
            .flat_map(|route| &route.action.backends)
            .filter_map(|(_, target)| target.as_ref().ok())
            .map(|c| (c.spec.clone(), c.clone()))
            .collect();
        let mut endpoints = HashSet::new();
        let mut routes = BTreeMap::new();
        for (address, socket) in &gateway.sockets {
            let router = Router::compile(socket.routes.iter().map(|route| {
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
                                    endpoints.extend(spec.endpoints.iter().map(
                                        |&address| EndpointKey {
                                            address,
                                            protocol: spec.protocol,
                                            tls: spec.tls.clone(),
                                        },
                                    ));
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
            routes.insert(
                *address,
                tcp::Route { binding: socket.listener.binding(), target: router },
            );
        }
        Ok((
            Self {
                revision: snapshot.revision,
                deadline: gateway.forwarding.response_header_timeout,
                routes,
            },
            endpoints,
        ))
    }
}

/// worker-local HTTP routing and upstream forwarding service.
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
            WorkerView::prepare(&snapshot, Some(&self.view.borrow()))?;
        // commit has no await or fallible operation; requests observe one complete local view.
        self.pool.reconcile(&endpoints);
        *self.view.borrow_mut() = next;
        Ok(snapshot.revision)
    }

    pub fn close(&self) {
        self.pool.close();
    }

    fn route_request(
        &self,
        listener: &Binding,
        request: &mut Request<Incoming>,
    ) -> Result<(Rc<Endpoint>, Duration), ProxyError> {
        let authority = normalize_authority(request)?;
        if request.method() == http::Method::CONNECT
            || request.headers().contains_key(header::UPGRADE)
        {
            return Err(ProxyError::UnsupportedUpgrade);
        }
        let host = authority.host().to_ascii_lowercase();
        // release the worker-local routing view before starting I/O.
        let view = self.view.borrow();
        let router = view
            .routes
            .get(&listener.address)
            .and_then(|route| route.find(listener))
            .ok_or(ProxyError::NoRoute)?;
        let action = router
            .find(&host, request.uri().path(), request.method(), request.headers())
            .ok_or(ProxyError::NoRoute)?;
        if let Some(redirect) = &action.redirect {
            let scheme = if listener.tls { "https" } else { "http" };
            let host = redirect.hostname.as_deref().unwrap_or_else(|| authority.host());
            let port = listener.port;
            let suffix = if (scheme == "http" && port == 80)
                || (scheme == "https" && port == 443)
            {
                String::new()
            } else {
                format!(":{port}")
            };
            let path = request.uri().path_and_query().map_or("/", |p| p.as_str());
            // an asterisk targets the whole server and has no path to append.
            let path = if path == "*" { "/" } else { path };
            let location =
                HeaderValue::try_from(format!("{scheme}://{host}{suffix}{path}"))
                    .map_err(|_| ProxyError::InvalidTarget)?;
            return Err(ProxyError::Redirect(redirect.status, location));
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
        let authority = if action.headers.is_empty() {
            authority
        } else {
            host_authority(request.headers())?
        };
        let target = super::super::super::ir::weighted(&action.backends);
        let cluster = target
            .and_then(|t| t.as_ref().ok())
            .ok_or(ProxyError::InvalidBackend)?;
        let address = cluster.select().ok_or(ProxyError::NoEndpoints)?;
        let endpoint = self.pool.endpoint(address).ok_or(ProxyError::NoEndpoints)?;
        prepare_request(request, authority, endpoint.key.protocol, listener.tls)?;
        Ok((endpoint, view.deadline))
    }

    /// routing and forwarding failures become HTTP responses rather than service errors.
    pub async fn call(
        &self,
        listener: &Binding,
        mut request: Request<Incoming>,
    ) -> Result<Response<Body>, !> {
        let downstream_version = request.version();
        let (endpoint, deadline) = match self.route_request(listener, &mut request) {
            Ok(selected) => selected,
            Err(error) => return Ok(error.response()),
        };
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
    Redirect(StatusCode, HeaderValue),
    InvalidTarget,
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
                let mut response = Response::new(full_body(Bytes::new()));
                *response.status_mut() = status;
                response.headers_mut().insert(header::LOCATION, location);
                return response;
            }
            Self::InvalidTarget => (StatusCode::BAD_REQUEST, "invalid request target\n"),
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
        let mut response = Response::new(full_body(Bytes::from_static(text.as_bytes())));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

/// build a complete local response without involving an upstream stream.
fn full_body(bytes: Bytes) -> Body {
    Box::pin(Full::new(bytes).map_err(|never| -> hyper::Error { never }))
}

/// parse the single Host field used by HTTP/1 or supplied by a route filter.
fn host_authority(headers: &HeaderMap) -> Result<Authority, ProxyError> {
    let mut hosts = headers.get_all(header::HOST).iter();
    let value = hosts.next().ok_or(ProxyError::InvalidHost)?;
    let authority =
        Authority::try_from(value.as_bytes()).map_err(|_| ProxyError::InvalidHost)?;
    let host = authority.host();
    let port = authority.as_str().strip_prefix(host).ok_or(ProxyError::InvalidHost)?;
    if hosts.next().is_some()
        || host.is_empty()
        || authority.as_str().contains(['@', ','])
        || (!port.is_empty() && (!port.starts_with(':') || authority.port().is_none()))
    {
        return Err(ProxyError::InvalidHost);
    }
    Ok(authority)
}

/// expose :authority to header matches and reject conflicting downstream identities.
fn normalize_authority(request: &mut Request<Incoming>) -> Result<Authority, ProxyError> {
    if !request.headers().contains_key(header::HOST)
        && let Some(authority) = request.uri().authority()
    {
        let value = HeaderValue::from_str(authority.as_str())
            .map_err(|_| ProxyError::InvalidHost)?;
        request.headers_mut().insert(header::HOST, value);
    }
    let host = host_authority(request.headers())?;
    if request.uri().authority().is_some_and(|authority| authority != &host) {
        return Err(ProxyError::InvalidHost);
    }
    Ok(host)
}

fn prepare_request(
    request: &mut Request<Incoming>,
    authority: Authority,
    protocol: Protocol,
    downstream_tls: bool,
) -> Result<(), ProxyError> {
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
    let mut uri = request.uri().clone().into_parts();
    uri.path_and_query
        .get_or_insert_with(|| http::uri::PathAndQuery::from_static("/"));
    match protocol {
        Protocol::Http1 => {
            *request.version_mut() = Version::HTTP_11;
            // restore the filtered Host even if Connection nominated it for removal.
            if !request.headers().contains_key(header::HOST) {
                let host = HeaderValue::from_str(authority.as_str())
                    .map_err(|_| ProxyError::InvalidHost)?;
                request.headers_mut().insert(header::HOST, host);
            }
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
    *request.uri_mut() = Uri::from_parts(uri).map_err(|_| ProxyError::InvalidTarget)?;
    Ok(())
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
