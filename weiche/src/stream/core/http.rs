//! worker-local service composition; keep-alive requests borrow the current view.
mod cors;
mod filter;
mod router;

use super::{
    super::{
        super::{
            ir::{
                Backend, Binding, Filters, HeaderChange, PathMatch, Protocol,
                PublishedSnapshot, Revision, RouteAction,
            },
            rtconf::RuntimeConfig,
        },
        up::{ConnectionPool, Deadlines, Tunnel, Upgrade, UpstreamClient},
    },
    Cluster, EndpointKey, tcp,
};
use bytes::Bytes;
use http::{
    HeaderMap, HeaderValue, Request, Response, StatusCode, Uri, Version, header,
    uri::Authority,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use router::{Hosts, Router};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

/// streaming response leases stay on the owning compio worker.
pub type Body = Pin<Box<dyn hyper::body::Body<Data = Bytes, Error = std::io::Error>>>;

/// routing state installed atomically on one worker.
struct WorkerView {
    /// active snapshot revision.
    revision: Revision,
    /// gateway default for receiving backend headers.
    deadline: Duration,
    /// routers indexed by downstream listening address.
    routes: BTreeMap<SocketAddr, tcp::Route<Hosts<Router>>>,
}

impl WorkerView {
    /// compile immutable rules and reuse cluster selection state.
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
            .flat_map(|socket| socket.target.values())
            .flat_map(Router::routes)
            .flat_map(|route| &route.action.backends)
            .filter_map(|backend| backend.cluster.as_ref().ok())
            .map(|c| (c.spec.clone(), c.clone()))
            .collect();
        let mut endpoints = HashSet::new();
        let mut routes = BTreeMap::new();
        for (address, socket) in &gateway.sockets {
            let mut prepare = |action: &RouteAction| {
                let backends = action
                    .backends
                    .iter()
                    .map(|backend| Backend {
                        weight: backend.weight,
                        filters: backend.filters.clone(),
                        cluster: backend
                            .cluster
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
                    })
                    .collect();
                RouteAction {
                    backends,
                    filters: action.filters.clone(),
                    timeouts: action.timeouts,
                }
            };
            let mut by_listener: BTreeMap<_, Vec<_>> =
                socket.hostnames.iter().map(|h| (h.as_str(), Vec::new())).collect();
            for route in &socket.routes {
                let listener = route.host.as_ref().map_or("", |h| h.listener.as_str());
                by_listener.entry(listener).or_default().push(route);
            }
            let mut listeners = Vec::new();
            for (hostname, rules) in by_listener {
                let router = Router::compile(rules, &mut prepare)?;
                listeners.push((hostname.to_owned(), router));
            }
            routes.insert(
                *address,
                tcp::Route {
                    binding: socket.listener.binding(),
                    target: listeners.into_iter().collect(),
                },
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
    /// current route snapshot for new requests.
    view: RefCell<WorkerView>,
    /// pool reconciled when the active snapshot changes.
    pool: Rc<ConnectionPool>,
    /// client dispatching on the worker runtime.
    upstream: UpstreamClient,
}

impl ProxyService {
    /// initialize a service and its active endpoint generations.
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

    /// publish the snapshot and retire endpoints removed by the controller.
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

    /// close the worker pool during shutdown.
    pub fn close(&self) {
        self.pool.close();
    }

    /// select the route, backend and CORS policy against the original request.
    fn select(
        &self,
        listener: &Binding,
        authority: &Authority,
        request: &Request<Incoming>,
        start: Instant,
        grpc: bool,
    ) -> Result<Selection, ProxyError> {
        let view = self.view.borrow();
        let hostname = authority.host().to_ascii_lowercase();
        let router = view
            .routes
            .get(&listener.address)
            .and_then(|r| r.find(listener))
            .and_then(|hosts| hosts.matching(&hostname).next())
            .ok_or(ProxyError::NoRoute)?;
        let (route, backend) = router
            .find(&hostname, request.uri(), request.method(), request.headers(), grpc)
            .ok_or(ProxyError::NoRoute)?;
        let action = &route.action;
        let cors = backend
            .and_then(|b| b.filters.cors.as_ref())
            .or(action.filters.cors.as_ref())
            .map(|policy| {
                cors::Decision::new(policy, request.method(), request.headers())
            });
        let backend = backend
            .and_then(|b| Some((b.cluster.as_ref().ok()?.clone(), b.filters.clone())));
        Ok(Selection {
            grpc: matches!(route.http.path, PathMatch::Grpc { .. }),
            rule: action.filters.clone(),
            backend,
            cors,
            budget: Deadlines::new(start, action.timeouts, view.deadline),
        })
    }

    /// route the original request and apply response filters to the selected response.
    pub async fn call(
        &self,
        listener: &Binding,
        peer: SocketAddr,
        mut request: Request<Incoming>,
    ) -> (Response<Body>, Option<Tunnel>) {
        let start = Instant::now();
        let version = request.version();
        if request.method() == http::Method::CONNECT {
            return (ProxyError::UnsupportedUpgrade.response(), None);
        }
        let authority = match normalize_authority(&mut request) {
            Ok(authority) => authority,
            Err(error) => return (error.response(), None),
        };
        let grpc = is_grpc(&request);
        let selection = match self.select(listener, &authority, &request, start, grpc) {
            Ok(s) => s,
            Err(error) => return (error.for_request(grpc), None),
        };
        let (mut response, tunnel) =
            if selection.cors.as_ref().is_some_and(|c| c.preflight) {
                let mut response = Response::new(full_body(Bytes::new()));
                *response.status_mut() = StatusCode::NO_CONTENT;
                (response, None)
            } else {
                self.forward(listener, peer, request, &authority, &selection)
                    .await
                    .unwrap_or_else(|error| (error.for_request(selection.grpc), None))
            };
        *response.version_mut() = version;
        let headers = response.headers_mut();
        let backend_changes = selection
            .backend
            .as_ref()
            .map_or(&[][..], |(_, filters)| filters.response_headers.as_slice());
        if !backend_changes.is_empty() || !selection.rule.response_headers.is_empty() {
            filter::modify(headers, backend_changes);
            filter::modify(headers, &selection.rule.response_headers);
            strip_hop_headers(headers);
        }
        if response.version() != Version::HTTP_2
            && response.headers().contains_key(header::TRAILER)
        {
            response.headers_mut().remove(header::CONTENT_LENGTH);
        }
        if let Some(tunnel) = &tunnel {
            tunnel.restore(response.headers_mut());
        }
        if let Some(cors) = selection.cors {
            cors.apply(response.headers_mut());
        }
        (response, tunnel)
    }

    /// apply the selected filters and forward on the chosen endpoint generation.
    async fn forward(
        &self,
        listener: &Binding,
        peer: SocketAddr,
        mut request: Request<Incoming>,
        original: &Authority,
        selection: &Selection,
    ) -> Result<(Response<Body>, Option<Tunnel>), ProxyError> {
        let rule = &selection.rule;
        let backend = selection.backend.as_ref();
        if let Some(r) = rule.redirect.as_ref().or_else(|| backend?.1.redirect.as_ref()) {
            return Err(ProxyError::Redirect(
                r.status,
                filter::location(r, listener, original, request.uri())?,
            ));
        }
        let (cluster, filters) = backend.ok_or(ProxyError::InvalidBackend)?;
        let key = cluster.select().ok_or(ProxyError::NoEndpoints)?;
        let endpoint = self.pool.endpoint(key).ok_or(ProxyError::NoEndpoints)?;
        let upgrade = if request.headers().contains_key(header::UPGRADE) {
            if endpoint.key.protocol != Protocol::Http1 {
                return Err(ProxyError::UnsupportedUpgrade);
            }
            Some(Upgrade::request(&mut request).ok_or(ProxyError::UnsupportedUpgrade)?)
        } else {
            None
        };
        let (authority, trailers) = filter::request_headers(
            request.headers_mut(),
            peer,
            original,
            listener.tls,
            rule,
            filters,
        )?;
        let path = filters
            .path
            .as_ref()
            .or(rule.path.as_ref())
            .map(|p| filter::apply(p, request.uri()))
            .transpose()?;
        prepare_request(
            &mut request,
            authority,
            &endpoint.key,
            listener.tls,
            selection.grpc,
            trailers,
            path,
        )?;
        if let Some(upgrade) = &upgrade {
            upgrade.restore(request.headers_mut());
        }
        let address = endpoint.key.address;
        let exchange = self
            .upstream
            .send(endpoint, request, upgrade, selection.budget)
            .await
            .map_err(|error| {
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::TimedOut)
                {
                    ProxyError::Timeout
                } else {
                    tracing::warn!(%address, %error, "http backend request failed");
                    ProxyError::Upstream
                }
            })?;
        let (mut parts, body) = exchange.response.into_parts();
        strip_hop_headers(&mut parts.headers);
        Ok((Response::from_parts(parts, Box::pin(body)), exchange.tunnel))
    }
}

/// routing decisions retained across asynchronous admission and forwarding.
struct Selection {
    /// whether the selected route uses native gRPC forwarding and errors.
    grpc: bool,
    /// listener rule and its response filters.
    rule: Arc<Filters>,
    /// selected target and backend-specific filters.
    backend: Option<(Rc<Cluster>, Arc<Filters>)>,
    /// cross-origin decision made before request filters or upstream forwarding.
    cors: Option<cors::Decision>,
    /// request and attempt deadlines.
    budget: Deadlines,
}

/// errors mapped to local HTTP responses.
enum ProxyError {
    /// local redirect with its validated Location.
    Redirect(StatusCode, HeaderValue),
    /// request URI cannot be represented for the selected hop.
    InvalidTarget,
    /// unsupported tunnel method or handshake.
    UnsupportedUpgrade,
    /// missing, ambiguous or malformed request authority.
    InvalidHost,
    /// no attached route matches the request.
    NoRoute,
    /// selected reference has no valid backend.
    InvalidBackend,
    /// backend has no available addresses.
    NoEndpoints,
    /// backend connection or request failed.
    Upstream,
    /// the request or backend attempt expired before headers.
    Timeout,
}

/// recognize native gRPC media types with an optional codec suffix and parameters.
fn is_grpc<B>(request: &Request<B>) -> bool {
    request.version() == Version::HTTP_2
        && request.method() == http::Method::POST
        && request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                let media = v.split(';').next().unwrap_or("").trim();
                let base = media.split_once('+').map_or(media, |(base, _)| base);
                base.eq_ignore_ascii_case("application/grpc")
            })
}

impl ProxyError {
    /// emit a trailers-only gRPC response when a call cannot be dispatched.
    fn for_request(self, grpc: bool) -> Response<Body> {
        if grpc {
            let code = match self {
                Self::NoRoute => "12",
                Self::InvalidBackend | Self::NoEndpoints | Self::Upstream => "14",
                Self::Timeout => "4",
                _ => return self.response(),
            };
            let mut response = Response::new(full_body(Bytes::new()));
            *response.version_mut() = Version::HTTP_2;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            );
            response
                .headers_mut()
                .insert("grpc-status", HeaderValue::from_static(code));
            return response;
        }
        self.response()
    }

    /// format a local response with the corresponding HTTP status.
    fn response(self) -> Response<Body> {
        let (status, text) = match self {
            Self::Redirect(status, location) => {
                let mut response = Response::new(full_body(Bytes::new()));
                *response.status_mut() = status;
                response.headers_mut().insert(header::LOCATION, location);
                return response;
            }
            Self::InvalidTarget => (StatusCode::BAD_REQUEST, "invalid request target\n"),
            Self::UnsupportedUpgrade => {
                (StatusCode::NOT_IMPLEMENTED, "unsupported upgrade or CONNECT request\n")
            }
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
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "request timed out\n"),
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
    Box::pin(Full::new(bytes).map_err(|never| -> std::io::Error { never }))
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

/// encode the filtered authority and path for the selected upstream protocol.
fn prepare_request(
    request: &mut Request<Incoming>,
    authority: Authority,
    endpoint: &EndpointKey,
    downstream_tls: bool,
    grpc: bool,
    trailers: bool,
    path: Option<http::uri::PathAndQuery>,
) -> Result<(), ProxyError> {
    if trailers || grpc {
        request
            .headers_mut()
            .insert(header::TE, http::HeaderValue::from_static("trailers"));
    }
    let mut uri = request.uri().clone().into_parts();
    if path.is_some() {
        uri.path_and_query = path;
    }
    uri.path_and_query
        .get_or_insert_with(|| http::uri::PathAndQuery::from_static("/"));
    match endpoint.protocol {
        Protocol::Http1 => {
            *request.version_mut() = Version::HTTP_11;
            if !request.headers().contains_key(header::HOST) {
                request.headers_mut().insert(
                    header::HOST,
                    HeaderValue::from_str(authority.as_str())
                        .map_err(|_| ProxyError::InvalidHost)?,
                );
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
            if grpc || uri.scheme.is_none() {
                let tls = if grpc { endpoint.tls.is_some() } else { downstream_tls };
                uri.scheme = Some(if tls {
                    http::uri::Scheme::HTTPS
                } else {
                    http::uri::Scheme::HTTP
                });
            }
            uri.authority = Some(authority);
        }
    }
    *request.uri_mut() = Uri::from_parts(uri).map_err(|_| ProxyError::InvalidTarget)?;
    Ok(())
}

/// remove connection-specific fields and fields nominated by this hop.
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
