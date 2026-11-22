//! worker-local service composition; keep-alive requests borrow the current view.
mod body;
mod cors;
mod filter;
mod router;
pub mod server;
pub mod wire;

use super::{
    super::ir::{Backend, Binding, PublishedSnapshot, Revision, RouteAction},
    admission::{Cluster, EndpointKey, Route},
    config::RuntimeConfig,
    up::{ConnectionPool, Exchange, UpstreamClient},
};
pub use body::{RequestBody, ResponseBody};
use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, Version, header, uri::Authority};
use router::{Hosts, Router, Selection};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use wire::{
    ProxyError, is_grpc, normalize_authority, prepare_request, strip_hop_headers,
};

/// routing state installed atomically on one worker.
struct WorkerView {
    /// active snapshot revision.
    revision: Revision,
    /// gateway default for receiving backend headers.
    deadline: Duration,
    /// routers indexed by downstream listening address.
    routes: BTreeMap<SocketAddr, Route<Hosts<Router>>>,
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
                Route {
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
    /// alternative services for the HTTPS sockets with a bound QUIC endpoint.
    advertised: RefCell<BTreeMap<SocketAddr, (Rc<Binding>, HeaderValue)>>,
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
            advertised: RefCell::default(),
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

    /// publish or withdraw the default alternative for one bound QUIC endpoint.
    pub fn set_http3(&self, binding: &Rc<Binding>, port: Option<u16>) {
        let mut active = self.advertised.borrow_mut();
        if let Some(port) = port {
            let value =
                format!("h3=\":{port}\"; ma=86400").parse().expect("numeric port");
            active.insert(binding.address, (binding.clone(), value));
        } else {
            active.remove(&binding.address);
        }
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
        request: &Request<RequestBody>,
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
        router
            .find(&hostname, request, grpc, start, view.deadline)
            .ok_or(ProxyError::NoRoute)
    }

    /// route the original request and apply response filters to the selected response.
    pub async fn call(
        &self,
        listener: &Binding,
        peer: SocketAddr,
        mut request: Request<RequestBody>,
    ) -> Exchange {
        let start = Instant::now();
        let version = request.version();
        let advertise = |mut exchange: Exchange| {
            if listener.tls
                && version != Version::HTTP_3
                && let Some((active, value)) =
                    self.advertised.borrow().get(&listener.address)
                && **active == *listener
            {
                exchange
                    .response
                    .headers_mut()
                    .entry(header::ALT_SVC)
                    .or_insert_with(|| value.clone());
            }
            exchange
        };
        if request.method() == http::Method::CONNECT
            || (version == Version::HTTP_3
                && request.headers().contains_key(header::UPGRADE))
        {
            return advertise(ProxyError::UnsupportedUpgrade.response());
        }
        let authority = match normalize_authority(&mut request) {
            Ok(authority) => authority,
            Err(error) => return advertise(error.response()),
        };
        let grpc = is_grpc(&request);
        let selection = match self.select(listener, &authority, &request, start, grpc) {
            Ok(s) => s,
            Err(error) => return advertise(error.for_request(grpc)),
        };
        let exchange = if selection.cors.as_ref().is_some_and(|c| c.preflight) {
            let mut response = Response::new(ResponseBody::full(Bytes::new()));
            *response.status_mut() = StatusCode::NO_CONTENT;
            Exchange::local(response)
        } else {
            self.forward(listener, peer, request, &authority, &selection)
                .await
                .unwrap_or_else(|error| error.for_request(selection.grpc))
        };
        let mut exchange = advertise(exchange);
        let response = &mut exchange.response;
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
        if !matches!(version, Version::HTTP_2 | Version::HTTP_3)
            && response.headers().contains_key(header::TRAILER)
        {
            response.headers_mut().remove(header::CONTENT_LENGTH);
        }
        if let Some(tunnel) = &exchange.tunnel {
            tunnel.restore(response.headers_mut());
        }
        if let Some(cors) = selection.cors {
            cors.apply(response.headers_mut());
        }
        exchange
    }

    /// apply the selected filters and forward on the chosen endpoint generation.
    async fn forward(
        &self,
        listener: &Binding,
        peer: SocketAddr,
        mut request: Request<RequestBody>,
        original: &Authority,
        selection: &Selection,
    ) -> Result<Exchange, ProxyError> {
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
        let upgrade = wire::take_upgrade(&mut request, endpoint.key.protocol)?;
        if request.version() == Version::HTTP_3 {
            // http/3 can receive trailers even without an explicit TE field.
            request
                .headers_mut()
                .entry(header::TE)
                .or_insert(HeaderValue::from_static("trailers"));
        }
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
        let mut exchange = self
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
        strip_hop_headers(exchange.response.headers_mut());
        Ok(exchange)
    }
}
