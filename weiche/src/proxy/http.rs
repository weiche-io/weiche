//! worker-local service composition; keep-alive requests borrow the current view.
mod body;
mod cors;
mod filter;
mod router;
pub mod server;
pub mod wire;

use super::{
    super::ir::{Binding, ClusterSpec, PublishedSnapshot, Revision},
    admission::{Cluster, EndpointKey},
    config::RuntimeConfig,
    up::{ConnectionPool, Exchange, Upgrade},
};
pub use body::{RequestBody, ResponseBody};
use http::{HeaderValue, Request, StatusCode, Version, header, uri::Authority};
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
    /// round-robin state shared by every occurrence of an active backend.
    clusters: HashMap<Arc<ClusterSpec>, Rc<Cluster>>,
    /// accepted binding and hostname routers at each downstream address.
    routes: BTreeMap<SocketAddr, (Binding, Hosts<Router>)>,
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
        let mut clusters = HashMap::new();
        let mut endpoints = HashSet::new();
        let mut resolve = |spec: &Arc<ClusterSpec>| {
            clusters
                .entry(spec.clone())
                .or_insert_with(|| {
                    let cluster = previous
                        .and_then(|view| view.clusters.get(spec))
                        .cloned()
                        .unwrap_or_else(|| Rc::new(Cluster::new(spec.clone())));
                    endpoints.extend(
                        spec.endpoints.iter().map(|&addr| cluster.endpoint(addr)),
                    );
                    cluster
                })
                .clone()
        };
        let mut routes = BTreeMap::new();
        for (&address, socket) in &gateway.sockets {
            // a TLS listener owns its hostname even when no route is attached to it.
            let mut by_listener: BTreeMap<_, Vec<_>> = socket
                .hostnames
                .iter()
                .map(String::as_str)
                .chain(
                    socket.listener.tls.iter().map(|identity| identity.hostname.as_str()),
                )
                .map(|name| (name, Vec::new()))
                .collect();
            for route in &socket.routes {
                let listener = route.host.as_ref().map_or("", |h| h.listener.as_str());
                by_listener.entry(listener).or_default().push(route);
            }
            let target = by_listener
                .into_iter()
                .map(|(host, rules)| {
                    Ok((host.to_owned(), Router::compile(rules, &mut resolve)?))
                })
                .collect::<Result<_, regex::Error>>()?;
            routes.insert(address, (socket.listener.binding(), target));
        }
        let view = Self {
            revision: snapshot.revision,
            deadline: gateway.forwarding.response_header_timeout,
            routes,
            clusters,
        };
        Ok((view, endpoints))
    }
}

/// worker-local HTTP routing and upstream forwarding service.
pub struct ProxyService {
    /// current route snapshot for new requests.
    view: RefCell<WorkerView>,
    /// pool reconciled when the active snapshot changes.
    pool: Rc<ConnectionPool>,
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
            pool,
        })
    }

    /// publish the snapshot and retire endpoints removed by the controller.
    pub fn apply(&self, snapshot: &PublishedSnapshot) -> anyhow::Result<Revision> {
        if self.view.borrow().revision == snapshot.revision {
            return Ok(snapshot.revision);
        }
        // preparation cannot mutate the live view or retire its connections.
        let (next, endpoints) = WorkerView::prepare(snapshot, Some(&self.view.borrow()))?;
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
        tls_listener: Option<&str>,
        authority: &Authority,
        request: &Request<RequestBody>,
        start: Instant,
        grpc: bool,
    ) -> Result<Selection, ProxyError> {
        let view = self.view.borrow();
        let hostname = authority.host().to_ascii_lowercase();
        let (binding, hosts) =
            view.routes.get(&listener.address).ok_or(ProxyError::NoRoute)?;
        if binding != listener {
            return Err(ProxyError::NoRoute);
        }
        let (pattern, router) =
            hosts.matching(&hostname).next().ok_or(if tls_listener.is_some() {
                ProxyError::NoHttpsListener
            } else {
                ProxyError::NoRoute
            })?;
        // compare listener patterns so different hosts on the same wildcard can share TLS.
        if tls_listener.is_some_and(|selected| selected != pattern) {
            return Err(ProxyError::Misdirected);
        }
        router
            .find(&hostname, request, grpc, start, view.deadline)
            .ok_or(ProxyError::NoRoute)
    }

    /// route the original request and apply response filters to the selected response.
    pub async fn call(
        &self,
        listener: &Binding,
        tls_listener: Option<&str>,
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
            return advertise(ProxyError::UnsupportedUpgrade.response(false));
        }
        let authority = match normalize_authority(&mut request) {
            Ok(authority) => authority,
            Err(error) => return advertise(error.response(false)),
        };
        let grpc = is_grpc(&request);
        let selection = match self.select(
            listener,
            tls_listener,
            &authority,
            &request,
            start,
            grpc,
        ) {
            Ok(selection) => selection,
            Err(error) => return advertise(error.response(grpc)),
        };
        let exchange = if selection.cors.as_ref().is_some_and(|c| c.preflight) {
            Exchange::local(StatusCode::NO_CONTENT, "")
        } else {
            self.forward(listener, peer, request, &authority, &selection)
                .await
                .unwrap_or_else(|error| error.response(selection.grpc))
        };
        let mut exchange = advertise(exchange);
        filter::response(&mut exchange, version, selection);
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
            let location = filter::location(r, listener, original, request.uri())?;
            let mut exchange = Exchange::local(r.status, "");
            exchange.response.headers_mut().insert(header::LOCATION, location);
            return Ok(exchange);
        }
        let (cluster, filters) = backend.ok_or(ProxyError::InvalidBackend)?;
        let key = cluster.select().ok_or(ProxyError::NoEndpoints)?;
        let endpoint = self.pool.endpoint(key).ok_or(ProxyError::NoEndpoints)?;
        let upgrade = Upgrade::request(&mut request, endpoint.key.protocol)?;
        let (mut request, body) = request.into_parts();
        let (authority, trailers) = filter::request_headers(
            &mut request,
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
            .map(|p| filter::apply(p, &request.uri))
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
            upgrade.restore(&mut request.headers);
        }
        let request = Request::from_parts(request, body);
        let address = endpoint.key.address;
        let mut exchange = self
            .pool
            .send(endpoint, request, upgrade, selection.budget)
            .await
            .map_err(|error| ProxyError::backend(address, error))?;
        strip_hop_headers(exchange.response.headers_mut());
        Ok(exchange)
    }
}
