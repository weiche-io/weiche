//! translate watched route resources against the controller's local stores.
use super::{
    super::ir::{
        Backend, ClusterSpec, Filters, GrpcMethod, Http, PathMatch, Protocol, RefError,
        RouteAction, StreamRoute, Timeouts, Transport, ValueMatch,
    },
    GatewayStores, filters,
    tls::Policies,
};
use k8s_openapi::api::core::v1::{Service, ServicePort};
use kube::{ResourceExt, runtime::reflector::ObjectRef};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use weiche_gtwapi::v1::httproutes::{
    HttpRouteRules, HttpRouteRulesBackendRefs, HttpRouteRulesMatches,
    HttpRouteRulesMatchesHeadersType as HeaderType,
    HttpRouteRulesMatchesMethod as Method, HttpRouteRulesMatchesPathType as PathType,
    HttpRouteRulesMatchesQueryParamsType as QueryType,
};
use weiche_gtwapi::v1::{
    grpcroutes::{
        GrpcRouteRules, GrpcRouteRulesBackendRefs, GrpcRouteRulesMatches,
        GrpcRouteRulesMatchesHeadersType as GrpcHeader,
        GrpcRouteRulesMatchesMethodType as MethodType,
    },
    tcproutes::{TcpRoute, TcpRouteRulesBackendRefs},
    tlsroutes::{TlsRoute, TlsRouteRulesBackendRefs},
    udproutes::{UdpRoute, UdpRouteRulesBackendRefs},
};

/// errors which prevent the route itself from being accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolveError {
    /// a match, filter value or timeout cannot be implemented.
    UnsupportedValue,
    /// a rule combines filters that cannot coexist.
    IncompatibleFilters,
}

impl ResolveError {
    /// gateway API reason for rejecting the route.
    pub fn reason(self) -> &'static str {
        match self {
            Self::UnsupportedValue => "UnsupportedValue",
            Self::IncompatibleFilters => "IncompatibleFilters",
        }
    }
}

/// resolved predicates and forwarding action for one HTTP or gRPC rule.
pub struct HttpRule {
    /// alternative predicates that select this action.
    pub matches: Vec<Http>,
    /// shared forwarding and filter configuration.
    pub action: RouteAction,
}

impl GatewayStores {
    /// resolve one rule independently so a bad filter cannot detach sibling rules.
    pub fn resolve_http(
        &self,
        namespace: &str,
        rule: &HttpRouteRules,
        policies: &mut Policies,
    ) -> Result<HttpRule, ResolveError> {
        if rule.retry.is_some() {
            return Err(ResolveError::UnsupportedValue);
        }
        let matches = match rule.matches.as_deref() {
            None | Some([]) => vec![Http::default()],
            Some(matches) => matches.iter().map(http_match).collect::<Result<_, _>>()?,
        };
        let timeouts = timeouts(rule)?;
        let filters =
            filters::rule_filters(rule, rule.filters.as_deref().unwrap_or_default())?;
        let references = rule.backend_refs.as_deref().unwrap_or_default();
        if filters.redirect.is_some() && !references.is_empty() {
            return Err(ResolveError::IncompatibleFilters);
        }
        // validate the entire rule before recording any backend TLS policy status.
        let mut validated = Vec::with_capacity(references.len());
        for reference in references {
            let backend = filters::backend_filters(
                rule,
                reference.filters.as_deref().unwrap_or_default(),
            )?;
            if backend.redirect.is_some()
                && rule.filters.iter().flatten().any(|f| f.url_rewrite.is_some())
            {
                return Err(ResolveError::IncompatibleFilters);
            }
            validated.push((
                reference.weight.unwrap_or(1),
                ServiceRef::from(reference),
                backend,
            ));
        }
        let backends = self.resolve_backends(namespace, "HTTPRoute", policies, validated);
        Ok(HttpRule {
            matches,
            action: RouteAction { backends, filters: Arc::new(filters), timeouts },
        })
    }

    /// resolve weighted gRPC targets after validating every match and header operation.
    pub fn resolve_grpc(
        &self,
        namespace: &str,
        rule: &GrpcRouteRules,
        policies: &mut Policies,
    ) -> Result<HttpRule, ResolveError> {
        let matches = match rule.matches.as_deref() {
            None | Some([]) => vec![grpc_match(None)?],
            Some(matches) => matches
                .iter()
                .map(|m| grpc_match(Some(m)))
                .collect::<Result<_, _>>()?,
        };
        let filters =
            filters::grpc_rule_filters(rule.filters.as_deref().unwrap_or_default())?;
        let mut validated = Vec::new();
        for r in rule.backend_refs.iter().flatten() {
            let backend =
                filters::grpc_backend_filters(r.filters.as_deref().unwrap_or_default())?;
            validated.push((r.weight.unwrap_or(1), ServiceRef::from(r), backend));
        }
        let backends = self.resolve_backends(namespace, "GRPCRoute", policies, validated);
        Ok(HttpRule {
            matches,
            action: RouteAction {
                backends,
                filters: Arc::new(filters),
                timeouts: Default::default(),
            },
        })
    }

    /// keep an unresolved reference in its weighted slot and bind filters to that slot.
    fn resolve_backends(
        &self,
        namespace: &str,
        source: &str,
        policies: &mut Policies,
        validated: Vec<(u32, ServiceRef<'_>, Filters)>,
    ) -> Vec<Backend<Arc<ClusterSpec>>> {
        validated
            .into_iter()
            .map(|(weight, reference, filters)| Backend {
                weight,
                cluster: resolve_backend(self, policies, namespace, source, reference)
                    .map(Arc::new),
                filters: Arc::new(filters),
            })
            .collect()
    }
}

/// parse one to four GEP-2257 components, each with one to five decimal digits.
fn duration(mut input: &str) -> Result<Duration, ResolveError> {
    let mut millis = 0;
    for _ in 0..4 {
        let digits = input.bytes().take_while(u8::is_ascii_digit).count();
        if !(1..=5).contains(&digits) {
            break;
        }
        let (value, suffix) = input.split_at(digits);
        let Some((unit, scale)) =
            [("ms", 1), ("s", 1000), ("m", 60_000), ("h", 3_600_000)]
                .into_iter()
                .find(|(unit, _)| suffix.starts_with(unit))
        else {
            break;
        };
        millis +=
            value.parse::<u64>().map_err(|_| ResolveError::UnsupportedValue)? * scale;
        input = &suffix[unit.len()..];
        if input.is_empty() {
            return Ok(Duration::from_millis(millis));
        }
    }
    Err(ResolveError::UnsupportedValue)
}

/// validate the request budget and preserve explicit zero durations.
fn timeouts(rule: &HttpRouteRules) -> Result<Timeouts, ResolveError> {
    let Some(t) = &rule.timeouts else { return Ok(Timeouts::default()) };
    let request = t.request.as_deref().map(duration).transpose()?;
    let backend_request = t.backend_request.as_deref().map(duration).transpose()?;
    if let (Some(total), Some(attempt)) = (request, backend_request)
        && !total.is_zero()
        && attempt > total
    {
        return Err(ResolveError::UnsupportedValue);
    }
    Ok(Timeouts { request, backend_request })
}

/// validate the same unanchored Rust regex that the worker will compile.
fn regex(pattern: &str) -> Result<String, ResolveError> {
    regex::Regex::new(pattern).map_err(|_| ResolveError::UnsupportedValue)?;
    Ok(pattern.to_owned())
}

impl ValueMatch<String> {
    /// validate a literal or Rust regex for one method component or metadata value.
    fn resolve(value: &str, regular: bool) -> Result<Self, ResolveError> {
        if regular {
            Ok(Self::RegularExpression { regex: regex(value)? })
        } else {
            Ok(Self::Exact(value.into()))
        }
    }
}

/// retain the first predicate per name, ignoring later entries even for validation.
fn named_matches<'a>(
    predicates: impl Iterator<Item = (String, &'a str, bool)>,
) -> Result<Vec<(String, ValueMatch<String>)>, ResolveError> {
    let mut seen = HashSet::new();
    let mut matches = Vec::new();
    for (name, value, regular) in predicates {
        if seen.insert(name.clone()) {
            matches.push((name, ValueMatch::resolve(value, regular)?));
        }
    }
    Ok(matches)
}

/// resolve optional method components independently of the HTTP path matcher.
fn grpc_match(predicate: Option<&GrpcRouteRulesMatches>) -> Result<Http, ResolveError> {
    let mut grpc = GrpcMethod::default();
    if let Some(m) = predicate.and_then(|p| p.method.as_ref()) {
        let regular = matches!(m.r#type, Some(MethodType::RegularExpression));
        let component = |value: Option<&str>| {
            value
                .filter(|s| !s.is_empty())
                .map(|s| ValueMatch::resolve(s, regular))
                .transpose()
        };
        grpc.service = component(m.service.as_deref())?;
        grpc.method = component(m.method.as_deref())?;
        if grpc.service.is_none() && grpc.method.is_none() {
            return Err(ResolveError::UnsupportedValue);
        }
    }
    let headers = named_matches(
        predicate
            .and_then(|p| p.headers.as_deref())
            .unwrap_or_default()
            .iter()
            .map(|h| {
                (
                    h.name.to_ascii_lowercase(),
                    h.value.as_str(),
                    matches!(h.r#type, Some(GrpcHeader::RegularExpression)),
                )
            }),
    )?;
    Ok(Http {
        path: PathMatch::Grpc { grpc },
        headers,
        ..Http::default()
    })
}

/// normalize path elements and named predicates before resolving any backend.
fn http_match(predicate: &HttpRouteRulesMatches) -> Result<Http, ResolveError> {
    let path = predicate.path.as_ref();
    let value = path.and_then(|p| p.value.as_deref()).unwrap_or("/");
    let path = match path.and_then(|p| p.r#type.as_ref()) {
        Some(PathType::RegularExpression) => {
            PathMatch::RegularExpression { regex: regex(value)? }
        }
        Some(PathType::Exact) => PathMatch::Literal { path: value.into(), exact: true },
        None | Some(PathType::PathPrefix) => {
            let prefix = value.trim_end_matches('/');
            PathMatch::Literal {
                path: if prefix.is_empty() { "/" } else { prefix }.into(),
                exact: false,
            }
        }
    };
    Ok(Http {
        path,
        method: predicate.method.as_ref().map(|method| {
            match method {
                Method::Get => "GET",
                Method::Head => "HEAD",
                Method::Post => "POST",
                Method::Put => "PUT",
                Method::Delete => "DELETE",
                Method::Connect => "CONNECT",
                Method::Options => "OPTIONS",
                Method::Trace => "TRACE",
                Method::Patch => "PATCH",
            }
            .into()
        }),
        headers: named_matches(predicate.headers.iter().flatten().map(|h| {
            (
                h.name.to_ascii_lowercase(),
                h.value.as_str(),
                matches!(h.r#type, Some(HeaderType::RegularExpression)),
            )
        }))?,
        query_params: named_matches(predicate.query_params.iter().flatten().map(|p| {
            (
                p.name.clone(),
                p.value.as_str(),
                matches!(p.r#type, Some(QueryType::RegularExpression)),
            )
        }))?,
    })
}

impl RefError {
    /// gateway API reason for the parent's ResolvedRefs condition.
    pub fn reason(self) -> &'static str {
        match self {
            Self::InvalidKind => "InvalidKind",
            Self::RefNotPermitted => "RefNotPermitted",
            Self::BackendNotFound => "BackendNotFound",
            Self::InvalidTlsPolicy => "InvalidBackendTLSPolicy",
            Self::UnsupportedProtocol => "UnsupportedProtocol",
        }
    }
}

/// application protocol declared on a backend Service port.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AppProtocol {
    /// the route selects the wire protocol.
    None,
    /// serve HTTP with optional transport security.
    Http,
    /// secure HTTP.
    Https,
    /// negotiate HTTP/2 over TLS.
    H2,
    /// cleartext HTTP/2.
    H2c,
    /// cleartext WebSocket.
    Ws,
    /// secure WebSocket.
    Wss,
    /// unrecognized service name or domain-prefixed protocol.
    Unsupported,
}

impl AppProtocol {
    /// recognize case-insensitive IANA names and exact Kubernetes protocol names.
    fn parse(raw: Option<&str>) -> Self {
        match raw {
            None => Self::None,
            Some("kubernetes.io/http") => Self::Http,
            Some("kubernetes.io/h2c") => Self::H2c,
            Some("kubernetes.io/ws") => Self::Ws,
            Some("kubernetes.io/wss") => Self::Wss,
            Some(name) if name.eq_ignore_ascii_case("http") => Self::Http,
            Some(name) if name.eq_ignore_ascii_case("https") => Self::Https,
            Some(name) if name.eq_ignore_ascii_case("h2") => Self::H2,
            Some(_) => Self::Unsupported,
        }
    }
}

/// resolve a Service backend into a validated cluster or a Gateway API status reason.
fn resolve_backend(
    stores: &GatewayStores,
    policies: &mut Policies,
    namespace: &str,
    source: &str,
    reference: ServiceRef<'_>,
) -> Result<ClusterSpec, RefError> {
    let (service, port) = reference.resolve(stores, namespace, source, Transport::Tcp)?;
    let namespace = reference.namespace.unwrap_or(namespace);
    let app = AppProtocol::parse(port.app_protocol.as_deref());
    let protocol =
        if source == "GRPCRoute" || matches!(app, AppProtocol::H2 | AppProtocol::H2c) {
            Protocol::Http2
        } else {
            Protocol::Http1
        };
    // record the policy's Gateway ancestor even if this route cannot use the port.
    let tls =
        policies.backend(stores, namespace, &service, port.name.as_deref(), protocol);
    if app == AppProtocol::Unsupported
        || (source == "GRPCRoute" && matches!(app, AppProtocol::Ws | AppProtocol::Wss))
    {
        return Err(RefError::UnsupportedProtocol);
    }
    let tls = tls?;
    if matches!(
        (app, &tls),
        (AppProtocol::Https | AppProtocol::H2 | AppProtocol::Wss, None)
            | (AppProtocol::H2c | AppProtocol::Ws, Some(_))
    ) {
        return Err(RefError::UnsupportedProtocol);
    }
    let endpoints = endpoints(stores, namespace, reference.name, &port, Transport::Tcp);
    Ok(ClusterSpec { endpoints, protocol, tls })
}

/// collect only ready endpoints for the selected Service port and transport.
fn endpoints(
    stores: &GatewayStores,
    namespace: &str,
    name: &str,
    port: &ServicePort,
    transport: Transport,
) -> Vec<SocketAddr> {
    let mut endpoints = vec![];
    for slice in stores.endpoint_slices.state() {
        if slice.namespace().as_deref() != Some(namespace)
            || slice.metadata.deletion_timestamp.is_some()
            || slice.labels().get("kubernetes.io/service-name").map(String::as_str)
                != Some(name)
            || !matches!(slice.address_type.as_str(), "IPv4" | "IPv6")
        {
            continue;
        }
        for slice_port in slice.ports.iter().flatten().filter(|candidate| {
            candidate.name.as_deref().unwrap_or("") == port.name.as_deref().unwrap_or("")
                && candidate.protocol.as_deref().unwrap_or("TCP") == transport.protocol()
        }) {
            let Some(endpoint_port) = slice_port
                .port
                .and_then(|port| u16::try_from(port).ok())
                .filter(|&port| port != 0)
            else {
                continue;
            };
            for endpoint in slice.endpoints.iter().flatten() {
                let conditions = endpoint.conditions.as_ref();
                if conditions.is_some_and(|conditions| {
                    conditions.ready == Some(false)
                        || conditions.terminating == Some(true)
                        || conditions.serving == Some(false)
                }) {
                    continue;
                }
                for address in &endpoint.addresses {
                    if let Ok(ip) = address.parse::<IpAddr>() {
                        endpoints.push(SocketAddr::new(ip, endpoint_port));
                    }
                }
            }
        }
    }
    endpoints.sort_unstable();
    endpoints.dedup();
    endpoints
}

/// the identical core Service reference fields generated for each Route kind.
struct ServiceRef<'a> {
    /// referenced API group.
    group: Option<&'a str>,
    /// referenced resource kind.
    kind: Option<&'a str>,
    /// target namespace when different from the route.
    namespace: Option<&'a str>,
    /// referenced Service name.
    name: &'a str,
    /// requested Service port.
    port: Option<u16>,
}
/// borrow common Service reference fields from each generated type.
macro_rules! service_ref {
    ($ty:ty) => {
        impl<'a> From<&'a $ty> for ServiceRef<'a> {
            /// borrow the backend identity and port from the route reference.
            fn from(r: &'a $ty) -> Self {
                Self {
                    group: r.group.as_deref(),
                    kind: r.kind.as_deref(),
                    namespace: r.namespace.as_deref(),
                    name: &r.name,
                    port: r.port,
                }
            }
        }
    };
}
service_ref!(HttpRouteRulesBackendRefs);
service_ref!(GrpcRouteRulesBackendRefs);
service_ref!(TcpRouteRulesBackendRefs);
service_ref!(TlsRouteRulesBackendRefs);
service_ref!(UdpRouteRulesBackendRefs);

impl ServiceRef<'_> {
    /// check the grant and locate the requested Service port.
    fn resolve(
        &self,
        stores: &GatewayStores,
        namespace: &str,
        source: &str,
        transport: Transport,
    ) -> Result<(Arc<Service>, ServicePort), RefError> {
        if !self.group.unwrap_or("").is_empty()
            || self.kind.unwrap_or("Service") != "Service"
        {
            return Err(RefError::InvalidKind);
        }
        let target = self.namespace.unwrap_or(namespace);
        if !stores.permits(namespace, source, target, "Service", self.name) {
            return Err(RefError::RefNotPermitted);
        }
        let num = self.port.ok_or(RefError::BackendNotFound)?;
        let service = stores
            .services
            .get(&ObjectRef::new(self.name).within(target))
            .filter(|s| s.metadata.deletion_timestamp.is_none())
            .ok_or(RefError::BackendNotFound)?;
        let port = service
            .spec
            .as_ref()
            .and_then(|s| s.ports.as_ref())
            .and_then(|ps| {
                ps.iter().find(|p| {
                    p.port == i32::from(num)
                        && p.protocol.as_deref().unwrap_or("TCP") == transport.protocol()
                })
            })
            .cloned()
            .ok_or(RefError::BackendNotFound)?;
        Ok((service, port))
    }
    /// resolve a transport reference to its ready endpoint addresses.
    fn stream(
        &self,
        stores: &GatewayStores,
        namespace: &str,
        source: &str,
        transport: Transport,
    ) -> Result<Vec<SocketAddr>, RefError> {
        let (_, port) = self.resolve(stores, namespace, source, transport)?;
        Ok(endpoints(
            stores,
            self.namespace.unwrap_or(namespace),
            self.name,
            &port,
            transport,
        ))
    }
}

/// resolve one stream rule using the referring route kind for cross-namespace grants.
macro_rules! stream_resolver {
    ($method:ident, $route:ty, $transport:expr) => {
        /// retain the configured weights of both resolved and invalid backends.
        pub fn $method(&self, route: &$route) -> Result<StreamRoute, ResolveError> {
            let [rule] = route.spec.rules.as_slice() else {
                return Err(ResolveError::UnsupportedValue);
            };
            let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
            let kind = <$route as kube::Resource>::kind(&());
            let backends = rule
                .backend_refs
                .iter()
                .map(|r| {
                    let target =
                        ServiceRef::from(r).stream(self, namespace, &kind, $transport);
                    (r.weight.unwrap_or(1), target)
                })
                .collect();
            Ok(StreamRoute { backends })
        }
    };
}

impl GatewayStores {
    stream_resolver!(resolve_tcp, TcpRoute, Transport::Tcp);
    stream_resolver!(resolve_tls, TlsRoute, Transport::Tcp);
    stream_resolver!(resolve_udp, UdpRoute, Transport::Udp);
    /// validate an unattached TLSRoute's references without enumerating unused endpoints.
    pub fn tls_ref_error(&self, route: &TlsRoute) -> Option<RefError> {
        let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
        route.spec.rules.iter().flat_map(|rule| &rule.backend_refs).find_map(
            |reference| {
                ServiceRef::from(reference)
                    .resolve(self, namespace, "TLSRoute", Transport::Tcp)
                    .err()
            },
        )
    }
}
