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
    HttpRouteRules, HttpRouteRulesMatches,
    HttpRouteRulesMatchesHeadersType as HeaderType,
    HttpRouteRulesMatchesMethod as Method, HttpRouteRulesMatchesPathType as PathType,
    HttpRouteRulesMatchesQueryParamsType as QueryType,
};
use weiche_gtwapi::v1::{
    grpcroutes::{
        GrpcRouteRules, GrpcRouteRulesMatches,
        GrpcRouteRulesMatchesHeadersType as GrpcHeader,
        GrpcRouteRulesMatchesMethodType as MethodType,
    },
    tcproutes::TcpRoute,
    tlsroutes::TlsRoute,
    udproutes::UdpRoute,
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

/// the common weighted Service reference carried by all generated route kinds.
struct ServiceRef<'a> {
    /// whether the group and kind designate a core Service.
    service: bool,
    /// effective target namespace, including the route namespace default.
    namespace: &'a str,
    /// referenced Service name.
    name: &'a str,
    /// requested Service port.
    port: Option<u16>,
    /// backend weight, including the Gateway API default.
    weight: u32,
}

/// borrow a Service reference without naming each generated backend type.
macro_rules! service_ref {
    ($r:ident, $namespace:expr) => {
        ServiceRef {
            service: $r.group.as_deref().unwrap_or("").is_empty()
                && $r.kind.as_deref().unwrap_or("Service") == "Service",
            namespace: $r.namespace.as_deref().unwrap_or($namespace),
            name: &$r.name,
            port: $r.port,
            weight: $r.weight.unwrap_or(1),
        }
    };
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
        let filters = filters::rule_filters(rule, rule.filters.as_deref())?;
        let references = rule.backend_refs.as_deref().unwrap_or_default();
        // validate the entire rule before recording any backend TLS policy status.
        let mut validated = Vec::with_capacity(references.len());
        for reference in references {
            let backend = filters::backend_filters(rule, reference.filters.as_deref())?;
            // the CRD checks redirect/rewrite conflicts only within a single filter list.
            if backend.redirect.is_some()
                && rule.filters.iter().flatten().any(|f| f.url_rewrite.is_some())
            {
                return Err(ResolveError::IncompatibleFilters);
            }
            validated.push((service_ref!(reference, namespace), backend));
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
        let filters = filters::grpc_rule_filters(rule.filters.as_deref())?;
        let mut validated = Vec::new();
        for r in rule.backend_refs.iter().flatten() {
            let backend = filters::grpc_backend_filters(r.filters.as_deref())?;
            validated.push((service_ref!(r, namespace), backend));
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
        validated: Vec<(ServiceRef<'_>, Filters)>,
    ) -> Vec<Backend<Arc<ClusterSpec>>> {
        validated
            .into_iter()
            .map(|(reference, filters)| Backend {
                weight: reference.weight,
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

/// parse both request budgets, preserving explicit zero durations.
fn timeouts(rule: &HttpRouteRules) -> Result<Timeouts, ResolveError> {
    let Some(t) = &rule.timeouts else { return Ok(Timeouts::default()) };
    Ok(Timeouts {
        request: t.request.as_deref().map(duration).transpose()?,
        backend_request: t.backend_request.as_deref().map(duration).transpose()?,
    })
}

/// reject a bad rule before its regex reaches workers installing the whole snapshot.
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

/// retain the first header or query predicate per normalized name, even for validation.
macro_rules! named_matches {
    ($entries:expr, $kind:ident, $name:expr) => {{
        let mut seen = HashSet::new();
        let mut matches = Vec::new();
        for entry in $entries {
            let name = $name(&entry.name);
            if seen.insert(name.clone()) {
                let regular = matches!(entry.r#type, Some($kind::RegularExpression));
                matches.push((name, ValueMatch::resolve(&entry.value, regular)?));
            }
        }
        matches
    }};
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
    let headers = predicate.and_then(|p| p.headers.as_deref()).unwrap_or_default();
    Ok(Http {
        path: PathMatch::Grpc { grpc },
        headers: named_matches!(headers, GrpcHeader, |n: &String| n.to_ascii_lowercase()),
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
        headers: named_matches!(
            predicate.headers.iter().flatten(),
            HeaderType,
            |n: &String| n.to_ascii_lowercase()
        ),
        query_params: named_matches!(
            predicate.query_params.iter().flatten(),
            QueryType,
            String::clone
        ),
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

/// resolve a Service backend into a validated cluster or a Gateway API status reason.
fn resolve_backend(
    stores: &GatewayStores,
    policies: &mut Policies,
    namespace: &str,
    source: &str,
    reference: ServiceRef<'_>,
) -> Result<ClusterSpec, RefError> {
    let (service, port) = reference.resolve(stores, namespace, source, Transport::Tcp)?;
    let target = reference.namespace;
    let grpc = source == "GRPCRoute";
    let app = port.app_protocol.as_deref();
    let h2 = app.is_some_and(|name| name.eq_ignore_ascii_case("h2"));
    let protocol = if grpc || h2 || app == Some("kubernetes.io/h2c") {
        Protocol::Http2
    } else {
        Protocol::Http1
    };
    // record the policy's ancestor even when the port protocol is unsupported.
    let tls = policies.backend(stores, target, &service, port.name.as_deref(), protocol);
    let requires_tls = match app {
        None | Some("kubernetes.io/http") => None,
        Some(name) if name.eq_ignore_ascii_case("http") => None,
        Some(name) if h2 || name.eq_ignore_ascii_case("https") => Some(true),
        Some("kubernetes.io/h2c") => Some(false),
        Some("kubernetes.io/ws") if !grpc => Some(false),
        Some("kubernetes.io/wss") if !grpc => Some(true),
        _ => return Err(RefError::UnsupportedProtocol),
    };
    let tls = tls?;
    if requires_tls.is_some_and(|required| required != tls.is_some()) {
        return Err(RefError::UnsupportedProtocol);
    }
    let endpoints = reference.endpoints(stores, &port, Transport::Tcp);
    Ok(ClusterSpec { endpoints, protocol, tls })
}

impl ServiceRef<'_> {
    /// check the grant and locate the requested Service port.
    fn resolve(
        &self,
        stores: &GatewayStores,
        namespace: &str,
        source: &str,
        transport: Transport,
    ) -> Result<(Arc<Service>, ServicePort), RefError> {
        if !self.service {
            return Err(RefError::InvalidKind);
        }
        if !stores.permits(namespace, source, self.namespace, "Service", self.name) {
            return Err(RefError::RefNotPermitted);
        }
        let num = self.port.ok_or(RefError::BackendNotFound)?;
        let service = stores
            .services
            .get(&ObjectRef::new(self.name).within(self.namespace))
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
    /// collect only ready endpoints for the selected Service port and transport.
    fn endpoints(
        &self,
        stores: &GatewayStores,
        port: &ServicePort,
        transport: Transport,
    ) -> Vec<SocketAddr> {
        let mut endpoints = vec![];
        for slice in stores.endpoint_slices.state() {
            if slice.namespace().as_deref() != Some(self.namespace)
                || slice.metadata.deletion_timestamp.is_some()
                || slice.labels().get("kubernetes.io/service-name").map(String::as_str)
                    != Some(self.name)
                || !matches!(slice.address_type.as_str(), "IPv4" | "IPv6")
            {
                continue;
            }
            for slice_port in slice.ports.iter().flatten().filter(|candidate| {
                candidate.name.as_deref().unwrap_or("")
                    == port.name.as_deref().unwrap_or("")
                    && candidate.protocol.as_deref().unwrap_or("TCP")
                        == transport.protocol()
            }) {
                let Some(endpoint_port @ 1..=65535) = slice_port.port else { continue };
                for endpoint in slice.endpoints.iter().flatten() {
                    if endpoint.conditions.as_ref().is_some_and(|c| {
                        c.ready == Some(false)
                            || c.terminating == Some(true)
                            || c.serving == Some(false)
                    }) {
                        continue;
                    }
                    for address in &endpoint.addresses {
                        if let Ok(ip) = address.parse::<IpAddr>() {
                            endpoints.push(SocketAddr::new(ip, endpoint_port as u16));
                        }
                    }
                }
            }
        }
        endpoints.sort_unstable();
        endpoints.dedup();
        endpoints
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
                    let reference = service_ref!(r, namespace);
                    let target = reference
                        .resolve(self, namespace, &kind, $transport)
                        .map(|(_, port)| reference.endpoints(self, &port, $transport));
                    (reference.weight, target)
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
                service_ref!(reference, namespace)
                    .resolve(self, namespace, "TLSRoute", Transport::Tcp)
                    .err()
            },
        )
    }
}
