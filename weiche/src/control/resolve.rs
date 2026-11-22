//! translate watched route resources against the controller's local stores.
mod filters;

use super::{
    super::ir::{
        Backend, ClusterSpec, Http, PathMatch, Protocol, RefError, RouteAction,
        StreamRoute, Timeouts, Transport, ValueMatch,
    },
    GatewayStores,
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
    tcproutes::{TcpRoute, TcpRouteRulesBackendRefs},
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

/// resolved matches and action for one HTTPRoute rule.
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
            validated.push((reference, backend));
        }
        let backends = validated
            .into_iter()
            .map(|(reference, backend)| Backend {
                weight: reference.weight.unwrap_or(1),
                cluster: resolve_backend(self, policies, namespace, reference)
                    .map(Arc::new),
                filters: Arc::new(backend),
            })
            .collect();
        Ok(HttpRule {
            matches,
            action: RouteAction { backends, filters: Arc::new(filters), timeouts },
        })
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

/// retain the first predicate per name, ignoring later entries even for validation.
fn named_matches<'a>(
    predicates: impl Iterator<Item = (String, &'a str, bool)>,
) -> Result<Vec<(String, ValueMatch<String>)>, ResolveError> {
    let mut seen = HashSet::new();
    let mut matches = Vec::new();
    for (name, value, regular) in predicates {
        if seen.insert(name.clone()) {
            let predicate = if regular {
                ValueMatch::RegularExpression { regex: regex(value)? }
            } else {
                ValueMatch::Exact(value.into())
            };
            matches.push((name, predicate));
        }
    }
    Ok(matches)
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

/// resolve a Service backend into a validated cluster or a Gateway API status reason.
fn resolve_backend(
    stores: &GatewayStores,
    policies: &mut Policies,
    namespace: &str,
    backend: &HttpRouteRulesBackendRefs,
) -> Result<ClusterSpec, RefError> {
    let reference = ServiceRef::from(backend);
    let (service, port) =
        reference.resolve(stores, namespace, "HTTPRoute", Transport::Tcp)?;
    let namespace = reference.namespace.unwrap_or(namespace);
    // https and h2 require an authenticated transport; h2c explicitly uses cleartext.
    let protocol = match port.app_protocol.as_deref() {
        None
        | Some(
            "http" | "kubernetes.io/http" | "https" | "kubernetes.io/ws"
            | "kubernetes.io/wss",
        ) => Protocol::Http1,
        Some("kubernetes.io/h2c" | "h2") => Protocol::Http2,
        _ => return Err(RefError::UnsupportedProtocol),
    };
    let tls =
        policies.backend(stores, namespace, &service, port.name.as_deref(), protocol)?;
    match (port.app_protocol.as_deref(), &tls) {
        (Some("https" | "h2" | "kubernetes.io/wss"), None)
        | (Some("kubernetes.io/h2c" | "kubernetes.io/ws"), Some(_)) => {
            return Err(RefError::UnsupportedProtocol);
        }
        _ => {}
    }
    let endpoints = endpoints(stores, namespace, &backend.name, &port, Transport::Tcp);
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
service_ref!(TcpRouteRulesBackendRefs);
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
        transport: Transport,
    ) -> Result<Vec<SocketAddr>, RefError> {
        let (_, port) =
            self.resolve(stores, namespace, transport.route_kind(), transport)?;
        Ok(endpoints(
            stores,
            self.namespace.unwrap_or(namespace),
            self.name,
            &port,
            transport,
        ))
    }
}

impl GatewayStores {
    /// retain the weights of valid and invalid stream backends.
    fn resolve_stream<'a>(
        &self,
        namespace: &str,
        transport: Transport,
        backends: impl Iterator<Item = (u32, ServiceRef<'a>)>,
    ) -> StreamRoute {
        StreamRoute {
            backends: backends
                .map(|(w, r)| (w, r.stream(self, namespace, transport)))
                .collect(),
        }
    }

    /// resolve a TCPRoute into weighted backend endpoint sets.
    pub fn resolve_tcp(&self, route: &TcpRoute) -> Result<StreamRoute, ResolveError> {
        let [rule] = route.spec.rules.as_slice() else {
            return Err(ResolveError::UnsupportedValue);
        };
        let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
        Ok(self.resolve_stream(
            namespace,
            Transport::Tcp,
            rule.backend_refs
                .iter()
                .map(|r| (r.weight.unwrap_or(1), ServiceRef::from(r))),
        ))
    }

    /// resolve a UDPRoute into weighted backend endpoint sets.
    pub fn resolve_udp(&self, route: &UdpRoute) -> Result<StreamRoute, ResolveError> {
        let [rule] = route.spec.rules.as_slice() else {
            return Err(ResolveError::UnsupportedValue);
        };
        let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
        Ok(self.resolve_stream(
            namespace,
            Transport::Udp,
            rule.backend_refs
                .iter()
                .map(|r| (r.weight.unwrap_or(1), ServiceRef::from(r))),
        ))
    }
}
