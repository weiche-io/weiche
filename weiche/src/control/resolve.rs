//! translate watched route resources against the controller's local stores.
use super::{
    super::ir::{
        ClusterSpec, HeaderChange, Http, Protocol, Redirect, RefError, RouteAction,
        StreamRoute, Transport,
    },
    GatewayStores,
    tls::Policies,
};
use k8s_openapi::api::core::v1::{Service, ServicePort};
use kube::{ResourceExt, runtime::reflector::ObjectRef};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use weiche_gtwapi::v1::httproutes::{
    HttpRoute, HttpRouteRules, HttpRouteRulesBackendRefs, HttpRouteRulesMatches,
    HttpRouteRulesMatchesHeadersType as HeaderType,
    HttpRouteRulesMatchesMethod as Method, HttpRouteRulesMatchesPathType as PathType,
};
use weiche_gtwapi::v1::{
    tcproutes::{TcpRoute, TcpRouteRulesBackendRefs},
    udproutes::{UdpRoute, UdpRouteRulesBackendRefs},
};

/// errors which prevent the route itself from being accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolveError {
    UnsupportedValue,
}

/// resolved matches and action for one HTTPRoute rule.
pub struct HttpRule {
    pub matches: Vec<Http>,
    pub action: RouteAction,
}

impl GatewayStores {
    /// retain policy status for the Service ports used by accepted routes.
    pub fn resolve_http(
        &self,
        route: &HttpRoute,
        policies: &mut Policies,
    ) -> Result<Vec<HttpRule>, ResolveError> {
        let rules = route.spec.rules.as_deref().unwrap_or_default();
        if rules.iter().any(unsupported_rule) {
            return Err(ResolveError::UnsupportedValue);
        }

        let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
        rules
            .iter()
            .map(|rule| {
                let mut action = filters(rule)?;
                for backend in rule.backend_refs.iter().flatten() {
                    action.backends.push((
                        backend.weight.unwrap_or(1),
                        resolve_backend(self, policies, namespace, backend).map(Arc::new),
                    ));
                }
                Ok(HttpRule {
                    matches: rule.matches.as_ref().map_or_else(
                        || vec![Http::default()],
                        |m| m.iter().map(http_match).collect(),
                    ),
                    action,
                })
            })
            .collect()
    }
}

fn unsupported_rule(rule: &HttpRouteRules) -> bool {
    rule.retry.is_some()
        || rule.timeouts.is_some()
        || rule
            .backend_refs
            .iter()
            .flatten()
            .any(|b| b.filters.as_ref().is_some_and(|f| !f.is_empty()))
        || rule.matches.iter().flatten().any(|predicate| {
            predicate
                .query_params
                .as_ref()
                .is_some_and(|params| !params.is_empty())
                || matches!(
                    predicate.path.as_ref().and_then(|path| path.r#type.as_ref()),
                    Some(PathType::RegularExpression)
                )
                || predicate.headers.iter().flatten().any(|header| {
                    matches!(header.r#type, Some(HeaderType::RegularExpression))
                })
        })
}

/// a redirect hostname is a DNS name without a wildcard, userinfo or port.
fn valid_redirect_hostname(host: &str) -> bool {
    host.len() <= 253
        && host.parse::<IpAddr>().is_err()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

fn filters(rule: &HttpRouteRules) -> Result<RouteAction, ResolveError> {
    use weiche_gtwapi::v1::httproutes::HttpRouteRulesFiltersType as Kind;
    let invalid = ResolveError::UnsupportedValue;
    let mut action = RouteAction {
        backends: Vec::new(),
        headers: Vec::new(),
        redirect: None,
    };
    for filter in rule.filters.iter().flatten() {
        match filter.r#type {
            Kind::RequestHeaderModifier => {
                let h = filter.request_header_modifier.as_ref().ok_or(invalid)?;
                for entry in h.set.iter().flatten() {
                    action.headers.push(HeaderChange::Set(
                        entry.name.parse().map_err(|_| invalid)?,
                        entry.value.parse().map_err(|_| invalid)?,
                    ));
                }
                for entry in h.add.iter().flatten() {
                    action.headers.push(HeaderChange::Add(
                        entry.name.parse().map_err(|_| invalid)?,
                        entry.value.parse().map_err(|_| invalid)?,
                    ));
                }
                for name in h.remove.iter().flatten() {
                    action
                        .headers
                        .push(HeaderChange::Remove(name.parse().map_err(|_| invalid)?));
                }
            }
            Kind::RequestRedirect => {
                let r = filter.request_redirect.as_ref().ok_or(invalid)?;
                if r.path.is_some()
                    || r.scheme.is_some()
                    || r.port.is_some()
                    || r.hostname
                        .as_deref()
                        .is_some_and(|host| !valid_redirect_hostname(host))
                {
                    return Err(invalid);
                }
                let status = match r.status_code.unwrap_or(302) {
                    301 => http::StatusCode::MOVED_PERMANENTLY,
                    302 => http::StatusCode::FOUND,
                    _ => return Err(invalid),
                };
                action.redirect = Some(Redirect { status, hostname: r.hostname.clone() });
            }
            _ => return Err(invalid),
        }
    }
    Ok(action)
}

fn http_match(predicate: &HttpRouteRulesMatches) -> Http {
    let path = predicate.path.as_ref();
    let exact =
        matches!(path.and_then(|path| path.r#type.as_ref()), Some(PathType::Exact));
    let path = path.and_then(|path| path.value.as_deref()).unwrap_or("/");
    let path = if exact { path } else { path.trim_end_matches('/') };
    Http {
        path: if path.is_empty() { "/".into() } else { path.into() },
        exact,
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
        headers: predicate
            .headers
            .iter()
            .flatten()
            .map(|header| (header.name.to_ascii_lowercase(), header.value.clone()))
            .collect(),
    }
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
        None | Some("http" | "kubernetes.io/http" | "https") => Protocol::Http1,
        Some("kubernetes.io/h2c" | "h2") => Protocol::Http2,
        _ => return Err(RefError::UnsupportedProtocol),
    };
    let tls =
        policies.backend(stores, namespace, &service, port.name.as_deref(), protocol)?;
    match (port.app_protocol.as_deref(), &tls) {
        (Some("https" | "h2"), None) | (Some("kubernetes.io/h2c"), Some(_)) => {
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
    group: Option<&'a str>,
    kind: Option<&'a str>,
    namespace: Option<&'a str>,
    name: &'a str,
    port: Option<u16>,
}
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
