//! translate watched route resources against the controller's local stores.
use super::{
    super::ir::{Backend, ClusterSpec, Http, Protocol, RefError, Rule},
    GatewayStores,
    tls::Policies,
};
use kube::{ResourceExt, runtime::reflector::ObjectRef};
use std::net::{IpAddr, SocketAddr};
use weiche_gtwapi::v1::httproutes::{
    HttpRoute, HttpRouteRules, HttpRouteRulesBackendRefs, HttpRouteRulesMatches,
    HttpRouteRulesMatchesHeadersType as HeaderType,
    HttpRouteRulesMatchesMethod as Method, HttpRouteRulesMatchesPathType as PathType,
};

/// resolve a route using the resources visible to this controller.
pub trait Resolve<Route> {
    type Output;

    fn resolve(&self, route: &Route) -> Result<Self::Output, ResolveError>;
}

/// errors which prevent the route itself from being accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolveError {
    UnsupportedValue,
}

impl Resolve<HttpRoute> for GatewayStores {
    type Output = Backend<Http>;

    fn resolve(&self, route: &HttpRoute) -> Result<Self::Output, ResolveError> {
        self.resolve_http(route, &mut Policies::default())
    }
}

impl GatewayStores {
    /// retain policy status for the Service ports used by accepted routes.
    pub(super) fn resolve_http(
        &self,
        route: &HttpRoute,
        policies: &mut Policies,
    ) -> Result<Backend<Http>, ResolveError> {
        let rules = route.spec.rules.as_deref().unwrap_or_default();
        if rules.iter().any(unsupported_rule) {
            return Err(ResolveError::UnsupportedValue);
        }

        let namespace = route.metadata.namespace.as_deref().unwrap_or_default();
        let rules = rules
            .iter()
            .map(|rule| Rule {
                matches: match &rule.matches {
                    Some(matches) => matches.iter().map(http_match).collect(),
                    None => vec![Http::default()],
                },
                upstream: resolve_backend(
                    self,
                    policies,
                    namespace,
                    rule.backend_refs.as_ref().and_then(|refs| refs.first()),
                ),
            })
            .collect();
        Ok(Backend { rules })
    }
}

fn unsupported_rule(rule: &HttpRouteRules) -> bool {
    rule.filters.as_ref().is_some_and(|filters| !filters.is_empty())
        || rule.retry.is_some()
        || rule.timeouts.is_some()
        || rule.backend_refs.as_ref().is_some_and(|refs| {
            refs.len() > 1
                || refs.iter().any(|backend| {
                    backend.filters.as_ref().is_some_and(|filters| !filters.is_empty())
                        || backend.weight.is_some_and(|weight| weight != 1)
                })
        })
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
    backend: Option<&HttpRouteRulesBackendRefs>,
) -> Result<ClusterSpec, RefError> {
    let backend = backend.ok_or(RefError::BackendNotFound)?;
    if !backend.group.as_deref().unwrap_or("").is_empty()
        || backend.kind.as_deref().unwrap_or("Service") != "Service"
    {
        return Err(RefError::InvalidKind);
    }
    let target = backend.namespace.as_deref().unwrap_or(namespace);
    if !stores.permits(namespace, "HTTPRoute", target, "Service", &backend.name) {
        return Err(RefError::RefNotPermitted);
    }
    let namespace = target;
    let backend_port = backend.port.ok_or(RefError::BackendNotFound)?;
    let key = ObjectRef::new(&backend.name).within(namespace);
    let service = stores
        .services
        .get(&key)
        .filter(|service| service.metadata.deletion_timestamp.is_none())
        .ok_or(RefError::BackendNotFound)?;
    let port = service
        .spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|ports| {
            ports.iter().find(|candidate| {
                candidate.port == i32::from(backend_port)
                    && candidate.protocol.as_deref().unwrap_or("TCP") == "TCP"
            })
        })
        .ok_or(RefError::BackendNotFound)?;
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
    let mut endpoints = vec![];
    for slice in stores.endpoint_slices.state() {
        if slice.namespace().as_deref() != Some(namespace)
            || slice.metadata.deletion_timestamp.is_some()
            || slice.labels().get("kubernetes.io/service-name") != Some(&backend.name)
            || !matches!(slice.address_type.as_str(), "IPv4" | "IPv6")
        {
            continue;
        }
        for slice_port in slice.ports.iter().flatten().filter(|candidate| {
            candidate.name.as_deref().unwrap_or("") == port.name.as_deref().unwrap_or("")
                && candidate.protocol.as_deref().unwrap_or("TCP") == "TCP"
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
    Ok(ClusterSpec { endpoints, protocol, tls })
}
