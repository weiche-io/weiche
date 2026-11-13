//! compile a deliberately small HTTP Gateway API subset into immutable snapshots.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use super::{
    cluster::{ClusterId, ClusterSpec},
    controller::GatewayStores,
    router::{Route, RouteAction, Router, hostname_matches},
    snapshot::GatewaySnapshot,
};
use kube::ResourceExt;
use weiche_gtwapi::v1::{
    gateways::GatewayListeners,
    httproutes::{HttpRoute, HttpRouteParentRefs, HttpRouteRules, HttpRouteRulesBackendRefs},
};

pub const CONTROLLER_NAME: &str = "weiche.io/gateway-controller";

#[derive(Clone, Debug)]
pub struct Selection {
    pub namespace: String,
    pub gateway: String,
    pub listener: String,
    pub bind_ip: IpAddr,
}

impl Selection {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            namespace: std::env::var("WEICHE_NAMESPACE").unwrap_or_else(|_| "weiche-demo".into()),
            gateway: std::env::var("WEICHE_GATEWAY").unwrap_or_else(|_| "weiche".into()),
            listener: std::env::var("WEICHE_LISTENER").unwrap_or_else(|_| "http".into()),
            bind_ip: std::env::var("WEICHE_BIND_IP")
                .unwrap_or_else(|_| "0.0.0.0".into())
                .parse()?,
        })
    }
}

fn intersection(a: &str, b: &str) -> Option<String> {
    if a.is_empty() {
        return Some(b.into());
    }
    if b.is_empty() {
        return Some(a.into());
    }
    if a == b {
        return Some(a.into());
    }
    if !a.starts_with("*.") && hostname_matches(b, a) {
        return Some(a.into());
    }
    if !b.starts_with("*.") && hostname_matches(a, b) {
        return Some(b.into());
    }
    if a.starts_with("*.") && b.starts_with("*.") {
        if hostname_matches(a, &b[2..]) {
            return Some(b.into());
        }
        if hostname_matches(b, &a[2..]) {
            return Some(a.into());
        }
    }
    None
}

#[derive(Clone, Debug)]
pub struct RouteReport {
    pub route: Arc<HttpRoute>,
    pub parent: HttpRouteParentRefs,
    pub accepted: bool,
    pub reason: &'static str,
    pub resolved: bool,
    pub refs_reason: &'static str,
    pub message: String,
}

pub struct Compiled {
    pub snapshot: GatewaySnapshot,
    pub reports: Vec<RouteReport>,
    pub accepted: bool,
    pub reason: &'static str,
    pub message: String,
}

pub fn compile(stores: &GatewayStores, selection: &Selection) -> Compiled {
    let mut result = Compiled {
        snapshot: GatewaySnapshot::default(),
        reports: vec![],
        accepted: false,
        reason: "Invalid",
        message: "selected Gateway or listener is unavailable".into(),
    };
    let Some(gateway) = stores.gateways.state().into_iter().find(|g| {
        g.namespace().as_deref() == Some(&selection.namespace)
            && g.name_any() == selection.gateway
            && g.metadata.deletion_timestamp.is_none()
    }) else {
        return result;
    };
    let Some(class) = stores.gateway_classes.state().into_iter().find(|c| {
        c.name_any() == gateway.spec.gateway_class_name && c.metadata.deletion_timestamp.is_none()
    }) else {
        return result;
    };
    if class.spec.controller_name != CONTROLLER_NAME {
        return result;
    }
    let listener = gateway
        .spec
        .listeners
        .iter()
        .find(|l| l.name == selection.listener);
    if class.spec.parameters_ref.is_some()
        || gateway.spec.tls.is_some()
        || gateway.spec.listeners.len() != 1
    {
        result.message =
            "demo requires one listener, no class parameters and no Gateway TLS".into();
    } else if let Some(l) = listener {
        if l.protocol == "HTTP" && l.tls.is_none() && (1..=65535).contains(&l.port) {
            result.accepted = true;
            result.reason = "Accepted";
            result.message = "selected HTTP listener is valid".into();
            result.snapshot.listener = Some(SocketAddr::new(selection.bind_ip, l.port as u16));
        } else {
            result.reason = "UnsupportedProtocol";
            result.message = "only plaintext HTTP listeners are supported".into();
        }
    }
    let mut compiled_routes = Vec::new();
    let mut routes = stores.http_routes.state();
    routes.sort_by_key(|r| {
        (
            r.metadata.creation_timestamp.clone(),
            r.namespace(),
            r.name_any(),
        )
    });
    for route in routes {
        for parent in route.spec.parent_refs.iter().flatten() {
            if parent
                .group
                .as_deref()
                .unwrap_or("gateway.networking.k8s.io")
                != "gateway.networking.k8s.io"
                || parent.kind.as_deref().unwrap_or("Gateway") != "Gateway"
                || parent.name != selection.gateway
                || parent
                    .namespace
                    .as_deref()
                    .or(route.metadata.namespace.as_deref())
                    != Some(&selection.namespace)
            {
                continue;
            }
            let mut report = RouteReport {
                route: route.clone(),
                parent: parent.clone(),
                accepted: false,
                reason: "NoMatchingParent",
                resolved: true,
                refs_reason: "ResolvedRefs",
                message: "selected listener does not match parentRef".into(),
            };
            if result.accepted
                && let Some(l) = listener
                && parent
                    .section_name
                    .as_deref()
                    .is_none_or(|name| name == l.name)
                && parent.port.is_none_or(|port| port == l.port)
            {
                if !allows_route(l, &selection.namespace, &route) {
                    report.reason = "NotAllowedByListeners";
                    report.message = "listener does not allow this route namespace or kind".into();
                } else {
                    let hosts = route
                        .spec
                        .hostnames
                        .clone()
                        .unwrap_or_else(|| vec![String::new()]);
                    let hosts: Vec<_> = hosts
                        .iter()
                        .filter_map(|h| intersection(l.hostname.as_deref().unwrap_or(""), h))
                        .collect();
                    if hosts.is_empty() {
                        report.reason = "NoMatchingListenerHostname";
                        report.message = "route and listener hostnames do not intersect".into();
                    } else if route.spec.rules.iter().flatten().any(unsupported_rule) {
                        report.reason = "UnsupportedValue";
                        report.message = "unsupported rule: use Exact/PathPrefix, exact headers, method and one same-namespace HTTP Service backend".into();
                    } else {
                        report.accepted = true;
                        report.reason = "Accepted";
                        report.message = "route attached to the selected listener".into();
                        for rule in route.spec.rules.iter().flatten() {
                            let (endpoints, invalid, refs_reason) = resolve_backend(
                                stores,
                                &route,
                                rule.backend_refs.as_ref().and_then(|b| b.first()),
                            );
                            if invalid {
                                report.resolved = false;
                                report.refs_reason = refs_reason;
                            }
                            let action = if invalid {
                                RouteAction::RejectInvalidBackend
                            } else {
                                let backend = &rule.backend_refs.as_ref().unwrap()[0];
                                let id = ClusterId {
                                    namespace: route.namespace().unwrap_or_default(),
                                    service: backend.name.clone(),
                                    port: backend.port.unwrap() as u16,
                                };
                                result
                                    .snapshot
                                    .clusters
                                    .insert(id.clone(), ClusterSpec { endpoints });
                                RouteAction::Forward(id)
                            };
                            let matches = rule.matches.clone().unwrap_or_else(|| {
                                vec![weiche_gtwapi::v1::httproutes::HttpRouteRulesMatches {
                                    headers: None,
                                    method: None,
                                    path: None,
                                    query_params: None,
                                }]
                            });
                            for m in matches {
                                let exact = m.path.as_ref().and_then(|p| p.r#type.as_ref()).is_some_and(|t| matches!(t, weiche_gtwapi::v1::httproutes::HttpRouteRulesMatchesPathType::Exact));
                                let raw_path = m
                                    .path
                                    .as_ref()
                                    .and_then(|p| p.value.as_deref())
                                    .unwrap_or("/");
                                let path = if exact {
                                    raw_path
                                } else {
                                    raw_path.trim_end_matches('/')
                                };
                                for hostname in &hosts {
                                    compiled_routes.push(Route {
                                        hostname: hostname.clone(),
                                        path: if path.is_empty() {
                                            "/".into()
                                        } else {
                                            path.into()
                                        },
                                        exact,
                                        method: m.method.as_ref().map(|m| {
                                            serde_json::to_value(m)
                                                .unwrap()
                                                .as_str()
                                                .unwrap()
                                                .into()
                                        }),
                                        headers: m
                                            .headers
                                            .iter()
                                            .flatten()
                                            .map(|h| (h.name.to_ascii_lowercase(), h.value.clone()))
                                            .collect(),
                                        action: action.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            result.reports.push(report);
        }
    }
    result.snapshot.router = Router::compile(compiled_routes);
    result
}

fn allows_route(l: &GatewayListeners, namespace: &str, route: &HttpRoute) -> bool {
    let Some(allowed) = &l.allowed_routes else {
        return route.namespace().as_deref() == Some(namespace);
    };
    if allowed.kinds.as_ref().is_some_and(|kinds| {
        !kinds.iter().any(|k| {
            k.kind == "HTTPRoute"
                && k.group.as_deref().unwrap_or("gateway.networking.k8s.io")
                    == "gateway.networking.k8s.io"
        })
    }) {
        return false;
    }
    use weiche_gtwapi::v1::gateways::GatewayListenersAllowedRoutesNamespacesFrom as From;
    match allowed.namespaces.as_ref().and_then(|n| n.from.as_ref()) {
        Some(From::All) => true,
        Some(From::Selector) => false,
        _ => route.namespace().as_deref() == Some(namespace),
    }
}

fn unsupported_rule(rule: &HttpRouteRules) -> bool {
    rule.filters.as_ref().is_some_and(|f| !f.is_empty()) || rule.retry.is_some() || rule.timeouts.is_some()
        || rule.backend_refs.as_ref().is_some_and(|refs| refs.len() > 1 || refs.iter().any(|b| b.filters.as_ref().is_some_and(|f| !f.is_empty()) || b.weight.is_some_and(|w| w != 1)))
        || rule.matches.iter().flatten().any(|m| {
            m.query_params.as_ref().is_some_and(|q| !q.is_empty())
                || m.path.as_ref().and_then(|p| p.r#type.as_ref()).is_some_and(|t| matches!(t, weiche_gtwapi::v1::httproutes::HttpRouteRulesMatchesPathType::RegularExpression))
                || m.headers.iter().flatten().any(|h| h.r#type.as_ref().is_some_and(|t| matches!(t, weiche_gtwapi::v1::httproutes::HttpRouteRulesMatchesHeadersType::RegularExpression)))
        })
}

fn resolve_backend(
    stores: &GatewayStores,
    route: &HttpRoute,
    backend: Option<&HttpRouteRulesBackendRefs>,
) -> (Vec<SocketAddr>, bool, &'static str) {
    let Some(b) = backend else {
        return (vec![], true, "BackendNotFound");
    };
    if !b.group.as_deref().unwrap_or("").is_empty()
        || b.kind.as_deref().unwrap_or("Service") != "Service"
    {
        return (vec![], true, "InvalidKind");
    }
    let ns = route.namespace().unwrap_or_default();
    if b.namespace.as_deref().is_some_and(|n| n != ns) {
        return (vec![], true, "RefNotPermitted");
    }
    let Some(service) = stores.services.state().into_iter().find(|s| {
        s.namespace().as_deref() == Some(&ns)
            && s.name_any() == b.name
            && s.metadata.deletion_timestamp.is_none()
    }) else {
        return (vec![], true, "BackendNotFound");
    };
    let Some(port) = service
        .spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|ports| {
            ports
                .iter()
                .find(|p| Some(p.port) == b.port && p.protocol.as_deref().unwrap_or("TCP") == "TCP")
        })
    else {
        return (vec![], true, "BackendNotFound");
    };
    if port
        .app_protocol
        .as_deref()
        .is_some_and(|p| p != "http" && p != "kubernetes.io/http")
    {
        return (vec![], true, "UnsupportedProtocol");
    }
    let mut endpoints = vec![];
    for slice in stores.endpoint_slices.state() {
        if slice.namespace().as_deref() != Some(&ns)
            || slice.metadata.deletion_timestamp.is_some()
            || slice.labels().get("kubernetes.io/service-name") != Some(&b.name)
            || !matches!(slice.address_type.as_str(), "IPv4" | "IPv6")
        {
            continue;
        }
        for p in slice.ports.iter().flatten().filter(|p| {
            p.name.as_deref().unwrap_or("") == port.name.as_deref().unwrap_or("")
                && p.protocol.as_deref().unwrap_or("TCP") == "TCP"
        }) {
            let Some(endpoint_port) = p.port.filter(|p| (1..=65535).contains(p)) else {
                continue;
            };
            for endpoint in slice.endpoints.iter().flatten() {
                let conditions = endpoint.conditions.as_ref();
                if conditions.is_some_and(|c| {
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
    endpoints.sort();
    endpoints.dedup();
    (endpoints, false, "ResolvedRefs")
}
