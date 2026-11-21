//! compile a deliberately small HTTP Gateway API subset into immutable snapshots.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use super::super::ir::{GatewaySnapshot, Listener, Route, RouteAction, hostname_matches};
use super::{
    GatewayStores, ResolveError,
    tls::{Policies, TlsError, listener_tls},
};
use kube::ResourceExt;
use weiche_gtwapi::v1::{
    gateways::{GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom as From},
    httproutes::{HttpRoute, HttpRouteParentRefs},
};

/// gateway class controller owned by this process.
pub const CONTROLLER_NAME: &str = "weiche.io/gateway-controller";

/// gateway and listener selected for this gateway instance.
#[derive(Clone, Debug)]
pub struct Selection {
    pub namespace: String,
    pub gateway: String,
    pub listener: String,
    pub bind_ip: IpAddr,
}

impl Selection {
    /// read the listener selection and bind address from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            namespace: std::env::var("WEICHE_NAMESPACE")
                .unwrap_or_else(|_| "weiche-demo".into()),
            gateway: std::env::var("WEICHE_GATEWAY").unwrap_or_else(|_| "weiche".into()),
            listener: std::env::var("WEICHE_LISTENER").unwrap_or_else(|_| "http".into()),
            bind_ip: std::env::var("WEICHE_BIND_IP")
                .unwrap_or_else(|_| "::".into())
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

/// reconciliation status for one route parent.
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

/// data plane snapshot and corresponding control plane status.
pub struct Compiled {
    pub policies: Policies,
    pub certificate_error: Option<TlsError>,
    pub snapshot: GatewaySnapshot,
    pub reports: Vec<RouteReport>,
    pub accepted: bool,
    pub reason: &'static str,
    pub message: String,
}

/// bind resolved routes to the selected listener and assemble the next snapshot.
pub fn compile(stores: &GatewayStores, selection: &Selection) -> Compiled {
    let mut result = Compiled {
        policies: Policies::default(),
        certificate_error: None,
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
        c.name_any() == gateway.spec.gateway_class_name
            && c.metadata.deletion_timestamp.is_none()
    }) else {
        return result;
    };
    if class.spec.controller_name != CONTROLLER_NAME {
        return result;
    }
    let listener = gateway.spec.listeners.iter().find(|l| l.name == selection.listener);
    if class.spec.parameters_ref.is_some()
        || gateway.spec.tls.is_some()
        || gateway.spec.listeners.len() != 1
    {
        result.message =
            "expected one listener without class parameters or gateway-wide TLS options"
                .into();
    } else if let Some(l) = listener {
        match listener_tls(stores, &selection.namespace, l) {
            Ok(tls) => {
                result.accepted = true;
                result.reason = "Accepted";
                result.message = format!("selected {} listener is valid", l.protocol);
                result.snapshot.listener = Some(Listener {
                    address: SocketAddr::new(selection.bind_ip, l.port),
                    tls,
                });
            }
            Err(error) => {
                result.message = error.message.clone();
                if matches!(error.reason, "InvalidCertificateRef" | "RefNotPermitted") {
                    result.certificate_error = Some(error);
                } else {
                    result.reason = error.reason;
                }
            }
        }
    }
    let mut routes = stores.http_routes.state();
    routes.sort_by_key(|r| {
        (r.metadata.creation_timestamp.clone(), r.namespace(), r.name_any())
    });
    for route in routes {
        for parent in route.spec.parent_refs.iter().flatten() {
            if parent.group.as_deref().unwrap_or("gateway.networking.k8s.io")
                != "gateway.networking.k8s.io"
                || parent.kind.as_deref().unwrap_or("Gateway") != "Gateway"
                || parent.name != selection.gateway
                || parent.namespace.as_deref().or(route.metadata.namespace.as_deref())
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
                && parent.section_name.as_deref().is_none_or(|name| name == l.name)
                && parent.port.is_none_or(|port| port == l.port)
            {
                if !allows_route(stores, l, &selection.namespace, &route) {
                    report.reason = "NotAllowedByListeners";
                    report.message =
                        "listener does not allow this route namespace or kind".into();
                } else {
                    let hosts: Vec<_> = route
                        .spec
                        .hostnames
                        .as_deref()
                        .unwrap_or(&[String::new()])
                        .iter()
                        .filter_map(|h| {
                            intersection(l.hostname.as_deref().unwrap_or(""), h)
                        })
                        .collect();
                    if hosts.is_empty() {
                        report.reason = "NoMatchingListenerHostname";
                        report.message =
                            "route and listener hostnames do not intersect".into();
                    } else {
                        match stores.resolve_http(&route, &mut result.policies) {
                            Err(ResolveError::UnsupportedValue) => {
                                report.reason = "UnsupportedValue";
                                report.message = "unsupported rule: use Exact/PathPrefix, exact headers, method and one same-namespace HTTP Service backend".into();
                            }
                            Ok(backend) => {
                                report.accepted = true;
                                report.reason = "Accepted";
                                report.message =
                                    "route attached to the selected listener".into();
                                for rule in backend.rules {
                                    let action = match rule.upstream {
                                        Ok(cluster) => {
                                            RouteAction::Forward(Arc::new(cluster))
                                        }
                                        Err(error) => {
                                            report.resolved = false;
                                            report.refs_reason = error.reason();
                                            RouteAction::RejectInvalidBackend
                                        }
                                    };
                                    for http in rule.matches {
                                        for hostname in &hosts {
                                            result.snapshot.routes.push(Route {
                                                hostname: hostname.clone(),
                                                http: http.clone(),
                                                action: action.clone(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            result.reports.push(report);
        }
    }
    result
}

fn allows_route(
    stores: &GatewayStores,
    l: &GatewayListeners,
    namespace: &str,
    route: &HttpRoute,
) -> bool {
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
    match allowed.namespaces.as_ref().and_then(|n| n.from.as_ref()) {
        Some(From::All) => true,
        Some(From::Selector) => {
            let Some(selector) =
                allowed.namespaces.as_ref().and_then(|n| n.selector.as_ref())
            else {
                return false;
            };
            let Some(ns) = route.namespace().and_then(|name| {
                stores
                    .namespaces
                    .get(&kube::runtime::reflector::ObjectRef::new(&name))
            }) else {
                return false;
            };
            let labels = ns.labels();
            selector
                .match_labels
                .iter()
                .flatten()
                .all(|(k, v)| labels.get(k) == Some(v))
                && selector.match_expressions.iter().flatten().all(|e| {
                    let value = labels.get(&e.key);
                    let listed =
                        value.is_some_and(|v| e.values.iter().flatten().any(|w| w == v));
                    match e.operator.as_str() {
                        "In" => listed,
                        "NotIn" => !listed,
                        "Exists" => value.is_some(),
                        "DoesNotExist" => value.is_none(),
                        _ => false,
                    }
                })
        }
        _ => route.namespace().as_deref() == Some(namespace),
    }
}
