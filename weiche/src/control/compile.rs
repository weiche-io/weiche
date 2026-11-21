//! compile attached HTTP routes independently for each listener.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use super::super::ir::{Route, hostname_matches};
use super::{
    GatewayStores, ResolveError,
    tls::{Policies, TlsError, listener_tls},
};
use kube::ResourceExt;
use weiche_gtwapi::v1::{
    gateways::{
        Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom as From,
    },
    httproutes::{HttpRoute, HttpRouteParentRefs},
};

/// gateway class controller owned by this process.
pub const CONTROLLER_NAME: &str = "weiche.io/gateway-controller";

/// gateway and listener selected for this gateway instance.
#[derive(Clone, Debug)]
pub struct Selection {
    pub namespace: String,
    pub gateway: String,
    pub managed: bool,
    pub bind_ip: IpAddr,
}

impl Selection {
    /// read the listener selection and bind address from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            namespace: std::env::var("WEICHE_NAMESPACE")
                .unwrap_or_else(|_| "weiche-demo".into()),
            gateway: std::env::var("WEICHE_GATEWAY").unwrap_or_else(|_| "weiche".into()),
            managed: std::env::var("WEICHE_MANAGE_GATEWAYS")
                .map_or(Ok(false), |v| v.parse())?,
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
    pub address: SocketAddr,
    pub tls: Option<Arc<super::super::ir::ServerTls>>,
    pub supported_http: bool,
    pub routes: Vec<Route>,
    pub reports: Vec<RouteReport>,
    pub accepted: bool,
    pub reason: &'static str,
    pub message: String,
}

/// bind resolved routes to the selected listener and assemble the next snapshot.
pub fn compile(
    stores: &GatewayStores,
    gateway: &Gateway,
    l: &GatewayListeners,
    address: SocketAddr,
) -> Compiled {
    let namespace = gateway.namespace().unwrap_or_default();
    let kinds = l.allowed_routes.as_ref().and_then(|r| r.kinds.as_ref());
    let supports = |group: Option<&str>, kind: &str| {
        group.unwrap_or("gateway.networking.k8s.io") == "gateway.networking.k8s.io"
            && kind == "HTTPRoute"
    };
    let http = matches!(l.protocol.as_str(), "HTTP" | "HTTPS");
    let supported_http = http
        && kinds
            .is_none_or(|ks| ks.iter().any(|k| supports(k.group.as_deref(), &k.kind)));
    let mut result = Compiled {
        policies: Policies::default(),
        certificate_error: None,
        address,
        tls: None,
        supported_http,
        routes: vec![],
        reports: vec![],
        accepted: false,
        reason: "Invalid",
        message: "selected Gateway or listener is unavailable".into(),
    };
    let conflict = gateway.spec.listeners.iter().any(|other| {
        other.name != l.name
            && other.port == l.port
            && (other.protocol != l.protocol || other.hostname == l.hostname)
    });
    if conflict && http {
        result.reason = "Conflicted";
        result.message =
            "listeners have conflicting protocols or hostnames on this port".into();
    } else if gateway.spec.tls.is_some() {
        result.message = "unsupported gateway-wide TLS options".into();
    } else {
        match listener_tls(stores, &namespace, l) {
            Ok(tls) => {
                result.accepted = true;
                result.reason = "Accepted";
                result.message = format!("selected {} listener is valid", l.protocol);
                result.tls = tls;
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
    if http
        && kinds
            .is_some_and(|ks| ks.iter().any(|k| !supports(k.group.as_deref(), &k.kind)))
    {
        result.certificate_error = Some(TlsError {
            reason: "InvalidRouteKinds",
            message: "listener contains unsupported Route kinds".into(),
        });
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
                || parent.name != gateway.name_any()
                || parent.namespace.as_deref().or(route.metadata.namespace.as_deref())
                    != Some(&namespace)
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
            if http
                && parent.section_name.as_deref().is_none_or(|name| name == l.name)
                && parent.port.is_none_or(|port| port == l.port)
            {
                if !allows_route(stores, l, &namespace, &route) {
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
                                report.message =
                                    "unsupported HTTPRoute rule field or filter".into();
                            }
                            Ok(backend) => {
                                report.accepted = true;
                                report.reason = "Accepted";
                                report.message =
                                    "route attached to the selected listener".into();
                                for rule in backend.rules {
                                    let action = rule.action;
                                    for (_, target) in &action.backends {
                                        if let Err(error) = target {
                                            report.resolved = false;
                                            report.refs_reason = error.reason();
                                        }
                                    }
                                    for http in rule.matches {
                                        for hostname in &hosts {
                                            result.routes.push(Route {
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
