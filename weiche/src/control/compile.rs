//! compile attached routes independently for each listener.
use std::{net::SocketAddr, sync::Arc};

use super::super::ir::{Route, StreamRoute, Transport, hostname_matches};
use super::{
    GatewayStores, Resolve, ResolveError,
    tls::{Policies, TlsError, listener_tls},
};
use kube::{Resource, ResourceExt};
use serde::Serialize;
use weiche_gtwapi::v1::{
    gateways::{
        Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom as From,
    },
    httproutes::{HttpRoute, HttpRouteParentRefs},
    tcproutes::{TcpRoute, TcpRouteParentRefs},
    udproutes::{UdpRoute, UdpRouteParentRefs},
};

/// gateway class controller owned by this process.
pub const CONTROLLER_NAME: &str = "weiche.io/gateway-controller";

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
    pub uid: Option<String>,
    pub parent: serde_json::Value,
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
    pub conflict: Option<&'static str>,
    pub supported_kind: Option<&'static str>,
    pub stream: Option<StreamRoute>,
    pub transport: Transport,
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
    let transport = Transport::for_protocol(&l.protocol);
    let raw = matches!(l.protocol.as_str(), "TCP" | "UDP");
    let route_kind = match l.protocol.as_str() {
        "HTTP" | "HTTPS" => Some("HTTPRoute"),
        "TCP" | "UDP" => Some(transport.route_kind()),
        _ => None,
    };
    let supports = |group: Option<&str>, kind: &str| {
        group.unwrap_or("gateway.networking.k8s.io") == "gateway.networking.k8s.io"
            && Some(kind) == route_kind
    };
    let supported_kind = route_kind.filter(|_| {
        kinds.is_none_or(|ks| ks.iter().any(|k| supports(k.group.as_deref(), &k.kind)))
    });
    let mut result = Compiled {
        policies: Policies::default(),
        certificate_error: None,
        address,
        tls: None,
        conflict: None,
        supported_kind,
        transport,
        stream: None,
        routes: vec![],
        reports: vec![],
        accepted: false,
        reason: "Invalid",
        message: "selected Gateway or listener is unavailable".into(),
    };
    result.conflict = gateway.spec.listeners.iter().find_map(|other| {
        if other.name == l.name
            || other.port != l.port
            || Transport::for_protocol(&other.protocol) != transport
        {
            return None;
        }
        if other.protocol != l.protocol || raw {
            Some("ProtocolConflict")
        } else if other.hostname == l.hostname {
            Some("HostnameConflict")
        } else {
            None
        }
    });
    if result.conflict.is_some() && route_kind.is_some() {
        result.reason = "Conflicted";
        result.message =
            "listeners have conflicting protocols or hostnames on this port".into();
    } else if gateway.spec.tls.is_some() {
        result.message = "unsupported gateway-wide TLS options".into();
    } else if raw {
        if l.tls.is_none() && l.hostname.as_deref().unwrap_or("").is_empty() {
            result.accepted = true;
            result.reason = "Accepted";
            result.message = format!("{} listener is valid", l.protocol);
        } else {
            result.message = "raw transport listeners require an empty hostname and no TLS configuration".into();
        }
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
    if route_kind.is_some()
        && kinds
            .is_some_and(|ks| ks.iter().any(|k| !supports(k.group.as_deref(), &k.kind)))
    {
        result.certificate_error = Some(TlsError {
            reason: "InvalidRouteKinds",
            message: "listener contains unsupported Route kinds".into(),
        });
    }
    collect(stores.http_routes.state(), stores, gateway, l, &mut result);
    collect(stores.tcp_routes.state(), stores, gateway, l, &mut result);
    collect(stores.udp_routes.state(), stores, gateway, l, &mut result);
    result
}

/// expose common attachment fields without coupling generated resource types to each other.
trait Routed: Resource<DynamicType = ()> {
    type Parent: Serialize;
    fn parents(&self) -> &[Self::Parent];
    fn parent(p: &Self::Parent) -> Parent<'_>;
    fn bind(
        &self,
        stores: &GatewayStores,
        listener: &GatewayListeners,
        compiled: &mut Compiled,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError>;
}
struct Parent<'a> {
    group: Option<&'a str>,
    kind: Option<&'a str>,
    namespace: Option<&'a str>,
    name: &'a str,
    section: Option<&'a str>,
    port: Option<u16>,
}
macro_rules! parents {
    ($parent:ty) => {
        type Parent = $parent;
        fn parents(&self) -> &[Self::Parent] {
            self.spec.parent_refs.as_deref().unwrap_or_default()
        }
        fn parent(p: &Self::Parent) -> Parent<'_> {
            Parent {
                group: p.group.as_deref(),
                kind: p.kind.as_deref(),
                namespace: p.namespace.as_deref(),
                name: &p.name,
                section: p.section_name.as_deref(),
                port: p.port,
            }
        }
    };
}
impl RouteReport {
    fn reject(&mut self, reason: &'static str, message: &str) {
        self.accepted = false;
        self.reason = reason;
        self.message = message.into();
    }
    fn references<T>(
        &mut self,
        backends: &[(u32, Result<T, super::super::ir::RefError>)],
    ) {
        if let Some(error) = backends.iter().find_map(|(_, r)| r.as_ref().err()) {
            self.resolved = false;
            self.refs_reason = error.reason();
        }
    }
}
impl Routed for HttpRoute {
    parents!(HttpRouteParentRefs);
    fn bind(
        &self,
        stores: &GatewayStores,
        l: &GatewayListeners,
        result: &mut Compiled,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError> {
        let hosts: Vec<_> = self
            .spec
            .hostnames
            .as_deref()
            .unwrap_or(&[String::new()])
            .iter()
            .filter_map(|h| intersection(l.hostname.as_deref().unwrap_or(""), h))
            .collect();
        if hosts.is_empty() {
            report.reject(
                "NoMatchingListenerHostname",
                "route and listener hostnames do not intersect",
            );
            return Ok(());
        }
        for rule in stores.resolve_http(self, &mut result.policies)?.rules {
            report.references(&rule.action.backends);
            for http in rule.matches {
                for hostname in &hosts {
                    result.routes.push(Route {
                        hostname: hostname.clone(),
                        http: http.clone(),
                        action: rule.action.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}
macro_rules! stream_route {
    ($route:ty,$parent:ty) => {
        impl Routed for $route {
            parents!($parent);
            fn bind(
                &self,
                stores: &GatewayStores,
                _: &GatewayListeners,
                result: &mut Compiled,
                report: &mut RouteReport,
            ) -> Result<(), ResolveError> {
                let backend = stores.resolve(self)?;
                report.references(&backend.backends);
                // routes arrive oldest first. every allowed parent receives a status entry.
                result.stream.get_or_insert(backend);
                Ok(())
            }
        }
    };
}
stream_route!(TcpRoute, TcpRouteParentRefs);
stream_route!(UdpRoute, UdpRouteParentRefs);

fn collect<R: Routed>(
    mut routes: Vec<Arc<R>>,
    stores: &GatewayStores,
    gateway: &Gateway,
    l: &GatewayListeners,
    result: &mut Compiled,
) {
    let namespace = gateway.namespace().unwrap_or_default();
    let kind = R::kind(&());
    routes.sort_by_key(|r| {
        (r.meta().creation_timestamp.clone(), r.namespace(), r.name_any())
    });
    for route in routes {
        let source = route.namespace().unwrap_or_default();
        for reference in route.parents() {
            let p = R::parent(reference);
            if p.group.unwrap_or("gateway.networking.k8s.io")
                != "gateway.networking.k8s.io"
                || p.kind.unwrap_or("Gateway") != "Gateway"
                || p.name != gateway.name_any()
                || p.namespace.unwrap_or(&source) != namespace
            {
                continue;
            }
            let mut report = RouteReport {
                uid: route.uid(),
                parent: serde_json::to_value(reference)
                    .expect("parent reference serializes"),
                accepted: true,
                reason: "Accepted",
                message: "route attached to listener".into(),
                resolved: true,
                refs_reason: "ResolvedRefs",
            };
            if p.section.is_some_and(|n| n != l.name)
                || p.port.is_some_and(|port| port != l.port)
            {
                report.reject("NoMatchingParent", "listener does not match parentRef");
            } else if result.supported_kind != Some(kind.as_ref())
                || !allows_route(stores, l, &namespace, &source)
            {
                report.reject(
                    "NotAllowedByListeners",
                    "listener does not allow this route namespace or kind",
                );
            } else if route.bind(stores, l, result, &mut report).is_err() {
                report.reject("UnsupportedValue", "unsupported route rule");
            }
            result.reports.push(report);
        }
    }
}

fn allows_route(
    stores: &GatewayStores,
    l: &GatewayListeners,
    namespace: &str,
    source: &str,
) -> bool {
    let Some(allowed) = &l.allowed_routes else {
        return source == namespace;
    };
    match allowed.namespaces.as_ref().and_then(|n| n.from.as_ref()) {
        Some(From::All) => true,
        Some(From::Selector) => {
            let Some(selector) =
                allowed.namespaces.as_ref().and_then(|n| n.selector.as_ref())
            else {
                return false;
            };
            let Some(ns) = stores
                .namespaces
                .get(&kube::runtime::reflector::ObjectRef::new(source))
            else {
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
        _ => source == namespace,
    }
}
