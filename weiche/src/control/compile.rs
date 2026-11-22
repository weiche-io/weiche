//! compile attached routes independently for each listener.
use std::{net::SocketAddr, sync::Arc};

use super::super::ir::{
    RefError, Route, RouteHost, ServerTls, SniRoutes, StreamRoute, Transport,
    hostname_matches,
};
use super::{
    GatewayStores, ResolveError,
    resolve::HttpRule,
    tls::{Policies, TlsError, listener_tls, passthrough},
};
use kube::{Resource, ResourceExt};
use serde::Serialize;
use weiche_gtwapi::v1::{
    gateways::{
        Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom as From,
    },
    grpcroutes::{GrpcRoute, GrpcRouteParentRefs},
    httproutes::{HttpRoute, HttpRouteParentRefs},
    tcproutes::{TcpRoute, TcpRouteParentRefs},
    tlsroutes::{TlsRoute, TlsRouteParentRefs},
    udproutes::{UdpRoute, UdpRouteParentRefs},
};

/// gateway class controller owned by this process.
pub const CONTROLLER_NAME: &str = "weiche.io/gateway-controller";

/// reconciliation status for one route parent.
#[derive(Clone, Debug)]
pub struct RouteReport {
    /// route UID used to associate reports with the current resource.
    pub uid: Option<String>,
    /// original parent reference for status publication.
    pub parent: serde_json::Value,
    /// whether the controller accepted this attachment or listener.
    pub accepted: bool,
    /// gateway API reason for the condition.
    pub reason: &'static str,
    /// whether all backend references resolved.
    pub resolved: bool,
    /// reason for the ResolvedRefs condition.
    pub refs_reason: &'static str,
    /// explanation required when valid rules survive alongside invalid rules.
    pub partially_invalid: Option<String>,
    /// description published with the condition.
    pub message: String,
}

/// data plane snapshot and corresponding control plane status.
pub struct Compiled {
    /// backend TLS policy results referenced by this listener.
    pub policies: Policies,
    /// listener reference validation failure.
    pub refs_error: Option<TlsError>,
    /// local listening address.
    pub address: SocketAddr,
    /// terminating TLS configuration when configured.
    pub tls: Option<Arc<ServerTls>>,
    /// protocol or hostname collision on the listener port.
    pub conflict: Option<&'static str>,
    /// route kinds permitted by the protocol and listener policy.
    pub supported_kinds: Vec<&'static str>,
    /// selected raw transport route.
    pub stream: Option<StreamRoute>,
    /// earliest attached stream route per intersecting route hostname.
    pub sni_routes: SniRoutes,
    /// underlying socket protocol.
    pub transport: Transport,
    /// resolved HTTP routes in attachment precedence order.
    pub routes: Vec<Route>,
    /// conditions collected for the matching route parents.
    pub reports: Vec<RouteReport>,
    /// whether the controller accepted this attachment or listener.
    pub accepted: bool,
    /// gateway API reason for the condition.
    pub reason: &'static str,
    /// description published with the condition.
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
    let kinds = l
        .allowed_routes
        .as_ref()
        .and_then(|r| r.kinds.as_deref())
        .unwrap_or_default();
    let transport = Transport::for_protocol(&l.protocol);
    let raw = matches!(l.protocol.as_str(), "TCP" | "UDP");
    let protocol_kinds: &[&str] = match l.protocol.as_str() {
        "HTTP" | "HTTPS" => &["HTTPRoute", "GRPCRoute"],
        "TCP" => &["TCPRoute"],
        "TLS" if passthrough(l) => &["TLSRoute"],
        "UDP" => &["UDPRoute"],
        _ => &[],
    };
    let mut invalid_kinds = false;
    let supported_kinds = if kinds.is_empty() {
        protocol_kinds.to_vec()
    } else {
        let mut supported = Vec::new();
        for k in kinds {
            if k.group.as_deref().unwrap_or("gateway.networking.k8s.io")
                == "gateway.networking.k8s.io"
                && let Some(&kind) = protocol_kinds.iter().find(|kind| **kind == k.kind)
            {
                if !supported.contains(&kind) {
                    supported.push(kind);
                }
            } else {
                invalid_kinds = true;
            }
        }
        supported
    };
    let mut result = Compiled {
        policies: Policies::default(),
        refs_error: None,
        address,
        tls: None,
        conflict: None,
        supported_kinds,
        transport,
        stream: None,
        sni_routes: SniRoutes::new(),
        routes: vec![],
        reports: vec![],
        accepted: false,
        reason: "Invalid",
        message: "selected Gateway or listener is unavailable".into(),
    };
    let mut peers = gateway.spec.listeners.iter().filter(|other| {
        other.name != l.name
            && other.port == l.port
            && Transport::for_protocol(&other.protocol) == transport
    });
    let mixed = l.protocol == "TLS"
        && peers
            .clone()
            .any(|other| other.protocol == "TLS" && passthrough(other) != passthrough(l));
    result.conflict =
        if mixed || peers.clone().any(|other| other.protocol != l.protocol || raw) {
            Some("ProtocolConflict")
        } else if peers.any(|other| {
            other
                .hostname
                .as_deref()
                .unwrap_or_default()
                .eq_ignore_ascii_case(l.hostname.as_deref().unwrap_or_default())
        }) {
            Some("HostnameConflict")
        } else {
            None
        };
    if result.conflict.is_some() && (mixed || !protocol_kinds.is_empty()) {
        result.reason = if mixed { "ProtocolConflict" } else { "Conflicted" };
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
                    result.refs_error = Some(error);
                } else {
                    result.reason = error.reason;
                }
            }
        }
    }
    if result.refs_error.is_none() && !protocol_kinds.is_empty() && invalid_kinds {
        result.refs_error = Some(TlsError {
            reason: "InvalidRouteKinds",
            message: "listener contains unsupported Route kinds".into(),
        });
    }
    collect(stores.http_routes.state(), stores, gateway, l, &mut result);
    collect(stores.grpc_routes.state(), stores, gateway, l, &mut result);
    collect(stores.tcp_routes.state(), stores, gateway, l, &mut result);
    collect(stores.tls_routes.state(), stores, gateway, l, &mut result);
    collect(stores.udp_routes.state(), stores, gateway, l, &mut result);
    result
}

/// expose common attachment fields without coupling generated resource types to each other.
trait Routed: Resource<DynamicType = ()> {
    /// generated parent reference type.
    type Parent: Serialize;
    /// enumerate the declared parents.
    fn parents(&self) -> &[Self::Parent];
    /// borrow common parent fields.
    fn parent(p: &Self::Parent) -> Parent<'_>;
    /// report backend reference failures when attachment was rejected before binding.
    fn unattached_ref_error(&self, _stores: &GatewayStores) -> Option<RefError> {
        None
    }
    /// attach this resource to a listener and record its status.
    fn bind(
        &self,
        stores: &GatewayStores,
        listener: &GatewayListeners,
        compiled: &mut Compiled,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError>;
}
/// borrowed parent reference fields shared across route kinds.
struct Parent<'a> {
    /// target API group.
    group: Option<&'a str>,
    /// target resource kind.
    kind: Option<&'a str>,
    /// namespace override for the parent.
    namespace: Option<&'a str>,
    /// name of the referenced gateway.
    name: &'a str,
    /// optional listener name.
    section: Option<&'a str>,
    /// optional listener port.
    port: Option<u16>,
}
/// adapt generated parent references into the common attachment interface.
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
    /// mark an attachment as unaccepted.
    fn reject(&mut self, reason: &'static str, message: &str) {
        self.accepted = false;
        self.reason = reason;
        self.message = message.into();
    }
    /// record the first unresolved backend reference.
    fn references(&mut self, error: Option<&RefError>) {
        if let Some(error) = error {
            self.resolved = false;
            self.refs_reason = error.reason();
        }
    }
}
/// return effective and original hostnames for every allowed listener intersection.
fn intersecting_hosts(
    listener_host: &str,
    hostnames: &[String],
    report: &mut RouteReport,
) -> Vec<(String, String)> {
    let hosts: Vec<_> = hostnames
        .iter()
        .map(String::as_str)
        .chain(hostnames.is_empty().then_some(""))
        .map(str::to_ascii_lowercase)
        .filter_map(|route| {
            let effective = if hostname_matches(listener_host, &route) {
                route.clone()
            } else if hostname_matches(&route, listener_host) {
                listener_host.to_owned()
            } else {
                return None;
            };
            Some((effective, route))
        })
        .collect();
    if hosts.is_empty() {
        report.reject(
            "NoMatchingListenerHostname",
            "route and listener hostnames do not intersect",
        );
    }
    hosts
}

/// attach HTTP and gRPC rules using their original hostnames within the selected listener.
fn bind_rules<R>(
    listener: &GatewayListeners,
    result: &mut Compiled,
    report: &mut RouteReport,
    hostnames: Option<&[String]>,
    rules: &[R],
    mut resolve: impl FnMut(&R, &mut Policies) -> Result<HttpRule, ResolveError>,
) -> Result<(), ResolveError> {
    let listener_host =
        listener.hostname.as_deref().unwrap_or_default().to_ascii_lowercase();
    let hosts = intersecting_hosts(&listener_host, hostnames.unwrap_or_default(), report);
    if hosts.is_empty() {
        return Ok(());
    }
    let mut dropped = Vec::new();
    let mut rejection = None;
    for (index, rule) in rules.iter().enumerate() {
        let rule = match resolve(rule, &mut result.policies) {
            Ok(rule) => rule,
            Err(error) => {
                dropped.push(format!("rules[{index}] ({})", error.reason()));
                rejection.get_or_insert(error);
                continue;
            }
        };
        report.references(
            rule.action.backends.iter().find_map(|b| b.cluster.as_ref().err()),
        );
        for http in rule.matches {
            for (hostname, host) in &hosts {
                result.routes.push(Route {
                    hostname: hostname.clone(),
                    host: (!listener_host.is_empty()).then(|| RouteHost {
                        listener: listener_host.clone(),
                        route: host.clone(),
                    }),
                    http: http.clone(),
                    action: rule.action.clone(),
                });
            }
        }
    }
    if let Some(error) = rejection {
        if dropped.len() == rules.len() {
            return Err(error);
        }
        report.partially_invalid = Some(format!("Dropped Rule: {}", dropped.join(", ")));
    }
    Ok(())
}

/// adapt each generated rule type to the shared listener attachment logic.
macro_rules! http_route {
    ($route:ty, $parent:ty, $resolve:ident) => {
        impl Routed for $route {
            parents!($parent);
            fn bind(
                &self,
                stores: &GatewayStores,
                listener: &GatewayListeners,
                result: &mut Compiled,
                report: &mut RouteReport,
            ) -> Result<(), ResolveError> {
                let namespace = self.metadata.namespace.as_deref().unwrap_or_default();
                bind_rules(
                    listener,
                    result,
                    report,
                    self.spec.hostnames.as_deref(),
                    self.spec.rules.as_deref().unwrap_or_default(),
                    |rule, policies| stores.$resolve(namespace, rule, policies),
                )
            }
        }
    };
}
http_route!(HttpRoute, HttpRouteParentRefs, resolve_http);
http_route!(GrpcRoute, GrpcRouteParentRefs, resolve_grpc);

/// bind a raw transport route to its first eligible listener target.
macro_rules! stream_route {
    ($route:ty,$parent:ty,$resolve:ident) => {
        impl Routed for $route {
            parents!($parent);
            fn bind(
                &self,
                stores: &GatewayStores,
                _: &GatewayListeners,
                result: &mut Compiled,
                report: &mut RouteReport,
            ) -> Result<(), ResolveError> {
                let backend = stores.$resolve(self)?;
                report.references(
                    backend.backends.iter().find_map(|(_, b)| b.as_ref().err()),
                );
                // routes arrive oldest first. every allowed parent receives a status entry.
                result.stream.get_or_insert(backend);
                Ok(())
            }
        }
    };
}
stream_route!(TcpRoute, TcpRouteParentRefs, resolve_tcp);
stream_route!(UdpRoute, UdpRouteParentRefs, resolve_udp);

impl Routed for TlsRoute {
    parents!(TlsRouteParentRefs);

    fn unattached_ref_error(&self, stores: &GatewayStores) -> Option<RefError> {
        stores.tls_ref_error(self)
    }

    /// keep the oldest backend per original hostname within this listener.
    fn bind(
        &self,
        stores: &GatewayStores,
        listener: &GatewayListeners,
        result: &mut Compiled,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError> {
        let listener_host =
            listener.hostname.as_deref().unwrap_or_default().to_ascii_lowercase();
        let hosts = intersecting_hosts(&listener_host, &self.spec.hostnames, report);
        if hosts.is_empty() {
            return Ok(());
        }
        let backend = Arc::new(stores.resolve_tls(self)?);
        report.references(backend.backends.iter().find_map(|(_, b)| b.as_ref().err()));
        for (_, original) in hosts {
            result.sni_routes.entry(original).or_insert_with(|| backend.clone());
        }
        Ok(())
    }
}

/// evaluate matching parent references in stable route precedence order.
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
                partially_invalid: None,
            };
            if p.section.is_some_and(|n| n != l.name)
                || p.port.is_some_and(|port| port != l.port)
            {
                report.reject("NoMatchingParent", "listener does not match parentRef");
            } else if !result.supported_kinds.contains(&kind.as_ref())
                || !allows_route(stores, l, &namespace, &source)
            {
                report.reject(
                    "NotAllowedByListeners",
                    "listener does not allow this route namespace or kind",
                );
            } else if let Err(error) = route.bind(stores, l, result, &mut report) {
                report.reject(error.reason(), "unsupported route rule");
            }
            if !report.accepted {
                report.references(route.unattached_ref_error(stores).as_ref());
            }
            result.reports.push(report);
        }
    }
}

/// check listener namespace policy against the route namespace.
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
