//! compile attached routes independently for each listener.
use std::sync::Arc;

use super::super::ir::{
    RefError, Route, RouteHost, ServerTls, SniRoutes, StreamRoute, Transport,
    hostname_matches,
};
use super::{
    GatewayStores, ResolveError,
    resolve::HttpRule,
    tls::{Policies, TlsError, listener_tls, passthrough},
};
use kube::{Resource, ResourceExt, runtime::reflector::ObjectRef};
use serde::Serialize;
use weiche_gtwapi::v1::{
    gateways::{
        Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom as From,
    },
    tlsroutes::TlsRoute,
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
#[derive(Default)]
pub struct Compiled {
    /// backend TLS policy results referenced by this listener.
    pub policies: Policies,
    /// listener reference validation failure.
    pub refs_error: Option<TlsError>,
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
            if k.group.as_deref().is_none_or(|g| g == "gateway.networking.k8s.io")
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
        supported_kinds,
        transport,
        reason: "Invalid",
        ..Default::default()
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
    let host = l.hostname.as_deref().unwrap_or_default().to_ascii_lowercase();
    let binding = Binding {
        stores,
        listener: l,
        namespace: &namespace,
        host: &host,
    };
    let name = gateway.name_any();
    /// bridge the common parent fields of the five generated route schemas.
    macro_rules! collect {
        ($store:ident, |$r:ident, $out:ident, $report:ident| $bind:expr, $unattached:expr) => {
            for $r in oldest_first(stores.$store.state()) {
                let source = $r.metadata.namespace.as_deref().unwrap_or_default();
                for p in $r.spec.parent_refs.iter().flatten() {
                    if p.group
                        .as_deref()
                        .is_some_and(|g| g != "gateway.networking.k8s.io")
                        || p.kind.as_deref().is_some_and(|k| k != "Gateway")
                        || p.name != name
                        || p.namespace.as_deref().unwrap_or(source) != namespace
                    {
                        continue;
                    }
                    let matches = p.section_name.as_deref().is_none_or(|n| n == l.name)
                        && p.port.is_none_or(|port| port == l.port);
                    let report = binding.attach(
                        $r.as_ref(),
                        p,
                        matches,
                        &mut result,
                        |$out, $report| $bind,
                        || $unattached,
                    );
                    result.reports.push(report);
                }
            }
        };
    }
    /// adapt HTTP and gRPC rule lists to the same hostname and rule precedence.
    macro_rules! http {
        ($store:ident, $resolve:ident) => {
            collect!(
                $store,
                |r, out, report| {
                    let ns = r.metadata.namespace.as_deref().unwrap_or_default();
                    bind_rules(
                        &host,
                        out,
                        report,
                        r.spec.hostnames.as_deref(),
                        r.spec.rules.as_deref().unwrap_or_default(),
                        |rule, policies| stores.$resolve(ns, rule, policies),
                    )
                },
                None
            );
        };
    }
    http!(http_routes, resolve_http);
    http!(grpc_routes, resolve_grpc);
    collect!(
        tcp_routes,
        |r, out, report| out.bind_stream(stores.resolve_tcp(&r)?, report),
        None
    );
    collect!(
        tls_routes,
        |r, out, report| binding.bind_tls(&r, out, report),
        stores.tls_ref_error(&r)
    );
    collect!(
        udp_routes,
        |r, out, report| out.bind_stream(stores.resolve_udp(&r)?, report),
        None
    );
    result
}

/// listener context shared by parent matching and route attachment.
struct Binding<'a> {
    /// caches used for namespace grants and backend lookups.
    stores: &'a GatewayStores,
    /// namespace of the selected gateway.
    namespace: &'a str,
    /// normalized listener hostname.
    host: &'a str,
    /// listener receiving routes from permitted namespaces.
    listener: &'a GatewayListeners,
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
    listener_host: &str,
    result: &mut Compiled,
    report: &mut RouteReport,
    hostnames: Option<&[String]>,
    rules: &[R],
    mut resolve: impl FnMut(&R, &mut Policies) -> Result<HttpRule, ResolveError>,
) -> Result<(), ResolveError> {
    let hosts = intersecting_hosts(listener_host, hostnames.unwrap_or_default(), report);
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
                        listener: listener_host.into(),
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

impl Compiled {
    /// retain the oldest stream route and report all eligible attachments.
    fn bind_stream(
        &mut self,
        backend: StreamRoute,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError> {
        report.references(backend.backends.iter().find_map(|(_, b)| b.as_ref().err()));
        self.stream.get_or_insert(backend);
        Ok(())
    }
}

impl Binding<'_> {
    /// keep the oldest backend per original hostname within this listener.
    fn bind_tls(
        &self,
        route: &TlsRoute,
        result: &mut Compiled,
        report: &mut RouteReport,
    ) -> Result<(), ResolveError> {
        let hosts = intersecting_hosts(self.host, &route.spec.hostnames, report);
        if hosts.is_empty() {
            return Ok(());
        }
        let backend = Arc::new(self.stores.resolve_tls(route)?);
        report.references(backend.backends.iter().find_map(|(_, b)| b.as_ref().err()));
        for (_, original) in hosts {
            result.sni_routes.entry(original).or_insert_with(|| backend.clone());
        }
        Ok(())
    }

    /// evaluate a gateway parent against this listener and bind an eligible route.
    fn attach<R: Resource<DynamicType = ()>>(
        &self,
        route: &R,
        parent: &impl Serialize,
        matches: bool,
        result: &mut Compiled,
        bind: impl FnOnce(&mut Compiled, &mut RouteReport) -> Result<(), ResolveError>,
        unattached: impl FnOnce() -> Option<RefError>,
    ) -> RouteReport {
        let source = route.meta().namespace.as_deref().unwrap_or_default();
        let mut report = RouteReport {
            uid: route.uid(),
            parent: serde_json::to_value(parent).expect("parent reference serializes"),
            accepted: true,
            reason: "Accepted",
            message: "route attached to listener".into(),
            resolved: true,
            refs_reason: "ResolvedRefs",
            partially_invalid: None,
        };
        if !matches {
            report.reject("NoMatchingParent", "listener does not match parentRef");
        } else if !result.supported_kinds.contains(&R::kind(&()).as_ref())
            || !allows_route(self.stores, self.listener, self.namespace, source)
        {
            report.reject(
                "NotAllowedByListeners",
                "listener does not allow this route namespace or kind",
            );
        } else if let Err(error) = bind(result, &mut report) {
            report.reject(error.reason(), "unsupported route rule");
        }
        if !report.accepted {
            report.references(unattached().as_ref());
        }
        report
    }
}

/// resolve timestamp ties by namespace and name before attaching routes.
fn oldest_first<R: Resource>(mut routes: Vec<Arc<R>>) -> Vec<Arc<R>> {
    routes.sort_by_key(|r| {
        (r.meta().creation_timestamp.clone(), r.namespace(), r.name_any())
    });
    routes
}

/// check listener namespace policy against the route namespace.
fn allows_route(
    stores: &GatewayStores,
    l: &GatewayListeners,
    namespace: &str,
    source: &str,
) -> bool {
    let policy = l.allowed_routes.as_ref().and_then(|r| r.namespaces.as_ref());
    match policy.and_then(|n| n.from.as_ref()) {
        Some(From::All) => true,
        Some(From::Selector) => {
            let Some(selector) = policy.and_then(|n| n.selector.as_ref()) else {
                return false;
            };
            let Some(ns) = stores.namespaces.get(&ObjectRef::new(source)) else {
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
