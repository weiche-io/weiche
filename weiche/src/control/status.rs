//! report gateway and route conditions for the applied worker revision.
use super::{
    super::ir::RefError, GatewayStores, compile::RouteReport, service::Plan,
    tls::TlsError,
};
use k8s_openapi::NamespaceResourceScope;
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{Patch, PatchParams},
    runtime::reflector::ObjectRef,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// condition type, truth value, reason and user-facing message.
type Cond<'a> = (&'a str, bool, &'a str, &'a str);

/// report a reference failure or the corresponding success message.
fn resolved<'a>(error: Option<&'a TlsError>, message: &'a str) -> Cond<'a> {
    match error {
        Some(e) => ("ResolvedRefs", false, e.reason, &e.message),
        None => ("ResolvedRefs", true, "ResolvedRefs", message),
    }
}

/// borrow status entries, treating a missing list as empty.
fn entries<'a>(status: &'a Value, key: &str) -> &'a [Value] {
    status[key].as_array().map_or(&[], Vec::as_slice)
}

/// retain transition times for unchanged truth values and preserve foreign conditions.
fn stamp_conditions(
    previous: Option<&Value>,
    generation: Option<i64>,
    desired: &[Cond<'_>],
) -> Value {
    let old = entries(previous.unwrap_or(&Value::Null), "conditions");
    let mut conditions = Vec::new();
    for &(kind, truth, reason, message) in desired {
        let status = if truth { "True" } else { "False" };
        let time = old
            .iter()
            .find(|c| c["type"] == kind && c["status"] == status)
            .and_then(|c| c.get("lastTransitionTime"))
            .cloned()
            .unwrap_or_else(|| {
                json!(
                    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                )
            });
        conditions.push(json!({
            "type": kind, "status": status, "reason": reason, "message": message,
            "observedGeneration": generation.unwrap_or(1), "lastTransitionTime": time,
        }));
    }
    for previous in old {
        if previous["type"].as_str().is_some_and(|t| {
            t.contains('/') && !conditions.iter().any(|c| c["type"] == t)
        }) {
            conditions.push(previous.clone());
        }
    }
    Value::Array(conditions)
}

/// scope a status request to the namespace observed by the reflector.
fn namespaced<K: Resource<DynamicType = (), Scope = NamespaceResourceScope>>(
    client: &Client,
    resource: &K,
) -> Api<K> {
    Api::namespaced(
        client.clone(),
        resource.meta().namespace.as_deref().unwrap_or_default(),
    )
}

/// apply changed status with optimistic concurrency.
async fn patch_status<K>(
    api: Api<K>,
    resource: &K,
    status: Option<&impl Serialize>,
    update: impl FnOnce(&mut Value),
) -> anyhow::Result<()>
where
    K: Resource<DynamicType = ()> + Clone + std::fmt::Debug + DeserializeOwned,
{
    let old = status.map_or_else(|| Ok(json!({})), serde_json::to_value)?;
    let mut desired = old.clone();
    update(&mut desired);
    if old == desired {
        return Ok(());
    }
    let patch = json!({"metadata":{"resourceVersion": resource.resource_version()}, "status":desired});
    api.patch_status(&resource.name_any(), &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}

/// supported Gateway API features in the order published on GatewayClass.
const FEATURES: &[&str] = &[
    "BackendTLSPolicy",
    "GRPCRoute",
    "GRPCRouteNamedRouteRule",
    "GRPCRouteRequestHeaderModifier",
    "Gateway",
    "GatewayHTTPListenerIsolation",
    "GatewayInfrastructure",
    "GatewayPort8080",
    "GatewayRouteHostnameIntersectionPrecedence",
    "HTTPRoute",
    "HTTPRoute303RedirectStatusCode",
    "HTTPRoute307RedirectStatusCode",
    "HTTPRoute308RedirectStatusCode",
    "HTTPRouteBackendProtocolH2C",
    "HTTPRouteBackendProtocolWebSocket",
    "HTTPRouteBackendRequestHeaderModification",
    "HTTPRouteBackendTimeout",
    "HTTPRouteBackendURLRewrite",
    "HTTPRouteCORS",
    "HTTPRouteDestinationPortMatching",
    "HTTPRouteHostRewrite",
    "HTTPRouteMethodMatching",
    "HTTPRouteNamedRouteRule",
    "HTTPRouteParentRefPort",
    "HTTPRoutePathRedirect",
    "HTTPRoutePathRewrite",
    "HTTPRoutePortRedirect",
    "HTTPRouteQueryParamMatching",
    "HTTPRouteRequestTimeout",
    "HTTPRouteResponseHeaderModification",
    "HTTPRouteSchemeRedirect",
    "ReferenceGrant",
    "TCPRoute",
    "TLSRoute",
    "UDPRoute",
];

/// patch only changed conditions, preserving their transition times.
pub async fn update_status(
    client: &Client,
    stores: &GatewayStores,
    controller_name: &str,
    plans: &[Plan],
) -> anyhow::Result<()> {
    for class in stores.gateway_classes.state() {
        if class.spec.controller_name != controller_name {
            continue;
        }
        patch_status(
            Api::all(client.clone()),
            &*class,
            class.status.as_ref(),
            |status| {
                let accepted = class.spec.parameters_ref.is_none();
                let reason = if accepted { "Accepted" } else { "InvalidParameters" };
                status["conditions"] = stamp_conditions(
                    Some(status),
                    class.metadata.generation,
                    &[("Accepted", accepted, reason, "Weiche gateway controller")],
                );
                status["supportedFeatures"] =
                    FEATURES.iter().map(|name| json!({"name":name})).collect();
            },
        )
        .await?;
    }
    for plan in plans {
        let g = plan.gateway.as_ref();
        patch_status(namespaced(client, g), g, g.status.as_ref(), |old| {
            gateway_status(plan, old)
        })
        .await?;
    }
    update_policies(client, stores, controller_name, plans).await?;

    let mut reports_by_uid = HashMap::<_, Vec<_>>::new();
    for report in plans.iter().flat_map(|p| &p.listeners).flat_map(|l| &l.reports) {
        reports_by_uid.entry(report.uid.as_deref()).or_default().push(report);
    }

    /// publish parent conditions for each supported route kind.
    macro_rules! routes {
        ($($store:ident),+) => { $(
            for route in stores.$store.state() {
                let r = route.as_ref();
                let reports = reports_by_uid.get(&r.metadata.uid.as_deref())
                    .map_or(&[][..], Vec::as_slice);
                patch_status(namespaced(client, r), r, r.status.as_ref(), |old| {
                    route_status(old, r.metadata.generation, controller_name, reports)
                }).await?;
            }
        )+ };
    }
    routes!(http_routes, grpc_routes, tcp_routes, tls_routes, udp_routes);
    Ok(())
}

/// merge parent conditions for any route kind while preserving foreign controller entries.
fn route_status(
    old: &mut Value,
    generation: Option<i64>,
    controller_name: &str,
    route_reports: &[&RouteReport],
) {
    let old_parents = entries(old, "parents");
    let owned_parent = |p: &Value| p["controllerName"] == controller_name;
    let mut parents: Vec<_> =
        old_parents.iter().filter(|p| !owned_parent(p)).cloned().collect();
    let mut reports = BTreeMap::new();
    for &report in route_reports {
        let key = report.parent.to_string();
        let score = |r: &RouteReport| {
            // report the closest eligible listener when every attachment was rejected.
            let matched = match r.reason {
                "NoMatchingParent" => 0,
                "NotAllowedByListeners" => 1,
                "NoMatchingListenerHostname" => 2,
                _ => 3,
            };
            (r.accepted, matched, r.refs_error.is_some())
        };
        let chosen = reports.entry(key).or_insert(report);
        if score(report) > score(chosen) {
            *chosen = report;
        }
    }
    for report in reports.into_values() {
        let parent = &report.parent;
        let previous = old_parents
            .iter()
            .find(|p| owned_parent(p) && &p["parentRef"] == parent);
        let refs_message = match report.refs_error {
            None => "backend references resolved",
            Some(RefError::InvalidTlsPolicy) => {
                "the referenced Service has an invalid BackendTLSPolicy"
            }
            Some(_) => "backend reference is missing, unsupported or not permitted",
        };
        let refs_reason = report.refs_error.map_or("ResolvedRefs", RefError::reason);
        let mut conditions: Vec<Cond> = vec![
            ("Accepted", report.accepted, report.reason, report.message.as_str()),
            ("ResolvedRefs", report.refs_error.is_none(), refs_reason, refs_message),
        ];
        if let Some(message) = &report.partially_invalid {
            conditions.push(("PartiallyInvalid", true, "UnsupportedValue", message));
        }
        let conditions = stamp_conditions(previous, generation, &conditions);
        parents.push(json!({
            "controllerName": controller_name,
            "parentRef": parent,
            "conditions": conditions,
        }));
    }
    if !parents.is_empty() || !old_parents.is_empty() {
        *old = json!({"parents":parents});
    }
}

/// default programming condition message for gateways and their listeners.
const READINESS: &str = "proxy snapshot and listener readiness";

/// publish independent listener status and the address actually assigned by Kubernetes.
fn gateway_status(plan: &Plan, status: &mut Value) {
    let gateway = &plan.gateway;
    let generation = gateway.metadata.generation;
    let accepted = plan.accepted();
    let all_valid = plan.listeners.iter().all(|l| l.accepted);
    let programmed = plan.programmed && accepted;
    let (reason, message) = if plan.denied {
        (
            "NamespaceNotAllowed",
            "gateway namespace is outside the configured deployment scope",
        )
    } else if plan.invalid_parameters {
        ("InvalidParameters", "gateway infrastructure parametersRef is not supported")
    } else {
        (
            if all_valid { "Accepted" } else { "ListenersNotValid" },
            "evaluated gateway listeners",
        )
    };
    let ready_reason = if programmed {
        "Programmed"
    } else if plan.issue.is_some() {
        plan.issue_reason.unwrap_or("NoResources")
    } else {
        "Pending"
    };
    let ready_message = plan.issue.as_deref().unwrap_or(READINESS);
    status["conditions"] = stamp_conditions(
        Some(status),
        generation,
        &[
            ("Accepted", accepted, reason, message),
            ("Programmed", programmed, ready_reason, ready_message),
        ],
    );
    status["addresses"] = plan
        .addresses
        .iter()
        .map(|(kind, value)| json!({"type":kind,"value":value}))
        .collect();
    let mut statuses = Vec::new();
    for (l, report) in gateway.spec.listeners.iter().zip(&plan.listeners) {
        let previous = entries(status, "listeners").iter().find(|v| v["name"] == l.name);
        let bound = plan.programmed && report.accepted;
        let error = report.refs_error.as_ref();
        let attached = report
            .reports
            .iter()
            .filter(|r| r.accepted)
            .map(|r| &r.uid)
            .collect::<BTreeSet<_>>()
            .len();
        let kinds: Vec<_> = report
            .supported_kinds
            .iter()
            .map(|kind| json!({"group":"gateway.networking.k8s.io", "kind":kind}))
            .collect();
        let conflict = report.conflict.unwrap_or("NoConflicts");
        let ready = if bound { "Programmed" } else { "Pending" };
        statuses.push(json!({
            "name": l.name, "attachedRoutes": attached, "supportedKinds": kinds,
            "conditions": stamp_conditions(previous, generation, &[
                ("Accepted", report.accepted, report.reason, &report.message),
                ("Programmed", bound, ready, READINESS),
                resolved(error, "listener references resolved"),
                ("Conflicted", report.conflict.is_some(), conflict, "listener distinctness"),
            ]),
        }));
    }
    status["listeners"] = Value::Array(statuses);
}

/// publish all gateway ancestors for a policy against a single observed resource version.
async fn update_policies(
    client: &Client,
    stores: &GatewayStores,
    controller_name: &str,
    plans: &[Plan],
) -> anyhow::Result<()> {
    let mut reports = HashMap::<_, BTreeMap<_, _>>::new();
    for plan in plans {
        let gateway =
            (plan.gateway.namespace().unwrap_or_default(), plan.gateway.name_any());
        for listener in &plan.listeners {
            for (policy, report) in &listener.policies.reports {
                reports
                    .entry(policy)
                    .or_default()
                    .entry(gateway.clone())
                    .or_insert(report);
            }
        }
    }
    for policy in stores.tls_policies.state() {
        let key = ObjectRef::from_obj(policy.as_ref());
        let reports = reports.remove(&key).unwrap_or_default();
        let p = policy.as_ref();
        patch_status(namespaced(client, p), p, p.status.as_ref(), |old| {
            let entries = entries(old, "ancestors");
            let owned = |p: &Value| {
                let ancestor = &p["ancestorRef"];
                p["controllerName"] == controller_name
                    && ancestor["kind"] == "Gateway"
                    && ancestor["group"] == "gateway.networking.k8s.io"
            };
            let mut ancestors: Vec<_> =
                entries.iter().filter(|entry| !owned(entry)).cloned().collect();
            if reports.is_empty() && ancestors.len() == entries.len() {
                return;
            }
            for ((namespace, name), report) in reports {
                let previous = entries.iter().find(|entry| {
                    owned(entry)
                        && entry["ancestorRef"]["name"] == name
                        && entry["ancestorRef"]["namespace"] == namespace
                });
                let error = report.as_ref().err();
                let (reason, refs_error) = match error.map(|e| e.reason) {
                    Some("InvalidCACertificateRef" | "InvalidKind") => {
                        ("NoValidCACertificate", error)
                    }
                    Some("TargetNotFound") => ("TargetNotFound", error),
                    other => (other.unwrap_or("Accepted"), None),
                };
                let message =
                    error.map_or("backend TLS policy applied", |e| e.message.as_str());
                ancestors.push(json!({
                    "ancestorRef": {
                        "group": "gateway.networking.k8s.io", "kind": "Gateway",
                        "name": name, "namespace": namespace
                    },
                    "controllerName": controller_name,
                    "conditions": stamp_conditions(previous, p.metadata.generation, &[
                        ("Accepted", error.is_none(), reason, message),
                        resolved(refs_error, "policy references resolved"),
                    ])
                }));
            }
            *old = json!({"ancestors":ancestors});
        })
        .await?;
    }
    Ok(())
}
