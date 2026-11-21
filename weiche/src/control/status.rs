//! report gateway and route conditions for the applied worker revision.
use super::{
    super::{ir::Revision, stream::down::WorkerGroupState},
    CONTROLLER_NAME, GatewayStores, Selection,
    compile::RouteReport,
    service::Plan,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{Patch, PatchParams},
    runtime::reflector::ObjectRef,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use weiche_gtwapi::v1::{
    backendtlspolicies::BackendTlsPolicy, gatewayclasses::GatewayClass, gateways::Gateway,
};

fn condition(
    kind: &str,
    status: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Value {
    json!({"type": kind, "status": if status {"True"} else {"False"}, "reason": reason,
        "message": message, "observedGeneration": generation.unwrap_or(1)})
}

// keep transition times stable, so reconciliation does not generate status/watch loops.
fn stamp(mut conditions: Vec<Value>, old: Option<&Value>) -> Value {
    for condition in &mut conditions {
        let previous = old.and_then(Value::as_array).and_then(|old| {
            old.iter().find(|c| {
                c["type"] == condition["type"] && c["status"] == condition["status"]
            })
        });
        condition["lastTransitionTime"] = previous
            .and_then(|c| c.get("lastTransitionTime"))
            .cloned()
            .unwrap_or_else(|| {
                json!(
                    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                )
            });
    }
    if let Some(old) = old.and_then(Value::as_array) {
        for previous in old {
            if previous["type"].as_str().is_some_and(|t| {
                t.contains('/') && !conditions.iter().any(|c| c["type"] == t)
            }) {
                conditions.push(previous.clone());
            }
        }
    }
    json!(conditions)
}

async fn patch_status<K>(
    api: Api<K>,
    resource: &K,
    old: &Value,
    desired: Value,
) -> anyhow::Result<()>
where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + DeserializeOwned
        + Serialize,
{
    if old == &desired {
        return Ok(());
    }
    let patch = json!({"metadata":{"resourceVersion": resource.resource_version()}, "status":desired});
    api.patch_status(&resource.name_any(), &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}

/// patch only changed conditions, preserving their transition times.
pub async fn update_status(
    client: &Client,
    stores: &GatewayStores,
    selection: &Selection,
    plans: &[Plan],
    revision: Revision,
    workers: &WorkerGroupState,
) -> anyhow::Result<()> {
    for class in stores
        .gateway_classes
        .state()
        .into_iter()
        .filter(|c| c.spec.controller_name == CONTROLLER_NAME)
    {
        let class_old = class
            .status
            .as_ref()
            .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
        let class_accepted = class.spec.parameters_ref.is_none();
        let mut class_status = class_old.clone();
        class_status["conditions"] = stamp(
            vec![condition(
                "Accepted",
                class_accepted,
                if class_accepted { "Accepted" } else { "InvalidParameters" },
                "Weiche gateway controller",
                class.metadata.generation,
            )],
            class_old.get("conditions"),
        );
        if selection.managed {
            class_status["supportedFeatures"] = json!([{"name":"Gateway"},{"name":"HTTPRoute"},{"name":"ReferenceGrant"},{"name":"TCPRoute"},{"name":"UDPRoute"}]);
        }
        patch_status(
            Api::<GatewayClass>::all(client.clone()),
            class.as_ref(),
            &class_old,
            class_status,
        )
        .await?;
    }
    for plan in plans {
        update_gateway(client, plan, revision, workers).await?;
        update_policies(client, stores, plan).await?;
    }

    macro_rules! routes {
        ($store:expr) => {
            for route in $store.state() {
                let old = route
                    .status
                    .as_ref()
                    .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
                let api = Api::namespaced(
                    client.clone(),
                    &route.namespace().unwrap_or_default(),
                );
                update_route(api, route.as_ref(), old, selection, plans).await?;
            }
        };
    }
    routes!(stores.http_routes);
    routes!(stores.tcp_routes);
    routes!(stores.udp_routes);
    Ok(())
}

/// merge parent conditions for any Route kind while preserving foreign controller entries.
async fn update_route<K>(
    api: Api<K>,
    route: &K,
    old: Value,
    selection: &Selection,
    plans: &[Plan],
) -> anyhow::Result<()>
where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + DeserializeOwned
        + Serialize,
{
    let old_parents = old
        .get("parents")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let owned_parent = |p: &Value| {
        p["controllerName"] == CONTROLLER_NAME
            && (selection.managed
                || (p["parentRef"]["name"] == selection.gateway
                    && p["parentRef"]["namespace"]
                        .as_str()
                        .or(route.meta().namespace.as_deref())
                        == Some(&selection.namespace)))
    };
    let mut parents: Vec<_> =
        old_parents.iter().filter(|p| !owned_parent(p)).cloned().collect();
    let mut reports = std::collections::BTreeMap::new();
    for report in plans
        .iter()
        .flat_map(|p| &p.listeners)
        .flat_map(|l| &l.reports)
        .filter(|r| r.uid == route.uid())
    {
        let key = serde_json::to_string(&report.parent)?;
        let score =
            |r: &RouteReport| (r.accepted, r.reason != "NoMatchingParent", !r.resolved);
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
        parents.push(json!({"controllerName": CONTROLLER_NAME, "parentRef": parent,
                "conditions": stamp(vec![condition("Accepted", report.accepted, report.reason, &report.message, route.meta().generation),
                    condition("ResolvedRefs", report.resolved, report.refs_reason, if report.resolved { "backend references resolved" } else if report.refs_reason == "InvalidBackendTLSPolicy" { "the referenced Service has an invalid BackendTLSPolicy" } else { "backend reference is missing, unsupported or not permitted" }, route.meta().generation)], previous.and_then(|p| p.get("conditions")))}));
    }
    if parents.is_empty() && old_parents.is_empty() {
        return Ok(());
    }
    patch_status(api, route, &old, json!({"parents":parents})).await
}

/// publish independent listener status and the address actually assigned by Kubernetes.
async fn update_gateway(
    client: &Client,
    plan: &Plan,
    revision: Revision,
    workers: &WorkerGroupState,
) -> anyhow::Result<()> {
    let gateway = &plan.gateway;
    let old = gateway
        .status
        .as_ref()
        .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
    let generation = gateway.metadata.generation;
    let accepted = plan.listeners.iter().any(|l| l.accepted);
    let all_valid = plan.listeners.iter().all(|l| l.accepted);
    let programmed = plan.available
        && accepted
        && plan
            .listeners
            .iter()
            .filter(|l| l.accepted)
            .all(|l| workers.programmed(revision, (l.transport, l.address)));
    let mut desired = old.clone();
    desired["conditions"] = stamp(
        vec![
            condition(
                "Accepted",
                accepted,
                if all_valid { "Accepted" } else { "ListenersNotValid" },
                "evaluated gateway listeners",
                generation,
            ),
            condition(
                "Programmed",
                programmed,
                if programmed { "Programmed" } else { "Pending" },
                "worker socket programming",
                generation,
            ),
        ],
        old.get("conditions"),
    );
    if let Some(addresses) = &plan.addresses {
        desired["addresses"] = json!(
            addresses
                .iter()
                .map(|ip| json!({"type":"IPAddress","value":ip}))
                .collect::<Vec<_>>()
        );
    }
    let mut statuses = Vec::new();
    for (l, report) in gateway.spec.listeners.iter().zip(&plan.listeners) {
        let previous = old["listeners"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|v| v["name"] == l.name);
        let bound = plan.available
            && report.accepted
            && workers.programmed(revision, (report.transport, report.address));
        let error = report.certificate_error.as_ref();
        let attached = report
            .reports
            .iter()
            .filter(|r| r.accepted)
            .map(|r| &r.uid)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let kinds = if let Some(kind) = report.supported_kind {
            json!([{"group":"gateway.networking.k8s.io","kind":kind}])
        } else {
            json!([])
        };
        statuses.push(json!({"name":l.name,"attachedRoutes":attached,"supportedKinds":kinds,"conditions":stamp(vec![
            condition("Accepted",report.accepted,report.reason,&report.message,generation),
            condition("Programmed",bound,if bound {"Programmed"}else{"Pending"},"worker socket programming",generation),
            condition("ResolvedRefs",error.is_none(),error.map_or("ResolvedRefs",|e|e.reason),error.map_or("listener references resolved",|e| &e.message),generation),
            condition("Conflicted",report.conflict.is_some(),report.conflict.unwrap_or("NoConflicts"),"listener distinctness",generation)
        ],previous.and_then(|v|v.get("conditions")))}));
    }
    desired["listeners"] = json!(statuses);
    patch_status(
        Api::<Gateway>::namespaced(
            client.clone(),
            &gateway.namespace().unwrap_or_default(),
        ),
        gateway.as_ref(),
        &old,
        desired,
    )
    .await
}

/// reconcile this gateway's ancestor entry without overwriting other controllers' reports.
async fn update_policies(
    client: &Client,
    stores: &GatewayStores,
    plan: &Plan,
) -> anyhow::Result<()> {
    let namespace = plan.gateway.namespace().unwrap_or_default();
    let name = plan.gateway.name_any();
    for policy in stores.tls_policies.state() {
        let old = policy
            .status
            .as_ref()
            .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
        let entries = old["ancestors"].as_array().map_or(&[][..], Vec::as_slice);
        let owned = |p: &Value| {
            let ancestor = &p["ancestorRef"];
            p["controllerName"] == CONTROLLER_NAME
                && ancestor["name"] == name
                && ancestor["namespace"] == namespace
                && ancestor["kind"] == "Gateway"
                && ancestor["group"] == "gateway.networking.k8s.io"
        };
        let previous = entries.iter().find(|entry| owned(entry));
        let key = ObjectRef::from_obj(policy.as_ref());
        let report = plan.listeners.iter().find_map(|l| l.policies.reports.get(&key));
        if previous.is_none() && report.is_none() {
            continue;
        }
        let mut ancestors: Vec<_> =
            entries.iter().filter(|entry| !owned(entry)).cloned().collect();
        if let Some(report) = report {
            let error = report.as_ref().err();
            let refs_error = error.filter(|e| {
                matches!(
                    e.reason,
                    "InvalidCACertificateRef" | "InvalidKind" | "TargetNotFound"
                )
            });
            let reason = if refs_error.is_some_and(|e| e.reason != "TargetNotFound") {
                "NoValidCACertificate"
            } else {
                error.map_or("Accepted", |e| e.reason)
            };
            let message =
                error.map_or("backend TLS policy applied", |e| e.message.as_str());
            let generation = policy.metadata.generation;
            let conditions = stamp(
                vec![
                    condition("Accepted", error.is_none(), reason, message, generation),
                    condition(
                        "ResolvedRefs",
                        refs_error.is_none(),
                        refs_error.map_or("ResolvedRefs", |e| e.reason),
                        refs_error
                            .map_or("policy references resolved", |e| e.message.as_str()),
                        generation,
                    ),
                ],
                previous.and_then(|entry| entry.get("conditions")),
            );
            ancestors.push(json!({
                "ancestorRef": {
                    "group": "gateway.networking.k8s.io", "kind": "Gateway",
                    "name": name, "namespace": namespace
                },
                "controllerName": CONTROLLER_NAME,
                "conditions": conditions
            }));
        }
        patch_status(
            Api::<BackendTlsPolicy>::namespaced(
                client.clone(),
                &policy.namespace().unwrap_or_default(),
            ),
            policy.as_ref(),
            &old,
            json!({"ancestors":ancestors}),
        )
        .await?;
    }
    Ok(())
}
