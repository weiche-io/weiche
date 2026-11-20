//! report gateway and route conditions for the applied worker revision.
use super::{
    super::{ir::Revision, stream::r#in::WorkerGroupState},
    CONTROLLER_NAME, Compiled, GatewayStores, Selection,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{Patch, PatchParams},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use weiche_gtwapi::v1::{
    gatewayclasses::GatewayClass, gateways::Gateway, httproutes::HttpRoute,
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
    compiled: &Compiled,
    revision: Revision,
    listener: &WorkerGroupState,
) -> anyhow::Result<()> {
    let Some(gateway) = stores.gateways.state().into_iter().find(|g| {
        g.namespace().as_deref() == Some(&selection.namespace)
            && g.name_any() == selection.gateway
    }) else {
        return Ok(());
    };
    let Some(class) = stores.gateway_classes.state().into_iter().find(|c| {
        c.name_any() == gateway.spec.gateway_class_name
            && c.spec.controller_name == CONTROLLER_NAME
    }) else {
        return Ok(());
    };
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
            "Weiche plaintext HTTP demo",
            class.metadata.generation,
        )],
        class_old.get("conditions"),
    );
    patch_status(
        Api::<GatewayClass>::all(client.clone()),
        class.as_ref(),
        &class_old,
        class_status,
    )
    .await?;

    let old = gateway
        .status
        .as_ref()
        .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
    let programmed =
        compiled.accepted && listener.programmed(revision, compiled.snapshot.listener);
    let programmed_message = listener.error.as_deref().unwrap_or(if programmed {
        "all workers applied the gateway snapshot and bound the listener"
    } else {
        "waiting for all workers to apply the gateway snapshot and bind the listener"
    });
    let conditions = stamp(
        vec![
            condition(
                "Accepted",
                compiled.accepted,
                compiled.reason,
                &compiled.message,
                gateway.metadata.generation,
            ),
            condition(
                "Programmed",
                programmed,
                if programmed { "Programmed" } else { "Pending" },
                programmed_message,
                gateway.metadata.generation,
            ),
        ],
        old.get("conditions"),
    );
    let old_listener = old
        .get("listeners")
        .and_then(Value::as_array)
        .and_then(|ls| ls.iter().find(|l| l["name"] == selection.listener));
    let accepted_routes: std::collections::BTreeSet<_> = compiled
        .reports
        .iter()
        .filter(|r| r.accepted)
        .map(|r| (r.route.namespace(), r.route.name_any()))
        .collect();
    let listeners = if gateway.spec.listeners.iter().any(|l| l.name == selection.listener)
    {
        json!([{
            "name": selection.listener, "attachedRoutes": accepted_routes.len(),
            "supportedKinds": [{"group":"gateway.networking.k8s.io", "kind":"HTTPRoute"}],
            "conditions": stamp(vec![condition("Accepted", compiled.accepted, compiled.reason, &compiled.message, gateway.metadata.generation),
                condition("Programmed", programmed, if programmed {"Programmed"} else {"Pending"}, programmed_message, gateway.metadata.generation),
                condition("ResolvedRefs", true, "ResolvedRefs", "HTTP listener has no certificate references", gateway.metadata.generation)], old_listener.and_then(|l| l.get("conditions")))
        }])
    } else {
        json!([])
    };
    let mut desired = old.clone();
    desired["conditions"] = conditions;
    desired["listeners"] = listeners;
    patch_status(
        Api::<Gateway>::namespaced(client.clone(), &selection.namespace),
        gateway.as_ref(),
        &old,
        desired,
    )
    .await?;

    for route in stores.http_routes.state() {
        let old = route
            .status
            .as_ref()
            .map_or_else(|| Ok(json!({})), serde_json::to_value)?;
        let old_parents = old
            .get("parents")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice);
        let owned_parent = |p: &Value| {
            p["controllerName"] == CONTROLLER_NAME
                && p["parentRef"]["name"] == selection.gateway
                && p["parentRef"]["namespace"]
                    .as_str()
                    .or(route.metadata.namespace.as_deref())
                    == Some(&selection.namespace)
        };
        let mut parents: Vec<_> =
            old_parents.iter().filter(|p| !owned_parent(p)).cloned().collect();
        for report in compiled.reports.iter().filter(|r| {
            r.route.name_any() == route.name_any()
                && r.route.namespace() == route.namespace()
        }) {
            let parent = serde_json::to_value(&report.parent)?;
            let previous = old_parents
                .iter()
                .find(|p| owned_parent(p) && p["parentRef"] == parent);
            parents.push(json!({"controllerName": CONTROLLER_NAME, "parentRef": parent,
                "conditions": stamp(vec![condition("Accepted", report.accepted, report.reason, &report.message, route.metadata.generation),
                    condition("ResolvedRefs", report.resolved, report.refs_reason, if report.resolved { "backend references resolved" } else { "backend reference is missing, unsupported or not permitted" }, route.metadata.generation)], previous.and_then(|p| p.get("conditions")))}));
        }
        if parents.is_empty() && old_parents.is_empty() {
            continue;
        }
        patch_status(
            Api::<HttpRoute>::namespaced(
                client.clone(),
                &route.namespace().unwrap_or_default(),
            ),
            route.as_ref(),
            &old,
            json!({"parents":parents}),
        )
        .await?;
    }
    Ok(())
}
