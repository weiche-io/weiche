//! Connect Kubernetes caches, configuration compilation, status and the compio worker.
use crate::{
    config::{self, CONTROLLER_NAME, Compiled, Selection},
    controller::{GatewayController, GatewayStores},
    dataplane::{self, ListenerState},
};
use anyhow::Context;
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{Patch, PatchParams},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::watch;
use weiche_gtwapi::v1::{gatewayclasses::GatewayClass, gateways::Gateway, httproutes::HttpRoute};

pub async fn run(
    client: Client,
    selection: Selection,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let controller = GatewayController::new(client.clone());
    let stores = controller.stores();
    let (stop, stopped) = watch::channel(false);
    let mut controller_stop = stopped.clone();
    let controller = tokio::spawn(controller.run(async move {
        let _ = controller_stop.changed().await;
    }));
    let (config_tx, config_rx) = watch::channel(Arc::new(config::RoutingTable::default()));
    let (listener_tx, listener_rx) = watch::channel(ListenerState::default());
    let mut worker = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let runtime = compio::runtime::Runtime::new()
            .context("create compio runtime (requires io_uring on Linux)")?;
        tracing::info!(driver = ?runtime.driver_type(), "starting compio data plane");
        runtime.block_on(dataplane::run(config_rx, listener_tx, stopped))
    });
    let reconcile = async {
        tokio::try_join!(
            stores.gateway_classes.wait_until_ready(),
            stores.gateways.wait_until_ready(),
            stores.http_routes.wait_until_ready(),
            stores.services.wait_until_ready(),
            stores.endpoint_slices.wait_until_ready()
        )?;
        tracing::info!(namespace = %selection.namespace, gateway = %selection.gateway, listener = %selection.listener, "Kubernetes caches ready");
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let compiled = config::compile(&stores, &selection);
            if **config_tx.borrow() != compiled.table {
                tracing::info!(bind = ?compiled.table.bind, routes = compiled.table.routes.len(), "publishing HTTP routing configuration");
                config_tx.send_replace(Arc::new(compiled.table.clone()));
            }
            let state = listener_rx.borrow().clone();
            if let Err(error) = update_status(&client, &stores, &selection, &compiled, &state).await
            {
                tracing::warn!(%error, "failed to update Gateway API status; will retry");
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };
    let mut worker_done = false;
    let result = tokio::select! {
        _ = shutdown => Ok(()),
        result = reconcile => result,
        result = &mut worker => { worker_done = true; result.context("compio worker panicked").and_then(|result| result) }
    };
    stop.send_replace(true);
    controller.await.context("controller task panicked")?;
    if !worker_done {
        worker.await.context("compio worker panicked")??;
    }
    result
}

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

// Keep transition times stable, so reconciliation does not generate status/watch loops.
fn stamp(mut conditions: Vec<Value>, old: Option<&Value>) -> Value {
    for condition in &mut conditions {
        let previous = old.and_then(Value::as_array).and_then(|old| {
            old.iter()
                .find(|c| c["type"] == condition["type"] && c["status"] == condition["status"])
        });
        condition["lastTransitionTime"] = previous
            .and_then(|c| c.get("lastTransitionTime"))
            .cloned()
            .unwrap_or_else(|| {
                json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
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
    K: Resource<DynamicType = ()> + Clone + std::fmt::Debug + DeserializeOwned + Serialize,
{
    if old == &desired {
        return Ok(());
    }
    let patch =
        json!({"metadata":{"resourceVersion": resource.resource_version()}, "status":desired});
    api.patch_status(
        &resource.name_any(),
        &PatchParams::default(),
        &Patch::Merge(patch),
    )
    .await?;
    Ok(())
}

async fn update_status(
    client: &Client,
    stores: &GatewayStores,
    selection: &Selection,
    compiled: &Compiled,
    listener: &ListenerState,
) -> anyhow::Result<()> {
    let Some(gateway) = stores.gateways.state().into_iter().find(|g| {
        g.namespace().as_deref() == Some(&selection.namespace) && g.name_any() == selection.gateway
    }) else {
        return Ok(());
    };
    let Some(class) = stores.gateway_classes.state().into_iter().find(|c| {
        c.name_any() == gateway.spec.gateway_class_name && c.spec.controller_name == CONTROLLER_NAME
    }) else {
        return Ok(());
    };
    let class_old = serde_json::to_value(&class.status)?
        .as_object()
        .cloned()
        .map(Value::Object)
        .unwrap_or(json!({}));
    let class_accepted = class.spec.parameters_ref.is_none();
    let mut class_status = class_old.clone();
    class_status["conditions"] = stamp(
        vec![condition(
            "Accepted",
            class_accepted,
            if class_accepted {
                "Accepted"
            } else {
                "InvalidParameters"
            },
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

    let old = serde_json::to_value(&gateway.status)?
        .as_object()
        .cloned()
        .map(Value::Object)
        .unwrap_or(json!({}));
    let programmed =
        compiled.accepted && listener.bound.is_some() && listener.bound == compiled.table.bind;
    let programmed_message = listener.error.as_deref().unwrap_or(if programmed {
        "compio HTTP listener is bound"
    } else {
        "waiting for a valid listener to bind"
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
    let listeners = if gateway
        .spec
        .listeners
        .iter()
        .any(|l| l.name == selection.listener)
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
        let old = serde_json::to_value(&route.status)?
            .as_object()
            .cloned()
            .map(Value::Object)
            .unwrap_or(json!({}));
        let old_parents = old
            .get("parents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let owned_parent = |p: &Value| {
            p["controllerName"] == CONTROLLER_NAME
                && p["parentRef"]["name"] == selection.gateway
                && p["parentRef"]["namespace"]
                    .as_str()
                    .or(route.metadata.namespace.as_deref())
                    == Some(&selection.namespace)
        };
        let mut parents: Vec<_> = old_parents
            .iter()
            .filter(|p| !owned_parent(p))
            .cloned()
            .collect();
        for report in compiled.reports.iter().filter(|r| {
            r.route.name_any() == route.name_any() && r.route.namespace() == route.namespace()
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
            Api::<HttpRoute>::namespaced(client.clone(), &route.namespace().unwrap_or_default()),
            route.as_ref(),
            &old,
            json!({"parents":parents}),
        )
        .await?;
    }
    Ok(())
}
