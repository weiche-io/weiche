//! reconcile Gateway-owned resources in platform-approved namespaces.
use super::{
    super::ir::{GatewaySnapshot, wire},
    GatewayStores,
    config::{ServiceType, Settings},
    manifest::{Target, requested_addresses},
    service::{Plan, controlled_by},
};
use anyhow::{Context, ensure};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Secret, Service, ServiceAccount},
    rbac::v1::{Role, RoleBinding},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{
        ApiResource, DeleteParams, DynamicObject, Patch, PatchParams, PostParams,
        Preconditions,
    },
    runtime::reflector::ObjectRef,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// cache manifest digests until the next drift check.
#[derive(Default)]
pub struct Applied(
    /// last successful digest and apply time by namespace, resource name and kind.
    BTreeMap<(String, String, String), (String, Instant)>,
);

impl Applied {
    /// forget cached applies after the drift check interval.
    pub fn expire(&mut self) {
        self.0.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(30));
    }

    /// forget retired resources so an eligible gateway can recreate them immediately.
    fn forget(&mut self, namespace: &str, name: &str) {
        self.0
            .retain(|(ns, resource, _), _| ns != namespace || resource != name);
    }

    /// create or update fields owned by this gateway.
    async fn ensure<K: Resource<DynamicType = ()> + Serialize>(
        &mut self,
        client: &Client,
        target: &Target<'_>,
        manifest: K,
    ) -> anyhow::Result<()> {
        let name = manifest.name_any();
        let kind = ApiResource::erase::<K>(&());
        let mut fields = serde_json::to_value(manifest)?;
        fields["apiVersion"] = json!(kind.api_version);
        fields["kind"] = json!(kind.kind);
        let key = (target.namespace.to_owned(), name.clone(), kind.kind.clone());
        let digest = wire::version(&serde_json::to_vec(&fields)?);
        if self.0.get(&key).is_some_and(|(old, _)| old == &digest) {
            return Ok(());
        }
        let ns = target.namespace;
        let name = name.as_str();
        let api = Api::<DynamicObject>::namespaced_with(client.clone(), ns, &kind);
        if let Some(old) = api.get_opt(name).await? {
            // a forced apply would otherwise take over an unrelated object with the same name.
            ensure!(
                controlled_by(&old, &target.owner.uid),
                "{} {ns}/{name} has another owner",
                kind.kind
            );
            // pin the checked object across replacement while allowing other field managers
            // to update their own fields, including the proxy's ConfigMap acknowledgements.
            let uid = old.uid().context("missing workload uid")?;
            fields["metadata"]["uid"] = json!(uid);
            let params = PatchParams::apply("weiche");
            // replace the chart's pod template and the listener ports as units.
            // SSA manages the remaining fields, including shared resource metadata.
            let exact = match kind.kind.as_str() {
                "Deployment" => Some("template"),
                "Service" => Some("ports"),
                _ => None,
            };
            if let Some(key) = exact {
                let value = fields["spec"].as_object_mut().and_then(|s| s.remove(key));
                let mut patch = json!({"metadata":{"uid":uid}, "spec":{(key):value}});
                let patch = if key == "template" {
                    patch["spec"][key]["$patch"] = json!("replace");
                    Patch::Strategic(patch)
                } else {
                    Patch::Merge(patch)
                };
                api.patch(name, &params, &patch).await?;
            }
            api.patch(name, &params.force(), &Patch::Apply(fields)).await?;
        } else {
            let params = PostParams {
                field_manager: Some("weiche".into()),
                ..Default::default()
            };
            api.create(&params, &serde_json::from_value(fields)?).await?;
        }
        self.0.insert(key, (digest, Instant::now()));
        Ok(())
    }
}

/// publish the snapshot, maintain its per-Gateway RBAC, then observe ready replicas' acknowledgements.
pub async fn reconcile(
    client: &Client,
    stores: &GatewayStores,
    settings: &Settings,
    applied: &mut Applied,
    plan: &mut Plan,
    snapshot: &GatewaySnapshot,
) -> anyhow::Result<()> {
    let options = &settings.installation;
    plan.programmed = false;
    plan.addresses.clear();
    let g = &plan.gateway;
    let ns = g.namespace().context("missing gateway namespace")?;
    let uid = g.uid().context("missing gateway uid")?;
    // a distinct prefix also allows migration from the legacy GatewayClass-owned Services.
    let name = format!("weiche-gw-{uid}");
    let existing_service = stores
        .services
        .get(&ObjectRef::new(&name).within(&ns))
        .filter(|service| controlled_by(service.as_ref(), &uid));
    let existing_report = stores
        .config_maps
        .get(&ObjectRef::new(&name).within(&ns))
        .filter(|report| controlled_by(report.as_ref(), &uid));
    let requested_ips = requested_addresses(g, &options.service)
        .inspect_err(|error| {
            plan.issue_reason = Some("AddressNotAssigned");
            plan.issue = Some(error.to_string());
        })
        .ok();
    let payload = if plan.accepted() && plan.issue.is_none() && !snapshot.is_empty() {
        snapshot
            .encode()
            .inspect_err(|err| plan.issue = Some(err.to_string()))
            .ok()
    } else {
        None
    };
    let active = payload.is_some();
    let (payload, listener_revision) = match payload {
        Some(payload) => (payload, snapshot.listener_version()?),
        // withdraw routes and private keys when a provisioned gateway cannot serve.
        None => {
            let empty = GatewaySnapshot::default();
            (empty.encode()?, empty.listener_version()?)
        }
    };
    let mut owner = g.controller_owner_ref(&()).context("missing gateway owner")?;
    owner.block_owner_deletion = None;
    let target = Target {
        namespace: &ns,
        name: &name,
        instance: &options.release,
        owner: &owner,
        infra: g.spec.infrastructure.as_ref(),
    };
    /// apply each typed resource in order, stopping at the first failed request.
    macro_rules! apply {
        ($($resource:expr),+ $(,)?) => { $(applied.ensure(client, &target, $resource).await?;)+ };
    }
    apply!(
        target.owned::<ConfigMap>(),
        target.snapshot(payload),
        target.owned::<ServiceAccount>(),
        target.role(&options.provisioner_binding),
        target.role_binding(),
        target.deployment(settings),
    );
    let Some(ips) = requested_ips else { return Ok(()) };
    let ports = plan.ports(options.http3);
    if !ports.is_empty() {
        apply!(target.service(ports, &options.service, &ips));
    }
    let Some(service) = existing_service else { return Ok(()) };
    plan.addresses = if options.service.r#type == ServiceType::LoadBalancer {
        service
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref()?.ingress.as_deref())
            .unwrap_or_default()
            .iter()
            .flat_map(|i| {
                i.ip.iter()
                    .map(|v| ("IPAddress".into(), v.clone()))
                    .chain(i.hostname.iter().map(|v| ("Hostname".into(), v.clone())))
            })
            .collect()
    } else {
        service
            .spec
            .as_ref()
            .and_then(|s| s.cluster_ips.as_deref())
            .unwrap_or_default()
            .iter()
            .map(|ip| ("IPAddress".into(), ip.clone()))
            .collect()
    };
    let Some(report) = existing_report else { return Ok(()) };
    let acknowledgements = report.data.as_ref();
    let service_uid = service.uid().context("missing service uid")?;
    let mut members = BTreeSet::new();
    let mut acknowledged = false;
    for slice in stores.endpoint_slices.state() {
        if slice.namespace().as_deref() != Some(&ns)
            || slice.metadata.deletion_timestamp.is_some()
            || !slice.owner_references().iter().any(|r| r.uid == service_uid)
        {
            continue;
        }
        for ep in slice.endpoints.iter().flatten() {
            let Some(pod) = ep.target_ref.as_ref().and_then(|r| r.uid.as_ref()) else {
                continue;
            };
            members.insert(pod.clone());
            if ep
                .conditions
                .as_ref()
                .is_some_and(|c| c.ready == Some(true) && c.terminating != Some(true))
                && acknowledgements.and_then(|a| a.get(pod)) == Some(&listener_revision)
            {
                acknowledged = true;
            }
        }
    }
    // compact departed pods' entries after EndpointSlices have caught up.
    if let Some(data) = acknowledgements
        && data.len() > members.len() + 16
    {
        let stale: BTreeMap<_, _> = data
            .keys()
            .filter(|k| !members.contains(*k))
            .map(|k| (k, Value::Null))
            .collect();
        Api::<ConfigMap>::namespaced(client.clone(), &ns)
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({
                    "metadata": {"resourceVersion": report.resource_version()},
                    "data": stale,
                })),
            )
            .await?;
    }
    plan.programmed = active && !plan.addresses.is_empty() && acknowledged;
    Ok(())
}

/// delete the observed resource only while its gateway ownership still matches.
async fn remove(
    client: &Client,
    kind: ApiResource,
    ns: &str,
    name: &str,
    owner_uid: &str,
) -> anyhow::Result<()> {
    let api = Api::<DynamicObject>::namespaced_with(client.clone(), ns, &kind);
    let Some(obj) = api.get_opt(name).await? else { return Ok(()) };
    if controlled_by(&obj, owner_uid) {
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: obj.uid(),
                resource_version: obj.resource_version(),
            }),
            ..Default::default()
        };
        match api.delete(name, &params).await {
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            result => {
                result?;
            }
        }
    }
    Ok(())
}

/// remove this installation's resources for gateways that lost their class or eligibility.
pub async fn retired(
    client: &Client,
    stores: &GatewayStores,
    settings: &Settings,
    plans: &[Plan],
    applied: &mut Applied,
) -> anyhow::Result<()> {
    let options = &settings.installation;
    for cm in stores.config_maps.state() {
        let labels = cm.labels();
        let label = |key: &str| labels.get(key).map(String::as_str);
        if label("app.kubernetes.io/managed-by") != Some("weiche")
            || label("app.kubernetes.io/instance").unwrap_or("weiche") != options.release
        {
            continue;
        }
        let Some(owner) = cm.owner_references().iter().find(|r| {
            r.controller == Some(true)
                && r.kind == "Gateway"
                && r.api_version == "gateway.networking.k8s.io/v1"
        }) else {
            continue;
        };
        let name = cm.name_any();
        let ns = cm.namespace().unwrap_or_default();
        if !settings.namespaces.contains(&ns)
            || name != format!("weiche-gw-{}", owner.uid)
            || plans.iter().any(|p| {
                !p.denied
                    && !p.invalid_parameters
                    && p.gateway.uid().as_deref() == Some(&owner.uid)
            })
        {
            continue;
        }
        // kubernetes garbage-collects workloads for deleted gateways.
        if !stores.gateways.state().iter().any(|g| {
            g.uid().as_deref() == Some(&owner.uid)
                && g.metadata.deletion_timestamp.is_none()
        }) {
            continue;
        }
        applied.forget(&ns, &name);
        applied.forget(&ns, &owner.name);
        remove(
            client,
            ApiResource::erase::<Deployment>(&()),
            &ns,
            &owner.name,
            &owner.uid,
        )
        .await?;
        for kind in [
            ApiResource::erase::<Secret>(&()),
            ApiResource::erase::<Service>(&()),
            ApiResource::erase::<RoleBinding>(&()),
            ApiResource::erase::<Role>(&()),
            ApiResource::erase::<ServiceAccount>(&()),
            ApiResource::erase::<ConfigMap>(&()),
        ] {
            remove(client, kind, &ns, &name, &owner.uid).await?;
        }
    }
    Ok(())
}
