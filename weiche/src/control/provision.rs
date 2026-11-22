//! reconcile Gateway-owned resources in platform-approved namespaces.
use super::{
    super::ir::{GatewaySnapshot, wire},
    GatewayStores,
    config::Settings,
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
    async fn ensure<K: Resource<DynamicType = ()>>(
        &mut self,
        client: &Client,
        target: &Target<'_>,
        manifest: impl Serialize,
    ) -> anyhow::Result<()> {
        let kind = ApiResource::erase::<K>(&());
        let mut fields = serde_json::to_value(manifest)?;
        fields["apiVersion"] = json!(kind.api_version);
        fields["kind"] = json!(kind.kind);
        let key =
            (target.namespace.to_owned(), target.name.to_owned(), kind.kind.clone());
        let digest = wire::version(&serde_json::to_vec(&fields)?);
        if self.0.get(&key).is_some_and(|(old, _)| old == &digest) {
            return Ok(());
        }
        let api = Api::<DynamicObject>::namespaced_with(
            client.clone(),
            target.namespace,
            &kind,
        );
        if let Some(old) = api.get_opt(target.name).await? {
            ensure!(
                controlled_by(&old, &target.owner.uid),
                "{} {}/{} has another owner",
                kind.kind,
                target.namespace,
                target.name
            );
            // pin the checked object across replacement while allowing other field managers
            // to update their own fields, including the proxy's ConfigMap acknowledgements.
            fields["metadata"]["uid"] = json!(old.uid().context("missing workload uid")?);
            api.patch(
                target.name,
                &PatchParams::apply("weiche").force(),
                &Patch::Apply(fields),
            )
            .await?;
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
    let requested_ips = match requested_addresses(g, &settings.service) {
        Ok(ips) => Some(ips),
        Err(message) => {
            plan.issue_reason = Some("AddressNotAssigned");
            plan.issue = Some(message);
            None
        }
    };
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
        None if existing_service.is_none() && existing_report.is_none() => return Ok(()),
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
        gateway_name: &g.name_any(),
        instance: &settings.release,
        owner: &owner,
        infra: g.spec.infrastructure.as_ref(),
    };
    applied.ensure::<ConfigMap>(client, &target, target.report()).await?;
    applied
        .ensure::<Secret>(client, &target, target.snapshot(payload))
        .await?;
    applied
        .ensure::<ServiceAccount>(client, &target, target.service_account())
        .await?;
    applied
        .ensure::<Role>(client, &target, target.role(&settings.provisioner_binding))
        .await?;
    applied
        .ensure::<RoleBinding>(client, &target, target.role_binding())
        .await?;
    let replicas = if active { settings.replicas } else { 0 };
    let selector = json!(target.selector());
    applied
        .ensure::<Deployment>(
            client,
            &target,
            proxy_deployment(&target, &selector, settings, replicas),
        )
        .await?;
    let Some(ips) = requested_ips else { return Ok(()) };
    let ports = plan.ports(settings.http3.enabled);
    if !ports.is_empty() {
        applied
            .ensure::<Service>(
                client,
                &target,
                target.service(ports, &settings.service, &ips),
            )
            .await?;
    }
    let Some(service) = existing_service else { return Ok(()) };
    plan.addresses = if settings.service.r#type == "LoadBalancer" {
        service
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .and_then(|s| s.ingress.as_ref())
            .into_iter()
            .flatten()
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
            .and_then(|s| s.cluster_ips.as_ref())
            .into_iter()
            .flatten()
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

/// render the per-Gateway proxy Deployment using platform and Gateway infrastructure settings.
fn proxy_deployment(
    target: &Target<'_>,
    selector: &Value,
    settings: &Settings,
    replicas: i32,
) -> Value {
    let name = target.name;
    let mut node_selector = settings.node_selector.clone();
    node_selector.insert("kubernetes.io/os".into(), "linux".into());

    let mut pod_spec = json!({
        "serviceAccountName": name,
        "nodeSelector": node_selector,
        "imagePullSecrets": settings.pull_secrets,
        "terminationGracePeriodSeconds": settings.termination_grace_period_seconds,
        "volumes": [{"name": "run", "emptyDir": {"medium": "Memory"}}],
        "containers": [target.container(settings)],
    });
    if settings.host_network {
        pod_spec["hostNetwork"] = json!(true);
        pod_spec["dnsPolicy"] =
            json!(settings.dns_policy.as_deref().unwrap_or("ClusterFirstWithHostNet"));
    } else {
        if let Some(dns_policy) = &settings.dns_policy {
            pod_spec["dnsPolicy"] = json!(dns_policy);
        }
        // network-namespaced sysctls are forbidden under hostNetwork.
        pod_spec["securityContext"] = json!({
            "sysctls": [{
                "name": "net.ipv4.ip_unprivileged_port_start",
                "value": "0",
            }]
        });
    }
    if !settings.tolerations.is_empty() {
        pod_spec["tolerations"] = json!(settings.tolerations);
    }
    if let Some(affinity) = &settings.affinity {
        pod_spec["affinity"] = json!(affinity);
    }
    if !settings.topology_spread_constraints.is_empty() {
        pod_spec["topologySpreadConstraints"] =
            json!(settings.topology_spread_constraints);
    }
    if let Some(priority_class) = &settings.priority_class_name {
        pod_spec["priorityClassName"] = json!(priority_class);
    }

    json!({
        "metadata": target.metadata(BTreeMap::new()),
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": selector},
            "template": {
                "metadata": target.pod_metadata(settings),
                "spec": pod_spec,
            },
        }
    })
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
    for cm in stores.config_maps.state() {
        if cm
            .labels()
            .get("app.kubernetes.io/managed-by")
            .is_none_or(|v| v != "weiche")
        {
            continue;
        }
        let instance = cm.labels().get("app.kubernetes.io/instance").map(String::as_str);
        if instance != Some(settings.release.as_str())
            && !(instance.is_none() && settings.release == "weiche")
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
        for kind in [
            ApiResource::erase::<Secret>(&()),
            ApiResource::erase::<Deployment>(&()),
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
