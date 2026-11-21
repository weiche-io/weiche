//! reconcile Gateway-owned resources in platform-approved namespaces.
use super::{
    super::ir::{
        GatewaySnapshot,
        wire::{self, SNAPSHOT_KEY},
    },
    GatewayStores,
    service::{OWNER, Plan},
};
use anyhow::{Context, ensure};
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{
            ConfigMap, LocalObjectReference, ResourceRequirements, SeccompProfile,
            Secret, Service, ServiceAccount,
        },
        rbac::v1::{Role, RoleBinding},
    },
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ApiResource, DynamicObject, Patch, PatchParams, PostParams},
    runtime::reflector::ObjectRef,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// installation policy supplied by the platform, outside of tenant-writable Gateway specs.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub bind_address: std::net::IpAddr,
    pub namespaces: BTreeSet<String>,
    image: String,
    pull_policy: String,
    pull_secrets: Vec<LocalObjectReference>,
    resources: ResourceRequirements,
    seccomp_profile: SeccompProfile,
    replicas: i32,
    workers: std::num::NonZeroUsize,
    service_type: String,
}
impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        let settings: Self =
            serde_json::from_reader(std::fs::File::open("/etc/weiche/proxy.json")?)?;
        ensure!(
            !settings.namespaces.is_empty()
                && settings.replicas > 0
                && !settings.image.is_empty(),
            "invalid proxy installation settings"
        );
        ensure!(
            matches!(settings.service_type.as_str(), "ClusterIP" | "LoadBalancer"),
            "unsupported gateway service type"
        );
        Ok(settings)
    }
}

/// cache successful applies; periodically recheck for external drift.
#[derive(Default)]
pub struct Applied(BTreeMap<(String, String, String), (String, Instant)>);

impl Applied {
    pub fn expire(&mut self) {
        self.0.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(30));
    }

    async fn ensure<K: Resource<DynamicType = ()>>(
        &mut self,
        client: &Client,
        namespace: &str,
        name: &str,
        owner: &Value,
        mut fields: Value,
    ) -> anyhow::Result<()> {
        let kind = ApiResource::erase::<K>(&());
        fields["apiVersion"] = json!(kind.api_version);
        fields["kind"] = json!(kind.kind);
        fields["metadata"]["labels"]["app.kubernetes.io/managed-by"] = json!("weiche");
        fields["metadata"]["name"] = json!(name);
        fields["metadata"]["namespace"] = json!(namespace);
        fields["metadata"]["ownerReferences"] = json!([owner]);
        let key = (namespace.to_owned(), name.to_owned(), kind.kind.clone());
        let digest = wire::version(&serde_json::to_vec(&fields)?);
        if self.0.get(&key).is_some_and(|(old, _)| old == &digest) {
            return Ok(());
        }
        let api = Api::<DynamicObject>::namespaced_with(client.clone(), namespace, &kind);
        if let Some(old) = api.get_opt(name).await? {
            ensure!(
                old.owner_references()
                    .iter()
                    .any(|r| r.controller == Some(true) && json!(r.uid) == owner["uid"]),
                "{} {namespace}/{name} has another owner",
                kind.kind
            );
            fields["metadata"]["resourceVersion"] = json!(old.resource_version());
            api.patch(name, &PatchParams::apply("weiche").force(), &Patch::Apply(fields))
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
    let g = &plan.gateway;
    let ns = g.namespace().context("missing gateway namespace")?;
    let uid = g.uid().context("missing gateway uid")?;
    // a distinct prefix also allows migration from the legacy GatewayClass-owned Services.
    let name = format!("weiche-gw-{uid}");
    let mut owner = g.controller_owner_ref(&()).context("missing gateway owner")?;
    owner.block_owner_deletion = None;
    let owner = json!(owner);
    let labels = json!({(OWNER):uid});
    // withdraw the previous routes if an oversized replacement cannot be delivered.
    let payload = snapshot.encode().or_else(|err| {
        plan.issue = Some(err.to_string());
        GatewaySnapshot::default().encode()
    })?;
    let revision = wire::version(&payload);
    applied
        .ensure::<ConfigMap>(client, &ns, &name, &owner, json!({}))
        .await?;
    applied
        .ensure::<Secret>(
            client,
            &ns,
            &name,
            &owner,
            json!({
                "type":"weiche.io/snapshot", "data":{(SNAPSHOT_KEY):ByteString(payload)}
            }),
        )
        .await?;
    applied
        .ensure::<ServiceAccount>(client, &ns, &name, &owner, json!({}))
        .await?;
    applied.ensure::<Role>(client, &ns, &name, &owner, json!({"rules":[
        {"apiGroups":[""],"resources":["secrets"],"resourceNames":[name],"verbs":["get","list","watch"]},
        {"apiGroups":[""],"resources":["configmaps"],"resourceNames":[name],"verbs":["patch"]},
        {"apiGroups":["rbac.authorization.k8s.io"],"resources":["rolebindings"],"resourceNames":["weiche-gateway-provisioner"],"verbs":["get"]}
    ]})).await?;
    applied.ensure::<RoleBinding>(client, &ns, &name, &owner, json!({
        "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":name},
        "subjects":[{"kind":"ServiceAccount","name":name,"namespace":ns}]
    })).await?;
    applied.ensure::<Deployment>(client, &ns, &name, &owner, json!({"spec":{
        "replicas":settings.replicas, "selector":{"matchLabels":labels},
        "template":{"metadata":{"labels":labels},"spec":{
            "serviceAccountName":name,"nodeSelector":{"kubernetes.io/os":"linux"},
            "imagePullSecrets":settings.pull_secrets,
            "securityContext":{"sysctls":[{"name":"net.ipv4.ip_unprivileged_port_start","value":"0"}]},
            "terminationGracePeriodSeconds":10,
            "volumes":[{"name":"run","emptyDir":{"medium":"Memory"}}],
            "containers":[{"name":"proxy","image":settings.image,"imagePullPolicy":settings.pull_policy,
                "args":["proxy",name],"resources":settings.resources,
                "env":[{"name":"WEICHE_WORKERS","value":settings.workers.to_string()},{"name":"POD_UID","valueFrom":{"fieldRef":{"fieldPath":"metadata.uid"}}}],
                "volumeMounts":[{"name":"run","mountPath":"/run/weiche"}],
                "readinessProbe":{"exec":{"command":["/usr/local/bin/weiche","ready"]},"periodSeconds":2,"failureThreshold":1},
                "securityContext":{"runAsNonRoot":true,"runAsUser":65532,"allowPrivilegeEscalation":false,
                    "capabilities":{"drop":["ALL"]},"seccompProfile":settings.seccomp_profile}
            }]
        }}
    }})).await?;
    let ports = plan.ports();
    applied.ensure::<Service>(client, &ns, &name, &owner, json!({"spec":{
        "type":settings.service_type,"ipFamilyPolicy":"PreferDualStack","selector":labels,"ports":ports
    }})).await?;
    let Some(service) = stores.services.get(&ObjectRef::new(&name).within(&ns)) else {
        return Ok(());
    };
    if !service
        .owner_references()
        .iter()
        .any(|r| r.uid == uid && r.controller == Some(true))
    {
        return Ok(());
    }
    plan.addresses = if settings.service_type == "LoadBalancer" {
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
    let Some(report) = stores.config_maps.get(&ObjectRef::new(&name).within(&ns)) else {
        return Ok(());
    };
    let acknowledgements = report.data.as_ref();
    let service_uid = service.uid().context("missing service uid")?;
    let mut members = BTreeSet::new();
    for slice in stores.endpoint_slices.state() {
        if slice.namespace().as_deref() != Some(&ns)
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
                && acknowledgements.and_then(|a| a.get(pod)) == Some(&revision)
            {
                plan.programmed = !plan.addresses.is_empty();
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
        Api::<ConfigMap>::namespaced(client.clone(),&ns).patch(&name,&PatchParams::default(),
            &Patch::Merge(json!({"metadata":{"resourceVersion":report.resource_version()},"data":stale}))).await?;
    }
    Ok(())
}
