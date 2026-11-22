//! reconcile Gateway-owned resources in platform-approved namespaces.
use super::{
    super::ir::{
        GatewaySnapshot,
        wire::{self, SNAPSHOT_KEY},
    },
    CONTROLLER_NAME, GatewayStores,
    service::{OWNER, Plan},
};
use anyhow::{Context, ensure};
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{
            Affinity, ConfigMap, EnvVar, LocalObjectReference, ResourceRequirements,
            SeccompProfile, Secret, Service, ServiceAccount, Toleration,
            TopologySpreadConstraint,
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

/// default Helm release instance label for generated workloads.
fn default_release() -> String {
    "weiche".into()
}

/// default GatewayClass controllerName owned by this installation.
fn default_controller_name() -> String {
    CONTROLLER_NAME.into()
}

/// default leader-election Lease name in the release namespace.
fn default_lease_name() -> String {
    "weiche-controller".into()
}

/// default provisioner RoleBinding and ClusterRole name in authorized namespaces.
fn default_provisioner_binding() -> String {
    "weiche-gateway-provisioner".into()
}

/// default graceful termination window in seconds for proxy pods.
fn default_termination_grace() -> i64 {
    30
}

/// default proxy log level.
fn default_log_level() -> String {
    "info".into()
}

/// default proxy log format.
fn default_log_format() -> String {
    "text".into()
}

/// platform logging defaults for provisioned proxy containers.
#[derive(Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LogSettings {
    /// default RUST_LOG filter applied to proxy pods.
    pub level: String,
    /// output log format (`text` or `json`).
    pub format: String,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

/// installation policy supplied by the platform, outside of tenant-writable Gateway specs.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// release instance name used to isolate multi-release workloads.
    #[serde(default = "default_release")]
    pub release: String,
    /// gateway class controller name matched by this controller.
    #[serde(default = "default_controller_name")]
    pub controller_name: String,
    /// coordination Lease name used for leader election.
    #[serde(default = "default_lease_name")]
    pub lease_name: String,
    /// provisioner RoleBinding and ClusterRole name checked by proxies.
    #[serde(default = "default_provisioner_binding")]
    pub provisioner_binding: String,
    /// local IP address bound by proxy listeners.
    pub bind_address: std::net::IpAddr,
    /// namespaces authorized to host Gateway proxy workloads.
    pub namespaces: BTreeSet<String>,
    /// container image rendered into per-Gateway proxy Deployments.
    image: String,
    /// Kubernetes image pull policy for proxy containers.
    pull_policy: String,
    /// image pull secrets copied onto proxy pods.
    pull_secrets: Vec<LocalObjectReference>,
    /// container resource requests and limits for proxy pods.
    resources: ResourceRequirements,
    /// seccomp profile required for proxy io_uring workers.
    seccomp_profile: SeccompProfile,
    /// desired proxy Deployment replica count.
    replicas: i32,
    /// compio worker thread count inside each proxy pod.
    workers: std::num::NonZeroUsize,
    /// bind proxy listeners directly on the host network namespace.
    #[serde(default)]
    host_network: bool,
    /// optional Kubernetes DNS policy override for proxy pods.
    #[serde(default)]
    dns_policy: Option<String>,
    /// graceful termination window in seconds for proxy pods.
    #[serde(default = "default_termination_grace")]
    termination_grace_period_seconds: i64,
    /// additional node selector labels merged with kubernetes.io/os=linux.
    #[serde(default)]
    node_selector: BTreeMap<String, String>,
    /// scheduling tolerations applied to proxy pods.
    #[serde(default)]
    tolerations: Vec<Toleration>,
    /// scheduling affinity rules applied to proxy pods.
    #[serde(default)]
    affinity: Option<Affinity>,
    /// topology spread constraints applied to proxy pods.
    #[serde(default)]
    topology_spread_constraints: Vec<TopologySpreadConstraint>,
    /// optional priorityClassName applied to proxy pods.
    #[serde(default)]
    priority_class_name: Option<String>,
    /// additional pod template labels applied to proxy pods.
    #[serde(default)]
    pod_labels: BTreeMap<String, String>,
    /// additional pod template annotations applied to proxy pods.
    #[serde(default)]
    pod_annotations: BTreeMap<String, String>,
    /// proxy container logging settings.
    #[serde(default)]
    log: LogSettings,
    /// extra environment variables appended to proxy containers.
    #[serde(default)]
    extra_env: Vec<EnvVar>,
    /// Kubernetes Service type provisioned for each Gateway.
    service_type: String,
}
impl Settings {
    /// read and validate platform installation settings from `/etc/weiche/proxy.json`.
    pub fn load() -> anyhow::Result<Self> {
        let mut settings: Self =
            serde_json::from_reader(std::fs::File::open("/etc/weiche/proxy.json")?)?;
        if settings.dns_policy.as_deref() == Some("") {
            settings.dns_policy = None;
        }
        if settings.priority_class_name.as_deref() == Some("") {
            settings.priority_class_name = None;
        }
        ensure!(
            !settings.release.is_empty()
                && !settings.controller_name.is_empty()
                && !settings.lease_name.is_empty()
                && !settings.provisioner_binding.is_empty()
                && !settings.namespaces.is_empty()
                && settings.replicas > 0
                && settings.termination_grace_period_seconds > 0
                && !settings.image.is_empty(),
            "invalid proxy installation settings"
        );
        ensure!(
            matches!(settings.log.format.as_str(), "text" | "json"),
            "unsupported proxy log format"
        );
        ensure!(
            matches!(settings.service_type.as_str(), "ClusterIP" | "LoadBalancer"),
            "unsupported gateway service type"
        );
        ensure!(
            !settings.extra_env.iter().any(|var| matches!(
                var.name.as_str(),
                "POD_UID" | "WEICHE_PROVISIONER_BINDING" | "WEICHE_WORKERS"
            )),
            "proxy extraEnv cannot override reserved environment variables"
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
        instance: &str,
        owner: &Value,
        mut fields: Value,
    ) -> anyhow::Result<()> {
        let kind = ApiResource::erase::<K>(&());
        fields["apiVersion"] = json!(kind.api_version);
        fields["kind"] = json!(kind.kind);
        fields["metadata"]["labels"]["app.kubernetes.io/managed-by"] = json!("weiche");
        fields["metadata"]["labels"]["app.kubernetes.io/instance"] = json!(instance);
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
            // pin the checked object across replacement while allowing other field managers
            // to update their own fields, including the proxy's ConfigMap acknowledgements.
            fields["metadata"]["uid"] = json!(old.uid().context("missing workload uid")?);
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
        .ensure::<ConfigMap>(client, &ns, &name, &settings.release, &owner, json!({}))
        .await?;
    applied
        .ensure::<Secret>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            json!({
                "type":"weiche.io/snapshot", "data":{(SNAPSHOT_KEY):ByteString(payload)}
            }),
        )
        .await?;
    applied
        .ensure::<ServiceAccount>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            json!({}),
        )
        .await?;
    applied
        .ensure::<Role>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            proxy_role(&name, &settings.provisioner_binding),
        )
        .await?;
    applied
        .ensure::<RoleBinding>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            proxy_role_binding(&name, &ns),
        )
        .await?;
    applied
        .ensure::<Deployment>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            proxy_deployment(&name, &uid, &labels, settings),
        )
        .await?;
    applied
        .ensure::<Service>(
            client,
            &ns,
            &name,
            &settings.release,
            &owner,
            proxy_service(&labels, plan.ports(), settings),
        )
        .await?;
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
    Ok(())
}

/// render least-privilege RBAC for one proxy pod.
fn proxy_role(name: &str, provisioner_binding: &str) -> Value {
    json!({
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["secrets"],
                "resourceNames": [name],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "resourceNames": [name],
                "verbs": ["patch"],
            },
            {
                "apiGroups": ["rbac.authorization.k8s.io"],
                "resources": ["rolebindings"],
                "resourceNames": [provisioner_binding],
                "verbs": ["get"],
            },
        ]
    })
}

fn proxy_role_binding(name: &str, namespace: &str) -> Value {
    json!({
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": name,
        },
        "subjects": [{
            "kind": "ServiceAccount",
            "name": name,
            "namespace": namespace,
        }],
    })
}

/// render the per-Gateway proxy Deployment using platform scheduling and runtime settings.
fn proxy_deployment(
    name: &str,
    uid: &str,
    selector: &Value,
    settings: &Settings,
) -> Value {
    let mut pod_labels = settings.pod_labels.clone();
    pod_labels.insert(OWNER.into(), uid.into());
    pod_labels.insert("app.kubernetes.io/managed-by".into(), "weiche".into());
    pod_labels.insert("app.kubernetes.io/instance".into(), settings.release.clone());

    let mut node_selector = settings.node_selector.clone();
    node_selector.insert("kubernetes.io/os".into(), "linux".into());

    let mut env = vec![
        json!({"name": "WEICHE_WORKERS", "value": settings.workers.to_string()}),
        json!({"name": "WEICHE_PROVISIONER_BINDING", "value": settings.provisioner_binding}),
        json!({"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}),
    ];
    if !settings.log.level.is_empty()
        && !settings.extra_env.iter().any(|v| v.name == "RUST_LOG")
    {
        env.push(json!({"name": "RUST_LOG", "value": settings.log.level}));
    }
    if !settings.extra_env.iter().any(|v| v.name == "WEICHE_LOG_FORMAT") {
        env.push(json!({"name": "WEICHE_LOG_FORMAT", "value": settings.log.format}));
    }
    for var in &settings.extra_env {
        env.push(json!(var));
    }

    let capabilities = if settings.host_network {
        json!({"drop": ["ALL"], "add": ["NET_BIND_SERVICE"]})
    } else {
        json!({"drop": ["ALL"]})
    };
    let mut pod_spec = json!({
        "serviceAccountName": name,
        "nodeSelector": node_selector,
        "imagePullSecrets": settings.pull_secrets,
        "terminationGracePeriodSeconds": settings.termination_grace_period_seconds,
        "volumes": [{"name": "run", "emptyDir": {"medium": "Memory"}}],
        "containers": [{
            "name": "proxy",
            "image": settings.image,
            "imagePullPolicy": settings.pull_policy,
            "args": ["proxy", name],
            "resources": settings.resources,
            "env": env,
            "volumeMounts": [{"name": "run", "mountPath": "/run/weiche"}],
            "readinessProbe": {
                "exec": {"command": ["/usr/local/bin/weiche", "ready"]},
                "periodSeconds": 2,
                "failureThreshold": 1,
            },
            "securityContext": {
                "runAsNonRoot": true,
                "runAsUser": 65532,
                "allowPrivilegeEscalation": false,
                "capabilities": capabilities,
                "seccompProfile": settings.seccomp_profile,
            },
        }],
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

    let mut pod_meta = json!({"labels": pod_labels});
    if !settings.pod_annotations.is_empty() {
        pod_meta["annotations"] = json!(settings.pod_annotations);
    }
    json!({
        "spec": {
            "replicas": settings.replicas,
            "selector": {"matchLabels": selector},
            "template": {
                "metadata": pod_meta,
                "spec": pod_spec,
            },
        }
    })
}

fn proxy_service(labels: &Value, ports: Vec<Value>, settings: &Settings) -> Value {
    json!({
        "spec": {
            "type": settings.service_type,
            "ipFamilyPolicy": "PreferDualStack",
            "selector": labels,
            "ports": ports,
        }
    })
}
