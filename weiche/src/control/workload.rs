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
    net::IpAddr,
    time::{Duration, Instant},
};
use weiche_gtwapi::v1::gateways::{Gateway, GatewayInfrastructure};

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

/// default Kubernetes IP family policy for Gateway Services.
fn default_ip_family_policy() -> String {
    "PreferDualStack".into()
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

/// platform networking defaults for provisioned Gateway Services.
#[derive(Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ServiceSettings {
    /// Kubernetes Service type (`ClusterIP`, `LoadBalancer`, or `NodePort`).
    pub r#type: String,
    /// Kubernetes IP family policy (`SingleStack`, `PreferDualStack`, or `RequireDualStack`).
    pub ip_family_policy: String,
    /// optional ordered IP families (`IPv4`, `IPv6`).
    pub ip_families: Vec<String>,
    /// optional external traffic policy (`Cluster` or `Local`) for LoadBalancer or NodePort.
    pub external_traffic_policy: Option<String>,
    /// default annotations merged onto every Gateway Service.
    pub annotations: BTreeMap<String, String>,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            r#type: String::new(),
            ip_family_policy: default_ip_family_policy(),
            ip_families: Vec::new(),
            external_traffic_policy: None,
            annotations: BTreeMap::new(),
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
    pub bind_address: IpAddr,
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
    /// Service networking defaults for every Gateway.
    #[serde(default)]
    service: ServiceSettings,
    /// legacy flat serviceType field retained for backward compatibility.
    #[serde(default)]
    service_type: Option<String>,
}
impl Settings {
    /// read and validate platform installation settings from `/etc/weiche/proxy.json`.
    pub fn load() -> anyhow::Result<Self> {
        let mut settings: Self =
            serde_json::from_reader(std::fs::File::open("/etc/weiche/proxy.json")?)?;
        if settings.service.r#type.is_empty() {
            settings.service.r#type =
                settings.service_type.take().unwrap_or_else(|| "ClusterIP".into());
        }
        if settings.service.external_traffic_policy.as_deref() == Some("") {
            settings.service.external_traffic_policy = None;
        }
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
            matches!(
                settings.service.r#type.as_str(),
                "ClusterIP" | "LoadBalancer" | "NodePort"
            ),
            "unsupported gateway service type"
        );
        ensure!(
            matches!(
                settings.service.ip_family_policy.as_str(),
                "SingleStack" | "PreferDualStack" | "RequireDualStack"
            ),
            "unsupported gateway service ipFamilyPolicy"
        );
        let mut seen = BTreeSet::new();
        for family in &settings.service.ip_families {
            ensure!(
                matches!(family.as_str(), "IPv4" | "IPv6")
                    && seen.insert(family.as_str()),
                "invalid gateway service ipFamilies"
            );
        }
        ensure!(
            !(settings.service.ip_family_policy == "SingleStack"
                && settings.service.ip_families.len() > 1)
                && !(settings.service.ip_family_policy == "RequireDualStack"
                    && settings.service.ip_families.len() == 1),
            "gateway service ipFamilies conflicts with ipFamilyPolicy"
        );
        if let Some(policy) = settings.service.external_traffic_policy.as_deref() {
            ensure!(
                matches!(policy, "Cluster" | "Local")
                    && matches!(
                        settings.service.r#type.as_str(),
                        "LoadBalancer" | "NodePort"
                    ),
                "invalid gateway service externalTrafficPolicy"
            );
        }
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

/// common identity and metadata shared by all resources owned by one Gateway.
struct Target<'a> {
    /// gateway namespace hosting the provisioned resources.
    namespace: &'a str,
    /// deterministic resource name (`weiche-gw-<uid>`).
    name: &'a str,
    /// Helm release instance label isolating multi-release ownership.
    instance: &'a str,
    /// controller ownerReference pointing back to the Gateway.
    owner: &'a Value,
    /// optional Gateway-level labels and annotations to propagate.
    infra: Option<&'a GatewayInfrastructure>,
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
        target: &Target<'_>,
        mut fields: Value,
    ) -> anyhow::Result<()> {
        let kind = ApiResource::erase::<K>(&());
        fields["apiVersion"] = json!(kind.api_version);
        fields["kind"] = json!(kind.kind);
        if let Some(infra) = target.infra {
            if let Some(labels) = &infra.labels {
                for (k, v) in labels {
                    fields["metadata"]["labels"][k] = json!(v);
                }
            }
            if let Some(annotations) = &infra.annotations {
                for (k, v) in annotations {
                    fields["metadata"]["annotations"][k] = json!(v);
                }
            }
        }
        fields["metadata"]["labels"]["app.kubernetes.io/managed-by"] = json!("weiche");
        fields["metadata"]["labels"]["app.kubernetes.io/instance"] =
            json!(target.instance);
        fields["metadata"]["name"] = json!(target.name);
        fields["metadata"]["namespace"] = json!(target.namespace);
        fields["metadata"]["ownerReferences"] = json!([target.owner]);
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
                old.owner_references()
                    .iter()
                    .any(|r| r.controller == Some(true)
                        && json!(r.uid) == target.owner["uid"]),
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
    let infra = g.spec.infrastructure.as_ref();
    let requested_ips = match requested_addresses(g, &settings.service) {
        Ok(ips) => ips,
        Err(message) => {
            plan.issue_reason = Some("AddressNotAssigned");
            plan.issue = Some(message);
            return Ok(());
        }
    };
    let target = Target {
        namespace: &ns,
        name: &name,
        instance: &settings.release,
        owner: &owner,
        infra,
    };
    applied.ensure::<ConfigMap>(client, &target, json!({})).await?;
    applied
        .ensure::<Secret>(
            client,
            &target,
            json!({
                "type":"weiche.io/snapshot", "data":{(SNAPSHOT_KEY):ByteString(payload)}
            }),
        )
        .await?;
    applied.ensure::<ServiceAccount>(client, &target, json!({})).await?;
    applied
        .ensure::<Role>(client, &target, proxy_role(&name, &settings.provisioner_binding))
        .await?;
    applied
        .ensure::<RoleBinding>(client, &target, proxy_role_binding(&name, &ns))
        .await?;
    applied
        .ensure::<Deployment>(
            client,
            &target,
            proxy_deployment(&name, &uid, &labels, settings, infra),
        )
        .await?;
    applied
        .ensure::<Service>(
            client,
            &target,
            proxy_service(&labels, plan.ports(), settings, &requested_ips),
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

/// return the Kubernetes IP family name for an IP address.
fn ip_family(ip: IpAddr) -> &'static str {
    if ip.is_ipv4() { "IPv4" } else { "IPv6" }
}

/// validate and parse optional static IP addresses requested in `Gateway.spec.addresses`.
fn requested_addresses(
    gateway: &Gateway,
    service: &ServiceSettings,
) -> Result<Vec<IpAddr>, String> {
    let mut ips = Vec::new();
    for addr in gateway.spec.addresses.as_deref().unwrap_or_default() {
        let kind = addr.r#type.as_deref().unwrap_or("IPAddress");
        if kind != "IPAddress" {
            return Err("only IPAddress gateway addresses are supported".into());
        }
        let Some(raw) = addr.value.as_deref().filter(|v| !v.is_empty()) else {
            return Err("requested gateway address value is required".into());
        };
        let ip: IpAddr = raw
            .parse()
            .map_err(|_| format!("invalid gateway IP address: {raw}"))?;
        if ips.contains(&ip) {
            return Err(format!("duplicate gateway IP address: {ip}"));
        }
        ips.push(ip);
    }
    if ips.is_empty() {
        return Ok(ips);
    }
    if service.r#type == "LoadBalancer" {
        if ips.len() > 1 {
            return Err(
                "LoadBalancer Service supports at most one requested IP address".into()
            );
        }
        if !service.ip_families.is_empty()
            && !service.ip_families.iter().any(|f| f == ip_family(ips[0]))
        {
            return Err("requested IP family does not match service ipFamilies".into());
        }
        return Ok(ips);
    }
    if ips.len() > 2
        || (service.ip_family_policy == "SingleStack" && ips.len() > 1)
        || (ips.len() == 2 && ips[0].is_ipv4() == ips[1].is_ipv4())
    {
        return Err(
            "requested gateway addresses are incompatible with Service IP families"
                .into(),
        );
    }
    if !service.ip_families.is_empty()
        && (ips.len() > service.ip_families.len()
            || ips
                .iter()
                .zip(&service.ip_families)
                .any(|(ip, family)| ip_family(*ip) != family))
    {
        return Err(
            "requested IP families do not match configured service ipFamilies".into()
        );
    }
    Ok(ips)
}

/// render the per-Gateway proxy Deployment using platform and Gateway infrastructure settings.
fn proxy_deployment(
    name: &str,
    uid: &str,
    selector: &Value,
    settings: &Settings,
    infra: Option<&GatewayInfrastructure>,
) -> Value {
    let mut pod_labels = settings.pod_labels.clone();
    if let Some(labels) = infra.and_then(|i| i.labels.as_ref()) {
        pod_labels.extend(labels.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    pod_labels.insert(OWNER.into(), uid.into());
    pod_labels.insert("app.kubernetes.io/managed-by".into(), "weiche".into());
    pod_labels.insert("app.kubernetes.io/instance".into(), settings.release.clone());

    let mut pod_annotations = settings.pod_annotations.clone();
    if let Some(annotations) = infra.and_then(|i| i.annotations.as_ref()) {
        pod_annotations.extend(annotations.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

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
    if !pod_annotations.is_empty() {
        pod_meta["annotations"] = json!(pod_annotations);
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

/// render the per-Gateway Kubernetes Service with configured IP family and address policy.
fn proxy_service(
    selector: &Value,
    ports: Vec<Value>,
    settings: &Settings,
    requested_ips: &[IpAddr],
) -> Value {
    let mut spec = json!({
        "type": settings.service.r#type,
        "ipFamilyPolicy": settings.service.ip_family_policy,
        "selector": selector,
        "ports": ports,
    });
    if !settings.service.ip_families.is_empty() {
        spec["ipFamilies"] = json!(settings.service.ip_families);
    }
    if let Some(policy) = &settings.service.external_traffic_policy {
        spec["externalTrafficPolicy"] = json!(policy);
    }
    if !requested_ips.is_empty() {
        if settings.service.r#type == "LoadBalancer" {
            spec["loadBalancerIP"] = json!(requested_ips[0].to_string());
        } else {
            let ips: Vec<_> = requested_ips.iter().map(ToString::to_string).collect();
            spec["clusterIP"] = json!(ips[0]);
            spec["clusterIPs"] = json!(ips);
        }
    }
    let mut service = json!({"spec": spec});
    if !settings.service.annotations.is_empty() {
        service["metadata"]["annotations"] = json!(settings.service.annotations);
    }
    service
}
