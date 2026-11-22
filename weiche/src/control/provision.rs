//! reconcile Gateway-owned resources in platform-approved namespaces.
use super::{
    super::ir::{
        GatewaySnapshot,
        wire::{self, SNAPSHOT_KEY},
    },
    GatewayStores,
    config::{ServiceSettings, Settings},
    service::{GATEWAY_NAME, OWNER, Plan, controlled_by},
};
use anyhow::{Context, ensure};
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{ConfigMap, Secret, Service, ServiceAccount},
        rbac::v1::{Role, RoleBinding},
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{
        ApiResource, DeleteParams, DynamicObject, Patch, PatchParams, PostParams,
        Preconditions,
    },
    runtime::reflector::ObjectRef,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use weiche_gtwapi::v1::gateways::{Gateway, GatewayInfrastructure};

/// common identity and metadata shared by all resources owned by one Gateway.
struct Target<'a> {
    /// gateway namespace hosting the provisioned resources.
    namespace: &'a str,
    /// deterministic resource name (`weiche-gw-<uid>`).
    name: &'a str,
    /// gateway name used in the standard infrastructure label.
    gateway_name: &'a str,
    /// release instance label isolating multi-release ownership.
    instance: &'a str,
    /// controller ownerReference pointing back to the Gateway.
    owner: &'a OwnerReference,
    /// optional Gateway-level labels and annotations to propagate.
    infra: Option<&'a GatewayInfrastructure>,
}

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
        fields["metadata"]["labels"][GATEWAY_NAME] = json!(target.gateway_name);
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
    let replicas = if active { settings.replicas } else { 0 };
    let selector = json!({(OWNER):uid});
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
                proxy_service(&selector, ports, settings, &ips),
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

/// bind the gateway role to its proxy service account.
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
    target: &Target<'_>,
    selector: &Value,
    settings: &Settings,
    replicas: i32,
) -> Value {
    let name = target.name;
    let infra = target.infra;
    let mut pod_labels = settings.pod_labels.clone();
    if let Some(labels) = infra.and_then(|i| i.labels.as_ref()) {
        pod_labels.extend(labels.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    pod_labels.insert(OWNER.into(), target.owner.uid.clone());
    pod_labels.insert(GATEWAY_NAME.into(), target.gateway_name.into());
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
    if settings.http3.enabled {
        env.push(json!({"name": "WEICHE_HTTP3", "value": "true"}));
        if let Some(port) = settings.http3.advertised_port {
            env.push(json!({"name": "WEICHE_HTTP3_PORT", "value": port.to_string()}));
        }
    }
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
                "periodSeconds": 1,
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
            "replicas": replicas,
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
