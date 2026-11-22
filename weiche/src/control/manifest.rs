//! build the Kubernetes resources owned by a gateway.
use super::{
    super::ir::{Transport, wire::SNAPSHOT_KEY},
    config::{ServiceSettings, Settings},
    service::{GATEWAY_NAME, OWNER},
};
use k8s_openapi::{
    ByteString,
    api::{
        core::v1::{
            Capabilities, ConfigMap, Container, EnvVar, EnvVarSource, ExecAction,
            ObjectFieldSelector, Probe, Secret, SecurityContext, Service, ServiceAccount,
            ServicePort, ServiceSpec, VolumeMount,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{
        apis::meta::v1::{ObjectMeta, OwnerReference},
        util::intstr::IntOrString,
    },
};
use std::{collections::BTreeMap, net::IpAddr};
use weiche_gtwapi::v1::gateways::{Gateway, GatewayInfrastructure};

/// common identity and metadata shared by all resources owned by one Gateway.
pub struct Target<'a> {
    /// gateway namespace hosting the provisioned resources.
    pub namespace: &'a str,
    /// deterministic resource name (`weiche-gw-<uid>`).
    pub name: &'a str,
    /// gateway name used in the standard infrastructure label.
    pub gateway_name: &'a str,
    /// release instance label isolating multi-release ownership.
    pub instance: &'a str,
    /// controller ownerReference pointing back to the Gateway.
    pub owner: &'a OwnerReference,
    /// optional Gateway-level labels and annotations to propagate.
    pub infra: Option<&'a GatewayInfrastructure>,
}

impl Target<'_> {
    /// overlay gateway infrastructure labels, reserving the controller's identity labels.
    fn labels(&self, mut labels: BTreeMap<String, String>) -> BTreeMap<String, String> {
        if let Some(infra) = self.infra.and_then(|i| i.labels.as_ref()) {
            labels.extend(infra.clone());
        }
        labels.insert(GATEWAY_NAME.into(), self.gateway_name.into());
        labels.insert("app.kubernetes.io/managed-by".into(), "weiche".into());
        labels.insert("app.kubernetes.io/instance".into(), self.instance.into());
        labels
    }

    /// merge gateway annotations over platform defaults and omit an empty result.
    fn annotations(
        &self,
        mut annotations: BTreeMap<String, String>,
    ) -> Option<BTreeMap<String, String>> {
        if let Some(infra) = self.infra.and_then(|i| i.annotations.as_ref()) {
            annotations.extend(infra.clone());
        }
        (!annotations.is_empty()).then_some(annotations)
    }

    /// identify an owned resource and attach its controller reference.
    pub fn metadata(&self, annotations: BTreeMap<String, String>) -> ObjectMeta {
        ObjectMeta {
            name: Some(self.name.into()),
            namespace: Some(self.namespace.into()),
            labels: Some(self.labels(BTreeMap::new())),
            annotations: self.annotations(annotations),
            owner_references: Some(vec![self.owner.clone()]),
            ..Default::default()
        }
    }

    /// label the pod template for selection by both the deployment and the service.
    pub fn pod_metadata(&self, settings: &Settings) -> ObjectMeta {
        let mut labels = self.labels(settings.pod_labels.clone());
        labels.insert(OWNER.into(), self.owner.uid.clone());
        ObjectMeta {
            labels: Some(labels),
            annotations: self.annotations(settings.pod_annotations.clone()),
            ..Default::default()
        }
    }

    /// leave report data to the pods that acknowledge listener revisions.
    pub fn report(&self) -> ConfigMap {
        ConfigMap {
            metadata: self.metadata(BTreeMap::new()),
            ..Default::default()
        }
    }

    /// publish the encoded gateway snapshot under the proxy's watched key.
    pub fn snapshot(&self, payload: Vec<u8>) -> Secret {
        Secret {
            metadata: self.metadata(BTreeMap::new()),
            type_: Some("weiche.io/snapshot".into()),
            data: Some(BTreeMap::from([(SNAPSHOT_KEY.into(), ByteString(payload))])),
            ..Default::default()
        }
    }

    /// identify the proxy pods to the Kubernetes API.
    pub fn service_account(&self) -> ServiceAccount {
        ServiceAccount {
            metadata: self.metadata(BTreeMap::new()),
            ..Default::default()
        }
    }

    /// restrict proxy access to its snapshot, report and provisioner binding.
    pub fn role(&self, provisioner_binding: &str) -> Role {
        let rule = |group: &str, resource: &str, name: &str, verbs: &[&str]| PolicyRule {
            api_groups: Some(vec![group.into()]),
            resources: Some(vec![resource.into()]),
            resource_names: Some(vec![name.into()]),
            verbs: verbs.iter().map(|v| (*v).into()).collect(),
            ..Default::default()
        };
        Role {
            metadata: self.metadata(BTreeMap::new()),
            rules: Some(vec![
                rule("", "secrets", self.name, &["get", "list", "watch"]),
                rule("", "configmaps", self.name, &["patch"]),
                rule(
                    "rbac.authorization.k8s.io",
                    "rolebindings",
                    provisioner_binding,
                    &["get"],
                ),
            ]),
        }
    }

    /// bind the gateway role to its namespaced proxy service account.
    pub fn role_binding(&self) -> RoleBinding {
        RoleBinding {
            metadata: self.metadata(BTreeMap::new()),
            role_ref: RoleRef {
                api_group: Some("rbac.authorization.k8s.io".into()),
                kind: "Role".into(),
                name: self.name.into(),
            },
            subjects: Some(vec![Subject {
                kind: "ServiceAccount".into(),
                name: self.name.into(),
                namespace: Some(self.namespace.into()),
                ..Default::default()
            }]),
        }
    }

    /// select only the pods serving this gateway uid.
    pub fn selector(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(OWNER.into(), self.owner.uid.clone())])
    }

    /// configure the proxy process, readiness command and container privileges.
    pub fn container(&self, settings: &Settings) -> Container {
        Container {
            name: "proxy".into(),
            image: Some(settings.image.clone()),
            image_pull_policy: Some(settings.pull_policy.clone()),
            args: Some(vec!["proxy".into(), self.name.into()]),
            resources: Some(settings.resources.clone()),
            env: Some(proxy_env(settings)),
            volume_mounts: Some(vec![VolumeMount {
                name: "run".into(),
                mount_path: "/run/weiche".into(),
                ..Default::default()
            }]),
            readiness_probe: Some(Probe {
                exec: Some(ExecAction {
                    command: Some(vec!["/usr/local/bin/weiche".into(), "ready".into()]),
                }),
                period_seconds: Some(1),
                failure_threshold: Some(1),
                ..Default::default()
            }),
            security_context: Some(SecurityContext {
                run_as_non_root: Some(true),
                run_as_user: Some(65532),
                allow_privilege_escalation: Some(false),
                capabilities: Some(Capabilities {
                    drop: Some(vec!["ALL".into()]),
                    add: settings.host_network.then(|| vec!["NET_BIND_SERVICE".into()]),
                }),
                seccomp_profile: Some(settings.seccomp_profile.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// expose listener ports with the installation's address and traffic policies.
    pub fn service(
        &self,
        ports: Vec<(Transport, u16)>,
        settings: &ServiceSettings,
        requested_ips: &[IpAddr],
    ) -> Service {
        let ports = ports
            .into_iter()
            .map(|(transport, port)| {
                let protocol = transport.protocol();
                ServicePort {
                    name: Some(format!("{}-{port}", protocol.to_ascii_lowercase())),
                    port: i32::from(port),
                    protocol: Some(protocol.into()),
                    target_port: Some(IntOrString::Int(i32::from(port))),
                    ..Default::default()
                }
            })
            .collect();
        let mut spec = ServiceSpec {
            type_: Some(settings.r#type.clone()),
            ip_family_policy: Some(settings.ip_family_policy.clone()),
            ip_families: (!settings.ip_families.is_empty())
                .then(|| settings.ip_families.clone()),
            external_traffic_policy: settings.external_traffic_policy.clone(),
            selector: Some(self.selector()),
            ports: Some(ports),
            ..Default::default()
        };
        if let Some(first) = requested_ips.first() {
            if settings.r#type == "LoadBalancer" {
                spec.load_balancer_ip = Some(first.to_string());
            } else {
                spec.cluster_ip = Some(first.to_string());
                spec.cluster_ips =
                    Some(requested_ips.iter().map(ToString::to_string).collect());
            }
        }
        Service {
            metadata: self.metadata(settings.annotations.clone()),
            spec: Some(spec),
            ..Default::default()
        }
    }
}

/// assemble the proxy environment with platform-supplied overrides in declaration order.
fn proxy_env(settings: &Settings) -> Vec<EnvVar> {
    let literal = |name: &str, value: String| EnvVar {
        name: name.into(),
        value: Some(value),
        ..Default::default()
    };
    let mut env = vec![
        literal("WEICHE_WORKERS", settings.workers.to_string()),
        literal("WEICHE_PROVISIONER_BINDING", settings.provisioner_binding.clone()),
        EnvVar {
            name: "POD_UID".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.uid".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];
    if settings.http3.enabled {
        env.push(literal("WEICHE_HTTP3", "true".into()));
        if let Some(port) = settings.http3.advertised_port {
            env.push(literal("WEICHE_HTTP3_PORT", port.to_string()));
        }
    }
    if !settings.log.level.is_empty()
        && !settings.extra_env.iter().any(|var| var.name == "RUST_LOG")
    {
        env.push(literal("RUST_LOG", settings.log.level.clone()));
    }
    if !settings.extra_env.iter().any(|var| var.name == "WEICHE_LOG_FORMAT") {
        env.push(literal("WEICHE_LOG_FORMAT", settings.log.format.clone()));
    }
    env.extend_from_slice(&settings.extra_env);
    env
}

/// return the Kubernetes IP family name for an IP address.
fn ip_family(ip: IpAddr) -> &'static str {
    if ip.is_ipv4() { "IPv4" } else { "IPv6" }
}

/// validate and parse optional static IP addresses requested in `Gateway.spec.addresses`.
pub fn requested_addresses(
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
