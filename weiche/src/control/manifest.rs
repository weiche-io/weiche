//! build the Kubernetes resources owned by a gateway.
use super::{
    super::ir::{Transport, wire::SNAPSHOT_KEY},
    config::{IpFamily, IpFamilyPolicy, ServiceSettings, ServiceType, Settings},
    service::{GATEWAY_NAME, OWNER},
};
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{
            Capabilities, Container, EmptyDirVolumeSource, EnvVar, EnvVarSource,
            ExecAction, ObjectFieldSelector, PodSecurityContext, PodSpec,
            PodTemplateSpec, Probe, Secret, SecurityContext, Service, ServicePort,
            ServiceSpec, Sysctl, Volume, VolumeMount,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::{
        apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference},
        util::intstr::IntOrString,
    },
};
use kube::Resource;
use std::{collections::BTreeMap, net::IpAddr};
use weiche_gtwapi::v1::gateways::{Gateway, GatewayInfrastructure};

/// Kubernetes labels or annotations keyed by their qualified names.
type Tags = BTreeMap<String, String>;

/// build Kubernetes string lists from static names and target identifiers.
fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.map(str::to_owned).into()
}

/// common identity and metadata shared by all resources owned by one Gateway.
pub struct Target<'a> {
    /// gateway namespace hosting the provisioned resources.
    pub namespace: &'a str,
    /// deterministic resource name (`weiche-gw-<uid>`).
    pub name: &'a str,
    /// release instance label isolating multi-release ownership.
    pub instance: &'a str,
    /// controller ownerReference pointing back to the Gateway.
    pub owner: &'a OwnerReference,
    /// optional Gateway-level labels and annotations to propagate.
    pub infra: Option<&'a GatewayInfrastructure>,
}

impl Target<'_> {
    /// apply the infrastructure metadata over platform tags, reserving controller labels.
    fn template(&self, mut labels: Tags, mut annotations: Tags) -> ObjectMeta {
        if let Some(infra) = self.infra {
            labels.extend(infra.labels.clone().unwrap_or_default());
            annotations.extend(infra.annotations.clone().unwrap_or_default());
        }
        labels.insert(GATEWAY_NAME.into(), self.owner.name.clone());
        labels.insert("app.kubernetes.io/managed-by".into(), "weiche".into());
        labels.insert("app.kubernetes.io/instance".into(), self.instance.into());
        ObjectMeta {
            labels: Some(labels),
            annotations: (!annotations.is_empty()).then_some(annotations),
            ..Default::default()
        }
    }

    /// identify an owned resource and attach its controller reference.
    fn metadata(&self, annotations: Tags) -> ObjectMeta {
        ObjectMeta {
            name: Some(self.name.into()),
            namespace: Some(self.namespace.into()),
            owner_references: Some(vec![self.owner.clone()]),
            ..self.template(Tags::new(), annotations)
        }
    }

    /// construct a metadata-only resource, leaving report data under proxy ownership.
    pub fn owned<K: Default + Resource>(&self) -> K {
        let mut resource = K::default();
        *resource.meta_mut() = self.metadata(Tags::new());
        resource
    }

    /// publish the encoded gateway snapshot under the proxy's watched key.
    pub fn snapshot(&self, payload: Vec<u8>) -> Secret {
        Secret {
            type_: Some("weiche.io/snapshot".into()),
            data: Some(BTreeMap::from([(SNAPSHOT_KEY.into(), ByteString(payload))])),
            ..self.owned()
        }
    }

    /// restrict proxy access to its snapshot, report and provisioner binding.
    pub fn role(&self, binding: &str) -> Role {
        let rule = |group: &str, resource: &str, name: &str, verbs: &[&str]| PolicyRule {
            api_groups: Some(strings([group])),
            resources: Some(strings([resource])),
            resource_names: Some(strings([name])),
            verbs: verbs.iter().map(|v| (*v).into()).collect(),
            ..Default::default()
        };
        Role {
            metadata: self.metadata(Tags::new()),
            rules: Some(vec![
                rule("", "secrets", self.name, &["get", "list", "watch"]),
                rule("", "configmaps", self.name, &["patch"]),
                rule("rbac.authorization.k8s.io", "rolebindings", binding, &["get"]),
            ]),
        }
    }

    /// bind the gateway role to its namespaced proxy service account.
    pub fn role_binding(&self) -> RoleBinding {
        RoleBinding {
            metadata: self.metadata(Tags::new()),
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
    fn selector(&self) -> Tags {
        BTreeMap::from([(OWNER.into(), self.owner.uid.clone())])
    }

    /// deploy the requested replicas with the same pod selector used by the service.
    pub fn deployment(&self, settings: &Settings, replicas: i32) -> Deployment {
        let options = &settings.installation;
        let mut pod =
            self.template(options.pod_labels.clone(), options.pod_annotations.clone());
        pod.labels.get_or_insert_default().extend(self.selector());
        Deployment {
            spec: DeploymentSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: Some(self.selector()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(pod),
                    spec: Some(self.pod_spec(settings)),
                },
                ..Default::default()
            },
            ..self.owned()
        }
    }

    /// configure the pod network namespace, scheduling and memory-backed runtime volume.
    fn pod_spec(&self, settings: &Settings) -> PodSpec {
        let options = &settings.installation;
        let mut node_selector = options.node_selector.clone();
        node_selector.insert("kubernetes.io/os".into(), "linux".into());
        let mut spec = PodSpec {
            service_account_name: Some(self.name.into()),
            node_selector: Some(node_selector),
            image_pull_secrets: Some(settings.pull_secrets.clone()),
            termination_grace_period_seconds: Some(
                options.termination_grace_period_seconds,
            ),
            volumes: Some(vec![Volume {
                name: "run".into(),
                empty_dir: Some(EmptyDirVolumeSource {
                    medium: Some("Memory".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            containers: vec![self.container(settings)],
            dns_policy: options.dns_policy.clone(),
            tolerations: (!options.tolerations.is_empty())
                .then(|| options.tolerations.clone()),
            affinity: options.affinity.clone(),
            topology_spread_constraints: (!options
                .topology_spread_constraints
                .is_empty())
            .then(|| options.topology_spread_constraints.clone()),
            priority_class_name: options.priority_class_name.clone(),
            ..Default::default()
        };
        if options.host_network {
            spec.host_network = Some(true);
            spec.dns_policy
                .get_or_insert_with(|| "ClusterFirstWithHostNet".into());
        } else {
            // the unprivileged port sysctl requires a private network namespace.
            spec.security_context = Some(PodSecurityContext {
                sysctls: Some(vec![Sysctl {
                    name: "net.ipv4.ip_unprivileged_port_start".into(),
                    value: "0".into(),
                }]),
                ..Default::default()
            });
        }
        spec
    }

    /// configure the proxy process, readiness command and container privileges.
    fn container(&self, settings: &Settings) -> Container {
        let options = &settings.installation;
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
                    add: options.host_network.then(|| vec!["NET_BIND_SERVICE".into()]),
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
            type_: Some(settings.r#type.as_str().into()),
            ip_family_policy: Some(settings.ip_family_policy.as_str().into()),
            ip_families: (!settings.ip_families.is_empty()).then(|| {
                settings.ip_families.iter().map(|f| f.as_str().into()).collect()
            }),
            external_traffic_policy: settings
                .external_traffic_policy
                .map(|p| p.as_str().into()),
            selector: Some(self.selector()),
            ports: Some(ports),
            ..Default::default()
        };
        if let Some(first) = requested_ips.first() {
            if settings.r#type == ServiceType::LoadBalancer {
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
    let options = &settings.installation;
    let literal = |name: &str, value: String| EnvVar {
        name: name.into(),
        value: Some(value),
        ..Default::default()
    };
    let mut env = vec![
        literal("WEICHE_WORKERS", settings.workers.to_string()),
        literal("WEICHE_PROVISIONER_BINDING", options.provisioner_binding.clone()),
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
    if options.http3.enabled {
        env.push(literal("WEICHE_HTTP3", "true".into()));
        if let Some(port) = options.http3.advertised_port {
            env.push(literal("WEICHE_HTTP3_PORT", port.to_string()));
        }
    }
    if !options.log.level.is_empty()
        && !options.extra_env.iter().any(|var| var.name == "RUST_LOG")
    {
        env.push(literal("RUST_LOG", options.log.level.clone()));
    }
    if !options.extra_env.iter().any(|var| var.name == "WEICHE_LOG_FORMAT") {
        env.push(literal("WEICHE_LOG_FORMAT", options.log.format.as_str().into()));
    }
    env.extend_from_slice(&options.extra_env);
    env
}

/// return the Kubernetes IP family name for an IP address.
fn ip_family(ip: IpAddr) -> IpFamily {
    if ip.is_ipv4() { IpFamily::IPv4 } else { IpFamily::IPv6 }
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
    if service.r#type == ServiceType::LoadBalancer {
        if ips.len() > 1 {
            return Err(
                "LoadBalancer Service supports at most one requested IP address".into()
            );
        }
        if !service.ip_families.is_empty()
            && !service.ip_families.contains(&ip_family(ips[0]))
        {
            return Err("requested IP family does not match service ipFamilies".into());
        }
        return Ok(ips);
    }
    if ips.len() > 2
        || (service.ip_family_policy == IpFamilyPolicy::SingleStack && ips.len() > 1)
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
                .any(|(ip, family)| ip_family(*ip) != *family))
    {
        return Err(
            "requested IP families do not match configured service ipFamilies".into()
        );
    }
    Ok(ips)
}
