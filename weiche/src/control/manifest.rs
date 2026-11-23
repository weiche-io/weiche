//! build the Kubernetes resources owned by a gateway.
use super::{
    super::ir::{Transport, wire::SNAPSHOT_KEY},
    config::{IpFamily, IpFamilyPolicy, ServiceSettings, ServiceType, Settings},
    service::{GATEWAY_NAME, OWNER},
};
use anyhow::{Context, ensure};
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{Secret, Service, ServicePort, ServiceSpec},
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

/// omit optional Kubernetes lists when the installation supplies no entries.
fn nonempty<T>(values: impl ExactSizeIterator<Item = T>) -> Option<Vec<T>> {
    (values.len() != 0).then(|| values.collect())
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
        // gateway names are DNS subdomains; the API only allows 63 bytes in a label value.
        if self.owner.name.len() <= 63 {
            labels.insert(GATEWAY_NAME.into(), self.owner.name.clone());
        } else {
            labels.remove(GATEWAY_NAME);
        }
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
            api_groups: Some(vec![group.into()]),
            resources: Some(vec![resource.into()]),
            resource_names: Some(vec![name.into()]),
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

    /// bind the platform pod template to this gateway's identity and snapshot.
    pub fn deployment(&self, settings: &Settings) -> Deployment {
        let mut template = settings.pod_template.clone();
        let meta = template.metadata.get_or_insert_default();
        let gateway = self.template(
            meta.labels.take().unwrap_or_default(),
            meta.annotations.take().unwrap_or_default(),
        );
        meta.labels = gateway.labels;
        meta.labels.get_or_insert_default().extend(self.selector());
        meta.annotations = gateway.annotations;
        template.spec.get_or_insert_default().service_account_name =
            Some(self.name.into());
        let mut deployment: Deployment = self.owned();
        deployment.metadata.name = Some(self.owner.name.clone());
        deployment.spec = DeploymentSpec {
            replicas: Some(settings.replicas),
            selector: LabelSelector {
                match_labels: Some(self.selector()),
                ..Default::default()
            },
            template,
            ..Default::default()
        };
        deployment
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
            ip_families: nonempty(settings.ip_families.iter().map(|f| f.as_str().into())),
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

/// return the Kubernetes IP family name for an IP address.
fn ip_family(ip: IpAddr) -> IpFamily {
    if ip.is_ipv4() { IpFamily::IPv4 } else { IpFamily::IPv6 }
}

/// validate and parse optional static IP addresses requested in `Gateway.spec.addresses`.
pub fn requested_addresses(
    gateway: &Gateway,
    service: &ServiceSettings,
) -> anyhow::Result<Vec<IpAddr>> {
    let mut ips = Vec::new();
    for addr in gateway.spec.addresses.iter().flatten() {
        // the Service fields used for static addresses can only carry IP literals.
        ensure!(
            addr.r#type.as_deref().unwrap_or("IPAddress") == "IPAddress",
            "only IPAddress gateway addresses are supported"
        );
        let raw = addr
            .value
            .as_deref()
            .filter(|v| !v.is_empty())
            .context("requested gateway address value is required")?;
        let ip: IpAddr = raw
            .parse()
            .with_context(|| format!("invalid gateway IP address: {raw}"))?;
        ips.push(ip);
    }
    let configured = &service.ip_families;
    if service.r#type == ServiceType::LoadBalancer {
        // loadBalancerIP carries one address, so report any extra request on the Gateway.
        ensure!(
            ips.len() <= 1,
            "LoadBalancer Service supports at most one requested IP address"
        );
        // report an external IP outside the configured families as AddressNotAssigned.
        ensure!(
            configured.is_empty()
                || ips.first().is_none_or(|ip| configured.contains(&ip_family(*ip))),
            "requested IP family does not match service ipFamilies"
        );
    } else {
        // clusterIPs can hold one address per family, subject to the Service's stack policy.
        let compatible = match ips.as_slice() {
            [] | [_] => true,
            [a, b] => {
                service.ip_family_policy != IpFamilyPolicy::SingleStack
                    && a.is_ipv4() != b.is_ipv4()
            }
            _ => false,
        };
        ensure!(
            compatible,
            "requested gateway addresses are incompatible with Service IP families"
        );
        // explicit positions must agree so Kubernetes can infer the rest from clusterIPs.
        ensure!(
            configured
                .iter()
                .zip(&ips)
                .all(|(&family, &ip)| family == ip_family(ip)),
            "requested IP families do not match configured service ipFamilies"
        );
    }
    Ok(ips)
}
