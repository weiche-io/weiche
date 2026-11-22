//! build the Kubernetes resources owned by a gateway.
use super::{super::ir::Transport, config::ServiceSettings, service::OWNER};
use k8s_openapi::{
    api::core::v1::{Service, ServicePort, ServiceSpec},
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
    /// select only the pods serving this gateway uid.
    pub fn selector(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(OWNER.into(), self.owner.uid.clone())])
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
            metadata: ObjectMeta {
                annotations: (!settings.annotations.is_empty())
                    .then(|| settings.annotations.clone()),
                ..Default::default()
            },
            spec: Some(spec),
            ..Default::default()
        }
    }
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
