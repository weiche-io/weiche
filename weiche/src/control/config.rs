//! read platform installation settings for gateway proxy workloads.
use super::CONTROLLER_NAME;
use anyhow::ensure;
use k8s_openapi::api::core::v1::{
    Affinity, EnvVar, LocalObjectReference, ResourceRequirements, SeccompProfile,
    Toleration, TopologySpreadConstraint,
};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, IntoDeserializer},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

/// declare each setting's accepted spellings together with its wire representation.
macro_rules! setting {
    ($(#[$doc:meta])* $name:ident { $($(#[$attr:meta])* $variant:ident = $wire:literal),+ }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
        pub enum $name {
            $(#[doc = concat!("use the `", $wire, "` setting.")]
              $(#[$attr])* #[serde(rename = $wire)] $variant),+
        }
        impl $name {
            /// return the spelling expected by the proxy or Kubernetes API.
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }
        }
    };
}

setting! {
    /// proxy log encoding.
    LogFormat { Text = "text", Json = "json" }
}
setting! {
    /// exposure of a gateway Service.
    #[derive(Default)]
    ServiceType {
        #[default] #[serde(alias = "")] ClusterIP = "ClusterIP",
        LoadBalancer = "LoadBalancer", NodePort = "NodePort"
    }
}
setting! {
    /// stack allocation policy for a gateway Service.
    #[derive(Default)]
    IpFamilyPolicy {
        SingleStack = "SingleStack", #[default] PreferDualStack = "PreferDualStack",
        RequireDualStack = "RequireDualStack"
    }
}
setting! {
    /// address family allocated to a gateway Service.
    IpFamily { IPv4 = "IPv4", IPv6 = "IPv6" }
}
setting! {
    /// scope of externally received Service traffic.
    ExternalTrafficPolicy { Cluster = "Cluster", Local = "Local" }
}

/// accept null, an omitted setting or Helm's empty string for an optional override.
fn optional<'de, T: DeserializeOwned, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<String>::deserialize(d)?
        .filter(|s| !s.is_empty())
        .map(|s| T::deserialize(s.into_deserializer()))
        .transpose()
}

/// platform logging defaults for provisioned proxy containers.
#[derive(Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LogSettings {
    /// default RUST_LOG filter applied to proxy pods.
    pub level: String,
    /// output log format (`text` or `json`).
    pub format: LogFormat,
}

impl Default for LogSettings {
    /// use the installation's default proxy log level and format.
    fn default() -> Self {
        Self { level: "info".into(), format: LogFormat::Text }
    }
}

/// platform networking defaults for provisioned Gateway Services.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ServiceSettings {
    /// gateway Service type (`ClusterIP`, `LoadBalancer`, or `NodePort`).
    pub r#type: ServiceType,
    /// gateway IP family policy (`SingleStack`, `PreferDualStack`, or `RequireDualStack`).
    pub ip_family_policy: IpFamilyPolicy,
    /// optional ordered IP families (`IPv4`, `IPv6`).
    pub ip_families: Vec<IpFamily>,
    /// optional external traffic policy (`Cluster` or `Local`) for LoadBalancer or NodePort.
    #[serde(deserialize_with = "optional")]
    pub external_traffic_policy: Option<ExternalTrafficPolicy>,
    /// default annotations merged onto every Gateway Service.
    pub annotations: BTreeMap<String, String>,
}

/// platform policy for exposing HTTPS listeners over QUIC.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Http3Settings {
    /// serve HTTP/3 on eligible HTTPS ports.
    pub enabled: bool,
    /// external UDP port advertised to HTTPS clients.
    pub advertised_port: Option<u16>,
}

/// installation defaults shared by the controller and its proxy workloads.
#[derive(Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Installation {
    /// release instance name used to isolate multi-release workloads.
    pub release: String,
    /// gateway class controller name matched by this controller.
    pub controller_name: String,
    /// coordination Lease name used for leader election.
    pub lease_name: String,
    /// provisioner RoleBinding and ClusterRole name checked by proxies.
    pub provisioner_binding: String,
    /// graceful termination window in seconds for proxy pods.
    pub termination_grace_period_seconds: i64,
}

impl Default for Installation {
    /// use the standard release identity, RBAC and pod shutdown window.
    fn default() -> Self {
        Self {
            release: "weiche".into(),
            controller_name: CONTROLLER_NAME.into(),
            lease_name: "weiche-controller".into(),
            provisioner_binding: "weiche-gateway-provisioner".into(),
            termination_grace_period_seconds: 30,
        }
    }
}

/// installation policy supplied by the platform, outside of tenant-writable Gateway specs.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// platform defaults accepted alongside the required workload settings.
    #[serde(flatten)]
    pub installation: Installation,
    /// local IP address bound by proxy listeners.
    pub bind_address: IpAddr,
    /// namespaces authorized to host Gateway proxy workloads.
    pub namespaces: BTreeSet<String>,
    /// container image rendered into per-Gateway proxy Deployments.
    pub image: String,
    /// image pull policy for proxy containers.
    pub pull_policy: String,
    /// image pull secrets copied onto proxy pods.
    pub pull_secrets: Vec<LocalObjectReference>,
    /// container resource requests and limits for proxy pods.
    pub resources: ResourceRequirements,
    /// seccomp profile required for proxy io_uring workers.
    pub seccomp_profile: SeccompProfile,
    /// desired proxy Deployment replica count.
    pub replicas: i32,
    /// compio worker thread count inside each proxy pod.
    pub workers: std::num::NonZeroUsize,
    /// bind proxy listeners directly on the host network namespace.
    #[serde(default)]
    pub host_network: bool,
    /// optional Kubernetes DNS policy override for proxy pods.
    #[serde(default, deserialize_with = "optional")]
    pub dns_policy: Option<String>,
    /// additional node selector labels merged with kubernetes.io/os=linux.
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    /// scheduling tolerations applied to proxy pods.
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
    /// scheduling affinity rules applied to proxy pods.
    pub affinity: Option<Affinity>,
    /// topology spread constraints applied to proxy pods.
    #[serde(default)]
    pub topology_spread_constraints: Vec<TopologySpreadConstraint>,
    /// optional priorityClassName applied to proxy pods.
    #[serde(default, deserialize_with = "optional")]
    pub priority_class_name: Option<String>,
    /// additional pod template labels applied to proxy pods.
    #[serde(default)]
    pub pod_labels: BTreeMap<String, String>,
    /// additional pod template annotations applied to proxy pods.
    #[serde(default)]
    pub pod_annotations: BTreeMap<String, String>,
    /// proxy container logging settings.
    #[serde(default)]
    pub log: LogSettings,
    /// extra environment variables appended to proxy containers.
    #[serde(default)]
    pub extra_env: Vec<EnvVar>,
    /// networking defaults for every Gateway Service.
    #[serde(default)]
    pub service: ServiceSettings,
    /// opt-in downstream HTTP/3 configuration for this installation.
    #[serde(default)]
    pub http3: Http3Settings,
}

impl Settings {
    /// read and validate platform installation settings from `/etc/weiche/proxy.json`.
    pub fn load() -> anyhow::Result<Self> {
        let settings: Self =
            serde_json::from_reader(std::fs::File::open("/etc/weiche/proxy.json")?)?;
        let installation = &settings.installation;
        let service = &settings.service;
        ensure!(
            !installation.release.is_empty()
                && !installation.controller_name.is_empty()
                && !installation.lease_name.is_empty()
                && !installation.provisioner_binding.is_empty()
                && !settings.namespaces.is_empty()
                && settings.replicas > 0
                && installation.termination_grace_period_seconds > 0
                && !settings.image.is_empty(),
            "invalid proxy installation settings"
        );
        let families = service.ip_families.as_slice();
        ensure!(
            families.len() <= 1 || matches!(families, [a, b] if a != b),
            "invalid gateway service ipFamilies"
        );
        ensure!(
            !matches!(
                (service.ip_family_policy, families.len()),
                (IpFamilyPolicy::SingleStack, 2) | (IpFamilyPolicy::RequireDualStack, 1)
            ),
            "gateway service ipFamilies conflicts with ipFamilyPolicy"
        );
        ensure!(
            service.external_traffic_policy.is_none()
                || service.r#type != ServiceType::ClusterIP,
            "invalid gateway service externalTrafficPolicy"
        );
        ensure!(
            settings.http3.advertised_port != Some(0),
            "invalid advertised HTTP/3 port"
        );
        ensure!(
            !settings.extra_env.iter().any(|var| matches!(
                var.name.as_str(),
                "POD_UID"
                    | "WEICHE_PROVISIONER_BINDING"
                    | "WEICHE_WORKERS"
                    | "WEICHE_HTTP3"
                    | "WEICHE_HTTP3_PORT"
            )),
            "proxy extraEnv cannot override reserved environment variables"
        );
        Ok(settings)
    }
}
