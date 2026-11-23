//! read platform installation settings for gateway proxy workloads.
use super::CONTROLLER_NAME;
use anyhow::ensure;
use k8s_openapi::api::core::v1::{
    Affinity, EnvVar, LocalObjectReference, PodTemplateSpec, ResourceRequirements, SeccompProfile,
    Toleration, TopologySpreadConstraint,
};
use serde::{Deserialize, Deserializer, de::Error};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    num::NonZeroU16,
};

/// declare each setting's accepted spellings together with its wire representation.
macro_rules! setting {
    ($($(#[$doc:meta])* $name:ident {
        $($variant:ident = $wire:literal $(| $alias:literal)?),+
    })+) => { $(
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
        #[serde(try_from = "String")]
        pub enum $name {
            $(#[doc = concat!("use the `", $wire, "` setting.")]
              $variant),+
        }
        impl TryFrom<String> for $name {
            /// error returned for an unknown setting.
            type Error = &'static str;
            /// accept only a string spelling of this setting.
            fn try_from(raw: String) -> Result<Self, Self::Error> {
                match raw.as_str() {
                    $($wire $(| $alias)? => Ok(Self::$variant),)+
                    _ => Err(concat!("unsupported ", stringify!($name))),
                }
            }
        }
        impl $name {
            /// return the spelling expected by the proxy or Kubernetes API.
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }
        }
    )+ };
}

setting! {
    /// proxy log encoding.
    LogFormat { Text = "text", Json = "json" }
    /// exposure of a gateway Service.
    ServiceType {
        ClusterIP = "ClusterIP" | "",
        LoadBalancer = "LoadBalancer", NodePort = "NodePort"
    }
    /// stack allocation policy for a gateway Service.
    #[expect(clippy::enum_variant_names, reason = "use the Kubernetes policy names")]
    IpFamilyPolicy {
        SingleStack = "SingleStack", PreferDualStack = "PreferDualStack",
        RequireDualStack = "RequireDualStack"
    }
    /// address family allocated to a gateway Service.
    IpFamily { IPv4 = "IPv4", IPv6 = "IPv6" }
    /// scope of externally received Service traffic.
    ExternalTrafficPolicy { Cluster = "Cluster", Local = "Local" }
}

/// accept null, an omitted setting or Helm's empty string for an optional override.
fn optional<'de, T: TryFrom<String>, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<T>, D::Error>
where
    T::Error: std::fmt::Display,
{
    Option::<String>::deserialize(d)?
        .filter(|s| !s.is_empty())
        .map(T::try_from)
        .transpose()
        .map_err(D::Error::custom)
}

/// keep the serde schema and its installation defaults in the same field declarations.
macro_rules! defaults {
    (#[doc = $doc:literal] $name:ident {
        $($(#[$attr:meta])* $field:ident: $ty:ty = $value:expr),+ $(,)?
    }) => {
        #[doc = $doc]
        #[derive(Deserialize)]
        #[serde(default, rename_all = "camelCase")]
        pub struct $name {
            $($(#[$attr])* pub $field: $ty,)+
        }
        impl Default for $name {
            /// use the platform defaults for omitted fields.
            fn default() -> Self {
                Self { $($field: $value,)+ }
            }
        }
    };
}

defaults! {
    /// platform logging defaults for provisioned proxy containers.
    LogSettings {
        /// default RUST_LOG filter applied to proxy pods.
        level: String = "info".into(),
        /// output log encoding.
        format: LogFormat = LogFormat::Text,
    }
}

defaults! {
    /// platform networking defaults for provisioned Gateway Services.
    ServiceSettings {
        /// gateway Service type (`ClusterIP`, `LoadBalancer`, or `NodePort`).
        r#type: ServiceType = ServiceType::ClusterIP,
        /// gateway IP family policy (`SingleStack`, `PreferDualStack`, or `RequireDualStack`).
        ip_family_policy: IpFamilyPolicy = IpFamilyPolicy::PreferDualStack,
        /// optional ordered IP families (`IPv4`, `IPv6`).
        ip_families: Vec<IpFamily> = Vec::new(),
        /// optional external traffic policy (`Cluster` or `Local`) for LoadBalancer or NodePort.
        #[serde(deserialize_with = "optional")]
        external_traffic_policy: Option<ExternalTrafficPolicy> = None,
        /// default annotations merged onto every Gateway Service.
        annotations: BTreeMap<String, String> = BTreeMap::new(),
    }
}

/// platform policy for exposing HTTPS listeners over QUIC.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Http3Settings {
    /// serve HTTP/3 on eligible HTTPS ports.
    pub enabled: bool,
    /// external UDP port advertised to HTTPS clients.
    pub advertised_port: Option<NonZeroU16>,
}

defaults! {
    /// optional platform configuration shared by the controller and proxy workloads.
    Installation {
        /// release instance name used to isolate multi-release workloads.
        release: String = "weiche".into(),
        /// gateway class controller name matched by this controller.
        controller_name: String = CONTROLLER_NAME.into(),
        /// coordination Lease name used for leader election.
        lease_name: String = "weiche-controller".into(),
        /// provisioner RoleBinding and ClusterRole name checked by proxies.
        provisioner_binding: String = "weiche-gateway-provisioner".into(),
        /// graceful termination window in seconds for proxy pods.
        termination_grace_period_seconds: i64 = 30,
        /// bind proxy listeners directly on the host network namespace.
        host_network: bool = false,
        /// optional Kubernetes DNS policy override for proxy pods.
        #[serde(deserialize_with = "optional")]
        dns_policy: Option<String> = None,
        /// additional node selector labels merged with kubernetes.io/os=linux.
        node_selector: BTreeMap<String, String> = BTreeMap::new(),
        /// scheduling tolerations applied to proxy pods.
        tolerations: Vec<Toleration> = Vec::new(),
        /// scheduling affinity rules applied to proxy pods.
        affinity: Option<Affinity> = None,
        /// topology spread constraints applied to proxy pods.
        topology_spread_constraints: Vec<TopologySpreadConstraint> = Vec::new(),
        /// optional priorityClassName applied to proxy pods.
        #[serde(deserialize_with = "optional")]
        priority_class_name: Option<String> = None,
        /// additional pod template labels applied to proxy pods.
        pod_labels: BTreeMap<String, String> = BTreeMap::new(),
        /// additional pod template annotations applied to proxy pods.
        pod_annotations: BTreeMap<String, String> = BTreeMap::new(),
        /// proxy container logging settings.
        log: LogSettings = LogSettings::default(),
        /// extra environment variables appended to proxy containers.
        extra_env: Vec<EnvVar> = Vec::new(),
        /// networking defaults for every Gateway Service.
        service: ServiceSettings = ServiceSettings::default(),
        /// opt-in downstream HTTP/3 configuration for this installation.
        http3: Http3Settings = Http3Settings::default(),
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
    /// native pod template rendered by the platform chart.
    pub pod_template: PodTemplateSpec,
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
}

impl Settings {
    /// read and validate platform installation settings from `/etc/weiche/proxy.json`.
    pub fn load() -> anyhow::Result<Self> {
        let settings: Self =
            serde_json::from_reader(std::fs::File::open("/etc/weiche/proxy.json")?)?;
        let installation = &settings.installation;
        let service = &installation.service;
        // a broken platform setting would affect every gateway managed by this controller.
        ensure!(
            !installation.release.is_empty()
                && !installation.controller_name.is_empty()
                && !installation.lease_name.is_empty()
                && !installation.provisioner_binding.is_empty()
                && !settings.namespaces.is_empty()
                && settings.replicas >= 0
                && installation.termination_grace_period_seconds >= 0
                && !settings.image.is_empty(),
            "invalid proxy installation settings"
        );
        let families = service.ip_families.as_slice();
        // a Service can request each family once, in primary-first order.
        ensure!(
            families.len() <= 1 || matches!(families, [a, b] if a != b),
            "invalid gateway service ipFamilies"
        );
        // dual-stack Services can leave the secondary family to Kubernetes.
        ensure!(
            service.ip_family_policy != IpFamilyPolicy::SingleStack
                || families.len() <= 1,
            "gateway service ipFamilies conflicts with ipFamilyPolicy"
        );
        // the provisioned ClusterIP Service has no externally facing address for this policy.
        ensure!(
            service.external_traffic_policy.is_none()
                || service.r#type != ServiceType::ClusterIP,
            "invalid gateway service externalTrafficPolicy"
        );
        let reserved = [
            "POD_UID",
            "WEICHE_PROVISIONER_BINDING",
            "WEICHE_HTTP3",
            "WEICHE_HTTP3_PORT",
        ];
        // extraEnv is appended last and could replace the pod identity or listener settings.
        ensure!(
            !installation
                .extra_env
                .iter()
                .any(|v| reserved.contains(&v.name.as_str())),
            "proxy extraEnv cannot override reserved environment variables"
        );
        Ok(settings)
    }
}
