//! read platform installation settings for gateway proxy workloads.
use super::CONTROLLER_NAME;
use anyhow::ensure;
use k8s_openapi::api::core::v1::PodTemplateSpec;
use serde::{Deserialize, Deserializer, de::Error};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
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
        /// networking defaults for every Gateway Service.
        service: ServiceSettings = ServiceSettings::default(),
        /// opt-in downstream HTTP/3 configuration for this installation.
        http3: bool = false,
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
                && settings.replicas >= 0,
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
        Ok(settings)
    }
}
