//! read platform installation settings for gateway proxy workloads.
use super::CONTROLLER_NAME;
use anyhow::ensure;
use k8s_openapi::api::core::v1::{
    Affinity, EnvVar, LocalObjectReference, ResourceRequirements, SeccompProfile,
    Toleration, TopologySpreadConstraint,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

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
    /// use the installation's default proxy log level and format.
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
    /// gateway Service type (`ClusterIP`, `LoadBalancer`, or `NodePort`).
    pub r#type: String,
    /// gateway IP family policy (`SingleStack`, `PreferDualStack`, or `RequireDualStack`).
    pub ip_family_policy: String,
    /// optional ordered IP families (`IPv4`, `IPv6`).
    pub ip_families: Vec<String>,
    /// optional external traffic policy (`Cluster` or `Local`) for LoadBalancer or NodePort.
    pub external_traffic_policy: Option<String>,
    /// default annotations merged onto every Gateway Service.
    pub annotations: BTreeMap<String, String>,
}

impl Default for ServiceSettings {
    /// prefer dual-stack allocation and leave the Service type to installation settings.
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

/// platform policy for exposing HTTPS listeners over QUIC.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Http3Settings {
    /// serve HTTP/3 on eligible HTTPS ports.
    pub enabled: bool,
    /// external UDP port advertised to HTTPS clients.
    pub advertised_port: Option<u16>,
}

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
    #[serde(default)]
    pub dns_policy: Option<String>,
    /// graceful termination window in seconds for proxy pods.
    #[serde(default = "default_termination_grace")]
    pub termination_grace_period_seconds: i64,
    /// additional node selector labels merged with kubernetes.io/os=linux.
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    /// scheduling tolerations applied to proxy pods.
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
    /// scheduling affinity rules applied to proxy pods.
    #[serde(default)]
    pub affinity: Option<Affinity>,
    /// topology spread constraints applied to proxy pods.
    #[serde(default)]
    pub topology_spread_constraints: Vec<TopologySpreadConstraint>,
    /// optional priorityClassName applied to proxy pods.
    #[serde(default)]
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
    /// legacy flat serviceType field retained for backward compatibility.
    #[serde(default)]
    service_type: Option<String>,
    /// opt-in downstream HTTP/3 configuration for this installation.
    #[serde(default)]
    pub http3: Http3Settings,
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
