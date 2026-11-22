//! read platform installation settings for gateway proxy workloads.
use serde::Deserialize;
use std::collections::BTreeMap;

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
