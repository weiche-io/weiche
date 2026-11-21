//! route definitions shared by the control plane and protocol filters.
use super::{ClusterSpec, Http, RefError};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// action selected after a route matches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAction<T = Arc<ClusterSpec>> {
    pub backends: Vec<(u32, Result<T, RefError>)>,
    pub headers: Vec<HeaderChange>,
    pub redirect: Option<Redirect>,
}

/// validated response status and optional replacement host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    #[serde(with = "http_serde_ext::status_code")]
    pub status: StatusCode,
    pub hostname: Option<String>,
}

/// validated request transformations in rule order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderChange {
    Set(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    Add(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    Remove(#[serde(with = "http_serde_ext::header_name")] HeaderName),
}

/// resolved HTTP match and its forwarding action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route<T = Arc<ClusterSpec>> {
    pub hostname: String,
    pub http: Http,
    pub action: RouteAction<T>,
}

/// match an exact, wildcard or unrestricted hostname.
pub fn hostname_matches(pattern: &str, host: &str) -> bool {
    pattern.is_empty()
        || pattern == host
        || pattern.strip_prefix("*.").is_some_and(|suffix| {
            host.strip_suffix(suffix)
                .is_some_and(|prefix| prefix.len() > 1 && prefix.ends_with('.'))
        })
}
