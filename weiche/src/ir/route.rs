//! route definitions shared by the control plane and protocol filters.
use super::{ClusterSpec, Cors, Http, RefError};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

/// action selected after a route matches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAction<T = Arc<ClusterSpec>> {
    /// weighted backend references, including the share of unresolved targets.
    pub backends: Vec<Backend<T>>,
    /// rule filters retain their original snapshot field names.
    #[serde(flatten)]
    pub filters: Arc<Filters>,
    /// route deadlines override the gateway's default header timeout.
    #[serde(default)]
    pub timeouts: Timeouts,
}

/// backend-specific filters travel with the selected weighted reference.
/// the weight and resolution result come first to read older two-element sequences.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Backend<T> {
    /// share of matching requests directed to this reference.
    pub weight: u32,
    /// invalid references retain their configured share of requests.
    pub cluster: Result<T, RefError>,
    /// transformations scoped to this backend reference.
    #[serde(default)]
    pub filters: Arc<Filters>,
}

/// independent request and per-attempt budgets, with zero disabling a budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeouts {
    /// maximum time across all attempts and body frames.
    pub request: Option<Duration>,
    /// maximum time for a single backend exchange.
    pub backend_request: Option<Duration>,
}

/// transformations shared by rule and backend reference filters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Filters {
    /// ordered changes to outgoing headers, including hostname rewrites.
    pub headers: Vec<HeaderChange>,
    /// ordered changes to returning headers.
    pub response_headers: Vec<HeaderChange>,
    /// local response terminating the request.
    pub redirect: Option<Redirect>,
    /// replacement for the upstream path.
    pub path: Option<PathModifier>,
    /// cross-origin sharing policy for the selected resource.
    pub cors: Option<Cors>,
}

/// validated response status and target components.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    /// status sent to the downstream client.
    #[serde(with = "http_serde_ext::status_code")]
    pub status: StatusCode,
    /// replacement DNS name, without a port.
    pub hostname: Option<String>,
    /// target scheme, defaulting to the listener scheme.
    pub scheme: Option<String>,
    /// explicit redirect port.
    pub port: Option<u16>,
    /// replacement applied before the original query string.
    pub path: Option<PathModifier>,
}

/// validated transformation of a URI path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathModifier {
    /// replace the entire path.
    Full(String),
    /// replace the matched path elements and retain their suffix.
    Prefix {
        /// matching prefix without its trailing slash.
        matched: String,
        /// new prefix without its trailing slash.
        replacement: String,
    },
}

/// validated header transformations in filter order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderChange {
    /// replace all values for a header.
    Set(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    /// append an additional header value.
    Add(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    /// remove all values for the header.
    Remove(#[serde(with = "http_serde_ext::header_name")] HeaderName),
}

/// resolved HTTP match and its forwarding action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route<T = Arc<ClusterSpec>, R = String> {
    /// listener and route hostname intersection.
    pub hostname: String,
    /// predicates which select this rule.
    pub http: Http<R>,
    /// transformations and forwarding targets.
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
