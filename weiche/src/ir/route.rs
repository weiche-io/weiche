//! route definitions shared by the control plane and protocol filters.
use super::{ClusterSpec, Http};
use std::sync::Arc;

/// action selected after a route matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteAction<T = Arc<ClusterSpec>> {
    Forward(T),
    RejectInvalidBackend,
}

/// resolved HTTP match and its forwarding action.
#[derive(Clone, Debug, PartialEq, Eq)]
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
