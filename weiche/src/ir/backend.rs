//! resolved HTTP match predicates and upstream cluster metadata.
use super::BackendTls;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

/// literal path fields preserve the representation of exact and prefix matches in older snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PathMatch<R> {
    /// match the request path with the configured regex dialect.
    RegularExpression {
        /// original pattern in the snapshot, or its compiled worker representation.
        regex: R,
    },
    /// compare the original path case-sensitively.
    Literal {
        /// full path or segment prefix without a trailing slash.
        path: String,
        /// require the complete path to match.
        exact: bool,
    },
}

/// predicate on one header or decoded query parameter value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueMatch<R> {
    /// match the complete value, retaining the original snapshot string format.
    Exact(String),
    /// search the value with a Rust regex, honoring explicit anchors.
    RegularExpression {
        /// original pattern in the snapshot, or its compiled worker representation.
        regex: R,
    },
}

/// one HTTP match whose path, method, header and query predicates must all hold.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Http<R = String> {
    /// path predicate shared with the worker after compilation.
    #[serde(flatten)]
    pub path: PathMatch<R>,
    /// optional case-sensitive HTTP method.
    pub method: Option<String>,
    /// predicates keyed by case-insensitive header name.
    pub headers: Vec<(String, ValueMatch<R>)>,
    /// predicates keyed by case-sensitive query parameter name.
    #[serde(default = "Vec::new")]
    pub query_params: Vec<(String, ValueMatch<R>)>,
}

impl Default for Http {
    /// match every path on the listener.
    fn default() -> Self {
        Self {
            path: PathMatch::Literal { path: "/".into(), exact: false },
            method: None,
            headers: Vec::new(),
            query_params: Vec::new(),
        }
    }
}

/// membership and wire protocol shared by rules targeting the same upstream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterSpec {
    /// ready backend endpoint addresses.
    pub endpoints: Vec<SocketAddr>,
    /// upstream HTTP wire protocol spoken by the backend.
    pub protocol: Protocol,
    /// optional authentication settings for upstream TLS.
    pub tls: Option<Arc<BackendTls>>,
}

/// supported upstream HTTP wire protocols.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Protocol {
    /// use HTTP/1.1 for upstream requests.
    #[default]
    Http1,
    /// use HTTP/2 for upstream requests.
    Http2,
}

/// why a rule cannot forward to its configured upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefError {
    /// unsupported group or kind on the referenced resource.
    InvalidKind,
    /// missing cross-namespace reference grant.
    RefNotPermitted,
    /// referenced service or port could not be resolved.
    BackendNotFound,
    /// service port uses an incompatible protocol.
    UnsupportedProtocol,
    /// referenced TLS policy could not be applied.
    InvalidTlsPolicy,
}
