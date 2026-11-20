//! resolved routing rules and upstreams, independent of the configuration source.
use std::net::SocketAddr;

/// rules contributed by one route, in declaration order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backend<P> {
    pub rules: Vec<Rule<P>>,
}

/// an invalid upstream rejects traffic matching this rule, not its siblings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule<P> {
    pub matches: Vec<P>,
    pub upstream: Result<ClusterSpec, RefError>,
}

/// one HTTP match; predicates within a match are combined with AND.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Http {
    pub path: String,
    pub exact: bool,
    pub method: Option<String>,
    pub headers: Vec<(String, String)>,
}

impl Default for Http {
    fn default() -> Self {
        Self {
            path: "/".into(),
            exact: false,
            method: None,
            headers: Vec::new(),
        }
    }
}

/// membership and wire protocol shared by rules targeting the same upstream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ClusterSpec {
    pub endpoints: Vec<SocketAddr>,
    pub protocol: Protocol,
}

/// supported upstream HTTP wire protocols.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Protocol {
    #[default]
    Http1,
    Http2,
}

/// why a rule cannot forward to its configured upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefError {
    InvalidKind,
    RefNotPermitted,
    BackendNotFound,
    UnsupportedProtocol,
}
