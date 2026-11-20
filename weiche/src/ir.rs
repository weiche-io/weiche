//! configuration-independent gateway model.
mod backend;
mod route;
mod snapshot;

pub use backend::{Backend, ClusterSpec, Http, Protocol, RefError, Rule};
pub use route::{Route, RouteAction, hostname_matches};
pub use snapshot::{ForwardingPolicy, GatewaySnapshot, PublishedSnapshot, Revision};
