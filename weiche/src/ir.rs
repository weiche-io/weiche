//! configuration-independent gateway model.
mod backend;
mod route;
mod snapshot;
mod tls;

pub use backend::{Backend, ClusterSpec, Http, Protocol, RefError, Rule};
pub use route::{Route, RouteAction, hostname_matches};
pub use snapshot::{
    ForwardingPolicy, GatewaySnapshot, Listener, PublishedSnapshot, Revision,
};
pub use tls::{BackendTls, ServerTls};
