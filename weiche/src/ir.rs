//! configuration-independent gateway model.
mod backend;
mod route;
mod snapshot;
mod tls;
mod transport;
pub use transport::{StreamListener, StreamRoute, Transport, weighted};

pub use backend::{Backend, ClusterSpec, Http, Protocol, RefError, Rule};
pub use route::{HeaderChange, Route, RouteAction, hostname_matches};
pub use snapshot::{
    ForwardingPolicy, GatewaySnapshot, Listener, PublishedSnapshot, Revision,
    SocketRoutes,
};
pub use tls::{BackendTls, ServerTls};
