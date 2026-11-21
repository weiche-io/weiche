//! configuration-independent gateway model.
mod backend;
mod route;
mod snapshot;
mod tls;
mod transport;
pub mod wire;
pub use transport::{StreamListener, StreamRoute, Transport, weighted};

pub use backend::{ClusterSpec, Http, Protocol, RefError};
pub use route::{HeaderChange, Redirect, Route, RouteAction, hostname_matches};
pub use snapshot::{
    ForwardingPolicy, GatewaySnapshot, Listener, PublishedSnapshot, Revision,
    SocketRoutes,
};
pub use tls::{BackendTls, ServerTls};
