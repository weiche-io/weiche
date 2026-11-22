//! configuration-independent gateway model.
mod backend;
mod cors;
mod route;
mod snapshot;
mod tls;
mod transport;
pub mod wire;
pub use transport::{StreamListener, StreamRoute, Transport, weighted, weighted_by};

pub use backend::{
    ClusterSpec, GrpcMethod, Http, PathMatch, Protocol, RefError, ValueMatch,
};
pub use cors::{Cors, Origin};
pub use route::{
    Backend, Filters, HeaderChange, PathModifier, Redirect, Route, RouteAction,
    RouteHost, Timeouts, hostname_matches,
};
pub use snapshot::{
    Binding, ForwardingPolicy, GatewaySnapshot, Listener, PublishedSnapshot, Revision,
    SocketRoutes,
};
pub use tls::{BackendTls, ServerTls};
