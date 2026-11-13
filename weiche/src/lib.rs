//! kubernetes control plane for Weiche.

#[cfg(not(target_os = "linux"))]
compile_error!("weiche supports only Linux");

pub mod cluster;
pub mod config;
pub mod connector;
pub mod controller;
pub mod dataplane;
pub mod gateway;
pub mod listener;
pub mod pool;
pub mod proxy;
pub mod router;
pub mod routing;
pub mod rtconf;
pub mod snapshot;
pub mod upstream;
pub mod worker;
