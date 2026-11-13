//! Kubernetes control plane for Weiche.

#[cfg(not(target_os = "linux"))]
compile_error!("weiche supports only Linux");

pub mod config;
pub mod controller;
pub mod dataplane;
pub mod gateway;
pub mod routing;
mod upstream;
