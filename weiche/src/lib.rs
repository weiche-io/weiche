//! gateway configuration, control plane and worker-local data plane.

#[cfg(not(target_os = "linux"))]
compile_error!("weiche supports only Linux");

pub mod control;
pub mod gateway;
pub mod ir;
pub mod routing;
pub mod rtconf;
pub mod stream;
