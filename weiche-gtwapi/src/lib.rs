//! Bindings for the official Gateway API Standard manifest, versioned with upstream.
//! Modules follow API version and resource plural name. Install CRDs from the
//! upstream manifest to retain their validation and admission policies.

#[allow(clippy::all)]
mod generated;

pub use generated::*;

/// Gateway API release used to generate these bindings.
pub const GATEWAY_API_VERSION: &str = concat!("v", env!("CARGO_PKG_VERSION"));
