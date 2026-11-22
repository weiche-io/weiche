//! dispatch the controller, proxy and readiness probe commands.
use anyhow::{Context, bail};
use tracing_subscriber::EnvFilter;
use weiche::{
    control,
    proxy::{self, config::RuntimeConfig},
};

/// configure logging and start the requested process role.
fn main() -> anyhow::Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match std::env::var("WEICHE_LOG_FORMAT").as_deref() {
        Ok("json") => tracing_subscriber::fmt().with_env_filter(filter).json().init(),
        Ok("text") | Ok("") | Err(std::env::VarError::NotPresent) => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
        Ok(other) => bail!("unsupported WEICHE_LOG_FORMAT: {other}"),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("WEICHE_LOG_FORMAT must be valid UTF-8")
        }
    }
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [role] if role == "controller" => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(control::run()),
        [role, resource] if role == "proxy" => {
            let runtime = RuntimeConfig::from_env()?;
            let io = proxy::runtime().context("create proxy runtime")?;
            tracing::info!(workers=runtime.workers.get(), driver=?io.driver_type(), "starting data plane");
            io.block_on(proxy::run(resource.clone(), runtime))
        }
        [role] if role == "ready" => proxy::ready(),
        _ => bail!("usage: weiche controller | proxy <gateway-resource-name> | ready"),
    }
}
