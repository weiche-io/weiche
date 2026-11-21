use anyhow::{Context, bail};
use tracing_subscriber::EnvFilter;
use weiche::{control, gateway, proxy, rtconf::RuntimeConfig, stream};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [role] if role == "controller" => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(control::run()),
        [role, resource] if role == "proxy" => {
            let runtime = RuntimeConfig::from_env()?;
            let io = stream::runtime().context("create proxy runtime")?;
            tracing::info!(workers=runtime.workers.get(), driver=?io.driver_type(), "starting data plane");
            io.block_on(gateway::run(resource.clone(), runtime))
        }
        [role] if role == "ready" => proxy::ready(),
        _ => bail!("usage: weiche controller | proxy <gateway-resource-name> | ready"),
    }
}
