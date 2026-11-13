use anyhow::Context;
use tracing_subscriber::EnvFilter;
use weiche::{config::Selection, gateway, rtconf::RuntimeConfig};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = RuntimeConfig::from_env()?;
    let workers = config.workers.get();
    let selection = Selection::from_env()?;
    let runtime = compio::runtime::Runtime::new().context("create main compio runtime")?;
    tracing::info!(worker = 0, workers, driver = ?runtime.driver_type(), "starting compio data plane");
    runtime.block_on(gateway::run(selection, config))
}
