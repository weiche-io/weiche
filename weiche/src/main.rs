use anyhow::Context;
use tracing_subscriber::EnvFilter;
use weiche::{config::Selection, gateway};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let workers = match std::env::var("WEICHE_WORKERS") {
        Ok(value) => value
            .parse::<std::num::NonZeroUsize>()
            .context("WEICHE_WORKERS must be a positive integer")?
            .get(),
        Err(std::env::VarError::NotPresent) => std::thread::available_parallelism()?.get(),
        Err(error) => return Err(error.into()),
    };
    let selection = Selection::from_env()?;
    let runtime = compio::runtime::Runtime::new().context("create main compio runtime")?;
    tracing::info!(worker = 0, workers, driver = ?runtime.driver_type(), "starting compio data plane");
    runtime.block_on(gateway::run(selection, workers))
}
