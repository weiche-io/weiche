//! dispatch the controller, proxy and readiness probe commands.
use anyhow::{Context, bail};
use mimalloc::MiMalloc;
use weiche::{
    control, log, logging,
    proxy::{self, config::RuntimeConfig},
};

/// serve worker-local allocations through mimalloc's per-thread heaps.
#[global_allocator]
static ALLOCATOR: MiMalloc = MiMalloc;

/// configure logging and start the requested process role.
fn main() -> anyhow::Result<()> {
    let _logs = logging::init()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [role] if role == "controller" => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(control::run()),
        [role, resource] if role == "proxy" => {
            let runtime = RuntimeConfig::from_env()?;
            let io = proxy::runtime().context("create proxy runtime")?;
            log!(
                info,
                "starting data plane",
                workers = runtime.workers.get(),
                driver = format!("{:?}", io.driver_type())
            );
            io.block_on(proxy::run(resource.clone(), runtime))
        }
        [role] if role == "ready" => proxy::ready(),
        _ => bail!("usage: weiche controller | proxy <gateway-resource-name> | ready"),
    }
}
