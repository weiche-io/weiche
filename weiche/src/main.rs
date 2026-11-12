use anyhow::Context;
use kube::Client;
use tracing_subscriber::EnvFilter;
use weiche::{config::Selection, gateway};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Uses KUBECONFIG/default kubeconfig locally, or the pod's service account.
    let client = Client::try_default()
        .await
        .context("failed to initialize Kubernetes client")?;

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        gateway::run(client, Selection::from_env()?, async move {
            tokio::select! {
                _ = terminate.recv() => {},
                _ = interrupt.recv() => {},
            }
        })
        .await?;
    }

    #[cfg(not(unix))]
    gateway::run(client, Selection::from_env()?, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for shutdown signal");
        }
    })
    .await?;

    Ok(())
}
