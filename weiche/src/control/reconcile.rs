//! reconcile committed store updates and worker status on the control runtime.
use super::{
    super::{
        routing,
        stream::{down::WorkerGroupState, stopped},
    },
    GatewayController, Selection, compile,
    status::update_status,
};
use anyhow::Context;
use kube::Client;
use std::time::Duration;
use tokio::sync::watch;

/// watch resources and publish snapshots until shutdown.
pub async fn run(
    selection: Selection,
    mut config_tx: routing::Writer,
    mut listener_rx: watch::Receiver<WorkerGroupState>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let reconcile = async {
        // initialize the client and all watches on their owning Tokio runtime.
        let client =
            Client::try_default().await.context("initialize Kubernetes client")?;
        let controller = GatewayController::new(client.clone());
        let stores = controller.stores();
        let (changes_tx, mut changes_rx) = watch::channel(());
        let compile = async {
            tokio::try_join!(
                stores.gateway_classes.wait_until_ready(),
                stores.gateways.wait_until_ready(),
                stores.http_routes.wait_until_ready(),
                stores.secrets.wait_until_ready(),
                stores.namespaces.wait_until_ready(),
                stores.grants.wait_until_ready(),
                stores.config_maps.wait_until_ready(),
                stores.tls_policies.wait_until_ready(),
                stores.services.wait_until_ready(),
                stores.endpoint_slices.wait_until_ready()
            )?;
            tracing::info!(namespace = %selection.namespace, gateway = %selection.gateway, listener = %selection.listener, "kubernetes caches ready");
            let mut published = None;
            loop {
                changes_rx.mark_unchanged();
                let compiled = compile(&stores, &selection);
                let revision = config_tx.publish(compiled.snapshot.clone());
                if published != Some(revision) {
                    published = Some(revision);
                    tracing::info!(revision, bind = ?compiled.snapshot.address(), routes = compiled.snapshot.routes.len(), "published gateway snapshot");
                }
                loop {
                    let state = listener_rx.borrow_and_update().clone();
                    let status = update_status(
                        &client, &stores, &selection, &compiled, revision, &state,
                    )
                    .await;
                    if let Err(error) = &status {
                        tracing::warn!(%error, "failed to update Gateway API status; will retry");
                    }
                    tokio::select! {
                        biased;
                        changed = changes_rx.changed() => {
                            changed.context("kubernetes resource watches stopped")?;
                            break;
                        }
                        changed = listener_rx.changed() => {
                            changed.context("worker status channel closed")?;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(500)), if status.is_err() => {}
                    }
                }
            }
        };
        tokio::select! {
            _ = controller.run(changes_tx) => anyhow::bail!("Kubernetes watches stopped unexpectedly"),
            result = compile => result,
        }
    };
    tokio::select! {
        result = shutdown_signal() => result,
        _ = stopped(shutdown) => Ok(()),
        result = reconcile => result,
    }
}

async fn shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = terminate.recv() => {},
        _ = interrupt.recv() => {},
    }
    Ok(())
}
