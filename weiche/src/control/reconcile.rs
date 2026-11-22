//! hold the Kubernetes writer lease while compiling and reconciling gateway workloads.
use super::{
    CONTROLLER_NAME, GatewayController, GatewayStores,
    service::Plan,
    status::update_status,
    workload::{self, Applied, Settings},
};
use anyhow::Context;
use kube::{Client, ResourceExt};
use kube_lease_manager::LeaseManagerBuilder;
use std::time::Duration;
use tokio::sync::watch;

/// start resource watches and reconcile while holding the writer lease.
pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::load()?;
    let mut lease_config = kube::Config::infer().await?;
    let client = Client::try_from(lease_config.clone())?;
    let namespace = client.default_namespace().to_owned();
    // bound lease requests independently of long-lived watches.
    lease_config.connect_timeout = Some(Duration::from_secs(2));
    lease_config.read_timeout = Some(Duration::from_secs(2));
    lease_config.write_timeout = Some(Duration::from_secs(2));
    let lease =
        LeaseManagerBuilder::new(Client::try_from(lease_config)?, "weiche-controller")
            .with_namespace(namespace)
            .with_identity(
                std::env::var("POD_UID").context("missing controller pod UID")?,
            )
            .with_duration(15)
            .with_grace(8)
            .build()
            .await?;
    let controller = GatewayController::new(client.clone());
    let stores = controller.stores();
    let (tx, rx) = watch::channel(());
    let election = async {
        let mut leader = false;
        loop {
            tokio::select! {
                biased;
                next = lease.changed() => {
                    leader = next.context("controller lease lost or unavailable")?;
                    tracing::info!(leader, "controller lease changed");
                }
                result = lead(&client, &stores, &settings, rx.clone()), if leader => return result,
            }
        }
    };
    let result = tokio::select! {
        result = election => result,
        _ = controller.run(tx) => anyhow::bail!("gateway watches stopped"),
        _ = super::super::shutdown() => Ok(()),
    };
    let _ = tokio::time::timeout(Duration::from_secs(2), lease.release()).await;
    result
}

/// wait for all initial lists and publish changes while elected.
async fn lead(
    client: &Client,
    stores: &GatewayStores,
    settings: &Settings,
    mut changes: watch::Receiver<()>,
) -> anyhow::Result<()> {
    tokio::try_join!(
        stores.gateway_classes.wait_until_ready(),
        stores.gateways.wait_until_ready(),
        stores.http_routes.wait_until_ready(),
        stores.grpc_routes.wait_until_ready(),
        stores.tcp_routes.wait_until_ready(),
        stores.udp_routes.wait_until_ready(),
        stores.secrets.wait_until_ready(),
        stores.namespaces.wait_until_ready(),
        stores.grants.wait_until_ready(),
        stores.config_maps.wait_until_ready(),
        stores.tls_policies.wait_until_ready(),
        stores.services.wait_until_ready(),
        stores.endpoint_slices.wait_until_ready()
    )?;
    let mut applied = Applied::default();
    loop {
        changes.mark_unchanged();
        applied.expire();
        let mut plans = Vec::new();
        let mut retry = false;
        for gateway in stores.gateways.state() {
            if gateway.metadata.deletion_timestamp.is_some()
                || !stores.gateway_classes.state().iter().any(|c| {
                    c.name_any() == gateway.spec.gateway_class_name
                        && c.spec.controller_name == CONTROLLER_NAME
                        && c.spec.parameters_ref.is_none()
                        && c.metadata.deletion_timestamp.is_none()
                })
            {
                continue;
            }
            if !settings.namespaces.contains(&gateway.namespace().unwrap_or_default()) {
                let mut plan = Plan::new(gateway);
                plan.denied = true;
                plans.push(plan);
                continue;
            }
            let (mut plan, snapshot) =
                Plan::compile(stores, gateway, settings.bind_address);
            if let Err(error) = workload::reconcile(
                client,
                stores,
                settings,
                &mut applied,
                &mut plan,
                &snapshot,
            )
            .await
            {
                tracing::warn!(gateway=%plan.gateway.name_any(), %error, "could not reconcile gateway workload");
                plan.issue = Some(
                    "could not provision proxy resources; see controller logs".into(),
                );
                retry = true;
            }
            plans.push(plan);
            // give lease renewal and watch delivery time during a large cached resync.
            tokio::task::yield_now().await;
        }
        if let Err(error) = update_status(client, stores, &plans).await {
            tracing::warn!(%error, "could not update gateway status");
            retry = true;
        }
        if let Err(error) =
            super::cleanup::retired(client, stores, settings, &plans).await
        {
            tracing::warn!(%error, "could not retire gateway resources");
            retry = true;
        }
        tokio::select! {
            change = changes.changed() => {change.context("gateway watches stopped")?;}
            _ = tokio::time::sleep(Duration::from_secs(if retry {1} else {30})) => {}
        }
    }
}
