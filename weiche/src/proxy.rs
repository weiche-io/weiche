//! consume this gateway's snapshot and acknowledge it after all local workers bind.
use super::{
    ir::{
        GatewaySnapshot, Transport,
        wire::{self, SNAPSHOT_KEY},
    },
    routing,
    stream::{down::WorkerGroupState, stopped},
};
use anyhow::{Context, ensure};
use futures::{StreamExt, pin_mut};
use k8s_openapi::api::{
    core::v1::{ConfigMap, Secret},
    rbac::v1::RoleBinding,
};
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{
        WatchStreamExt,
        reflector::{self, ObjectRef},
        watcher,
    },
};
use serde_json::json;
use std::{collections::BTreeSet, fs, io, net::SocketAddr, time::Duration};
use tokio::sync::watch;

/// readiness marker containing the last applied snapshot digest.
const READY: &str = "/run/weiche/ready";

/// kubelet exec probe, including the applied configuration digest for diagnostics.
pub fn ready() -> anyhow::Result<()> {
    println!("{}", fs::read_to_string(READY).context("listeners pending")?);
    Ok(())
}

/// withdraw the readiness marker until all workers acknowledge the new revision.
fn pending() -> io::Result<()> {
    match fs::remove_file(READY) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// snapshot identity and exact set of sockets that every worker must bind.
struct Desired {
    /// digest used to acknowledge this snapshot to the controller.
    version: String,
    /// local publication sequence reported by workers.
    revision: usize,
    /// expected transport and address pairs.
    sockets: BTreeSet<(Transport, SocketAddr)>,
}

impl Desired {
    /// decode the snapshot and publish its listener configuration to the workers.
    fn apply(
        secret: &Secret,
        tx: &mut routing::Writer,
        http3: bool,
    ) -> anyhow::Result<Self> {
        ensure!(
            secret.type_.as_deref() == Some("weiche.io/snapshot"),
            "unexpected snapshot Secret type"
        );
        let bytes = &secret
            .data
            .as_ref()
            .and_then(|data| data.get(SNAPSHOT_KEY))
            .context("missing snapshot payload")?
            .0;
        let snapshot = GatewaySnapshot::decode(bytes)?;
        let sockets = snapshot
            .streams
            .keys()
            .copied()
            .chain(
                snapshot
                    .sockets
                    .keys()
                    .chain(snapshot.tls.keys())
                    .map(|addr| (Transport::Tcp, *addr)),
            )
            .chain(
                snapshot
                    .sockets
                    .iter()
                    .filter(|(_, s)| http3 && !s.listener.tls.is_empty())
                    .map(|(addr, _)| (Transport::Udp, *addr)),
            )
            .collect::<BTreeSet<_>>();
        if sockets.is_empty() {
            tracing::info!(secret = %secret.name_any(), "snapshot has no active listeners; proxy readiness withdrawn");
        }
        Ok(Self {
            version: wire::version(bytes),
            revision: tx.publish(snapshot),
            sockets,
        })
    }

    /// check every expected socket against its worker acknowledgements.
    fn applied(&self, workers: &WorkerGroupState) -> bool {
        !self.sockets.is_empty()
            && self
                .sockets
                .iter()
                .all(|&socket| workers.programmed(self.revision, socket))
    }
}

/// default RoleBinding and ClusterRole name granting namespace authorization.
const DEFAULT_PROVISIONER_BINDING: &str = "weiche-gateway-provisioner";

/// a withdrawn Helm namespace binding also withdraws permission to serve cached routes.
async fn authorized(
    bindings: &Api<RoleBinding>,
    binding_name: &str,
) -> anyhow::Result<bool> {
    match bindings.get_opt(binding_name).await {
        Ok(binding) => Ok(binding.is_some_and(|b| {
            b.role_ref.kind == "ClusterRole" && b.role_ref.name == binding_name
        })),
        Err(kube::Error::Api(e)) if e.code == 403 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// watch only the named Secret; transient API errors leave the last valid snapshot serving.
pub async fn run(
    name: String,
    http3: bool,
    mut tx: routing::Writer,
    mut workers: watch::Receiver<WorkerGroupState>,
    stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    pending()?;
    let client = Client::try_default().await?;
    let ns = client.default_namespace();
    let pod = std::env::var("POD_UID").context("missing pod UID")?;
    let binding_name = std::env::var("WEICHE_PROVISIONER_BINDING")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_PROVISIONER_BINDING.to_owned());
    let bindings = Api::<RoleBinding>::namespaced(client.clone(), ns);
    ensure!(
        authorized(&bindings, &binding_name).await?,
        "gateway namespace authorization withdrawn"
    );
    let reports = Api::<ConfigMap>::namespaced(client.clone(), ns);
    let secrets = Api::<Secret>::namespaced(client.clone(), ns);
    let (store, writer) = reflector::store();
    let stream = reflector::reflector(
        writer,
        watcher(
            secrets,
            watcher::Config::default().fields(&format!("metadata.name={name}")),
        ),
    )
    .default_backoff();
    pin_mut!(stream);
    let secret_ref = ObjectRef::new(&name).within(ns);
    let mut desired: Option<Desired> = None;
    let mut local = None;
    let mut reported = None;
    let mut refresh_due = false;
    let mut refresh = tokio::time::interval(Duration::from_secs(15));
    let shutdown = super::shutdown();
    let stop = stopped(stop);
    tokio::pin!(stop, shutdown);
    loop {
        let applied = desired
            .as_ref()
            .filter(|d| d.applied(&workers.borrow()))
            .map(|d| d.version.clone());
        if local != applied {
            if let Some(version) = &applied {
                fs::write("/run/weiche/ready.tmp", version)?;
                fs::rename("/run/weiche/ready.tmp", READY)?;
            } else {
                pending()?;
            }
            local.clone_from(&applied);
        }
        if reported != applied || (refresh_due && applied.is_some()) {
            refresh_due = false;
            let patch = json!({"data":{(&pod):applied}});
            match reports
                .patch(&name, &PatchParams::default(), &Patch::Merge(patch))
                .await
            {
                Ok(_) => reported = applied,
                Err(e) => tracing::warn!(error=%e, "could not acknowledge snapshot"),
            }
        }
        tokio::select! {
            _ = &mut shutdown => break,
            _ = &mut stop => break,
            change = workers.changed() => { change.context("worker reports stopped")?; }
            _ = refresh.tick() => {
                match authorized(&bindings, &binding_name).await {
                    Ok(false) => { tracing::info!("gateway namespace authorization withdrawn"); break; }
                    Ok(true) => refresh_due = true,
                    Err(error) => tracing::warn!(%error, "could not refresh namespace authorization"),
                }
            }
            event = stream.next() => match event.context("snapshot watch stopped")? {
                Ok(
                    watcher::Event::Apply(_)
                    | watcher::Event::Delete(_)
                    | watcher::Event::InitDone,
                ) => match store.get(&secret_ref) {
                    Some(secret) => match Desired::apply(&secret, &mut tx, http3) {
                        Ok(next) => desired = Some(next),
                        Err(e) => {
                            tracing::error!(error = %e, secret = %secret.name_any(), "invalid proxy snapshot");
                            desired = None;
                            tx.publish(GatewaySnapshot::default());
                        }
                    },
                    None => {
                        desired = None;
                        tx.publish(GatewaySnapshot::default());
                    }
                },
                Ok(watcher::Event::Init | watcher::Event::InitApply(_)) => {}
                Err(e) => tracing::warn!(error = %e, "snapshot watch interrupted"),
            }
        }
    }
    pending()?;
    Ok(())
}
