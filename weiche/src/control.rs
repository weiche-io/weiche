//! Kubernetes watches and their live resource caches.
mod compile;
mod reconcile;
mod service;
mod status;
mod tls;
mod tryfrom;

pub use compile::{CONTROLLER_NAME, Compiled, Selection, compile};
pub use reconcile::run;
pub use tryfrom::{Resolve, ResolveError};

use futures::{
    StreamExt,
    stream::{BoxStream, select_all},
};
use k8s_openapi::api::{
    core::v1::{ConfigMap, Namespace, Secret, Service},
    discovery::v1::EndpointSlice,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    runtime::{
        WatchStreamExt,
        reflector::{self, Store},
        watcher,
    },
};
use serde::de::DeserializeOwned;
use std::{fmt::Debug, future::ready};
use tokio::sync::watch;
use weiche_gtwapi::v1::{
    backendtlspolicies::BackendTlsPolicy, gatewayclasses::GatewayClass,
    gateways::Gateway, httproutes::HttpRoute, referencegrants::ReferenceGrant,
};

/// Live snapshots maintained by kube-runtime, including updates and deletions.
/// Wait for each store's initial list before reading it. The caches are
/// eventually consistent and do not form an atomic snapshot.
#[derive(Clone)]
pub struct GatewayStores {
    pub gateway_classes: Store<GatewayClass>,
    pub gateways: Store<Gateway>,
    pub http_routes: Store<HttpRoute>,
    pub secrets: Store<Secret>,
    pub namespaces: Store<Namespace>,
    pub grants: Store<ReferenceGrant>,
    pub config_maps: Store<ConfigMap>,
    pub tls_policies: Store<BackendTlsPolicy>,
    pub services: Store<Service>,
    pub endpoint_slices: Store<EndpointSlice>,
}

/// maintains resource caches for the reconciliation loop.
/// Uses reflectors directly because these watches have no per-object reconcile.
pub struct GatewayController {
    stores: GatewayStores,
    watches: Vec<BoxStream<'static, ()>>,
}
impl GatewayController {
    pub fn new(client: Client) -> Self {
        let defaults = watcher::Config::default;
        let (gateway_classes, classes_watch) =
            cache(Api::all(client.clone()), defaults());
        let (gateways, gateways_watch) = cache(Api::all(client.clone()), defaults());
        let (http_routes, routes_watch) = cache(Api::all(client.clone()), defaults());
        let (secrets, secrets_watch) =
            cache(Api::all(client.clone()), defaults().fields("type=kubernetes.io/tls"));
        let (namespaces, namespaces_watch) = cache(Api::all(client.clone()), defaults());
        let (grants, grants_watch) = cache(Api::all(client.clone()), defaults());
        let (config_maps, maps_watch) = cache(Api::all(client.clone()), defaults());
        let (tls_policies, policies_watch) = cache(Api::all(client.clone()), defaults());
        let (services, services_watch) = cache(Api::all(client.clone()), defaults());
        let (endpoint_slices, endpoints_watch) = cache(Api::all(client), defaults());
        Self {
            stores: GatewayStores {
                gateway_classes,
                gateways,
                http_routes,
                secrets,
                namespaces,
                grants,
                config_maps,
                tls_policies,
                services,
                endpoint_slices,
            },
            watches: vec![
                classes_watch,
                gateways_watch,
                routes_watch,
                secrets_watch,
                namespaces_watch,
                grants_watch,
                maps_watch,
                policies_watch,
                services_watch,
                endpoints_watch,
            ],
        }
    }
    pub fn stores(&self) -> GatewayStores {
        self.stores.clone()
    }

    /// notify the compiler after a reflector commits an update to a visible store.
    pub async fn run(self, changes: watch::Sender<()>) {
        tracing::info!("starting kubernetes resource watches");
        select_all(self.watches)
            .for_each(|()| {
                changes.send_replace(());
                ready(())
            })
            .await;
        tracing::info!("kubernetes resource watches stopped");
    }
}

fn cache<K>(api: Api<K>, config: watcher::Config) -> (Store<K>, BoxStream<'static, ()>)
where
    K: Resource<DynamicType = ()>
        + Clone
        + Debug
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let (reader, writer) = reflector::store();
    let stream = reflector::reflector(writer, watcher::watcher(api, config))
        .default_backoff()
        .filter_map(|result| ready(match result {
            Ok(watcher::Event::Apply(resource) | watcher::Event::Delete(resource)) => {
                tracing::debug!(kind = %K::kind(&()), namespace = resource.namespace().as_deref().unwrap_or(""), name = %resource.name_any(), "observed resource change");
                Some(())
            }
            Ok(watcher::Event::InitDone) => Some(()),
            Ok(watcher::Event::Init | watcher::Event::InitApply(_)) => None,
            Err(error) => {
                tracing::error!(kind = %K::kind(&()), %error, "kubernetes resource watch failed");
                None
            }
        })).boxed();
    (reader, stream)
}

impl GatewayStores {
    /// authorize a core resource reference using grants in the target namespace.
    pub fn permits(
        &self,
        from: &str,
        source_kind: &str,
        target: &str,
        kind: &str,
        name: &str,
    ) -> bool {
        from == target
            || self.grants.state().iter().any(|grant| {
                grant.metadata.namespace.as_deref() == Some(target)
                    && grant.metadata.deletion_timestamp.is_none()
                    && grant.spec.from.iter().any(|r| {
                        r.group == "gateway.networking.k8s.io"
                            && r.kind == source_kind
                            && r.namespace == from
                    })
                    && grant.spec.to.iter().any(|r| {
                        r.group.is_empty()
                            && r.kind == kind
                            && r.name.as_deref().is_none_or(|n| n == name)
                    })
            })
    }
}
