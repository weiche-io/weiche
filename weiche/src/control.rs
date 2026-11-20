//! Kubernetes watches and their live resource caches.
mod compile;
mod reconcile;
mod status;
mod tryfrom;

pub use compile::{CONTROLLER_NAME, Compiled, Selection, compile};
pub use reconcile::run;
pub use tryfrom::{Resolve, ResolveError};

use futures::{
    StreamExt,
    stream::{BoxStream, select_all},
};
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
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
    gatewayclasses::GatewayClass, gateways::Gateway, httproutes::HttpRoute,
};

/// Live snapshots maintained by kube-runtime, including updates and deletions.
/// Wait for each store's initial list before reading it. The caches are
/// eventually consistent and do not form an atomic snapshot.
#[derive(Clone)]
pub struct GatewayStores {
    pub gateway_classes: Store<GatewayClass>,
    pub gateways: Store<Gateway>,
    pub http_routes: Store<HttpRoute>,
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
        let (gateway_classes, classes_watch) = cache(Api::all(client.clone()));
        let (gateways, gateways_watch) = cache(Api::all(client.clone()));
        let (http_routes, routes_watch) = cache(Api::all(client.clone()));
        let (services, services_watch) = cache(Api::all(client.clone()));
        let (endpoint_slices, endpoints_watch) = cache(Api::all(client));
        Self {
            stores: GatewayStores {
                gateway_classes,
                gateways,
                http_routes,
                services,
                endpoint_slices,
            },
            watches: vec![
                classes_watch,
                gateways_watch,
                routes_watch,
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

fn cache<K>(api: Api<K>) -> (Store<K>, BoxStream<'static, ()>)
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
    let stream = reflector::reflector(writer, watcher::watcher(api, watcher::Config::default()))
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
