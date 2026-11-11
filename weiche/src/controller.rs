//! Read-only Gateway API controllers and their live resource caches.

use std::{convert::Infallible, fmt::Debug, future::Future, sync::Arc};

use futures::{FutureExt, StreamExt, future::BoxFuture, future::Shared};
use kube::{
    Api, Client, Resource, ResourceExt,
    runtime::{Controller, controller::Action, reflector::Store, watcher},
};
use serde::de::DeserializeOwned;
use tracing::{debug, error, info};
use weiche_gtwapi::v1::{gatewayclasses::GatewayClass, gateways::Gateway, httproutes::HttpRoute};

/// Live snapshots maintained by kube-runtime, including updates and deletions.
/// Each cache becomes readable after its initial list completes; the three
/// caches are eventually consistent and do not form an atomic snapshot.
#[derive(Clone)]
pub struct GatewayStores {
    pub gateway_classes: Store<GatewayClass>,
    pub gateways: Store<Gateway>,
    pub http_routes: Store<HttpRoute>,
}

/// Watches the core Gateway API resources across all namespaces.
///
/// This initial control plane observes resources only. It does not claim a
/// GatewayClass, write status, or configure a data plane.
pub struct GatewayController {
    gateway_classes: Controller<GatewayClass>,
    gateways: Controller<Gateway>,
    http_routes: Controller<HttpRoute>,
}

impl GatewayController {
    pub fn new(client: Client) -> Self {
        Self {
            gateway_classes: Controller::new(Api::all(client.clone()), watcher::Config::default()),
            gateways: Controller::new(Api::all(client.clone()), watcher::Config::default()),
            http_routes: Controller::new(Api::all(client), watcher::Config::default()),
        }
    }

    /// Obtain cache readers before driving the controller with [`Self::run`].
    /// Use each store's `wait_until_ready()` before reading its initial state.
    pub fn stores(&self) -> GatewayStores {
        GatewayStores {
            gateway_classes: self.gateway_classes.store(),
            gateways: self.gateways.store(),
            http_routes: self.http_routes.store(),
        }
    }

    /// Drive all watches until shutdown, allowing in-flight reconciles to finish.
    /// kube-runtime retries failed list/watch requests with backoff.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send + 'static) {
        let shutdown = shutdown.boxed().shared();
        info!("starting Gateway API controllers");
        tokio::join!(
            run_resource(self.gateway_classes, shutdown.clone()),
            run_resource(self.gateways, shutdown.clone()),
            run_resource(self.http_routes, shutdown),
        );
        info!("Gateway API controllers stopped");
    }
}

async fn run_resource<K>(controller: Controller<K>, shutdown: Shared<BoxFuture<'static, ()>>)
where
    K: Resource<DynamicType = ()> + Clone + Debug + DeserializeOwned + Send + Sync + 'static,
{
    controller
        .graceful_shutdown_on(shutdown)
        .run(observe::<K>, error_policy::<K>, Arc::new(()))
        .for_each(|result| async move {
            if let Err(error) = result {
                error!(kind = %K::kind(&()), %error, "Gateway API controller error");
            }
        })
        .await;
}

// Reconciliation is intentionally read-only until a data plane is introduced.
async fn observe<K>(resource: Arc<K>, _context: Arc<()>) -> Result<Action, Infallible>
where
    K: Resource<DynamicType = ()> + Debug,
{
    info!(
        kind = %K::kind(&()),
        namespace = resource.namespace().as_deref().unwrap_or(""),
        name = %resource.name_any(),
        generation = ?resource.meta().generation,
        "observed Gateway API resource",
    );
    debug!(?resource, "Gateway API resource snapshot");
    Ok(Action::await_change())
}

fn error_policy<K>(_resource: Arc<K>, error: &Infallible, _context: Arc<()>) -> Action {
    match *error {}
}
