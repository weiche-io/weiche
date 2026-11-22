//! maintain Kubernetes watches and their live resource caches.
mod cleanup;
mod compile;
mod reconcile;
mod resolve;
mod service;
mod status;
mod tls;
mod workload;

pub use compile::{CONTROLLER_NAME, Compiled, compile};
pub use reconcile::run;
pub use resolve::ResolveError;

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
    gateways::Gateway, grpcroutes::GrpcRoute, httproutes::HttpRoute,
    referencegrants::ReferenceGrant, tcproutes::TcpRoute, tlsroutes::TlsRoute,
    udproutes::UdpRoute,
};

/// live snapshots maintained by kube-runtime, including updates and deletions.
/// wait for each store's initial list before reading it; caches are eventually
/// consistent and do not form an atomic snapshot.
#[derive(Clone)]
pub struct GatewayStores {
    /// gateway classes used to identify owned gateways.
    pub gateway_classes: Store<GatewayClass>,
    /// gateways that may own proxy workloads.
    pub gateways: Store<Gateway>,
    /// watched routes for TCP listeners.
    pub tcp_routes: Store<TcpRoute>,
    /// watched routes for TLS listeners.
    pub tls_routes: Store<TlsRoute>,
    /// watched routes for UDP listeners.
    pub udp_routes: Store<UdpRoute>,
    /// watched routes for HTTP and HTTPS listeners.
    pub http_routes: Store<HttpRoute>,
    /// watched gRPC routes available for listener attachment.
    pub grpc_routes: Store<GrpcRoute>,
    /// certificate chains and keys for terminating TLS.
    pub secrets: Store<Secret>,
    /// namespace labels used by listener selectors.
    pub namespaces: Store<Namespace>,
    /// permissions for references into another namespace.
    pub grants: Store<ReferenceGrant>,
    /// certificate authorities referenced by backend policies.
    pub config_maps: Store<ConfigMap>,
    /// authentication policies attached to backend Services.
    pub tls_policies: Store<BackendTlsPolicy>,
    /// backend Services and their declared ports.
    pub services: Store<Service>,
    /// addresses and ports of ready backends.
    pub endpoint_slices: Store<EndpointSlice>,
}

/// maintains resource caches for the reconciliation loop.
/// uses reflectors directly because these watches have no per-object reconcile.
pub struct GatewayController {
    /// resource stores shared with the compiler.
    stores: GatewayStores,
    /// watch streams that update the stores.
    watches: Vec<BoxStream<'static, ()>>,
}
impl GatewayController {
    /// create cluster-wide reflectors for the controller.
    pub fn new(client: Client) -> Self {
        let defaults = watcher::Config::default;
        let (gateway_classes, classes_watch) =
            cache(Api::all(client.clone()), defaults());
        let (gateways, gateways_watch) = cache(Api::all(client.clone()), defaults());
        let (tcp_routes, tcp_watch) = cache(Api::all(client.clone()), defaults());
        let (tls_routes, tls_watch) = cache(Api::all(client.clone()), defaults());
        let (udp_routes, udp_watch) = cache(Api::all(client.clone()), defaults());
        let (http_routes, routes_watch) = cache(Api::all(client.clone()), defaults());
        let (grpc_routes, grpc_watch) = cache(Api::all(client.clone()), defaults());
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
                grpc_routes,
                tcp_routes,
                tls_routes,
                udp_routes,
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
                grpc_watch,
                tcp_watch,
                tls_watch,
                udp_watch,
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
    /// clone the read handles for reconciliation.
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

/// watch one resource type and signal after applying changes.
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
