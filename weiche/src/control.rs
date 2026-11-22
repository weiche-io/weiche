//! maintain Kubernetes watches and their live resource caches.
mod compile;
mod config;
mod manifest;
mod provision;
mod reconcile;
mod resolve;
mod service;
mod status;
mod tls;

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

/// register each watched kind once for both reflector setup and initial readiness.
macro_rules! stores {
    ($(#[doc = $doc:literal] $field:ident: $kind:ty $(=> $selector:literal)?),+ $(,)?) => {
        /// live, eventually consistent snapshots maintained by the reflectors.
        #[derive(Clone)]
        pub struct GatewayStores {
            $(#[doc = $doc] pub $field: Store<$kind>,)+
        }

        impl GatewayStores {
            /// wait for every reflector to publish its initial list.
            pub async fn wait_until_ready(&self) -> anyhow::Result<()> {
                tokio::try_join!($(self.$field.wait_until_ready()),+)?;
                Ok(())
            }
        }

        impl GatewayController {
            /// create cluster-wide reflectors for the controller.
            pub fn new(client: Client) -> Self {
                let mut watches = Vec::new();
                let stores = GatewayStores {
                    $($field: cache(Api::all(client.clone()),
                        watcher::Config::default()$(.fields($selector))?, &mut watches),)+
                };
                Self { stores, watches }
            }
        }
    };
}

stores! {
    /// gateway classes used to identify owned gateways.
    gateway_classes: GatewayClass,
    /// gateways that may own proxy workloads.
    gateways: Gateway,
    /// watched routes for HTTP and HTTPS listeners.
    http_routes: HttpRoute,
    /// watched gRPC routes available for listener attachment.
    grpc_routes: GrpcRoute,
    /// watched routes for TCP listeners.
    tcp_routes: TcpRoute,
    /// watched routes for TLS listeners.
    tls_routes: TlsRoute,
    /// watched routes for UDP listeners.
    udp_routes: UdpRoute,
    /// certificate chains and keys for terminating TLS.
    secrets: Secret => "type=kubernetes.io/tls",
    /// namespace labels used by listener selectors.
    namespaces: Namespace,
    /// permissions for references into another namespace.
    grants: ReferenceGrant,
    /// certificate authorities referenced by backend policies.
    config_maps: ConfigMap,
    /// authentication policies attached to backend Services.
    tls_policies: BackendTlsPolicy,
    /// backend Services and their declared ports.
    services: Service,
    /// addresses and ports of ready backends.
    endpoint_slices: EndpointSlice,
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
fn cache<K>(
    api: Api<K>,
    config: watcher::Config,
    watches: &mut Vec<BoxStream<'static, ()>>,
) -> Store<K>
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
    watches.push(stream);
    reader
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
