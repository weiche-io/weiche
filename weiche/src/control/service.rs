//! expose each managed Gateway through a ClusterIP and distinct target ports on the shared workers.
use super::super::ir::{
    GatewaySnapshot, Listener, SocketRoutes, StreamListener, Transport,
};
use super::{CONTROLLER_NAME, Compiled, GatewayStores, Selection, compile};
use k8s_openapi::{
    api::core::v1::{Service, ServicePort},
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, Patch, PatchParams, PostParams, Preconditions},
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};
use weiche_gtwapi::v1::{gatewayclasses::GatewayClass, gateways::Gateway};

const OWNER: &str = "weiche.io/gateway-uid";
/// an independently addressable Gateway and its per-listener decisions, in spec order.
pub struct Plan {
    pub gateway: Arc<Gateway>,
    /// absent in single-gateway mode, assigned by the managed Service otherwise.
    pub addresses: Option<Vec<String>>,
    pub available: bool,
    pub listeners: Vec<Compiled>,
}

fn target(port: &ServicePort) -> Option<u16> {
    match port.target_port {
        Some(IntOrString::Int(n)) => u16::try_from(n).ok(),
        _ => None,
    }
}

fn owned(service: &Service) -> bool {
    service
        .labels()
        .get(OWNER)
        .is_some_and(|uid| service.name_any() == format!("weiche-{uid}"))
        && service.owner_references().iter().any(|r| {
            r.api_version == "gateway.networking.k8s.io/v1"
                && r.kind == "GatewayClass"
                && r.controller == Some(true)
        })
}

/// preserve existing port assignments across restarts and reserve newly allocated ports before publishing.
async fn expose(
    api: &Api<Service>,
    existing: Option<&Service>,
    gateway: &Gateway,
    class: &GatewayClass,
    used: &mut BTreeMap<u16, usize>,
) -> anyhow::Result<Service> {
    let uid = gateway.uid().ok_or_else(|| anyhow::anyhow!("gateway has no uid"))?;
    let name = format!("weiche-{uid}");
    let mut owner = class
        .controller_owner_ref(&())
        .ok_or_else(|| anyhow::anyhow!("gatewayclass has no uid"))?;
    owner.block_owner_deletion = None;
    if let Some(s) = existing {
        anyhow::ensure!(
            owned(s)
                && s.labels().get(OWNER) == Some(&uid)
                && s.owner_references().iter().any(|r| r.uid == owner.uid),
            "service {name} has another owner"
        );
    }
    let current = existing.and_then(|s| s.spec.as_ref()).and_then(|s| s.ports.as_ref());
    let mut ports = Vec::new();
    for (transport, port) in gateway
        .spec
        .listeners
        .iter()
        .map(|l| (Transport::for_protocol(&l.protocol), l.port))
        .collect::<BTreeSet<_>>()
    {
        let previous = current
            .into_iter()
            .flatten()
            .find(|p| {
                p.port == i32::from(port)
                    && p.protocol.as_deref().unwrap_or("TCP") == transport.protocol()
            })
            .and_then(target)
            .filter(|n| (10000..=60999).contains(n) && used.get(n) == Some(&1));
        let local = match previous {
            Some(n) => n,
            None => {
                use std::hash::{DefaultHasher, Hash, Hasher};
                let mut hash = DefaultHasher::new();
                (&uid, port, transport).hash(&mut hash);
                let start = hash.finish() % 51000;
                let n = (0..51000)
                    .map(|i| 10000 + ((start + i) % 51000) as u16)
                    .find(|n| !used.contains_key(n))
                    .ok_or_else(|| {
                        anyhow::anyhow!("all gateway target ports are allocated")
                    })?;
                used.insert(n, 1);
                n
            }
        };
        ports.push(ServicePort {
            name: Some(format!("{}-{port}", transport.protocol().to_ascii_lowercase())),
            port: i32::from(port),
            protocol: Some(transport.protocol().into()),
            target_port: Some(IntOrString::Int(i32::from(local))),
            ..Default::default()
        });
    }
    let selector = BTreeMap::from([("app".to_owned(), "weiche".to_owned())]);
    if let Some(s) = existing {
        if current == Some(&ports)
            && s.spec.as_ref().and_then(|spec| spec.selector.as_ref()) == Some(&selector)
        {
            return Ok(s.clone());
        }
        let patch = json!({"metadata":{"resourceVersion":s.resource_version()},"spec":{"selector":selector,"ports":ports}});
        return Ok(api
            .patch(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await?);
    }
    let value = json!({"metadata":{"name":name,"labels":{(OWNER):uid},"ownerReferences":[owner]},"spec":{"type":"ClusterIP","ipFamilyPolicy":"PreferDualStack","selector":selector,"ports":ports}});
    Ok(api
        .create(&PostParams::default(), &serde_json::from_value(value)?)
        .await?)
}

/// translate every owned Gateway into isolated address and hostname routing tables.
pub async fn plans(
    client: &Client,
    stores: &GatewayStores,
    selection: &Selection,
) -> (GatewaySnapshot, Vec<Plan>, bool) {
    let mut retry = false;
    let api = Api::<Service>::namespaced(client.clone(), &selection.namespace);
    let services: BTreeMap<_, _> = stores
        .services
        .state()
        .into_iter()
        .filter(|s| s.namespace().as_deref() == Some(&selection.namespace))
        .map(|s| (s.name_any(), s))
        .collect();
    let mut used = BTreeMap::new();
    for n in services
        .values()
        .filter(|s| owned(s))
        .filter_map(|s| s.spec.as_ref())
        .flat_map(|s| s.ports.iter().flatten().filter_map(target))
    {
        *used.entry(n).or_default() += 1;
    }
    let mut keep = BTreeSet::new();
    let mut snapshot = GatewaySnapshot::default();
    let mut plans = vec![];
    let mut gateways = stores.gateways.state();
    gateways.sort_by_key(|g| (g.namespace(), g.name_any()));
    for g in gateways {
        if g.metadata.deletion_timestamp.is_some()
            || !selection.managed
                && (g.name_any() != selection.gateway
                    || g.namespace().as_deref() != Some(&selection.namespace))
        {
            continue;
        }
        let Some(class) = stores.gateway_classes.state().into_iter().find(|c| {
            c.name_any() == g.spec.gateway_class_name
                && c.spec.controller_name == CONTROLLER_NAME
                && c.spec.parameters_ref.is_none()
                && c.metadata.deletion_timestamp.is_none()
        }) else {
            continue;
        };
        let uid = g.uid().unwrap_or_default();
        let mut mapping = BTreeMap::new();
        let mut addresses = None;
        let mut available = true;
        if selection.managed {
            let name = format!("weiche-{uid}");
            keep.insert(name.clone());
            match expose(
                &api,
                services.get(&name).map(AsRef::as_ref),
                &g,
                &class,
                &mut used,
            )
            .await
            {
                Ok(service) => {
                    if let Some(spec) = service.spec {
                        addresses = spec.cluster_ips;
                        for p in spec.ports.into_iter().flatten() {
                            if let Some(t) = target(&p) {
                                mapping.insert(
                                    (
                                        Transport::for_protocol(
                                            p.protocol.as_deref().unwrap_or("TCP"),
                                        ),
                                        p.port as u16,
                                    ),
                                    t,
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(gateway=%g.name_any(),%error,"could not reconcile gateway service");
                    retry = true;
                }
            }
            available = addresses.as_ref().is_some_and(|a| !a.is_empty())
                && g.spec.listeners.iter().all(|l| {
                    mapping.contains_key(&(Transport::for_protocol(&l.protocol), l.port))
                });
            if !available {
                addresses = Some(Vec::new());
            }
        }
        let mut listeners = vec![];
        for l in &g.spec.listeners {
            let address = SocketAddr::new(
                selection.bind_ip,
                mapping
                    .get(&(Transport::for_protocol(&l.protocol), l.port))
                    .copied()
                    .unwrap_or(l.port),
            );
            let mut result = compile(stores, &g, l, address);
            if available
                && result.accepted
                && matches!(l.protocol.as_str(), "TCP" | "UDP")
            {
                snapshot.streams.insert(
                    (result.transport, address),
                    StreamListener {
                        gateway_uid: uid.clone(),
                        route: Arc::new(result.stream.take().unwrap_or_default()),
                    },
                );
            } else if available && result.accepted {
                let socket =
                    snapshot.sockets.entry(address).or_insert_with(|| SocketRoutes {
                        listener: Listener {
                            address,
                            port: l.port,
                            gateway_uid: uid.clone(),
                            tls: vec![],
                        },
                        hosts: BTreeMap::new(),
                    });
                if let Some(tls) = &result.tls {
                    socket.listener.tls.push(tls.clone())
                }
                socket.hosts.insert(
                    l.hostname.clone().unwrap_or_default(),
                    std::mem::take(&mut result.routes),
                );
            }
            listeners.push(result);
        }
        plans.push(Plan { gateway: g, addresses, available, listeners });
    }
    for s in snapshot.sockets.values_mut() {
        s.listener.tls.sort_by_key(|tls| {
            std::cmp::Reverse((
                !tls.hostname.is_empty() && !tls.hostname.starts_with("*."),
                tls.hostname.len(),
            ))
        });
    }
    if selection.managed {
        for (name, s) in services.iter().filter(|(n, s)| owned(s) && !keep.contains(*n)) {
            let params = DeleteParams {
                preconditions: Some(Preconditions {
                    uid: s.uid(),
                    resource_version: s.resource_version(),
                }),
                ..Default::default()
            };
            if let Err(error) = api.delete(name, &params).await
                && !matches!(&error,kube::Error::Api(r) if r.code==404)
            {
                tracing::warn!(service=%name,%error,"could not remove obsolete gateway service");
                retry = true;
            }
        }
    }
    (snapshot, plans, retry)
}
