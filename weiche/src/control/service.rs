//! compile one Gateway and expose its proxy pods at the declared listener ports.
use super::{
    super::ir::{
        GatewaySnapshot, Listener, SocketRoutes, StreamListener, TlsSocket, Transport,
    },
    Compiled, GatewayStores, compile,
};
use kube::ResourceExt;
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use weiche_gtwapi::v1::gateways::Gateway;

/// label identifying the gateway that owns a workload.
pub const OWNER: &str = "weiche.io/gateway-uid";

/// standard Gateway API label for resources provisioned for a Gateway.
pub const GATEWAY_NAME: &str = "gateway.networking.k8s.io/gateway-name";

/// check the controller owner reference before using a provisioned resource.
pub fn controlled_by(resource: &impl ResourceExt, uid: &str) -> bool {
    resource
        .owner_references()
        .iter()
        .any(|r| r.controller == Some(true) && r.uid == uid)
}

/// controller decisions and the observed serving state for this Gateway.
pub struct Plan {
    /// gateway observed during this reconciliation.
    pub gateway: Arc<Gateway>,
    /// address types and values published on the gateway.
    pub addresses: Vec<(String, String)>,
    /// whether the workers have applied the desired revision.
    pub programmed: bool,
    /// whether the gateway is outside the deployment scope.
    pub denied: bool,
    /// whether the gateway requested unsupported infrastructure parameters.
    pub invalid_parameters: bool,
    /// gateway API reason paired with an observed provisioning failure.
    pub issue_reason: Option<&'static str>,
    /// provisioning failure to report on the gateway.
    pub issue: Option<String>,
    /// attachment and reference results for each listener.
    pub listeners: Vec<Compiled>,
}

impl Plan {
    /// start a plan for the observed gateway.
    pub fn new(gateway: Arc<Gateway>) -> Self {
        Self {
            gateway,
            addresses: vec![],
            programmed: false,
            denied: false,
            invalid_parameters: false,
            issue_reason: None,
            issue: None,
            listeners: vec![],
        }
    }

    /// group eligible routes by socket and listener hostname.
    pub fn compile(
        stores: &GatewayStores,
        gateway: Arc<Gateway>,
        bind: IpAddr,
    ) -> (Self, GatewaySnapshot) {
        let uid = gateway.uid().unwrap_or_default();
        let mut snapshot = GatewaySnapshot::default();
        let mut listeners = Vec::new();
        for listener in &gateway.spec.listeners {
            let address = SocketAddr::new(bind, listener.port);
            let mut result = compile(stores, &gateway, listener, address);
            if result.accepted {
                if matches!(listener.protocol.as_str(), "TCP" | "UDP") {
                    snapshot.streams.insert(
                        (result.transport, address),
                        StreamListener {
                            gateway_uid: uid.clone(),
                            route: Arc::new(result.stream.take().unwrap_or_default()),
                        },
                    );
                } else if listener.protocol == "TLS" {
                    let socket = snapshot.tls.entry(address).or_insert_with(|| {
                        Arc::new(TlsSocket {
                            gateway_uid: uid.clone(),
                            hosts: Default::default(),
                        })
                    });
                    Arc::make_mut(socket).hosts.insert(
                        listener
                            .hostname
                            .as_deref()
                            .unwrap_or_default()
                            .to_ascii_lowercase(),
                        std::mem::take(&mut result.sni_routes),
                    );
                } else {
                    let socket =
                        snapshot.sockets.entry(address).or_insert_with(|| SocketRoutes {
                            listener: Listener {
                                address,
                                port: listener.port,
                                gateway_uid: uid.clone(),
                                tls: vec![],
                            },
                            routes: vec![],
                            hostnames: Vec::new(),
                        });
                    socket.listener.tls.extend(result.tls.iter().cloned());
                    if let Some(hostname) = &listener.hostname {
                        socket.hostnames.push(hostname.to_ascii_lowercase());
                    }
                    socket.routes.append(&mut result.routes);
                }
            }
            listeners.push(result);
        }
        snapshot.order_certificates();
        (Self { listeners, ..Self::new(gateway) }, snapshot)
    }

    /// check that the gateway is eligible and has at least one accepted listener.
    pub fn accepted(&self) -> bool {
        !self.denied
            && !self.invalid_parameters
            && self.listeners.iter().any(|l| l.accepted)
    }

    /// expose each accepted port once across virtual hosts sharing a transport.
    pub fn ports(&self, http3: bool) -> Vec<(Transport, u16)> {
        let mut ports = BTreeSet::new();
        for (listener, result) in self
            .gateway
            .spec
            .listeners
            .iter()
            .zip(&self.listeners)
            .filter(|(_, result)| result.accepted)
        {
            ports.insert((result.transport, listener.port));
            if http3 && listener.protocol == "HTTPS" {
                ports.insert((Transport::Udp, listener.port));
            }
        }
        ports.into_iter().collect()
    }
}
