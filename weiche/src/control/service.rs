//! compile one Gateway and expose its proxy pods at the declared listener ports.
use super::{
    super::ir::{GatewaySnapshot, Listener, SocketRoutes, StreamListener},
    Compiled, GatewayStores, compile,
};
use kube::ResourceExt;
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use weiche_gtwapi::v1::gateways::Gateway;

pub const OWNER: &str = "weiche.io/gateway-uid";

/// controller decisions and the observed serving state for this Gateway.
pub struct Plan {
    pub gateway: Arc<Gateway>,
    pub addresses: Vec<(String, String)>,
    pub programmed: bool,
    pub denied: bool,
    pub issue: Option<String>,
    pub listeners: Vec<Compiled>,
}

impl Plan {
    pub fn new(gateway: Arc<Gateway>) -> Self {
        Self {
            gateway,
            addresses: vec![],
            programmed: false,
            denied: false,
            issue: None,
            listeners: vec![],
        }
    }

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
                } else {
                    let socket =
                        snapshot.sockets.entry(address).or_insert_with(|| SocketRoutes {
                            listener: Listener {
                                address,
                                port: listener.port,
                                gateway_uid: uid.clone(),
                                tls: vec![],
                            },
                            hosts: Default::default(),
                        });
                    socket.listener.tls.extend(result.tls.iter().cloned());
                    socket.hosts.insert(
                        listener.hostname.clone().unwrap_or_default(),
                        std::mem::take(&mut result.routes),
                    );
                }
            }
            listeners.push(result);
        }
        for socket in snapshot.sockets.values_mut() {
            socket.listener.tls.sort_by_key(|tls| {
                std::cmp::Reverse((!tls.hostname.is_empty(), tls.hostname.len()))
            });
        }
        (Self { listeners, ..Self::new(gateway) }, snapshot)
    }

    /// expose each requested port once across virtual hosts sharing a transport.
    pub fn ports(&self) -> Vec<serde_json::Value> {
        self.gateway.spec.listeners.iter()
            .map(|l| (super::super::ir::Transport::for_protocol(&l.protocol).protocol(), l.port))
            .collect::<BTreeSet<_>>().into_iter().map(|(protocol, port)| serde_json::json!({
                "name": format!("{}-{port}", protocol.to_ascii_lowercase()), "port":port,
                "protocol":protocol, "targetPort":port,
            })).collect()
    }
}
