//! downstream TCP listeners for HTTP/1 and HTTP/2.
use super::{
    super::{
        super::{
            ir::{StreamRoute, Transport},
            routing,
            rtconf::RuntimeConfig,
        },
        core::{http::ProxyService, tcp::Admission},
        stopped,
    },
    ListenerGroup, WorkerStatus, bind_socket,
    http::Server,
};
use compio::{
    net::TcpListener,
    runtime::{
        self,
        time::{sleep, timeout},
    },
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{collections::BTreeMap, io, net::SocketAddr, rc::Rc, sync::Arc};
use tokio::sync::watch;

/// bind a reusable listener on the worker's runtime.
pub fn listen(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, socket2::Protocol::TCP)?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}

/// run one listener on its owning compio runtime.
pub(in super::super) async fn run(
    id: usize,
    mut config: routing::Reader,
    runtime: Arc<RuntimeConfig>,
    admission: Arc<Admission>,
    listeners: Arc<ListenerGroup>,
    stop: watch::Sender<bool>,
) -> anyhow::Result<()> {
    let snapshot = config.snapshot();
    let service = ProxyService::new(snapshot, &runtime)?;
    let server = Rc::new(Server::new(service, &runtime));
    let mut sockets = BTreeMap::<
        SocketAddr,
        (super::super::super::ir::Listener, watch::Sender<bool>),
    >::new();
    let mut streams = BTreeMap::<
        (Transport, SocketAddr),
        (String, watch::Sender<Arc<StreamRoute>>),
    >::new();
    let mut serving = FuturesUnordered::new();
    let stopping = stopped(stop.subscribe());
    tokio::pin!(stopping);
    loop {
        let snapshot = config.snapshot();
        server.service.apply(snapshot.clone())?;
        sockets.retain(|address, (old, closing)| {
            if snapshot
                .gateway
                .sockets
                .get(address)
                .is_some_and(|s| &s.listener == old)
            {
                true
            } else {
                closing.send_replace(true);
                false
            }
        });
        for (address, routes) in &snapshot.gateway.sockets {
            if sockets.contains_key(address) {
                continue;
            }
            match listen(*address) {
                Ok(socket) => {
                    let (close, rx) = watch::channel(false);
                    sockets.insert(*address, (routes.listener.clone(), close));
                    serving.push(
                        accept(
                            socket,
                            server.clone(),
                            routes.listener.clone(),
                            admission.clone(),
                            rx,
                            runtime.worker_drain_timeout,
                        )
                        .boxed_local(),
                    );
                }
                Err(error) => tracing::warn!(%address,%error,"listener bind failed"),
            }
        }
        streams.retain(|key, (uid, _)| {
            snapshot
                .gateway
                .streams
                .get(key)
                .is_some_and(|l| &l.gateway_uid == uid)
        });
        for (&key, listener) in &snapshot.gateway.streams {
            if let Some((_, tx)) = streams.get(&key) {
                tx.send_if_modified(|current| {
                    if *current == listener.route {
                        false
                    } else {
                        *current = listener.route.clone();
                        true
                    }
                });
                continue;
            }
            let (tx, rx) = watch::channel(listener.route.clone());
            let (transport, address) = key;
            let task = match transport {
                Transport::Tcp => listen(address).map(|s| {
                    super::relay::serve(
                        s,
                        rx,
                        admission.clone(),
                        runtime.connection_drain_timeout,
                    )
                    .boxed_local()
                }),
                Transport::Udp => super::udp::listen(address).map(|s| {
                    super::udp::serve(
                        s,
                        rx,
                        admission.clone(),
                        runtime.upstream_idle_timeout,
                    )
                    .boxed_local()
                }),
            };
            match task {
                Ok(task) => {
                    serving.push(task);
                    streams.insert(key, (listener.gateway_uid.clone(), tx));
                }
                Err(error) => {
                    tracing::warn!(%address,?transport,%error,"transport listener bind failed")
                }
            }
        }
        listeners.report(
            id,
            WorkerStatus {
                revision: snapshot.revision,
                bound: sockets
                    .keys()
                    .map(|a| (Transport::Tcp, *a))
                    .chain(streams.keys().copied())
                    .collect(),
            },
        );
        let retry = sockets.len() != snapshot.gateway.sockets.len()
            || streams.len() != snapshot.gateway.streams.len();
        tokio::select! {
            biased;
            _=&mut stopping=>break,
            changed=config.changed()=>if changed.is_err(){break},
            result=serving.next(), if !serving.is_empty()=>{if let Some(Err(error))=result {return Err(error.into())}},
            _=sleep(runtime.listener_retry_interval),if retry=>{}
        }
    }
    for (_, (_, close)) in sockets {
        close.send_replace(true);
    }
    drop(streams);
    while serving.next().await.is_some() {}
    listeners.report(id, WorkerStatus::default());
    server.service.close();
    Ok(())
}

/// own the accept future and drain only the connections belonging to this binding.
async fn accept(
    socket: TcpListener,
    server: Rc<Server>,
    listener: super::super::super::ir::Listener,
    admission: Arc<Admission>,
    mut closing: watch::Receiver<bool>,
    drain: std::time::Duration,
) -> io::Result<()> {
    let (connections, _) = watch::channel(false);
    loop {
        tokio::select! {
            biased;
            _=closing.changed()=>break,
            accepted=socket.accept()=>{
                let (stream,peer)=accepted?;
                if let Some(permit)=admission.acquire() {
                    runtime::spawn(server.clone().serve(listener.clone(),stream,peer,permit,connections.subscribe())).detach();
                }
            }
        }
    }
    drop(socket);
    connections.send_replace(true);
    let _ = timeout(drain, connections.closed()).await;
    Ok(())
}
