//! downstream TCP listeners for HTTP/1 and HTTP/2.
use super::{
    super::{
        super::{routing, rtconf::RuntimeConfig},
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
use futures::{StreamExt, stream::FuturesUnordered};
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
                    serving.push(accept(
                        socket,
                        server.clone(),
                        routes.listener.clone(),
                        admission.clone(),
                        rx,
                        runtime.worker_drain_timeout,
                    ));
                }
                Err(error) => tracing::warn!(%address,%error,"listener bind failed"),
            }
        }
        listeners.report(
            id,
            WorkerStatus {
                revision: snapshot.revision,
                bound: sockets.keys().copied().collect(),
            },
        );
        let retry = sockets.len() != snapshot.gateway.sockets.len();
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
