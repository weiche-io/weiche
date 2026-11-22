//! worker socket lifecycle for HTTP and transport routes.
use super::{
    super::ir::{Binding, ServerTls, StreamRoute, TlsSocket, Transport},
    admission::Admission,
    config::{self, RuntimeConfig},
    http::{ProxyService, server::Server},
    net::{ListenerGroup, WorkerStatus, listen_tcp, quic, tcp, udp},
    stopped,
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

/// reconcile a worker's listeners on their owning compio runtime.
pub async fn run(
    id: usize,
    mut config: config::Reader,
    runtime: Arc<RuntimeConfig>,
    admission: Arc<Admission>,
    listeners: Arc<ListenerGroup>,
    stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let snapshot = config.snapshot();
    let service = ProxyService::new(snapshot, &runtime)?;
    let server = Rc::new(Server::new(service, &runtime));
    let mut sockets =
        BTreeMap::<SocketAddr, (Rc<Binding>, watch::Sender<Rc<[Arc<ServerTls>]>>)>::new();
    let mut streams = BTreeMap::<
        (Transport, SocketAddr),
        (String, watch::Sender<Arc<StreamRoute>>),
    >::new();
    let mut quic = BTreeMap::<SocketAddr, quic::Listener>::new();
    let mut revision = None;
    let mut tls = BTreeMap::<SocketAddr, watch::Sender<Arc<TlsSocket>>>::new();
    let mut serving = FuturesUnordered::new();
    let stopping = stopped(stop);
    tokio::pin!(stopping);
    loop {
        let snapshot = config.snapshot();
        server.service.apply(snapshot.clone())?;
        sockets.retain(|address, (binding, certs)| {
            let Some(socket) = snapshot.gateway.sockets.get(address) else {
                return false;
            };
            if socket.listener.binding() != **binding {
                return false;
            }
            certs.send_if_modified(|current| {
                if current.as_ref() == socket.listener.tls.as_slice() {
                    return false;
                }
                *current = Rc::from(socket.listener.tls.as_slice());
                true
            });
            true
        });
        for (address, routes) in &snapshot.gateway.sockets {
            if sockets.contains_key(address) {
                continue;
            }
            match listen_tcp(*address) {
                Ok(socket) => {
                    let binding = Rc::new(routes.listener.binding());
                    let (certs, rx) =
                        watch::channel(Rc::from(routes.listener.tls.as_slice()));
                    sockets.insert(*address, (binding.clone(), certs));
                    serving.push(
                        accept(
                            socket,
                            server.clone(),
                            binding,
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
        let changed = revision.replace(snapshot.revision) != Some(snapshot.revision);
        let mut retry_quic = false;
        if runtime.http3 {
            quic.retain(|addr, listener| {
                sockets.get(addr).is_some_and(|(b, _)| b == &listener.binding)
                    && !snapshot.gateway.streams.contains_key(&(Transport::Udp, *addr))
            });
            for (&address, (binding, certs)) in &sockets {
                if !binding.tls {
                    continue;
                }
                if snapshot.gateway.streams.contains_key(&(Transport::Udp, address)) {
                    if changed && id == 0 {
                        tracing::warn!(%address, "explicit udp listener takes precedence over http/3");
                    }
                    continue;
                }
                if let Some(listener) = quic.get(&address) {
                    if changed {
                        listener.update(&certs.borrow());
                    }
                    continue;
                }
                let endpoint = listeners
                    .datagrams
                    .acquire(address, id, runtime.workers.get(), true)
                    .and_then(|(socket, reader)| {
                        let (listener, endpoint) = quic::Listener::bind(
                            socket,
                            server.service.clone(),
                            binding.clone(),
                            &certs.borrow(),
                            id,
                            &runtime,
                        )?;
                        let task = listener.serve(
                            endpoint,
                            admission.clone(),
                            runtime.clone(),
                            reader,
                        );
                        Ok((listener, task))
                    });
                match endpoint {
                    Ok((listener, task)) => {
                        serving.push(task.boxed_local());
                        quic.insert(address, listener);
                    }
                    Err(error) => {
                        retry_quic = true;
                        if error.kind() != io::ErrorKind::WouldBlock {
                            tracing::warn!(%address, %error, "http/3 listener bind failed");
                        }
                    }
                }
            }
        }
        streams.retain(|key, (uid, _)| {
            snapshot
                .gateway
                .streams
                .get(key)
                .is_some_and(|l| &l.gateway_uid == uid)
        });
        tls.retain(|addr, tx| {
            snapshot
                .gateway
                .tls
                .get(addr)
                .is_some_and(|s| s.gateway_uid == tx.borrow().gateway_uid)
        });
        for (address, socket) in &snapshot.gateway.tls {
            if let Some(tx) = tls.get(address) {
                update(tx, socket);
                continue;
            }
            match listen_tcp(*address) {
                Ok(listener) => {
                    let (tx, rx) = watch::channel(socket.clone());
                    serving.push(
                        tcp::serve(
                            listener,
                            rx,
                            admission.clone(),
                            runtime.clone(),
                            tcp::passthrough,
                        )
                        .boxed_local(),
                    );
                    tls.insert(*address, tx);
                }
                Err(error) => {
                    tracing::warn!(%address, %error, "tls listener bind failed")
                }
            }
        }
        for (&key, listener) in &snapshot.gateway.streams {
            if let Some((_, tx)) = streams.get(&key) {
                update(tx, &listener.route);
                continue;
            }
            let (tx, rx) = watch::channel(listener.route.clone());
            let (transport, address) = key;
            let task = match transport {
                Transport::Tcp => listen_tcp(address).map(|s| {
                    tcp::serve(s, rx, admission.clone(), runtime.clone(), tcp::forward)
                        .boxed_local()
                }),
                Transport::Udp => {
                    let socket = if runtime.http3 {
                        listeners
                            .datagrams
                            .acquire(address, id, runtime.workers.get(), false)
                            .map(|(socket, reader)| (socket, Some(reader)))
                    } else {
                        udp::listen(address).map(|s| (s, None))
                    };
                    socket.map(|(socket, reader)| {
                        let admission = admission.clone();
                        let idle = runtime.upstream_idle_timeout;
                        async move {
                            if let Some(_reader) = reader {
                                let result =
                                    udp::serve(socket.clone(), rx, admission, idle).await;
                                socket.close().await?;
                                result
                            } else {
                                udp::serve(socket, rx, admission, idle).await
                            }
                        }
                        .boxed_local()
                    })
                }
            };
            match task {
                Ok(task) => {
                    serving.push(task);
                    streams.insert(key, (listener.gateway_uid.clone(), tx));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
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
                    .chain(tls.keys())
                    .map(|a| (Transport::Tcp, *a))
                    .chain(quic.keys().map(|a| (Transport::Udp, *a)))
                    .chain(streams.keys().copied())
                    .collect(),
            },
        );
        let retry = retry_quic
            || sockets.len() != snapshot.gateway.sockets.len()
            || tls.len() != snapshot.gateway.tls.len()
            || streams.len() != snapshot.gateway.streams.len();
        tokio::select! {
            biased;
            _=&mut stopping=>break,
            changed=config.changed()=>if changed.is_err(){break},
            result=serving.next(), if !serving.is_empty()=>{if let Some(Err(error))=result {return Err(error.into())}},
            _=sleep(runtime.listener_retry_interval),if retry=>{}
        }
    }
    drop(quic);
    drop(sockets);
    drop(streams);
    drop(tls);
    while serving.next().await.is_some() {}
    listeners.report(id, WorkerStatus::default());
    server.service.close();
    Ok(())
}

/// notify an existing socket only when its forwarding table changes.
fn update<T: PartialEq>(tx: &watch::Sender<Arc<T>>, desired: &Arc<T>) {
    tx.send_if_modified(|current| {
        if current == desired {
            return false;
        }
        *current = desired.clone();
        true
    });
}

/// own the accept future and drain only the connections belonging to this binding.
async fn accept(
    socket: TcpListener,
    server: Rc<Server>,
    binding: Rc<Binding>,
    admission: Arc<Admission>,
    mut certs: watch::Receiver<Rc<[Arc<ServerTls>]>>,
    drain: std::time::Duration,
) -> io::Result<()> {
    let (connections, _) = watch::channel(false);
    {
        let mut accepting = std::pin::pin!(socket.accept());
        loop {
            let (stream, peer) = tokio::select! {
                biased;
                changed = certs.changed() => {
                    if changed.is_err() { break }
                    continue;
                }
                accepted = &mut accepting => accepted?,
            };
            accepting.set(socket.accept());
            if let Some(permit) = admission.acquire() {
                runtime::spawn(server.clone().serve(
                    binding.clone(),
                    certs.borrow().clone(),
                    stream,
                    peer,
                    permit,
                    connections.subscribe(),
                ))
                .detach();
            }
        }
    }
    drop(socket);
    connections.send_replace(true);
    let _ = timeout(drain, connections.closed()).await;
    Ok(())
}
