//! serve downstream HTTP/1 and HTTP/2 connections on their owning runtime.
use super::super::super::log;
use super::super::{
    super::ir::{Binding, Listener, ServerTls},
    admission::Permit,
    config::RuntimeConfig,
    io::{AcceptedTls, Io, TlsIo},
    net::tcp,
    stopped,
    up::Tunnel,
    worker::{HttpListener, Task, Worker},
};
use super::{ProxyService, RequestBody};
use compio::{
    net::TcpStream,
    runtime::{self, time::timeout},
};
use cyper_core::{CompioExecutor, CompioTimer};
use futures::{FutureExt, stream};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::server::conn::auto::Builder;
use std::{io, net::SocketAddr, rc::Rc, sync::Arc, time::Duration};
use tokio::sync::watch;

/// downstream HTTP/1 and HTTP/2 server configured for one worker.
pub struct Server {
    /// worker-local routes and connection pools.
    pub service: Rc<ProxyService>,
    /// strict HTTP/1 server for a TLS connection negotiated with ALPN.
    http1: http1::Builder,
    /// cleartext protocol detection and HTTP/2 configuration.
    builder: Builder<CompioExecutor>,
}

impl Server {
    /// configure protocol limits on cleartext and ALPN-selected servers.
    pub fn new(service: ProxyService, config: &RuntimeConfig) -> Self {
        let mut builder = Builder::new(CompioExecutor);
        builder
            .http1()
            .max_headers(config.http_max_headers)
            .max_buf_size(config.http_max_buffer);
        builder
            .http2()
            .timer(CompioTimer)
            .max_concurrent_streams(config.http2_max_concurrent_streams)
            .max_send_buf_size(config.http_max_buffer)
            .max_header_list_size(config.http_max_buffer as u32);
        let mut http1 = http1::Builder::new();
        http1
            .max_headers(config.http_max_headers)
            .max_buf_size(config.http_max_buffer);
        Self { http1, service: Rc::new(service), builder }
    }
}

impl Worker {
    /// bind an HTTP socket and watch its certificate table until the accept loop drains.
    pub fn bind_http(
        self: &Rc<Self>,
        address: SocketAddr,
        listener: &Listener,
    ) -> io::Result<(HttpListener, Task)> {
        let socket = tcp::listen(address, &self.runtime.tcp, self.id)?;
        let binding = Rc::new(listener.binding());
        let (certs, mut rx) = watch::channel(Rc::from(listener.tls.as_slice()));
        let http = HttpListener { binding: binding.clone(), certs };
        let worker = self.clone();
        let task = async move {
            let (connections, _) = watch::channel(false);
            // hyper connections run independently so upgraded tunnels can finish draining.
            tcp::accept(
                &worker,
                socket,
                &mut rx,
                &mut stream::empty::<()>(),
                |(stream, peer), certs, permit, _| {
                    let connection = worker.clone().serve(
                        binding.clone(),
                        certs.borrow().clone(),
                        stream,
                        peer,
                        permit,
                        connections.subscribe(),
                    );
                    runtime::spawn(connection).detach();
                },
            )
            .await?;
            connections.send_replace(true);
            let _ =
                timeout(worker.runtime.worker_drain_timeout, connections.closed()).await;
            Ok(())
        };
        Ok((http, task.boxed_local()))
    }

    /// retain admission through HTTP serving and any subsequent tunnel.
    pub async fn serve(
        self: Rc<Self>,
        listener: Rc<Binding>,
        certs: Rc<[Arc<ServerTls>]>,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Permit,
        mut shutdown: watch::Receiver<bool>,
    ) {
        if *shutdown.borrow() || shutdown.has_changed().unwrap_or(true) {
            return;
        }
        if let Err(error) = stream.set_nodelay(self.runtime.tcp.nodelay) {
            log!(
                warn,
                "failed to set tcp nodelay",
                peer = peer.to_string(),
                error = error.to_string()
            );
            return;
        }
        let (stream, h2, tls_listener) = if listener.tls {
            let (stream, selected) = tokio::select! {
                _ = shutdown.changed() => return,
                result = timeout(self.runtime.tls_handshake_timeout, accept_tls(stream, &certs)) => match result {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        log!(debug, "tls handshake failed", peer = peer.to_string(), error = error.to_string());
                        return;
                    }
                    Err(_) => {
                        log!(debug, "tls handshake timed out", peer = peer.to_string());
                        return;
                    }
                },
            };
            let h2 = stream.negotiated_alpn() == Some(b"h2");
            (Io::tls(stream), Some(h2), Some(selected))
        } else {
            let Ok(stream) = Io::plain(stream).inspect_err(|error| {
                log!(
                    warn,
                    "failed to prepare HTTP socket",
                    peer = peer.to_string(),
                    error = error.to_string()
                );
            }) else {
                return;
            };
            (stream, None, None)
        };
        drop(certs);
        let drain = self.runtime.connection_drain_timeout;
        let owner = Rc::new(Owner {
            _permit: permit,
            tls_listener,
            shutdown: shutdown.clone(),
            drain,
        });
        let service = service_fn(|request| {
            let service = self.server.service.clone();
            let listener = listener.clone();
            let owner = owner.clone();
            async move {
                let exchange = service
                    .call(
                        &listener,
                        owner.tls_listener.as_deref(),
                        peer,
                        request.map(RequestBody::Incoming),
                    )
                    .await;
                if let Some(tunnel) = exchange.tunnel {
                    runtime::spawn(owner.relay(tunnel)).detach();
                }
                Ok::<_, std::convert::Infallible>(exchange.response)
            }
        });
        /// drive each hyper connection type through the same graceful shutdown path.
        macro_rules! serve {
            ($connection:expr) => {{
                let mut connection = std::pin::pin!($connection);
                tokio::select! {
                    result = &mut connection => {
                        if let Err(error) = result {
                            log!(debug, "http connection ended", peer = peer.to_string(), error = error.to_string());
                        }
                    }
                    _ = shutdown.changed() => {
                        connection.as_mut().graceful_shutdown();
                        let _ = timeout(drain, connection).await;
                    }
                }
            }};
        }
        let server = &self.server;
        match h2 {
            Some(true) => {
                let builder = server.builder.clone().http2_only();
                serve!(builder.serve_connection(stream, service));
            }
            Some(false) => {
                serve!(server.http1.serve_connection(stream, service).with_upgrades())
            }
            None => {
                serve!(server.builder.serve_connection_with_upgrades(stream, service))
            }
        }
    }
}

/// admission and listener shutdown state shared with an upgraded connection.
struct Owner {
    /// hostname pattern selected during TLS negotiation, including the empty catch-all.
    tls_listener: Option<String>,
    /// keep the downstream capacity reserved through tunnel EOF.
    _permit: Permit,
    /// observe removal of the accepting listener.
    shutdown: watch::Receiver<bool>,
    /// grace period for a tunnel that is still transferring bytes.
    drain: Duration,
}

impl Owner {
    /// relay locally and preserve the listener's normal drain interval on shutdown.
    async fn relay(self: Rc<Self>, tunnel: Tunnel) {
        let mut relay = std::pin::pin!(tunnel.relay());
        let result = tokio::select! {
            result = &mut relay => result,
            _ = stopped(self.shutdown.clone()) => match timeout(self.drain, relay).await {
                Ok(result) => result,
                Err(_) => return,
            },
        };
        if let Err(error) = result {
            log!(debug, "http tunnel ended", error = error.to_string());
        }
    }
}

/// a listener without a hostname also accepts clients that omit SNI.
async fn accept_tls(
    stream: TcpStream,
    certs: &[Arc<ServerTls>],
) -> io::Result<(TlsIo, String)> {
    let handshake = AcceptedTls::accept(stream).await?;
    let tls = ServerTls::for_sni(certs, handshake.client_hello().server_name())
        .ok_or_else(|| io::Error::other("SNI does not match a listener hostname"))?;
    Ok((handshake.into_stream(tls.config()).await?, tls.hostname.clone()))
}
