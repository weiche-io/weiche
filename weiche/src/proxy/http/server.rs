//! serve downstream HTTP/1 and HTTP/2 connections on their owning runtime.
use super::super::{
    super::ir::{Binding, ServerTls, hostname_matches},
    admission::{Admission, Permit},
    config::RuntimeConfig,
    io::{AcceptedTls, Io, TlsIo},
    stopped,
    up::Tunnel,
};
use super::{ProxyService, RequestBody};
use compio::{
    net::{TcpListener, TcpStream},
    runtime::{self, time::timeout},
};
use cyper_core::{CompioExecutor, CompioTimer};
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
    /// maximum grace period after listener shutdown.
    drain_timeout: Duration,
    /// maximum duration of a frontend TLS handshake.
    handshake_timeout: Duration,
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
        Self {
            http1,
            service: Rc::new(service),
            builder,
            drain_timeout: config.connection_drain_timeout,
            handshake_timeout: config.tls_handshake_timeout,
        }
    }

    /// own the accept future and drain only the connections belonging to this binding.
    pub async fn accept(
        self: Rc<Self>,
        socket: TcpListener,
        binding: Rc<Binding>,
        admission: Arc<Admission>,
        mut certs: watch::Receiver<Rc<[Arc<ServerTls>]>>,
        drain: Duration,
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
                    runtime::spawn(self.clone().serve(
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
        let (stream, h2) = if listener.tls {
            let handshake = tokio::select! {
                _ = shutdown.changed() => return,
                result = timeout(self.handshake_timeout, accept_tls(stream, &certs)) => result,
            };
            let stream = match handshake {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %error, "tls handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "tls handshake timed out");
                    return;
                }
            };
            let h2 = stream.negotiated_alpn() == Some(b"h2");
            (Io::tls(stream), Some(h2))
        } else {
            let stream = match Io::plain(stream) {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!(%peer, %error, "failed to prepare HTTP socket");
                    return;
                }
            };
            (stream, None)
        };
        drop(certs);
        let owner = Rc::new(Owner {
            _permit: permit,
            shutdown: shutdown.clone(),
            drain: self.drain_timeout,
        });
        let service = service_fn(|request| {
            let server = self.clone();
            let listener = listener.clone();
            let owner = owner.clone();
            async move {
                let (response, tunnel) = server
                    .service
                    .call(&listener, peer, request.map(RequestBody::Incoming))
                    .await;
                if let Some(tunnel) = tunnel {
                    compio::runtime::spawn(owner.relay(tunnel)).detach();
                }
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        /// drive each hyper connection type through the same graceful shutdown path.
        macro_rules! serve {
            ($connection:expr) => {{
                let connection = $connection;
                tokio::pin!(connection);
                tokio::select! {
                    result = &mut connection => {
                        if let Err(error) = result {
                            tracing::debug!(%peer, %error, "http connection ended");
                        }
                    }
                    _ = shutdown.changed() => {
                        connection.as_mut().graceful_shutdown();
                        let _ = timeout(self.drain_timeout, connection).await;
                    }
                }
            }};
        }
        match h2 {
            Some(true) => {
                let builder = self.builder.clone().http2_only();
                serve!(builder.serve_connection(stream, service));
            }
            Some(false) => {
                serve!(self.http1.serve_connection(stream, service).with_upgrades())
            }
            None => serve!(self.builder.serve_connection_with_upgrades(stream, service)),
        }
    }
}

/// admission and listener shutdown state shared with an upgraded connection.
struct Owner {
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
        let relay = tunnel.relay();
        tokio::pin!(relay);
        let result = tokio::select! {
            result = &mut relay => result,
            _ = stopped(self.shutdown.clone()) => match timeout(self.drain, relay).await {
                Ok(result) => result,
                Err(_) => return,
            },
        };
        if let Err(error) = result {
            tracing::debug!(%error, "http tunnel ended");
        }
    }
}

/// a listener without a hostname also accepts clients that omit SNI.
async fn accept_tls(stream: TcpStream, certs: &[Arc<ServerTls>]) -> io::Result<TlsIo> {
    let handshake = AcceptedTls::accept(stream).await?;
    let host = handshake
        .client_hello()
        .server_name()
        .unwrap_or("")
        .to_ascii_lowercase();
    let tls = certs
        .iter()
        .find(|tls| hostname_matches(&tls.hostname, &host))
        .ok_or_else(|| io::Error::other("SNI does not match a listener hostname"))?;
    handshake.into_stream(tls.config()).await
}
