//! serve downstream HTTP/1 and HTTP/2 connections on their owning runtime.
use super::super::{
    super::{
        ir::{Binding, ServerTls},
        rtconf::RuntimeConfig,
    },
    core::{http::ProxyService, tcp::Permit},
    io::Io,
};
use super::tls;
use compio::{net::TcpStream, runtime::time::timeout};
use cyper_core::{CompioExecutor, CompioTimer};
use hyper::service::service_fn;
use hyper_util::server::conn::auto::Builder;
use std::{net::SocketAddr, rc::Rc, sync::Arc, time::Duration};
use tokio::sync::watch;

/// downstream HTTP/1 and HTTP/2 server configured for one worker.
pub struct Server {
    pub service: ProxyService,
    builder: Builder<CompioExecutor>,
    drain_timeout: Duration,
    handshake_timeout: Duration,
}

impl Server {
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
        Self {
            service,
            builder,
            drain_timeout: config.connection_drain_timeout,
            handshake_timeout: config.tls_handshake_timeout,
        }
    }

    pub async fn serve(
        self: Rc<Self>,
        listener: Rc<Binding>,
        certs: Rc<[Arc<ServerTls>]>,
        stream: TcpStream,
        peer: SocketAddr,
        _permit: Permit,
        mut shutdown: watch::Receiver<bool>,
    ) {
        if *shutdown.borrow() || shutdown.has_changed().unwrap_or(true) {
            return;
        }
        let (stream, builder) = if listener.tls {
            let handshake = tokio::select! {
                _ = shutdown.changed() => return,
                result = timeout(self.handshake_timeout, tls::accept(stream, &certs)) => result,
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
            let builder = if h2 {
                self.builder.clone().http2_only()
            } else {
                self.builder.clone().http1_only()
            };
            (Io::tls(stream), builder)
        } else {
            let stream = match Io::plain(stream) {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!(%peer, %error, "failed to prepare HTTP socket");
                    return;
                }
            };
            (stream, self.builder.clone())
        };
        drop(certs);
        let service = service_fn(|request| {
            let server = self.clone();
            let listener = listener.clone();
            async move { server.service.call(&listener, request).await }
        });
        let connection = builder.serve_connection(stream, service);
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
    }
}
