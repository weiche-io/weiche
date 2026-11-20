//! serve downstream HTTP/1 and HTTP/2 connections on their owning runtime.
use super::super::{
    super::rtconf::RuntimeConfig,
    core::{http::ProxyService, tcp::Permit},
    up::io,
};
use compio::{net::TcpStream, runtime::time::timeout};
use cyper_core::{CompioExecutor, CompioTimer};
use hyper::service::service_fn;
use hyper_util::server::conn::auto::Builder;
use std::{net::SocketAddr, rc::Rc, time::Duration};
use tokio::sync::watch;

pub(super) struct Server {
    pub service: ProxyService,
    builder: Builder<CompioExecutor>,
    drain_timeout: Duration,
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
        }
    }

    pub async fn serve(
        self: Rc<Self>,
        listener: SocketAddr,
        stream: TcpStream,
        peer: SocketAddr,
        _permit: Permit,
        mut shutdown: watch::Receiver<bool>,
    ) {
        if *shutdown.borrow() {
            return;
        }
        let stream = match stream.set_nodelay(true).and_then(|()| io(stream)) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(%peer, %error, "failed to prepare HTTP socket");
                return;
            }
        };
        let service = service_fn(|request| {
            let server = self.clone();
            async move { server.service.call(listener, request).await }
        });
        let connection = self.builder.serve_connection(stream, service);
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
