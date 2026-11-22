//! maintain one QUIC endpoint per terminating HTTPS socket on each worker.
mod cid;
mod group;
mod h3;

use super::super::{
    super::ir::{Binding, ServerTls, hostname_matches},
    config::RuntimeConfig,
    core::{http::ProxyService, tcp::Admission},
    stopped,
};
use compio::{
    net::UdpSocket,
    quic::{
        Endpoint, EndpointConfig, ServerConfig, TransportConfig,
        crypto::rustls::QuicServerConfig,
    },
    runtime::{self, time::timeout_at},
    tls::rustls::{
        self,
        server::{ClientHello, ResolvesServerCert},
        sign::CertifiedKey,
    },
};
use futures::{StreamExt, stream::FuturesUnordered};
pub use group::Groups;
use std::{
    io,
    net::SocketAddr,
    rc::Rc,
    sync::{Arc, RwLock},
    time::Instant,
};
use tokio::sync::watch;

/// bind a standalone QUIC endpoint with caller-supplied TLS and transport settings.
pub fn listen(address: SocketAddr, config: ServerConfig) -> io::Result<Endpoint> {
    Endpoint::new(
        super::udp::listen(address)?,
        EndpointConfig::default(),
        Some(config),
        None,
    )
}

/// ordered certificate table shared by the listener and rustls handshakes.
#[derive(Debug, Default)]
struct Certificates(RwLock<Vec<(String, Arc<CertifiedKey>)>>);

impl Certificates {
    /// atomically replace the keys in the same specificity order used by TCP.
    fn update(&self, certs: &[Arc<ServerTls>]) {
        let keys = certs
            .iter()
            .map(|c| (c.hostname.clone(), c.certified_key()))
            .collect();
        *self.0.write().unwrap() = keys;
    }
}

impl ResolvesServerCert for Certificates {
    /// select the first matching key, including the fallback for clients without SNI.
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let host = hello.server_name().unwrap_or_default().to_ascii_lowercase();
        self.0
            .read()
            .unwrap()
            .iter()
            .find(|(pattern, _)| hostname_matches(pattern, &host))
            .map(|(_, key)| key.clone())
    }
}

/// control handle for a bound listener. dropping it starts graceful shutdown.
pub struct Listener {
    /// current gateway identity and advertised listener port.
    pub binding: Rc<Binding>,
    /// router shared with TCP and the owner of this listener's advertisement.
    service: Rc<ProxyService>,
    /// dynamically reloadable SNI resolver.
    certificates: Arc<Certificates>,
    /// closing this channel stops new handshakes and drains active connections.
    stop: watch::Sender<bool>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.service.set_http3(&self.binding, None);
    }
}

impl Listener {
    /// configure the TLS identity, QUIC version and transport limits before accepting.
    pub fn bind(
        socket: UdpSocket,
        service: Rc<ProxyService>,
        binding: Rc<Binding>,
        certs: &[Arc<ServerTls>],
        id: usize,
        runtime: &RuntimeConfig,
    ) -> io::Result<(Self, Endpoint)> {
        let certificates = Arc::new(Certificates::default());
        certificates.update(certs);
        let mut tls = rustls::ServerConfig::builder_with_protocol_versions(&[
            &rustls::version::TLS13,
        ])
        .with_no_client_auth()
        .with_cert_resolver(certificates.clone());
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let crypto = QuicServerConfig::try_from(tls).map_err(io::Error::other)?;
        let mut config = ServerConfig::with_crypto(Arc::new(crypto));
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(runtime.http2_max_concurrent_streams.into())
            .max_idle_timeout(Some(
                runtime.upstream_idle_timeout.try_into().map_err(io::Error::other)?,
            ))
            .datagram_receive_buffer_size(None);
        config.transport_config(Arc::new(transport));
        let mut endpoint = EndpointConfig::default();
        endpoint.supported_versions(vec![1]);
        if let Ok(worker) = u16::try_from(id) {
            endpoint.cid_generator(move || Box::new(cid::Generator(worker)));
        }
        let endpoint = Endpoint::new(socket, endpoint, Some(config), None)?;
        let (stop, _) = watch::channel(false);
        service.set_http3(
            &binding,
            Some(runtime.http3_advertised_port.unwrap_or(binding.port)),
        );
        Ok((Self { binding, service, certificates, stop }, endpoint))
    }

    /// swap only the certificate table when a snapshot changes.
    pub fn update(&self, certs: &[Arc<ServerTls>]) {
        self.certificates.update(certs);
    }

    /// drive admissions concurrently, then bound the lifetime of all remaining work.
    pub fn serve(
        &self,
        endpoint: Endpoint,
        admission: Arc<Admission>,
        config: Arc<RuntimeConfig>,
        reader: group::Reader,
    ) -> impl Future<Output = io::Result<()>> + use<> {
        let binding = self.binding.clone();
        let service = self.service.clone();
        let shutdown = self.stop.subscribe();
        async move {
            let stopping = stopped(shutdown.clone());
            tokio::pin!(stopping);
            let mut connections = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopping => break,
                    Some(()) = connections.next(), if !connections.is_empty() => {},
                    incoming = endpoint.wait_incoming() => {
                        let Some(incoming) = incoming else { break };
                        if let Some(permit) = admission.acquire() {
                            connections.push(h3::serve(incoming, &service, &binding,
                                &config, permit, shutdown.clone()));
                        } else {
                            incoming.refuse();
                        }
                    }
                }
            }
            endpoint.set_server_config(None);
            let deadline = Instant::now() + config.worker_drain_timeout;
            let _ = timeout_at(deadline, async {
                while connections.next().await.is_some() {}
            })
            .await;
            drop(connections);
            endpoint.close(0x100u32.into(), b"listener removed");
            let mut closing = Box::pin(async move {
                let result = endpoint.shutdown().await;
                drop(reader);
                result
            });
            match timeout_at(deadline, &mut closing).await {
                Ok(result) => result,
                Err(_) => {
                    // keep the UDP slot reserved until the driver actually releases it.
                    runtime::spawn(closing).detach();
                    Ok(())
                }
            }
        }
    }
}
