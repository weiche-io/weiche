//! maintain one QUIC endpoint per terminating HTTPS socket on each worker.
//! compio drives UDP I/O, packet dispatch and per-connection timers for quinn-proto.
use super::super::super::log;
use super::super::{
    super::ir::{Binding, ServerTls},
    admission::Permit,
    http::ProxyService,
    stopped,
    worker::{HttpListener, Task, Worker},
};
use super::{cid, group, h3};
use compio::{
    quic::{
        Connecting, Endpoint, EndpointConfig, ServerConfig, TransportConfig,
        crypto::rustls::QuicServerConfig,
    },
    runtime::{
        self,
        time::{timeout, timeout_at},
    },
    tls::rustls::{
        self,
        server::{ClientHello, ResolvesServerCert},
        sign::CertifiedKey,
    },
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{io, net::SocketAddr, rc::Rc, sync::Arc, time::Instant};
use tokio::sync::watch;

/// immutable listener table used to resolve both the certificate and the HTTP/3 listener.
#[derive(Debug)]
struct Certificates(Vec<Arc<ServerTls>>);

impl Certificates {
    /// build the TLS configuration and retain its resolver for connections using it.
    fn configure(
        certs: &[Arc<ServerTls>],
    ) -> io::Result<(Arc<Self>, Arc<QuicServerConfig>)> {
        let keys = Arc::new(Self(certs.to_vec()));
        let mut tls = rustls::ServerConfig::builder_with_protocol_versions(&[
            &rustls::version::TLS13,
        ])
        .with_no_client_auth()
        .with_cert_resolver(keys.clone());
        tls.alpn_protocols = vec![b"h3".to_vec()];
        Ok((keys, Arc::new(QuicServerConfig::try_from(tls).map_err(io::Error::other)?)))
    }
}

impl ResolvesServerCert for Certificates {
    /// select the first matching key, including the fallback for clients without SNI.
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(ServerTls::for_sni(&self.0, hello.server_name())?.certified_key())
    }
}

/// control handle for a bound listener. dropping it starts graceful shutdown.
pub struct Listener {
    /// current gateway identity and advertised listener port.
    pub binding: Rc<Binding>,
    /// router shared with TCP and the owner of this listener's advertisement.
    service: Rc<ProxyService>,
    /// closing this channel stops new handshakes and drains active connections.
    _stop: watch::Sender<bool>,
}

impl Drop for Listener {
    /// withdraw the HTTP/3 advertisement when this endpoint is retired.
    fn drop(&mut self) {
        self.service.set_http3(&self.binding, None);
    }
}

impl Listener {
    /// configure the TLS identity, QUIC version and transport limits before accepting.
    pub fn bind(
        worker: &Rc<Worker>,
        address: SocketAddr,
        http: &HttpListener,
    ) -> io::Result<(Self, Task)> {
        let quic = &worker.runtime.quic;
        let (socket, reader) =
            worker.group.bind_quic(address, worker.id, quic.buffers)?;
        let mut updates = http.certs.subscribe();
        let (keys, crypto) = Certificates::configure(&updates.borrow_and_update())?;
        let binding = http.binding.clone();
        let mut config = ServerConfig::with_crypto(crypto);
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(quic.max_concurrent_bidi_streams.into())
            .max_idle_timeout(Some(
                quic.idle_timeout.try_into().map_err(io::Error::other)?,
            ))
            .datagram_receive_buffer_size(None);
        config.transport_config(Arc::new(transport));
        let mut endpoint = EndpointConfig::default();
        endpoint.supported_versions(vec![1]);
        if let Ok(worker) = u16::try_from(worker.id) {
            endpoint.cid_generator(move || Box::new(cid::Generator(worker)));
        }
        let endpoint = Endpoint::new(socket, endpoint, Some(config.clone()), None)?;
        let (stop, shutdown) = watch::channel(false);
        let service = worker.server.service.clone();
        service.set_http3(
            &binding,
            Some(quic.advertised_port.map_or(binding.port, |port| port.get())),
        );
        let incoming = Acceptor { endpoint, config, keys, updates };
        let task = serve(incoming, worker.clone(), binding.clone(), shutdown, reader);
        Ok((Self { binding, service, _stop: stop }, task.boxed_local()))
    }
}

/// accept new connections with a consistent TLS configuration across certificate reloads.
struct Acceptor {
    /// receives incoming QUIC connections.
    endpoint: Endpoint,
    /// transport and TLS settings for the next handshake.
    config: ServerConfig,
    /// certificate resolver installed in that TLS configuration.
    keys: Arc<Certificates>,
    /// follows the same certificate rotations as HTTP over TCP.
    updates: watch::Receiver<Rc<[Arc<ServerTls>]>>,
}

/// drive admissions concurrently, then bound the lifetime of all remaining work.
async fn serve(
    acceptor: Acceptor,
    worker: Rc<Worker>,
    binding: Rc<Binding>,
    shutdown: watch::Receiver<bool>,
    reader: group::Reader,
) -> io::Result<()> {
    let Acceptor { endpoint, mut config, mut keys, mut updates } = acceptor;
    let stopping = stopped(shutdown.clone());
    tokio::pin!(stopping);
    let mut connections = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut stopping => break,
            changed = updates.changed() => {
                if changed.is_err() { break; }
                let (next, crypto) = Certificates::configure(&updates.borrow_and_update())?;
                keys = next;
                config.crypto = crypto;
                endpoint.set_server_config(Some(config.clone()));
            }
            Some(()) = connections.next(), if !connections.is_empty() => {},
            incoming = endpoint.wait_incoming() => {
                let Some(incoming) = incoming else { break };
                if let Some(permit) = worker.admission.acquire() {
                    // the first packet may predate a reload. pin the resolver used by this handshake.
                    match incoming.accept_with(config.clone()) {
                        Ok(connecting) => connections.push(connection(connecting, keys.clone(), &worker, &binding, permit, shutdown.clone())),
                        Err(error) => log!(debug, "quic accept failed", error = error.to_string()),
                    }
                } else {
                    incoming.refuse();
                }
            }
        }
    }
    endpoint.set_server_config(None);
    let deadline = Instant::now() + worker.runtime.worker_drain_timeout;
    let _ =
        timeout_at(deadline, async { while connections.next().await.is_some() {} }).await;
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

/// hold admission through QUIC negotiation, HTTP/3 setup and the session's drain.
async fn connection(
    connecting: Connecting,
    keys: Arc<Certificates>,
    worker: &Worker,
    binding: &Binding,
    _permit: Permit,
    shutdown: watch::Receiver<bool>,
) {
    let config = &worker.runtime;
    let mut stopping = std::pin::pin!(stopped(shutdown));
    let handshake = async {
        let mut transport = connecting.await?;
        let hello = transport.handshake_data()?;
        // a resumed connection must still select a listener from its handshake configuration.
        let selected = ServerTls::for_sni(&keys.0, hello.server_name.as_deref())
            .ok_or_else(|| io::Error::other("SNI does not match a listener hostname"))?
            .hostname
            .clone();
        drop(keys);
        Ok::<_, anyhow::Error>((
            h3::Session::new(transport, config.http_max_buffer).await?,
            selected,
        ))
    };
    let (session, selected) = tokio::select! {
        biased;
        _ = &mut stopping => return,
        result = timeout(config.tls_handshake_timeout, handshake) => match result {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => { log!(debug, "quic handshake failed", error = error.to_string()); return; }
            Err(_) => { log!(debug, "quic handshake timed out"); return; }
        }
    };
    session
        .serve(
            &worker.server.service,
            binding,
            &selected,
            stopping,
            config.connection_drain_timeout,
        )
        .await;
}
