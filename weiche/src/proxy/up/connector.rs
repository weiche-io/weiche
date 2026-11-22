//! upstream HTTP client handshakes and connections on the owning compio runtime.
use super::{
    super::{
        super::ir::Protocol,
        admission::EndpointKey,
        http::RequestBody,
        io::{Io, TlsIo},
    },
    deadline::TimedBody,
};
use anyhow::{Context, ensure};
use bytes::Bytes;
use compio::{net::TcpStream, runtime, tls::rustls::pki_types::ServerName};
use cyper_core::{CompioExecutor, CompioTimer};
use hyper::{
    body::Body as HttpBody,
    client::conn::{http1, http2},
};

/// request sender paired with its worker-local connection driver.
pub struct Connected<S = http1::SendRequest<TimedBody<RequestBody>>> {
    /// hyper handle for dispatching requests.
    pub sender: S,
    /// cancelling a lease also cancels its driver on the owning runtime.
    _driver: runtime::JoinHandle<()>,
}

/// open and configure the HTTP transport for an endpoint.
pub struct Connector {
    /// maximum HTTP connection buffer size.
    pub max_buffer: usize,
}

impl Connector {
    /// perform an HTTP/1 handshake with upgrade support.
    pub async fn connect(&self, endpoint: &EndpointKey) -> anyhow::Result<Connected> {
        let (sender, connection) = http1::Builder::new()
            .max_buf_size(self.max_buffer)
            .handshake(self.open(endpoint).await?)
            .await?;
        let address = endpoint.address;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.with_upgrades().await {
                tracing::debug!(%address, %error, "upstream connection ended");
            }
        });
        Ok(Connected { sender, _driver: driver })
    }

    /// perform an HTTP/2 handshake on the worker executor.
    pub async fn connect_http2<B>(
        &self,
        endpoint: &EndpointKey,
    ) -> anyhow::Result<Connected<http2::SendRequest<B>>>
    where
        B: HttpBody<Data = Bytes, Error = std::io::Error> + Unpin + 'static,
    {
        let (sender, connection) = http2::Builder::new(CompioExecutor)
            .timer(CompioTimer)
            .max_send_buf_size(self.max_buffer)
            .max_header_list_size(self.max_buffer as u32)
            .handshake(self.open(endpoint).await?)
            .await?;
        let address = endpoint.address;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream HTTP/2 connection ended");
            }
        });
        Ok(Connected { sender, _driver: driver })
    }

    /// open TCP and negotiate backend TLS when configured.
    async fn open(&self, endpoint: &EndpointKey) -> anyhow::Result<Io> {
        let stream = TcpStream::connect(endpoint.address).await?;
        let Some(tls) = &endpoint.tls else { return Ok(Io::plain(stream)?) };
        let name = ServerName::try_from(tls.hostname.clone())
            .context("invalid backend TLS hostname")?;
        let stream = TlsIo::connect(stream, name, tls.config()).await?;
        ensure!(
            endpoint.protocol != Protocol::Http2
                || stream.negotiated_alpn() == Some(b"h2"),
            "backend did not negotiate h2"
        );
        Ok(Io::tls(stream))
    }
}
