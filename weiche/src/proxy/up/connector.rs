//! upstream HTTP client handshakes and connections on the owning compio runtime.
use super::{
    super::{
        super::ir::Protocol,
        admission::EndpointKey,
        http::RequestBody,
        io::{Io, TlsIo},
    },
    deadline::TimedBody,
    http2::Upload,
    pool::ConnectionPool,
};
use anyhow::{Context, ensure};
use compio::{net::TcpStream, runtime, tls::rustls::pki_types::ServerName};
use cyper_core::{CompioExecutor, CompioTimer};
use futures::TryFutureExt;
use hyper::client::conn::{http1, http2};

/// request sender paired with its worker-local connection driver.
pub struct Connected<S = http1::SendRequest<TimedBody<RequestBody>>> {
    /// hyper handle for dispatching requests.
    pub sender: S,
    /// cancelling a lease also cancels its driver on the owning runtime.
    _driver: runtime::JoinHandle<()>,
}

impl ConnectionPool {
    /// perform an HTTP/1 handshake with upgrade support.
    pub async fn connect(&self, endpoint: &EndpointKey) -> anyhow::Result<Connected> {
        let (sender, connection) = http1::Builder::new()
            .max_buf_size(self.max_buffer)
            .handshake(open(endpoint).await?)
            .await?;
        let address = endpoint.address;
        let driver = connection.with_upgrades().unwrap_or_else(move |error| {
            tracing::debug!(%address, %error, "upstream connection ended");
        });
        Ok(Connected { sender, _driver: runtime::spawn(driver) })
    }

    /// perform an HTTP/2 handshake on the worker executor.
    pub async fn connect_http2(
        &self,
        endpoint: &EndpointKey,
    ) -> anyhow::Result<Connected<http2::SendRequest<Upload>>> {
        let (sender, connection) = http2::Builder::new(CompioExecutor)
            .timer(CompioTimer)
            .max_send_buf_size(self.max_buffer)
            .max_header_list_size(self.max_buffer as u32)
            .handshake(open(endpoint).await?)
            .await?;
        let address = endpoint.address;
        let driver = connection.unwrap_or_else(move |error| {
            tracing::debug!(%address, %error, "upstream HTTP/2 connection ended");
        });
        Ok(Connected { sender, _driver: runtime::spawn(driver) })
    }
}

/// open TCP and negotiate backend TLS when configured.
async fn open(endpoint: &EndpointKey) -> anyhow::Result<Io> {
    let stream = TcpStream::connect(endpoint.address).await?;
    let Some(tls) = &endpoint.tls else { return Ok(Io::plain(stream)?) };
    let name = ServerName::try_from(tls.hostname.clone())
        .context("invalid backend TLS hostname")?;
    let stream = TlsIo::connect(stream, name, tls.config()).await?;
    ensure!(
        endpoint.protocol != Protocol::Http2 || stream.negotiated_alpn() == Some(b"h2"),
        "backend did not negotiate h2"
    );
    Ok(Io::tls(stream))
}
