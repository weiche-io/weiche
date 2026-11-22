//! upstream HTTP client handshakes and connections on the owning compio runtime.
use super::{
    super::{core::EndpointKey, io::Io},
    tls,
};
use bytes::Bytes;
use compio::{net::TcpStream, runtime};
use cyper_core::{CompioExecutor, CompioTimer};
use hyper::{
    body::{Body, Incoming},
    client::conn::{http1, http2},
};

pub struct Connected<S = http1::SendRequest<Incoming>> {
    pub sender: S,
    // cancelling a lease also cancels its driver on the owning runtime.
    _driver: runtime::JoinHandle<()>,
}

pub struct Connector {
    pub max_buffer: usize,
}

impl Connector {
    pub async fn connect(&self, endpoint: &EndpointKey) -> anyhow::Result<Connected> {
        let (sender, connection) = http1::Builder::new()
            .max_buf_size(self.max_buffer)
            .handshake(self.open(endpoint).await?)
            .await?;
        let address = endpoint.address;
        let driver = runtime::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%address, %error, "upstream connection ended");
            }
        });
        Ok(Connected { sender, _driver: driver })
    }

    pub async fn connect_http2<B>(
        &self,
        endpoint: &EndpointKey,
    ) -> anyhow::Result<Connected<http2::SendRequest<B>>>
    where
        B: Body<Data = Bytes, Error = hyper::Error> + Unpin + 'static,
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

    async fn open(&self, endpoint: &EndpointKey) -> anyhow::Result<Io> {
        let stream = TcpStream::connect(endpoint.address).await?;
        match &endpoint.tls {
            Some(settings) => tls::connect(stream, settings, endpoint.protocol).await,
            None => Ok(Io::plain(stream)?),
        }
    }
}
