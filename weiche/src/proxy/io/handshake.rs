//! keep the ClientHello and any trailing records on the socket used for the handshake.
use super::{
    tls::{TlsIo, tls_error},
    transport::Transport,
};
use compio::{
    net::TcpStream,
    tls::rustls::{
        ClientConfig, ClientConnection, Error, ServerConfig,
        pki_types::ServerName,
        server::{Accepted, AcceptedAlert, Acceptor, ClientHello},
    },
};
use std::{future::poll_fn, io, sync::Arc};

/// send a rustls alert before reporting the original handshake failure.
async fn reject(
    transport: &mut Transport,
    (error, mut alert): (Error, AcceptedAlert),
) -> io::Error {
    let _ =
        poll_fn(|cx| transport.poll_send(cx, false, |buf| alert.write_all(buf))).await;
    tls_error(error)
}

/// parsed ClientHello retained until SNI selects a listener certificate.
pub struct AcceptedTls {
    /// parsed client hello awaiting its server configuration.
    accepted: Accepted,
    /// original socket with unread records from the first flight.
    transport: Transport,
}

impl AcceptedTls {
    /// read a ClientHello over io_uring before choosing a server configuration.
    pub async fn accept(stream: TcpStream) -> io::Result<Self> {
        let mut transport = Transport::new(stream)?;
        let mut acceptor = Acceptor::default();
        loop {
            poll_fn(|cx| transport.poll_input(cx)).await?;
            if acceptor.read_tls(&mut transport.input)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            match acceptor.accept() {
                Ok(Some(accepted)) => return Ok(Self { accepted, transport }),
                Ok(None) => {}
                Err(error) => return Err(reject(&mut transport, error).await),
            }
        }
    }

    /// inspect offered SNI and ALPN parameters from the ClientHello.
    pub fn client_hello(&self) -> ClientHello<'_> {
        self.accepted.client_hello()
    }

    /// complete the TLS handshake with the selected listener configuration.
    pub async fn into_stream(mut self, config: Arc<ServerConfig>) -> io::Result<TlsIo> {
        let conn = match self.accepted.into_connection(config) {
            Ok(conn) => conn,
            Err(error) => return Err(reject(&mut self.transport, error).await),
        };
        TlsIo::handshake(self.transport, conn.into()).await
    }
}

impl TlsIo {
    /// complete a client TLS handshake for one upstream backend connection.
    pub async fn connect(
        stream: TcpStream,
        name: ServerName<'static>,
        config: Arc<ClientConfig>,
    ) -> io::Result<Self> {
        let transport = Transport::new(stream)?;
        let conn = ClientConnection::new(config, name).map_err(tls_error)?;
        Self::handshake(transport, conn.into()).await
    }
}
