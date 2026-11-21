//! prepared TLS settings shared by workers and compared by certificate content.
use super::Protocol;
use anyhow::ensure;
use compio::tls::rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, DnsName, PrivateKeyDer, pem::PemObject},
};
use ring::digest::{self, SHA256};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// identify equivalent configurations across independent watch compilations.
struct Config<T> {
    inner: Arc<T>,
    fingerprint: [u8; 32],
}

impl<T> Config<T> {
    fn new(inner: T, inputs: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        let mut digest = digest::Context::new(&SHA256);
        for input in inputs {
            let bytes = input.as_ref();
            digest.update(&bytes.len().to_be_bytes());
            digest.update(bytes);
        }
        Self {
            inner: Arc::new(inner),
            fingerprint: digest.finish().as_ref().try_into().unwrap(),
        }
    }
}

impl<T> fmt::Debug for Config<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").finish_non_exhaustive()
    }
}

impl<T> PartialEq for Config<T> {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
    }
}
impl<T> Eq for Config<T> {}
impl<T> Hash for Config<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

/// one terminating listener's certificate chain, signing key and SNI pattern.
#[derive(Debug, PartialEq, Eq)]
pub struct ServerTls {
    pub hostname: String,
    config: Config<ServerConfig>,
}

impl ServerTls {
    /// validate the leaf certificate and its key before publishing the listener.
    pub fn from_pem(hostname: &str, chain: &[u8], key: &[u8]) -> anyhow::Result<Self> {
        let certs =
            CertificateDer::pem_slice_iter(chain).collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_slice(key)?;
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            hostname: hostname.to_owned(),
            config: Config::new(config, [chain]),
        })
    }

    /// clone the prepared configuration for one handshake.
    pub fn config(&self) -> Arc<ServerConfig> {
        self.config.inner.clone()
    }
}

/// authenticated backend identity and trust anchors for a pooled transport.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BackendTls {
    pub hostname: String,
    config: Config<ClientConfig>,
}

impl BackendTls {
    /// build a verifier for the supplied roots and advertise the backend's HTTP protocol.
    pub fn new(
        hostname: DnsName<'_>,
        certs: &[CertificateDer<'_>],
        protocol: Protocol,
    ) -> anyhow::Result<Self> {
        let mut roots = RootCertStore::empty();
        for cert in certs {
            roots.add(cert.clone().into_owned())?;
        }
        ensure!(!roots.is_empty(), "CA bundle is empty");
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let alpn = match protocol {
            Protocol::Http1 => &b"http/1.1"[..],
            Protocol::Http2 => b"h2",
        };
        config.alpn_protocols = vec![alpn.to_vec()];
        Ok(Self {
            hostname: hostname.as_ref().to_owned(),
            config: Config::new(config, certs.iter().map(AsRef::as_ref).chain([alpn])),
        })
    }

    /// clone the verifier and its session cache for one handshake.
    pub fn config(&self) -> Arc<ClientConfig> {
        self.config.inner.clone()
    }
}
