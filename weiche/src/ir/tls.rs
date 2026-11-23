//! prepared TLS settings shared by workers and compared by certificate content.
use super::Protocol;
use anyhow::ensure;
use compio::tls::rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, DnsName, PrivateKeyDer, pem::PemObject},
    sign::{CertifiedKey, SingleCertAndKey},
};
use ring::digest::{self, SHA256};
use serde::{Deserialize, Serialize, Serializer};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// identify equivalent configurations across independent watch compilations.
struct Config<T, S> {
    /// validated runtime configuration.
    inner: T,
    /// original inputs used to reconstruct the configuration on another worker.
    source: S,
    /// immutable identity for comparisons across snapshots.
    fingerprint: [u8; 32],
}

impl<T, S> Config<T, S> {
    /// fingerprint each length-delimited input while retaining the prepared state.
    fn new(
        inner: T,
        source: S,
        inputs: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Self {
        let mut digest = digest::Context::new(&SHA256);
        for input in inputs {
            let bytes = input.as_ref();
            digest.update(&bytes.len().to_be_bytes());
            digest.update(bytes);
        }
        Self {
            inner,
            source,
            fingerprint: digest.finish().as_ref().try_into().unwrap(),
        }
    }
}

impl<T, S> fmt::Debug for Config<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").finish_non_exhaustive()
    }
}

impl<T, S> PartialEq for Config<T, S> {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
    }
}
impl<T, S> Eq for Config<T, S> {}
impl<T, S> Hash for Config<T, S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

impl<T, S: Serialize> Serialize for Config<T, S> {
    fn serialize<A: Serializer>(&self, serializer: A) -> Result<A::Ok, A::Error> {
        self.source.serialize(serializer)
    }
}

/// rebuild rustls state from the authenticated snapshot's original inputs.
#[derive(Deserialize)]
struct Material<S> {
    /// listener pattern or backend verification name.
    hostname: String,
    /// original serialized TLS inputs.
    config: S,
}

/// serialized certificate chain and private key.
type Identity = (Vec<u8>, Vec<u8>);
/// serialized trust anchors and upstream ALPN selection.
type Trust = (Vec<Vec<u8>>, Protocol);

impl TryFrom<Material<Identity>> for ServerTls {
    type Error = anyhow::Error;
    fn try_from(material: Material<Identity>) -> anyhow::Result<Self> {
        Self::from_pem(&material.hostname, &material.config.0, &material.config.1)
    }
}

impl TryFrom<Material<Trust>> for BackendTls {
    type Error = anyhow::Error;
    fn try_from(material: Material<Trust>) -> anyhow::Result<Self> {
        let (roots, protocol) = material.config;
        let roots: Vec<_> = roots.into_iter().map(CertificateDer::from).collect();
        Self::new(DnsName::try_from(material.hostname)?, &roots, protocol)
    }
}

/// one terminating listener's certificate chain, signing key and SNI pattern.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Material<Identity>")]
pub struct ServerTls {
    /// lowercase SNI pattern or an empty catch-all.
    pub hostname: String,
    /// parsed identity shared by the TCP and QUIC handshake paths.
    config: Config<(Arc<ServerConfig>, Arc<CertifiedKey>), Identity>,
}

impl ServerTls {
    /// select the listener that accepts the ClientHello, including a catch-all without SNI.
    pub fn for_sni<'a>(certs: &'a [Arc<Self>], sni: Option<&str>) -> Option<&'a Self> {
        let host = sni.unwrap_or_default().to_ascii_lowercase();
        certs
            .iter()
            .map(Arc::as_ref)
            .find(|tls| super::hostname_matches(&tls.hostname, &host))
    }

    /// validate the leaf certificate and its key before publishing the listener.
    pub fn from_pem(hostname: &str, chain: &[u8], key: &[u8]) -> anyhow::Result<Self> {
        let certs =
            CertificateDer::pem_slice_iter(chain).collect::<Result<Vec<_>, _>>()?;
        let parsed_key = PrivateKeyDer::from_pem_slice(key)?;
        let builder = ServerConfig::builder();
        let certified = Arc::new(CertifiedKey::from_der(
            certs,
            parsed_key,
            builder.crypto_provider(),
        )?);
        let mut config = builder
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(SingleCertAndKey::from(certified.clone())));
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            hostname: hostname.to_ascii_lowercase(),
            config: Config::new(
                (Arc::new(config), certified),
                (chain.to_vec(), key.to_vec()),
                [chain, key],
            ),
        })
    }

    /// clone the prepared configuration for one handshake.
    pub fn config(&self) -> Arc<ServerConfig> {
        self.config.inner.0.clone()
    }

    /// share the validated certificate chain and signing key with QUIC.
    pub fn certified_key(&self) -> Arc<CertifiedKey> {
        self.config.inner.1.clone()
    }
}

/// authenticated backend identity and trust anchors for a pooled transport.
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Material<Trust>")]
pub struct BackendTls {
    /// verification name used in backend TLS handshakes.
    pub hostname: String,
    /// prepared client settings with their serialized trust inputs.
    config: Config<Arc<ClientConfig>, Trust>,
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
        // an empty trust store would accept the policy but fail every backend handshake.
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
            config: Config::new(
                Arc::new(config),
                (certs.iter().map(|c| c.as_ref().to_vec()).collect(), protocol),
                certs.iter().map(AsRef::as_ref).chain([alpn]),
            ),
        })
    }

    /// clone the verifier and its session cache for one handshake.
    pub fn config(&self) -> Arc<ClientConfig> {
        self.config.inner.clone()
    }
}
