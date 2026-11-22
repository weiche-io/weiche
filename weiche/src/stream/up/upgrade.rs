//! validate HTTP/1 upgrade negotiation and relay the switched protocol locally.
use super::pool::Lease;
use http::{HeaderMap, HeaderValue, Request, Response, Version, header};
use hyper::{
    body::Incoming,
    upgrade::{OnUpgrade, on},
};
use hyper_util::rt::TokioIo;

/// the offered protocols and the client's pending HTTP/1 transport.
pub struct Upgrade {
    /// downstream transport after the 101 has been flushed.
    downstream: OnUpgrade,
    /// validated protocol list as received on the wire.
    protocols: Vec<HeaderValue>,
}

/// check the connection tokens and the protocol-name[/version] grammar.
fn protocols(headers: &HeaderMap) -> Option<Vec<HeaderValue>> {
    let connection = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|s| s.trim().eq_ignore_ascii_case("upgrade"));
    let values: Vec<_> = headers.get_all(header::UPGRADE).iter().cloned().collect();
    if !connection || values.is_empty() {
        return None;
    }
    for value in &values {
        for protocol in value.to_str().ok()?.split(',') {
            let mut parts = protocol.trim().split('/');
            let token = |s: &str| header::HeaderName::from_bytes(s.as_bytes()).is_ok();
            if !parts.next().is_some_and(token)
                || parts.next().is_some_and(|s| !token(s))
                || parts.next().is_some()
            {
                return None;
            }
        }
    }
    Some(values)
}

impl Upgrade {
    /// capture a valid client offer before the request extensions reach the upstream.
    pub fn request(request: &mut Request<Incoming>) -> Option<Self> {
        if request.version() != Version::HTTP_11 {
            return None;
        }
        let protocols = protocols(request.headers())?;
        Some(Self { downstream: on(request), protocols })
    }

    /// restore only the handshake fields after stripping hop-by-hop headers.
    pub fn restore(&self, headers: &mut HeaderMap) {
        restore(headers, &self.protocols);
    }

    /// check that every protocol selected by the backend was actually offered.
    pub fn accept(
        self,
        response: &mut Response<Incoming>,
        lease: Lease,
    ) -> anyhow::Result<Tunnel> {
        let selected = protocols(response.headers())
            .ok_or_else(|| anyhow::anyhow!("invalid backend upgrade response"))?;
        for p in selected.iter().flat_map(tokens) {
            anyhow::ensure!(
                self.protocols
                    .iter()
                    .flat_map(tokens)
                    .any(|offer| same_protocol(offer, p)),
                "backend selected an unoffered protocol"
            );
        }
        Ok(Tunnel {
            downstream: self.downstream,
            upstream: on(response),
            protocols: selected,
            _lease: lease,
        })
    }
}

/// protocol names ignore case while protocol versions are opaque tokens.
fn same_protocol(offer: &str, selected: &str) -> bool {
    let (offered_name, offered_version) = offer.split_once('/').unwrap_or((offer, ""));
    let (name, version) = selected.split_once('/').unwrap_or((selected, ""));
    offered_name.eq_ignore_ascii_case(name) && offered_version == version
}

/// split a previously validated list into offered or selected protocols.
fn tokens(value: &HeaderValue) -> impl Iterator<Item = &str> {
    value
        .to_str()
        .expect("validated upgrade token")
        .split(',')
        .map(str::trim)
}

/// install a canonical Connection header and the original protocol list.
fn restore(headers: &mut HeaderMap, protocols: &[HeaderValue]) {
    let connection =
        if headers.contains_key(header::TE) { "upgrade, TE" } else { "upgrade" };
    headers.insert(header::CONNECTION, HeaderValue::from_static(connection));
    headers.remove(header::UPGRADE);
    for value in protocols {
        headers.append(header::UPGRADE, value.clone());
    }
}

/// pending bidirectional tunnel holding the dedicated upstream connection slot.
pub struct Tunnel {
    /// client's upgraded transport, including already-buffered bytes.
    downstream: OnUpgrade,
    /// backend's upgraded transport, including already-buffered bytes.
    upstream: OnUpgrade,
    /// protocols confirmed by the upstream.
    protocols: Vec<HeaderValue>,
    /// keep the handshake driver and capacity alive until the tunnel closes.
    _lease: Lease,
}

impl Tunnel {
    /// preserve the selected protocol on the switching response.
    pub fn restore(&self, headers: &mut HeaderMap) {
        restore(headers, &self.protocols);
    }

    /// copy both directions with half-closes using poll-trait adapters on the compio worker.
    pub async fn relay(self) -> anyhow::Result<()> {
        let (down, up) = futures::try_join!(self.downstream, self.upstream)?;
        tokio::io::copy_bidirectional(&mut TokioIo::new(down), &mut TokioIo::new(up))
            .await?;
        Ok(())
    }
}
