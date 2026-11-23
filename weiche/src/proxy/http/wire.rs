//! translate HTTP wire formats and produce local HTTP and gRPC responses.
use super::super::{super::ir::Protocol, admission::EndpointKey, up::Exchange};
use http::{
    HeaderMap, HeaderName, HeaderValue, Request, StatusCode, Uri, Version, header,
    request::Parts,
    uri::{Authority, PathAndQuery, Scheme},
};
use std::{io, net::SocketAddr};

/// errors mapped to local HTTP responses.
pub enum ProxyError {
    /// request URI cannot be represented for the selected hop.
    InvalidTarget,
    /// unsupported tunnel method or handshake.
    UnsupportedUpgrade,
    /// missing, ambiguous or malformed request authority.
    InvalidHost,
    /// no attached route matches the request.
    NoRoute,
    /// selected reference has no valid backend.
    InvalidBackend,
    /// backend has no available addresses.
    NoEndpoints,
    /// backend connection or request failed.
    Upstream,
    /// the request or backend attempt expired before headers.
    Timeout,
}

/// recognize native gRPC media types with an optional codec suffix and parameters.
pub fn is_grpc<B>(request: &Request<B>) -> bool {
    matches!(request.version(), Version::HTTP_2 | Version::HTTP_3)
        && request.method() == http::Method::POST
        && request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                let media = v.split(';').next().unwrap_or("").trim();
                let base = media.split_once('+').map_or(media, |(base, _)| base);
                base.eq_ignore_ascii_case("application/grpc")
            })
}

impl ProxyError {
    /// distinguish an expired dispatch from a failed backend transport.
    pub fn backend(address: SocketAddr, error: anyhow::Error) -> Self {
        if error
            .downcast_ref::<io::Error>()
            .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
        {
            Self::Timeout
        } else {
            tracing::warn!(%address, %error, "http backend request failed");
            Self::Upstream
        }
    }

    /// send local HTTP errors or trailers-only status for a native gRPC call.
    pub fn response(self, grpc: bool) -> Exchange {
        use StatusCode as S;
        let (status, text, grpc_code) = match self {
            Self::InvalidTarget => (S::BAD_REQUEST, "invalid request target\n", None),
            Self::UnsupportedUpgrade => {
                (S::NOT_IMPLEMENTED, "unsupported upgrade or CONNECT request\n", None)
            }
            Self::InvalidHost => (
                S::BAD_REQUEST,
                "a valid, consistent Host or :authority is required\n",
                None,
            ),
            Self::NoRoute => (S::NOT_FOUND, "no matching HTTPRoute\n", Some("12")),
            Self::InvalidBackend => (
                S::INTERNAL_SERVER_ERROR,
                "HTTPRoute has an invalid backend reference\n",
                Some("14"),
            ),
            Self::NoEndpoints => {
                (S::SERVICE_UNAVAILABLE, "Service has no ready endpoints\n", Some("14"))
            }
            Self::Upstream => (S::BAD_GATEWAY, "backend request failed\n", Some("14")),
            Self::Timeout => (S::GATEWAY_TIMEOUT, "request timed out\n", Some("4")),
        };
        if grpc && let Some(code) = grpc_code {
            let mut exchange = Exchange::local(S::OK, "");
            *exchange.response.version_mut() = Version::HTTP_2;
            let headers = exchange.response.headers_mut();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            );
            headers.insert("grpc-status", HeaderValue::from_static(code));
            return exchange;
        }
        let mut exchange = Exchange::local(status, text);
        exchange.response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        exchange
    }
}

/// retrieve an unambiguous singleton field.
pub fn single(headers: &HeaderMap, name: HeaderName) -> Option<&HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

/// parse the single Host field used by HTTP/1 or supplied by a route filter.
pub fn host_authority(headers: &HeaderMap) -> Result<Authority, ProxyError> {
    let value = single(headers, header::HOST).ok_or(ProxyError::InvalidHost)?;
    let authority =
        Authority::try_from(value.as_bytes()).map_err(|_| ProxyError::InvalidHost)?;
    let host = authority.host();
    let port = authority.as_str().strip_prefix(host).ok_or(ProxyError::InvalidHost)?;
    if host.is_empty()
        || authority.as_str().contains(['@', ','])
        || (!port.is_empty() && (!port.starts_with(':') || authority.port().is_none()))
    {
        return Err(ProxyError::InvalidHost);
    }
    Ok(authority)
}

/// restore the original authority when hop-by-hop fields or filters removed Host.
pub fn supply_host(
    headers: &mut HeaderMap,
    authority: &Authority,
) -> Result<(), ProxyError> {
    if let http::header::Entry::Vacant(host) = headers.entry(header::HOST) {
        host.insert(
            HeaderValue::from_str(authority.as_str())
                .map_err(|_| ProxyError::InvalidHost)?,
        );
    }
    Ok(())
}

/// expose :authority to header matches and reject conflicting downstream identities.
pub fn normalize_authority<B>(request: &mut Request<B>) -> Result<Authority, ProxyError> {
    if !request.headers().contains_key(header::HOST)
        && let Some(authority) = request.uri().authority()
    {
        let value = HeaderValue::from_str(authority.as_str())
            .map_err(|_| ProxyError::InvalidHost)?;
        request.headers_mut().insert(header::HOST, value);
    }
    let host = host_authority(request.headers())?;
    if request.uri().authority().is_some_and(|authority| authority != &host) {
        return Err(ProxyError::InvalidHost);
    }
    Ok(host)
}

/// encode the filtered authority and path for the selected upstream protocol.
pub fn prepare_request(
    request: &mut Parts,
    authority: Authority,
    endpoint: &EndpointKey,
    downstream_tls: bool,
    grpc: bool,
    trailers: bool,
    path: Option<PathAndQuery>,
) -> Result<(), ProxyError> {
    let headers = &mut request.headers;
    if trailers || grpc {
        headers.insert(header::TE, HeaderValue::from_static("trailers"));
    }
    let mut uri = std::mem::take(&mut request.uri).into_parts();
    uri.path_and_query = path.or(uri.path_and_query);
    uri.path_and_query
        .get_or_insert_with(|| PathAndQuery::from_static("/"));
    match endpoint.protocol {
        Protocol::Http1 => {
            // an HTTP/3 upload can carry data or trailers without declaring its length.
            // keep polling through FIN even after the advertised length has arrived.
            let streaming = request.version == Version::HTTP_3;
            request.version = Version::HTTP_11;
            supply_host(headers, &authority)?;
            uri.scheme = None;
            uri.authority = None;
            if trailers {
                headers.insert(header::CONNECTION, HeaderValue::from_static("TE"));
            }
            if streaming || headers.contains_key(header::TRAILER) {
                headers.remove(header::CONTENT_LENGTH);
                headers.insert(
                    header::TRANSFER_ENCODING,
                    HeaderValue::from_static("chunked"),
                );
            }
        }
        Protocol::Http2 => {
            request.version = Version::HTTP_2;
            // encode authority once; some HTTP/2 servers reject a duplicate Host field.
            headers.remove(header::HOST);
            if grpc || uri.scheme.is_none() {
                let tls = if grpc { endpoint.tls.is_some() } else { downstream_tls };
                uri.scheme = Some(if tls { Scheme::HTTPS } else { Scheme::HTTP });
            }
            uri.authority = Some(authority);
        }
    }
    request.uri = Uri::from_parts(uri).map_err(|_| ProxyError::InvalidTarget)?;
    Ok(())
}

/// visit comma-separated header tokens in wire order, skipping non-text field lines.
pub fn header_tokens(
    headers: &HeaderMap,
    name: HeaderName,
) -> impl Iterator<Item = &str> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
}

/// remove connection-specific fields and fields nominated by this hop.
pub fn strip_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = header_tokens(headers, header::CONNECTION)
        .filter_map(|name| name.parse::<HeaderName>().ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
