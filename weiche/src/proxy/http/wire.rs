//! translate HTTP wire formats and produce local HTTP and gRPC responses.
use super::super::{super::ir::Protocol, admission::EndpointKey, up::Upgrade};
use super::ResponseBody;
use bytes::Bytes;
use http::{
    HeaderMap, HeaderValue, Request, Response, StatusCode, Uri, Version, header,
    uri::Authority,
};

/// errors mapped to local HTTP responses.
pub enum ProxyError {
    /// local redirect with its validated Location.
    Redirect(
        /// status selected by the redirect filter.
        StatusCode,
        /// absolute URI for the Location header.
        HeaderValue,
    ),
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
    /// emit a trailers-only gRPC response when a call cannot be dispatched.
    pub fn for_request(self, grpc: bool) -> Response<ResponseBody> {
        if grpc {
            let code = match self {
                Self::NoRoute => "12",
                Self::InvalidBackend | Self::NoEndpoints | Self::Upstream => "14",
                Self::Timeout => "4",
                _ => return self.response(),
            };
            let mut response = Response::new(ResponseBody::full(Bytes::new()));
            *response.version_mut() = Version::HTTP_2;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            );
            response
                .headers_mut()
                .insert("grpc-status", HeaderValue::from_static(code));
            return response;
        }
        self.response()
    }

    /// format a local response with the corresponding HTTP status.
    pub fn response(self) -> Response<ResponseBody> {
        let (status, text) = match self {
            Self::Redirect(status, location) => {
                let mut response = Response::new(ResponseBody::full(Bytes::new()));
                *response.status_mut() = status;
                response.headers_mut().insert(header::LOCATION, location);
                return response;
            }
            Self::InvalidTarget => (StatusCode::BAD_REQUEST, "invalid request target\n"),
            Self::UnsupportedUpgrade => {
                (StatusCode::NOT_IMPLEMENTED, "unsupported upgrade or CONNECT request\n")
            }
            Self::InvalidHost => (
                StatusCode::BAD_REQUEST,
                "a valid, consistent Host or :authority is required\n",
            ),
            Self::NoRoute => (StatusCode::NOT_FOUND, "no matching HTTPRoute\n"),
            Self::InvalidBackend => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "HTTPRoute has an invalid backend reference\n",
            ),
            Self::NoEndpoints => {
                (StatusCode::SERVICE_UNAVAILABLE, "Service has no ready endpoints\n")
            }
            Self::Upstream => (StatusCode::BAD_GATEWAY, "backend request failed\n"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "request timed out\n"),
        };
        let mut response =
            Response::new(ResponseBody::full(Bytes::from_static(text.as_bytes())));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

/// parse the single Host field used by HTTP/1 or supplied by a route filter.
pub fn host_authority(headers: &HeaderMap) -> Result<Authority, ProxyError> {
    let mut hosts = headers.get_all(header::HOST).iter();
    let value = hosts.next().ok_or(ProxyError::InvalidHost)?;
    let authority =
        Authority::try_from(value.as_bytes()).map_err(|_| ProxyError::InvalidHost)?;
    let host = authority.host();
    let port = authority.as_str().strip_prefix(host).ok_or(ProxyError::InvalidHost)?;
    if hosts.next().is_some()
        || host.is_empty()
        || authority.as_str().contains(['@', ','])
        || (!port.is_empty() && (!port.starts_with(':') || authority.port().is_none()))
    {
        return Err(ProxyError::InvalidHost);
    }
    Ok(authority)
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

/// take the downstream upgrade before its handshake fields are stripped.
pub fn take_upgrade<B>(
    request: &mut Request<B>,
    protocol: Protocol,
) -> Result<Option<Upgrade>, ProxyError> {
    if !request.headers().contains_key(header::UPGRADE) {
        return Ok(None);
    }
    if protocol != Protocol::Http1 {
        return Err(ProxyError::UnsupportedUpgrade);
    }
    Upgrade::request(request)
        .map(Some)
        .ok_or(ProxyError::UnsupportedUpgrade)
}

/// encode the filtered authority and path for the selected upstream protocol.
pub fn prepare_request<B>(
    request: &mut Request<B>,
    authority: Authority,
    endpoint: &EndpointKey,
    downstream_tls: bool,
    grpc: bool,
    trailers: bool,
    path: Option<http::uri::PathAndQuery>,
) -> Result<(), ProxyError> {
    if trailers || grpc {
        request
            .headers_mut()
            .insert(header::TE, HeaderValue::from_static("trailers"));
    }
    let mut uri = request.uri().clone().into_parts();
    if path.is_some() {
        uri.path_and_query = path;
    }
    uri.path_and_query
        .get_or_insert_with(|| http::uri::PathAndQuery::from_static("/"));
    match endpoint.protocol {
        Protocol::Http1 => {
            // an HTTP/3 upload can carry data or trailers without declaring its length.
            // keep polling through FIN even after the advertised length has arrived.
            let streaming = request.version() == Version::HTTP_3;
            *request.version_mut() = Version::HTTP_11;
            if !request.headers().contains_key(header::HOST) {
                request.headers_mut().insert(
                    header::HOST,
                    HeaderValue::from_str(authority.as_str())
                        .map_err(|_| ProxyError::InvalidHost)?,
                );
            }
            uri.scheme = None;
            uri.authority = None;
            if trailers {
                request
                    .headers_mut()
                    .insert(header::CONNECTION, HeaderValue::from_static("TE"));
            }
            if streaming || request.headers().contains_key(header::TRAILER) {
                request.headers_mut().remove(header::CONTENT_LENGTH);
                request.headers_mut().insert(
                    header::TRANSFER_ENCODING,
                    HeaderValue::from_static("chunked"),
                );
            }
        }
        Protocol::Http2 => {
            *request.version_mut() = Version::HTTP_2;
            // encode authority once; some HTTP/2 servers reject a duplicate Host field.
            request.headers_mut().remove(header::HOST);
            if grpc || uri.scheme.is_none() {
                let tls = if grpc { endpoint.tls.is_some() } else { downstream_tls };
                uri.scheme = Some(if tls {
                    http::uri::Scheme::HTTPS
                } else {
                    http::uri::Scheme::HTTP
                });
            }
            uri.authority = Some(authority);
        }
    }
    *request.uri_mut() = Uri::from_parts(uri).map_err(|_| ProxyError::InvalidTarget)?;
    Ok(())
}

/// remove connection-specific fields and fields nominated by this hop.
pub fn strip_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|s| s.trim().parse::<header::HeaderName>().ok())
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
