//! translate HTTP wire formats and produce local HTTP and gRPC responses.
use super::Body;
use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, Version, header};
use http_body_util::{BodyExt, Full};

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
    pub fn for_request(self, grpc: bool) -> Response<Body> {
        if grpc {
            let code = match self {
                Self::NoRoute => "12",
                Self::InvalidBackend | Self::NoEndpoints | Self::Upstream => "14",
                Self::Timeout => "4",
                _ => return self.response(),
            };
            let mut response = Response::new(full_body(Bytes::new()));
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
    pub fn response(self) -> Response<Body> {
        let (status, text) = match self {
            Self::Redirect(status, location) => {
                let mut response = Response::new(full_body(Bytes::new()));
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
        let mut response = Response::new(full_body(Bytes::from_static(text.as_bytes())));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

/// build a complete local response without involving an upstream stream.
pub fn full_body(bytes: Bytes) -> Body {
    Box::pin(Full::new(bytes).map_err(|never| -> std::io::Error { never }))
}
