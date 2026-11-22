//! apply validated HTTPRoute filters without decoding the request path or query.
use super::super::super::super::ir::{PathModifier, Redirect};
use super::{
    Binding, Filters, HeaderChange, ProxyError, host_authority, strip_hop_headers,
};
use http::{
    HeaderMap, HeaderValue, Uri, header,
    uri::{Authority, PathAndQuery},
};
use std::net::SocketAddr;

/// apply set, add and remove in the order supplied by the controller.
pub fn modify(headers: &mut HeaderMap, changes: &[HeaderChange]) {
    for change in changes {
        match change {
            HeaderChange::Remove(k) => {
                headers.remove(k);
            }
            HeaderChange::Add(k, v) => {
                headers.append(k, v.clone());
            }
            HeaderChange::Set(k, v) => {
                headers.insert(k, v.clone());
            }
        }
    }
}

/// detect the only transfer coding that can pass through HTTP/2.
fn supports_trailers(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::TE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("trailers"))
}

/// preserve the original authority and TE semantics while applying request filters.
pub fn request_headers(
    headers: &mut HeaderMap,
    peer: SocketAddr,
    original: &Authority,
    tls: bool,
    rule: &Filters,
    backend: &Filters,
) -> Result<(Authority, bool), ProxyError> {
    let trailers = supports_trailers(headers);
    strip_hop_headers(headers);
    forwarded(headers, peer, original, tls);
    if rule.headers.is_empty() && backend.headers.is_empty() {
        return Ok((original.clone(), trailers));
    }
    // a nominated Host still supplies the authority seen by the filters.
    if !headers.contains_key(header::HOST) {
        headers.insert(
            header::HOST,
            HeaderValue::from_str(original.as_str())
                .map_err(|_| ProxyError::InvalidHost)?,
        );
    }
    if trailers {
        headers.insert(header::TE, HeaderValue::from_static("trailers"));
    }
    modify(headers, &rule.headers);
    modify(headers, &backend.headers);
    let authority = host_authority(headers)?;
    let trailers = supports_trailers(headers);
    strip_hop_headers(headers);
    Ok((authority, trailers))
}

/// add proxy metadata before configured header changes, preserving all earlier hops.
fn forwarded(
    headers: &mut HeaderMap,
    peer: SocketAddr,
    authority: &Authority,
    tls: bool,
) {
    let mut chain = Vec::new();
    for previous in headers.get_all("x-forwarded-for") {
        if !previous.is_empty() {
            chain.extend_from_slice(previous.as_bytes());
            chain.extend_from_slice(b", ");
        }
    }
    chain.extend_from_slice(peer.ip().to_string().as_bytes());
    headers.insert(
        "x-forwarded-for",
        HeaderValue::from_bytes(&chain).expect("validated header values and peer IP"),
    );
    headers.insert(
        "x-forwarded-proto",
        HeaderValue::from_static(if tls { "https" } else { "http" }),
    );
    headers.insert(
        "x-forwarded-host",
        HeaderValue::from_str(authority.as_str()).expect("validated Host header"),
    );
}

/// retain the original path suffix, percent escapes and even an empty query.
pub fn apply(modifier: &PathModifier, uri: &Uri) -> Result<PathAndQuery, ProxyError> {
    let mut path = match modifier {
        PathModifier::Full(path) => path.clone(),
        PathModifier::Prefix { matched, replacement } => {
            let path = match uri.path() {
                "*" | "" => "/",
                path => path,
            };
            let suffix = path.strip_prefix(matched).ok_or(ProxyError::InvalidTarget)?;
            let mut path = replacement.clone();
            path.push_str(suffix);
            if path.is_empty() {
                path.push('/');
            }
            path
        }
    };
    if let Some(query) = uri.query() {
        path.push('?');
        path.push_str(query);
    }
    path.parse().map_err(|_| ProxyError::InvalidTarget)
}

/// construct an absolute location using the advertised listener port and original authority.
pub fn location(
    redirect: &Redirect,
    listener: &Binding,
    authority: &Authority,
    uri: &Uri,
) -> Result<HeaderValue, ProxyError> {
    let scheme =
        redirect
            .scheme
            .as_deref()
            .unwrap_or(if listener.tls { "https" } else { "http" });
    let host = redirect.hostname.as_deref().unwrap_or_else(|| authority.host());
    let standard = if scheme == "https" { 443 } else { 80 };
    let port = redirect.port.unwrap_or(if redirect.scheme.is_some() {
        standard
    } else {
        listener.port
    });
    let mut location = format!("{scheme}://{host}");
    if port != standard {
        use std::fmt::Write;
        write!(location, ":{port}").expect("writing to string");
    }
    let path = redirect.path.as_ref().map(|p| apply(p, uri)).transpose()?;
    let path = path
        .as_ref()
        .or(uri.path_and_query())
        .map_or("/", PathAndQuery::as_str);
    location.push_str(if path == "*" { "/" } else { path });
    HeaderValue::try_from(location).map_err(|_| ProxyError::InvalidTarget)
}
