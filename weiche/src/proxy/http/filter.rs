//! apply validated HTTPRoute filters without decoding the request path or query.
use super::super::super::ir::{Binding, Filters, HeaderChange, PathModifier, Redirect};
use super::super::up::Exchange;
use super::router::Selection;
use super::wire::{
    ProxyError, header_tokens, host_authority, strip_hop_headers, supply_host,
};
use http::{
    HeaderMap, HeaderValue, Uri, Version, header,
    request::Parts,
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
    header_tokens(headers, header::TE).any(|v| v.eq_ignore_ascii_case("trailers"))
}

/// preserve the original authority and TE semantics while applying request filters.
pub fn request_headers(
    request: &mut Parts,
    peer: SocketAddr,
    original: &Authority,
    tls: bool,
    rule: &Filters,
    backend: &Filters,
) -> Result<(Authority, bool), ProxyError> {
    // an HTTP/3 client can receive trailers without an explicit TE field.
    let implicit_trailers =
        request.version == Version::HTTP_3 && !request.headers.contains_key(header::TE);
    let headers = &mut request.headers;
    let trailers = implicit_trailers || supports_trailers(headers);
    strip_hop_headers(headers);
    forwarded(headers, peer, original, tls);
    if rule.headers.is_empty() && backend.headers.is_empty() {
        return Ok((original.clone(), trailers));
    }
    // a nominated Host still supplies the authority seen by the filters.
    supply_host(headers, original)?;
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

/// apply backend and rule filters before restoring downstream framing and CORS headers.
pub fn response(exchange: &mut Exchange, version: Version, selection: Selection) {
    let response = &mut exchange.response;
    *response.version_mut() = version;
    let headers = response.headers_mut();
    let backend_changes = selection
        .backend
        .as_ref()
        .map_or(&[][..], |(_, filters)| filters.response_headers.as_slice());
    if !backend_changes.is_empty() || !selection.rule.response_headers.is_empty() {
        modify(headers, backend_changes);
        modify(headers, &selection.rule.response_headers);
        strip_hop_headers(headers);
    }
    if !matches!(version, Version::HTTP_2 | Version::HTTP_3)
        && headers.contains_key(header::TRAILER)
    {
        headers.remove(header::CONTENT_LENGTH);
    }
    if let Some(tunnel) = &exchange.tunnel {
        tunnel.restore(headers);
    }
    if let Some(cors) = selection.cors {
        cors.apply(headers);
    }
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
