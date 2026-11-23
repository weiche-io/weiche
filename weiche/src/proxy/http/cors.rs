//! evaluate cross-origin requests against the selected rule or backend policy.
use super::super::super::ir::{Cors, Origin};
use super::wire::{header_tokens, single};
use http::{HeaderMap, HeaderName, HeaderValue, Method, header::*};

/// identify a preflight by the presence of both Fetch request fields.
fn preflight(method: &Method, headers: &HeaderMap) -> bool {
    method == Method::OPTIONS
        && headers.contains_key(ORIGIN)
        && headers.contains_key(ACCESS_CONTROL_REQUEST_METHOD)
}

/// supply the intended method when evaluating a method-specific CORS route.
pub fn requested_method<'a>(method: &Method, headers: &'a HeaderMap) -> Option<&'a str> {
    if !preflight(method, headers) {
        return None;
    }
    single(headers, ACCESS_CONTROL_REQUEST_METHOD)?.to_str().ok()
}

/// check the origin before reflecting the original spelling to the client.
fn allowed_origin<'a>(policy: &Cors, headers: &'a HeaderMap) -> Option<&'a HeaderValue> {
    let origin = single(headers, ORIGIN)?;
    let value = origin.to_str().ok()?;
    let allowed = if value == "null" {
        policy.origins.contains(&Origin::Any)
    } else {
        if value.contains('*') {
            return None;
        }
        let actual = Origin::parse(value)?;
        policy.origins.iter().any(|pattern| pattern.matches(&actual))
    };
    allowed.then_some(origin)
}

/// write a nonempty list of validated response header tokens.
fn list(headers: &mut HeaderMap, name: HeaderName, values: &[impl AsRef<str>]) {
    if !values.is_empty() {
        let value = values.iter().map(AsRef::as_ref).collect::<Vec<_>>().join(", ");
        if let Ok(value) = HeaderValue::from_str(&value) {
            headers.insert(name, value);
        }
    }
}

/// authorize every requested header and the proposed method before granting a preflight.
fn allow_preflight(
    policy: &Cors,
    request: &HeaderMap,
    response: &mut HeaderMap,
) -> Option<()> {
    let method = single(request, ACCESS_CONTROL_REQUEST_METHOD)?;
    Method::from_bytes(method.as_bytes()).ok()?;
    let method_name = method.to_str().ok()?;
    let any_method = policy.methods.iter().any(|m| m == "*");
    if method_name == "*" {
        return None;
    }
    if !matches!(method_name, "GET" | "HEAD" | "POST")
        && !any_method
        && !policy.methods.iter().any(|m| m == method_name)
    {
        return None;
    }
    let any_header = policy.headers.iter().any(|h| h == "*");
    let mut names = Vec::new();
    for field in request.get_all(ACCESS_CONTROL_REQUEST_HEADERS) {
        for name in field.to_str().ok()?.split(',').map(str::trim) {
            let name = name.parse::<HeaderName>().ok()?;
            if name == "*" {
                return None;
            }
            if !any_header
                && !policy.headers.iter().any(|h| h.eq_ignore_ascii_case(name.as_str()))
            {
                return None;
            }
            names.push(name);
        }
    }
    if any_method {
        response.insert(ACCESS_CONTROL_ALLOW_METHODS, method.clone());
    } else {
        list(response, ACCESS_CONTROL_ALLOW_METHODS, &policy.methods);
    }
    if !names.is_empty() {
        if any_header {
            list(response, ACCESS_CONTROL_ALLOW_HEADERS, &names);
        } else {
            list(response, ACCESS_CONTROL_ALLOW_HEADERS, &policy.headers);
        }
    }
    response.insert(ACCESS_CONTROL_MAX_AGE, policy.max_age.into());
    Some(())
}

/// policy decisions retained while the request is forwarded or redirected.
pub struct Decision {
    /// send an empty local response instead of contacting the backend.
    pub preflight: bool,
    /// authoritative CORS fields derived from the original request.
    headers: HeaderMap,
    /// expand a credentialed expose wildcard against the final response headers.
    expose_all: bool,
}

impl Decision {
    /// evaluate the original headers before request modifiers run.
    pub fn new(policy: &Cors, method: &Method, request: &HeaderMap) -> Self {
        let mut result = Self {
            preflight: preflight(method, request),
            headers: HeaderMap::new(),
            expose_all: false,
        };
        let Some(origin) = allowed_origin(policy, request) else { return result };
        if result.preflight
            && allow_preflight(policy, request, &mut result.headers).is_none()
        {
            return result;
        }
        result.headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
        if policy.credentials {
            result.headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        result.expose_all = policy.credentials && policy.expose.iter().any(|h| h == "*");
        if !result.expose_all {
            list(&mut result.headers, ACCESS_CONTROL_EXPOSE_HEADERS, &policy.expose);
        }
        result
    }

    /// merge cache keys and replace upstream CORS fields after response modifiers.
    pub fn apply(self, response: &mut HeaderMap) {
        for name in [
            ACCESS_CONTROL_ALLOW_ORIGIN,
            ACCESS_CONTROL_ALLOW_CREDENTIALS,
            ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_HEADERS,
            ACCESS_CONTROL_EXPOSE_HEADERS,
            ACCESS_CONTROL_MAX_AGE,
        ] {
            response.remove(name);
        }
        vary(response, "Origin");
        if self.preflight {
            vary(response, "Access-Control-Request-Method");
            vary(response, "Access-Control-Request-Headers");
        }
        response.extend(self.headers);
        if self.expose_all {
            let names: Vec<_> = response
                .keys()
                .filter(|k| !matches!(k.as_str(), "set-cookie" | "set-cookie2" | "*"))
                .cloned()
                .collect();
            list(response, ACCESS_CONTROL_EXPOSE_HEADERS, &names);
        }
    }
}

/// append a missing cache key, preserving existing lines, token casing and Vary: *.
fn vary(headers: &mut HeaderMap, key: &'static str) {
    let present =
        header_tokens(headers, VARY).any(|v| v == "*" || v.eq_ignore_ascii_case(key));
    if !present {
        headers.append(VARY, HeaderValue::from_static(key));
    }
}
