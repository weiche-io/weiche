//! compile the parallel rule and backend filter schemas into the same operations.
use super::super::super::ir::{Filters, HeaderChange, PathModifier, Redirect};
use super::{HttpRouteRules, PathType, ResolveError};
use http::{HeaderName, HeaderValue, StatusCode, header};
use std::{collections::HashSet, net::IpAddr};
use weiche_gtwapi::v1::httproutes::*;

/// validate an explicit Gateway API precise hostname.
fn hostname(host: &str) -> Result<&str, ResolveError> {
    let valid = host.len() <= 253
        && host.parse::<IpAddr>().is_err()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        });
    valid.then_some(host).ok_or(ResolveError::UnsupportedValue)
}

/// bind a replacement to the rule's single explicit path prefix.
fn path(
    rule: &HttpRouteRules,
    prefix: bool,
    full: Option<&str>,
    part: Option<&str>,
) -> Result<PathModifier, ResolveError> {
    let invalid = ResolveError::UnsupportedValue;
    let value = match (prefix, full, part) {
        (true, None, Some(value)) | (false, Some(value), None) => value,
        _ => return Err(invalid),
    };
    if !value.is_empty()
        && (!value.starts_with('/')
            || value.contains(['?', '#'])
            || value.parse::<http::uri::PathAndQuery>().is_err())
    {
        return Err(invalid);
    }
    if !prefix {
        return Ok(PathModifier::Full(if value.is_empty() { "/" } else { value }.into()));
    }
    let [predicate] = rule.matches.as_deref().unwrap_or_default() else {
        return Err(invalid);
    };
    let pattern = predicate.path.as_ref().ok_or(invalid)?;
    if !matches!(pattern.r#type, None | Some(PathType::PathPrefix)) {
        return Err(invalid);
    }
    Ok(PathModifier::Prefix {
        matched: pattern.value.as_deref().unwrap_or("/").trim_end_matches('/').into(),
        replacement: value.trim_end_matches('/').into(),
    })
}

/// parse header names and values once, rejecting repeated case-insensitive names.
fn headers<'a>(
    set: impl Iterator<Item = (&'a str, &'a str)>,
    add: impl Iterator<Item = (&'a str, &'a str)>,
    remove: impl Iterator<Item = &'a String>,
) -> Result<Vec<HeaderChange>, ResolveError> {
    let invalid = ResolveError::UnsupportedValue;
    let mut names = HashSet::new();
    let mut name = |key: &str| {
        let key = key.parse::<HeaderName>().map_err(|_| invalid)?;
        names.insert(key.clone()).then_some(key).ok_or(invalid)
    };
    let mut changes = Vec::new();
    for (key, value) in set {
        changes.push(HeaderChange::Set(name(key)?, value.parse().map_err(|_| invalid)?));
    }
    for (key, value) in add {
        changes.push(HeaderChange::Add(name(key)?, value.parse().map_err(|_| invalid)?));
    }
    for key in remove {
        changes.push(HeaderChange::Remove(name(key)?));
    }
    Ok(changes)
}

/// adapt the distinct generated header entry types without serialization.
macro_rules! changes {
    ($h:expr) => {{
        let h = $h;
        headers(
            h.set.iter().flatten().map(|e| (e.name.as_str(), e.value.as_str())),
            h.add.iter().flatten().map(|e| (e.name.as_str(), e.value.as_str())),
            h.remove.iter().flatten(),
        )?
    }};
}

/// normalize the path union used by both redirect and rewrite filters.
macro_rules! modifier {
    ($rule:expr, $path:expr, $kind:ident) => {
        $path
            .as_ref()
            .map(|p| {
                path(
                    $rule,
                    matches!(p.r#type, $kind::ReplacePrefixMatch),
                    p.replace_full_path.as_deref(),
                    p.replace_prefix_match.as_deref(),
                )
            })
            .transpose()?
    };
}

/// compile identical filter semantics for both generated Gateway API shapes.
macro_rules! filters {
    ($name:ident, $filter:ty, $kind:ident, $scheme:ident, $redirect:ident, $rewrite:ident) => {
        /// resolve one ordered list of HTTP filters.
        pub fn $name(
            rule: &HttpRouteRules,
            filters: &[$filter],
        ) -> Result<Filters, ResolveError> {
            use $kind as Kind;
            let invalid = ResolveError::UnsupportedValue;
            let mut seen = HashSet::new();
            let mut result = Filters::default();
            for f in filters {
                if !seen.insert(std::mem::discriminant(&f.r#type)) {
                    return Err(ResolveError::IncompatibleFilters);
                }
                match f.r#type {
                    Kind::RequestHeaderModifier => result.headers.extend(changes!(
                        f.request_header_modifier.as_ref().ok_or(invalid)?
                    )),
                    Kind::ResponseHeaderModifier => {
                        result.response_headers =
                            changes!(f.response_header_modifier.as_ref().ok_or(invalid)?)
                    }
                    Kind::RequestRedirect => {
                        let r = f.request_redirect.as_ref().ok_or(invalid)?;
                        let status = match r.status_code.unwrap_or(302) {
                            code @ (301 | 302 | 303 | 307 | 308) => {
                                StatusCode::from_u16(code as u16).map_err(|_| invalid)?
                            }
                            _ => return Err(invalid),
                        };
                        if r.port == Some(0) {
                            return Err(invalid);
                        }
                        result.redirect = Some(Redirect {
                            status,
                            hostname: r
                                .hostname
                                .as_deref()
                                .map(hostname)
                                .transpose()?
                                .map(str::to_owned),
                            scheme: r.scheme.as_ref().map(|s| {
                                match s {
                                    $scheme::Http => "http",
                                    $scheme::Https => "https",
                                }
                                .into()
                            }),
                            port: r.port,
                            path: modifier!(rule, r.path, $redirect),
                        });
                    }
                    Kind::UrlRewrite => {
                        let r = f.url_rewrite.as_ref().ok_or(invalid)?;
                        if let Some(host) = &r.hostname {
                            result.headers.push(HeaderChange::Set(
                                header::HOST,
                                HeaderValue::from_str(hostname(host)?)
                                    .map_err(|_| invalid)?,
                            ));
                        }
                        result.path = modifier!(rule, r.path, $rewrite);
                    }
                    _ => return Err(invalid),
                }
            }
            if result.redirect.is_some()
                && seen.contains(&std::mem::discriminant(&Kind::UrlRewrite))
            {
                return Err(ResolveError::IncompatibleFilters);
            }
            Ok(result)
        }
    };
}

filters!(
    rule_filters,
    HttpRouteRulesFilters,
    HttpRouteRulesFiltersType,
    HttpRouteRulesFiltersRequestRedirectScheme,
    HttpRouteRulesFiltersRequestRedirectPathType,
    HttpRouteRulesFiltersUrlRewritePathType
);
filters!(
    backend_filters,
    HttpRouteRulesBackendRefsFilters,
    HttpRouteRulesBackendRefsFiltersType,
    HttpRouteRulesBackendRefsFiltersRequestRedirectScheme,
    HttpRouteRulesBackendRefsFiltersRequestRedirectPathType,
    HttpRouteRulesBackendRefsFiltersUrlRewritePathType
);
