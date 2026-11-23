//! compile the parallel rule and backend filter schemas into the same operations.
use super::super::ir::{Cors, Filters, HeaderChange, Origin, PathModifier, Redirect};
use super::ResolveError;
use http::{HeaderName, HeaderValue, StatusCode, header};
use std::{collections::HashSet, net::IpAddr};
use weiche_gtwapi::v1::httproutes::HttpRouteRulesMatchesPathType as PathType;
use weiche_gtwapi::v1::{grpcroutes::*, httproutes::*};

/// the CRD's hostname pattern admits IP literals and DNS labels longer than 63 bytes.
fn hostname(host: &str) -> Result<&str, ResolveError> {
    let valid =
        host.parse::<IpAddr>().is_err() && host.split('.').all(|label| label.len() <= 63);
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
    ($name:ident, $filter:ty, $kind:ident $(, $rule:ident: $scheme:ident, $redirect:ident, $rewrite_kind:ident, $rewrite:ident)?) => {
        /// resolve one ordered list of rule or backend filters.
        pub fn $name(
            $($rule: &HttpRouteRules,)?
            filters: Option<&[$filter]>,
        ) -> Result<Filters, ResolveError> {
            use $kind as Kind;
            let invalid = ResolveError::UnsupportedValue;
            let mut result = Filters::default();
            for f in filters.unwrap_or_default() {
                match f.r#type {
                    Kind::RequestHeaderModifier => result.headers.extend(changes!(
                        f.request_header_modifier.as_ref().ok_or(invalid)?
                    )),
                    Kind::ResponseHeaderModifier => {
                        result.response_headers =
                            changes!(f.response_header_modifier.as_ref().ok_or(invalid)?)
                    }
                    $(
                    Kind::Cors => {
                        let c = f.cors.as_ref().ok_or(invalid)?;
                        // admission checks the lists. workers need each origin's host and port.
                        result.cors = Some(Cors {
                            origins: c.allow_origins.iter().flatten()
                                .map(|origin| Origin::parse(origin).ok_or(invalid))
                                .collect::<Result<_, _>>()?,
                            methods: c.allow_methods.clone().unwrap_or_default(),
                            headers: c.allow_headers.clone().unwrap_or_default(),
                            expose: c.expose_headers.clone().unwrap_or_default(),
                            credentials: c.allow_credentials.unwrap_or(false),
                            max_age: c.max_age.unwrap_or(5),
                        });
                    }
                    Kind::RequestRedirect => {
                        let r = f.request_redirect.as_ref().ok_or(invalid)?;
                        // report unknown redirect codes as UnsupportedValue under Gateway API.
                        let status = match r.status_code.unwrap_or(302) {
                            301 => StatusCode::MOVED_PERMANENTLY,
                            302 => StatusCode::FOUND,
                            303 => StatusCode::SEE_OTHER,
                            307 => StatusCode::TEMPORARY_REDIRECT,
                            308 => StatusCode::PERMANENT_REDIRECT,
                            _ => return Err(invalid),
                        };
                        result.redirect = Some(Redirect {
                            status,
                            hostname: r.hostname.as_deref().map(hostname).transpose()?.map(str::to_owned),
                            scheme: r.scheme.as_ref().map(|s| {
                                match s {
                                    $scheme::Http => "http",
                                    $scheme::Https => "https",
                                }
                                .into()
                            }),
                            port: r.port,
                            path: modifier!($rule, r.path, $redirect),
                        });
                    }
                    Kind::$rewrite_kind => {
                        let r = f.url_rewrite.as_ref().ok_or(invalid)?;
                        if let Some(host) = &r.hostname {
                            result.headers.push(HeaderChange::Set(
                                header::HOST,
                                HeaderValue::from_str(hostname(host)?)
                                    .map_err(|_| invalid)?,
                            ));
                        }
                        result.path = modifier!($rule, r.path, $rewrite);
                    }
                    )?
                    _ => return Err(invalid),
                }
            }
            Ok(result)
        }
    };
}

filters!(
    rule_filters,
    HttpRouteRulesFilters,
    HttpRouteRulesFiltersType,
    rule: HttpRouteRulesFiltersRequestRedirectScheme,
    HttpRouteRulesFiltersRequestRedirectPathType,
    UrlRewrite, HttpRouteRulesFiltersUrlRewritePathType
);
filters!(
    backend_filters,
    HttpRouteRulesBackendRefsFilters,
    HttpRouteRulesBackendRefsFiltersType,
    rule: HttpRouteRulesBackendRefsFiltersRequestRedirectScheme,
    HttpRouteRulesBackendRefsFiltersRequestRedirectPathType,
    UrlRewrite, HttpRouteRulesBackendRefsFiltersUrlRewritePathType
);

filters!(grpc_rule_filters, GrpcRouteRulesFilters, GrpcRouteRulesFiltersType);
filters!(
    grpc_backend_filters,
    GrpcRouteRulesBackendRefsFilters,
    GrpcRouteRulesBackendRefsFiltersType
);
