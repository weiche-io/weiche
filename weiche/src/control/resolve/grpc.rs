//! compile native gRPC service, method and metadata predicates.
//! regular expressions use Rust regex syntax and search each component independently.
use super::{
    super::super::ir::GrpcMethod, GatewayStores, Http, HttpRule, PathMatch, Policies,
    ResolveError, RouteAction, ServiceRef, ValueMatch, filters, named_matches,
};
use std::sync::Arc;
use weiche_gtwapi::v1::grpcroutes::{
    GrpcRouteRules, GrpcRouteRulesMatches,
    GrpcRouteRulesMatchesHeadersType as HeaderType,
    GrpcRouteRulesMatchesMethodType as MethodType,
};

/// resolve optional method components independently of the HTTP path matcher.
fn grpc_match(predicate: Option<&GrpcRouteRulesMatches>) -> Result<Http, ResolveError> {
    let mut grpc = GrpcMethod::default();
    if let Some(m) = predicate.and_then(|p| p.method.as_ref()) {
        let regular = matches!(m.r#type, Some(MethodType::RegularExpression));
        let component = |value: Option<&str>| {
            value
                .filter(|s| !s.is_empty())
                .map(|s| ValueMatch::resolve(s, regular))
                .transpose()
        };
        grpc.service = component(m.service.as_deref())?;
        grpc.method = component(m.method.as_deref())?;
        if grpc.service.is_none() && grpc.method.is_none() {
            return Err(ResolveError::UnsupportedValue);
        }
    }
    let headers = named_matches(
        predicate
            .and_then(|p| p.headers.as_deref())
            .unwrap_or_default()
            .iter()
            .map(|h| {
                (
                    h.name.to_ascii_lowercase(),
                    h.value.as_str(),
                    matches!(h.r#type, Some(HeaderType::RegularExpression)),
                )
            }),
    )?;
    Ok(Http {
        path: PathMatch::Grpc { grpc },
        headers,
        ..Http::default()
    })
}

impl GatewayStores {
    /// resolve weighted gRPC targets after validating every match and header operation.
    pub fn resolve_grpc(
        &self,
        namespace: &str,
        rule: &GrpcRouteRules,
        policies: &mut Policies,
    ) -> Result<HttpRule, ResolveError> {
        let matches = match rule.matches.as_deref() {
            None | Some([]) => vec![grpc_match(None)?],
            Some(matches) => matches
                .iter()
                .map(|m| grpc_match(Some(m)))
                .collect::<Result<_, _>>()?,
        };
        let filters =
            filters::grpc_rule_filters(rule.filters.as_deref().unwrap_or_default())?;
        let mut validated = Vec::new();
        for r in rule.backend_refs.iter().flatten() {
            let backend =
                filters::grpc_backend_filters(r.filters.as_deref().unwrap_or_default())?;
            validated.push((r.weight.unwrap_or(1), ServiceRef::from(r), backend));
        }
        let backends = self.resolve_backends(namespace, "GRPCRoute", policies, validated);
        Ok(HttpRule {
            matches,
            action: RouteAction {
                backends,
                filters: Arc::new(filters),
                timeouts: Default::default(),
            },
        })
    }
}
