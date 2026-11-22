//! select HTTP routes by hostname, path, method, headers and query parameters.
use super::super::{
    super::ir::{
        Backend, ClusterSpec, Filters, GrpcMethod, Http, PathMatch, Route, RouteAction,
        ValueMatch, hostname_matches, weighted_by,
    },
    admission::Cluster,
    up::Deadlines,
};
use super::cors;
use ::http::{HeaderMap, Request, Uri};
use regex::Regex;
use std::{
    borrow::Cow,
    cell::OnceCell,
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

/// compiled route owned by one worker.
pub struct WorkerRoute {
    /// predicates with precompiled regular expressions.
    pub http: Http<Regex>,
    /// resolved targets and filters for this match.
    pub action: RouteAction<Rc<Cluster>>,
}

/// routing decisions retained across asynchronous admission and forwarding.
pub struct Selection {
    /// whether the selected route uses native gRPC forwarding and errors.
    pub grpc: bool,
    /// listener rule and its response filters.
    pub rule: Arc<Filters>,
    /// selected target and backend-specific filters.
    pub backend: Option<(Rc<Cluster>, Arc<Filters>)>,
    /// cross-origin decision made before request filters or upstream forwarding.
    pub cors: Option<cors::Decision>,
    /// request and attempt deadlines.
    pub budget: Deadlines,
}

/// decoded first values, borrowing from the URI when no unescaping is necessary.
type Query<'a> = HashMap<Cow<'a, str>, Cow<'a, str>>;

/// compile named predicates once when installing a snapshot.
fn compile_values(
    values: Vec<(String, ValueMatch<String>)>,
) -> Result<Vec<(String, ValueMatch<Regex>)>, regex::Error> {
    values
        .into_iter()
        .map(|(name, value)| Ok((name, value.compile()?)))
        .collect()
}

impl ValueMatch<String> {
    /// compile regular expressions once when installing a snapshot.
    fn compile(self) -> Result<ValueMatch<Regex>, regex::Error> {
        Ok(match self {
            Self::Exact(value) => ValueMatch::Exact(value),
            Self::RegularExpression { regex } => {
                ValueMatch::RegularExpression { regex: Regex::new(&regex)? }
            }
        })
    }
}

impl ValueMatch<Regex> {
    /// compare literal bytes or search a UTF-8 value with a compiled regex.
    fn matches(&self, bytes: &[u8]) -> bool {
        match self {
            Self::Exact(value) => bytes == value.as_bytes(),
            Self::RegularExpression { regex } => {
                std::str::from_utf8(bytes).is_ok_and(|s| regex.is_match(s))
            }
        }
    }
}

impl Http<Regex> {
    /// prepare the worker representation without changing the serialized snapshot.
    fn compile(http: Http) -> Result<Self, regex::Error> {
        let path = match http.path {
            PathMatch::Grpc { grpc } => PathMatch::Grpc {
                grpc: GrpcMethod {
                    service: grpc.service.map(ValueMatch::compile).transpose()?,
                    method: grpc.method.map(ValueMatch::compile).transpose()?,
                },
            },
            PathMatch::Literal { path, exact } => PathMatch::Literal { path, exact },
            PathMatch::RegularExpression { regex } => {
                PathMatch::RegularExpression { regex: Regex::new(&regex)? }
            }
        };
        Ok(Self {
            path,
            method: http.method,
            headers: compile_values(http.headers)?,
            query_params: compile_values(http.query_params)?,
        })
    }

    /// evaluate predicates against the original URI, decoding query pairs on first use.
    fn matches<'a>(
        &self,
        uri: &'a Uri,
        headers: &HeaderMap,
        query: &OnceCell<Query<'a>>,
    ) -> bool {
        let path = uri.path();
        let path_matches = match &self.path {
            PathMatch::Grpc { grpc } => path
                .strip_prefix('/')
                .and_then(|p| p.split_once('/'))
                .is_some_and(|(service, method)| {
                    !service.is_empty()
                        && !method.is_empty()
                        && !method.contains('/')
                        && grpc
                            .service
                            .as_ref()
                            .is_none_or(|v| v.matches(service.as_bytes()))
                        && grpc
                            .method
                            .as_ref()
                            .is_none_or(|v| v.matches(method.as_bytes()))
                }),
            PathMatch::RegularExpression { regex } => regex.is_match(path),
            PathMatch::Literal { path: pattern, exact } => {
                path == pattern
                    || (!exact
                        && (pattern == "/"
                            || path
                                .strip_prefix(pattern)
                                .is_some_and(|tail| tail.starts_with('/'))))
            }
        };
        if !path_matches
            || !self.headers.iter().all(|(name, predicate)| {
                headers.get_all(name).iter().any(|v| predicate.matches(v.as_bytes()))
            })
        {
            return false;
        }
        if self.query_params.is_empty() {
            return true;
        }
        let pairs = query.get_or_init(|| {
            let mut pairs = HashMap::new();
            for (name, value) in
                form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            {
                pairs.entry(name).or_insert(value);
            }
            pairs
        });
        self.query_params.iter().all(|(name, predicate)| {
            pairs
                .get(name.as_str())
                .is_some_and(|v| predicate.matches(v.as_bytes()))
        })
    }

    /// rank native gRPC components or HTTP paths before counting metadata predicates.
    fn rank(&self) -> (u8, usize, bool, usize, usize) {
        let (kind, length) = match &self.path {
            PathMatch::Grpc { grpc } => {
                let kind = match (&grpc.service, &grpc.method) {
                    (Some(ValueMatch::Exact(_)), Some(ValueMatch::Exact(_))) => 4,
                    (Some(ValueMatch::Exact(_)), None) => 3,
                    (None, Some(ValueMatch::Exact(_))) => 2,
                    (None, None) => 0,
                    _ => 1,
                };
                (kind, 0)
            }
            PathMatch::Literal { exact: true, .. } => (2, 0),
            PathMatch::Literal { path, exact: false } => (1, path.len()),
            PathMatch::RegularExpression { .. } => (0, 0),
        };
        (kind, length, self.method.is_some(), self.headers.len(), self.query_params.len())
    }
}

/// hostname index shared by listener selection and route matching.
pub struct Hosts<T> {
    /// exact hostnames match first.
    exact: BTreeMap<String, T>,
    /// wildcard hostnames ordered by descending suffix length.
    wildcard: Vec<(String, T)>,
    /// unrestricted hostname matches last.
    fallback: Option<T>,
}

impl<T> FromIterator<(String, T)> for Hosts<T> {
    /// index exact, wildcard and fallback hostnames in selection order.
    fn from_iter<I: IntoIterator<Item = (String, T)>>(entries: I) -> Self {
        let mut hosts = Self {
            exact: BTreeMap::new(),
            wildcard: Vec::new(),
            fallback: None,
        };
        for (hostname, value) in entries {
            if hostname.is_empty() {
                hosts.fallback = Some(value);
            } else if hostname.starts_with("*.") {
                hosts.wildcard.push((hostname, value));
            } else {
                hosts.exact.insert(hostname, value);
            }
        }
        hosts.wildcard.sort_by_key(|(h, _)| Reverse(h.len()));
        hosts
    }
}

impl<T> Hosts<T> {
    /// enumerate matching hostnames from most to least specific.
    pub fn matching<'a>(&'a self, hostname: &str) -> impl Iterator<Item = &'a T> {
        self.exact
            .get(hostname)
            .into_iter()
            .chain(
                self.wildcard
                    .iter()
                    .filter(move |(pattern, _)| hostname_matches(pattern, hostname))
                    .map(|(_, v)| v),
            )
            .chain(self.fallback.iter())
    }
}

/// independent routing tables for HTTP and native gRPC on one listener.
pub struct Router {
    /// ordinary HTTP routes, including gRPC traffic on HTTP-only hosts.
    http: Hosts<Vec<WorkerRoute>>,
    /// gRPC routes ranked independently of HTTP path matches.
    grpc: Hosts<Vec<WorkerRoute>>,
}

impl Router {
    /// preserve creation time, namespace/name, rule and match order on ties.
    pub fn compile<'a>(
        routes: impl IntoIterator<Item = &'a Route>,
        mut resolve: impl FnMut(&Arc<ClusterSpec>) -> Rc<Cluster>,
    ) -> Result<Self, regex::Error> {
        let mut http = BTreeMap::<_, Vec<_>>::new();
        let mut grpc = BTreeMap::<_, Vec<_>>::new();
        for route in routes {
            let hosts = if matches!(route.http.path, PathMatch::Grpc { .. }) {
                &mut grpc
            } else {
                &mut http
            };
            let hostname = route.host.as_ref().map_or(&route.hostname, |h| &h.route);
            hosts
                .entry(hostname.to_ascii_lowercase())
                .or_default()
                .push(WorkerRoute {
                    http: Http::compile(route.http.clone())?,
                    action: RouteAction {
                        filters: route.action.filters.clone(),
                        timeouts: route.action.timeouts,
                        backends: route
                            .action
                            .backends
                            .iter()
                            .map(|b| Backend {
                                weight: b.weight,
                                filters: b.filters.clone(),
                                cluster: b
                                    .cluster
                                    .as_ref()
                                    .map(&mut resolve)
                                    .map_err(|e| *e),
                            })
                            .collect(),
                    },
                });
        }
        let ranked = |buckets: BTreeMap<_, Vec<WorkerRoute>>| {
            buckets
                .into_iter()
                .map(|(hostname, mut routes)| {
                    // stable sorting preserves compiler-established tie order.
                    routes.sort_by_key(|route| Reverse(route.http.rank()));
                    (hostname, routes)
                })
                .collect()
        };
        Ok(Self { http: ranked(http), grpc: ranked(grpc) })
    }

    /// select a route and backend, allowing its CORS policy to handle a preflight
    /// for the requested method while preserving ordinary OPTIONS routing.
    pub fn find<B>(
        &self,
        hostname: &str,
        request: &Request<B>,
        grpc: bool,
        start: Instant,
        header_timeout: Duration,
    ) -> Option<Selection> {
        let method = request.method();
        let headers = request.headers();
        let requested = cors::requested_method(method, headers);
        let query = OnceCell::new();
        // once a hostname has a gRPC route, unmatched RPCs receive UNIMPLEMENTED.
        let table = if grpc && self.grpc.matching(hostname).next().is_some() {
            &self.grpc
        } else {
            &self.http
        };
        let (route, backend) = table.matching(hostname).flatten().find_map(|route| {
            let action = &route.action;
            let expected = route.http.method.as_deref();
            let direct = expected.is_none_or(|m| m == method.as_str());
            if (!direct && requested != expected)
                || !route.http.matches(request.uri(), headers, &query)
            {
                return None;
            }
            let backend = weighted_by(&action.backends, |b| b.weight);
            (direct
                || action.filters.cors.is_some()
                || backend.is_some_and(|b| b.filters.cors.is_some()))
            .then_some((route, backend))
        })?;
        let action = &route.action;
        let cors = backend
            .and_then(|b| b.filters.cors.as_ref())
            .or(action.filters.cors.as_ref())
            .map(|policy| cors::Decision::new(policy, method, headers));
        let backend = backend
            .and_then(|b| Some((b.cluster.as_ref().ok()?.clone(), b.filters.clone())));
        Some(Selection {
            grpc: matches!(route.http.path, PathMatch::Grpc { .. }),
            rule: action.filters.clone(),
            backend,
            cors,
            budget: Deadlines::new(start, action.timeouts, header_timeout),
        })
    }
}
