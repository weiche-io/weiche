//! select HTTP routes by hostname, path, method, headers and query parameters.
use super::super::{
    super::super::ir::{
        Backend, Http, PathMatch, Route, RouteAction, ValueMatch, hostname_matches,
        weighted_by,
    },
    Cluster,
};
use ::http::{HeaderMap, Method, Uri};
use regex::Regex;
use std::{
    borrow::Cow,
    cell::OnceCell,
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

/// compiled route owned by one worker.
type WorkerRoute = Route<Rc<Cluster>, Regex>;

/// route action and the single weighted backend selected for this request.
type Selected<'a> = (&'a RouteAction<Rc<Cluster>>, Option<&'a Backend<Rc<Cluster>>>);

/// decoded first values, borrowing from the URI when no unescaping is necessary.
type Query<'a> = HashMap<Cow<'a, str>, Cow<'a, str>>;

/// compile named predicates once when installing a snapshot.
fn compile_values(
    values: Vec<(String, ValueMatch<String>)>,
) -> Result<Vec<(String, ValueMatch<Regex>)>, regex::Error> {
    values
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                ValueMatch::Exact(value) => ValueMatch::Exact(value),
                ValueMatch::RegularExpression { regex } => {
                    ValueMatch::RegularExpression { regex: Regex::new(&regex)? }
                }
            };
            Ok((name, value))
        })
        .collect()
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

    /// rank regex paths below all literal paths, breaking regex ties by the other predicates.
    fn rank(&self) -> (u8, usize, bool, usize, usize) {
        let (kind, length) = match &self.path {
            PathMatch::Literal { exact: true, .. } => (2, 0),
            PathMatch::Literal { path, exact: false } => (1, path.len()),
            PathMatch::RegularExpression { .. } => (0, 0),
        };
        (kind, length, self.method.is_some(), self.headers.len(), self.query_params.len())
    }
}

/// worker-local route index sorted by hostname specificity and match precedence.
#[derive(Default)]
pub struct Router {
    /// routes with an exact hostname.
    exact: BTreeMap<String, Vec<WorkerRoute>>,
    /// wildcard hosts sorted by descending suffix length.
    wildcard: Vec<(String, Vec<WorkerRoute>)>,
    /// routes accepting any hostname.
    fallback: Vec<WorkerRoute>,
}

impl Router {
    /// preserve creation time, namespace/name, rule and match order on ties.
    pub fn compile(
        routes: impl IntoIterator<Item = Route<Rc<Cluster>>>,
    ) -> Result<Self, regex::Error> {
        let mut buckets: BTreeMap<String, Vec<_>> = BTreeMap::new();
        for route in routes {
            let hostname = route.hostname.to_ascii_lowercase();
            let route = Route {
                hostname: hostname.clone(),
                http: Http::compile(route.http)?,
                action: route.action,
            };
            buckets.entry(hostname).or_default().push(route);
        }
        let mut router = Self::default();
        for (hostname, mut routes) in buckets {
            // stable sorting preserves compiler-established tie order.
            routes.sort_by_key(|route| Reverse(route.http.rank()));
            if hostname.is_empty() {
                router.fallback = routes;
            } else if hostname.starts_with("*.") {
                router.wildcard.push((hostname, routes));
            } else {
                router.exact.insert(hostname, routes);
            }
        }
        router.wildcard.sort_by_key(|(hostname, _)| Reverse(hostname.len()));
        Ok(router)
    }

    /// select a route and backend, allowing its CORS policy to handle a preflight
    /// for the requested method while preserving ordinary OPTIONS routing.
    pub fn find(
        &self,
        hostname: &str,
        uri: &Uri,
        method: &Method,
        headers: &HeaderMap,
    ) -> Option<Selected<'_>> {
        let requested = super::cors::requested_method(method, headers);
        let query = OnceCell::new();
        self.exact
            .get(hostname)
            .into_iter()
            .chain(
                self.wildcard
                    .iter()
                    .filter(|(pattern, _)| hostname_matches(pattern, hostname))
                    .map(|(_, routes)| routes),
            )
            .chain(std::iter::once(&self.fallback))
            .flatten()
            .find_map(|route| {
                let action = &route.action;
                let expected = route.http.method.as_deref();
                let direct = expected.is_none_or(|m| m == method.as_str());
                if (!direct && requested != expected)
                    || !route.http.matches(uri, headers, &query)
                {
                    return None;
                }
                let backend = weighted_by(&action.backends, |b| b.weight);
                (direct
                    || action.filters.cors.is_some()
                    || backend.is_some_and(|b| b.filters.cors.is_some()))
                .then_some((action, backend))
            })
    }

    /// enumerate installed routes for cluster reuse during a snapshot change.
    pub fn routes(&self) -> impl Iterator<Item = &WorkerRoute> {
        self.exact
            .values()
            .chain(self.wildcard.iter().map(|(_, routes)| routes))
            .chain(std::iter::once(&self.fallback))
            .flatten()
    }
}
