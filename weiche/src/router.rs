//! compiled hostname buckets with stable Gateway API match precedence.
use super::cluster::ClusterId;
use http::{HeaderMap, Method};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteAction {
    Forward(ClusterId),
    RejectInvalidBackend,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    pub hostname: String,
    pub path: String,
    pub exact: bool,
    pub method: Option<String>,
    pub headers: Vec<(String, String)>,
    pub action: RouteAction,
}

impl Route {
    fn matches(&self, path: &str, method: &Method, headers: &HeaderMap) -> bool {
        (if self.exact {
            path == self.path
        } else {
            self.path == "/"
                || path == self.path
                || path
                    .strip_prefix(&self.path)
                    .is_some_and(|tail| tail.starts_with('/'))
        }) && self.method.as_deref().is_none_or(|m| m == method.as_str())
            && self.headers.iter().all(|(k, v)| {
                headers
                    .get_all(k)
                    .iter()
                    .any(|value| value.as_bytes() == v.as_bytes())
            })
    }

    fn rank(&self) -> (bool, usize, bool, usize) {
        (
            self.exact,
            self.path.len(),
            self.method.is_some(),
            self.headers.len(),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Router {
    exact: BTreeMap<String, Vec<Route>>,
    wildcard: Vec<(String, Vec<Route>)>,
    fallback: Vec<Route>,
}

impl Router {
    /// input order resolves ties: creation time, namespace/name, rule and match order.
    pub fn compile(routes: Vec<Route>) -> Self {
        let mut buckets: BTreeMap<String, Vec<Route>> = BTreeMap::new();
        for mut route in routes {
            route.hostname.make_ascii_lowercase();
            for (name, _) in &mut route.headers {
                name.make_ascii_lowercase();
            }
            buckets
                .entry(route.hostname.clone())
                .or_default()
                .push(route);
        }
        let mut router = Self::default();
        for (hostname, mut routes) in buckets {
            // stable sorting preserves compiler-established tie order.
            routes.sort_by_key(|route| std::cmp::Reverse(route.rank()));
            if hostname.is_empty() {
                router.fallback = routes;
            } else if hostname.starts_with("*.") {
                router.wildcard.push((hostname, routes));
            } else {
                router.exact.insert(hostname, routes);
            }
        }
        router
            .wildcard
            .sort_by_key(|(hostname, _)| std::cmp::Reverse(hostname.len()));
        router
    }

    pub fn find(
        &self,
        hostname: &str,
        path: &str,
        method: &Method,
        headers: &HeaderMap,
    ) -> Option<&RouteAction> {
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
            .flat_map(|routes| routes.iter())
            .find(|route| route.matches(path, method, headers))
            .map(|route| &route.action)
    }

    pub fn len(&self) -> usize {
        self.routes().count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn routes(&self) -> impl Iterator<Item = &Route> {
        self.exact
            .values()
            .chain(self.wildcard.iter().map(|(_, routes)| routes))
            .chain(std::iter::once(&self.fallback))
            .flatten()
    }
}

pub fn hostname_matches(pattern: &str, host: &str) -> bool {
    pattern.is_empty()
        || pattern == host
        || pattern.strip_prefix("*.").is_some_and(|suffix| {
            host.strip_suffix(suffix)
                .is_some_and(|prefix| prefix.len() > 1 && prefix.ends_with('.'))
        })
}
