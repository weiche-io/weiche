//! cluster membership and worker-local round-robin state, separate from HTTP pools.
use std::{
    cell::Cell,
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    rc::Rc,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId {
    pub namespace: String,
    pub service: String,
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterSpec {
    pub endpoints: Vec<SocketAddr>,
}

struct Cluster {
    spec: ClusterSpec,
    cursor: Cell<usize>,
}

#[derive(Default)]
pub struct Clusters(BTreeMap<ClusterId, Rc<Cluster>>);

impl Clusters {
    pub fn prepare(specs: &BTreeMap<ClusterId, ClusterSpec>, previous: &Self) -> Self {
        Self(
            specs
                .iter()
                .map(|(id, spec)| {
                    let old = previous.0.get(id);
                    let cluster = match old {
                        Some(old) if old.spec == *spec => old.clone(),
                        _ => Rc::new(Cluster {
                            spec: spec.clone(),
                            cursor: Cell::new(old.map_or(0, |cluster| cluster.cursor.get())),
                        }),
                    };
                    (id.clone(), cluster)
                })
                .collect(),
        )
    }

    pub fn select(&self, id: &ClusterId) -> Option<SocketAddr> {
        let cluster = self.0.get(id)?;
        if cluster.spec.endpoints.is_empty() {
            return None;
        }
        let index = cluster.cursor.get();
        cluster.cursor.set(index.wrapping_add(1));
        Some(cluster.spec.endpoints[index % cluster.spec.endpoints.len()])
    }

    pub fn endpoints(&self) -> HashSet<SocketAddr> {
        self.0
            .values()
            .flat_map(|cluster| cluster.spec.endpoints.iter().copied())
            .collect()
    }
}
