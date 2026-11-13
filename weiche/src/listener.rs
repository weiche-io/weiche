//! listener-group coordination and per-worker configuration application reports.
use super::{routing, snapshot::Revision};
use compio::net::TcpListener;
use std::{net::SocketAddr, sync::Mutex};
use tokio::sync::watch;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStatus {
    pub requested: Option<SocketAddr>,
    pub bound: Option<SocketAddr>,
    pub applied: Option<Revision>,
    pub failed_revision: Option<Revision>,
    pub apply_error: Option<String>,
    pub listener_error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerGroupState {
    pub published: Revision,
    pub applied: Option<Revision>,
    pub bound: Option<SocketAddr>,
    pub error: Option<String>,
    pub workers: Vec<WorkerStatus>,
}

impl WorkerGroupState {
    pub fn programmed(&self, revision: Revision, listener: Option<SocketAddr>) -> bool {
        self.published == revision
            && self.error.is_none()
            && self.applied == Some(revision)
            && self.bound.is_some()
            && self.bound == listener
    }
}

pub struct ListenerGroup {
    workers: Mutex<Vec<WorkerStatus>>,
    config: routing::Reader,
    state: watch::Sender<WorkerGroupState>,
}

impl ListenerGroup {
    pub fn new(
        count: usize,
        config: routing::Reader,
        state: watch::Sender<WorkerGroupState>,
    ) -> Self {
        Self {
            workers: Mutex::new(vec![WorkerStatus::default(); count]),
            config,
            state,
        }
    }

    pub fn applied(&self, id: usize, revision: Revision) {
        let mut workers = self.workers.lock().unwrap();
        workers[id].applied = Some(revision);
        workers[id].failed_revision = None;
        workers[id].apply_error = None;
        self.publish(&workers);
    }

    pub fn failed(&self, id: usize, revision: Revision, error: &anyhow::Error) {
        let mut workers = self.workers.lock().unwrap();
        workers[id].failed_revision = Some(revision);
        workers[id].apply_error = Some(format!("{error:#}"));
        self.publish(&workers);
    }

    pub fn bind(&self, id: usize, requested: SocketAddr) -> std::io::Result<TcpListener> {
        // only configuration changes take this lock; native accept never does.
        let mut workers = self.workers.lock().unwrap();
        // port-zero listeners join the same kernel-selected port.
        let address = workers
            .iter()
            .find(|worker| worker.requested == Some(requested) && worker.bound.is_some())
            .and_then(|worker| worker.bound)
            .unwrap_or(requested);
        let result = listen(address).and_then(|socket| Ok((socket.local_addr()?, socket)));
        let worker = &mut workers[id];
        worker.requested = Some(requested);
        match &result {
            Ok((address, _)) => {
                worker.bound = Some(*address);
                worker.listener_error = None;
            }
            Err(error) => {
                worker.bound = None;
                worker.listener_error = Some(error.to_string());
            }
        }
        self.publish(&workers);
        result.map(|(_, socket)| socket)
    }

    pub fn clear_listener(&self, id: usize) {
        let mut workers = self.workers.lock().unwrap();
        workers[id].requested = None;
        workers[id].bound = None;
        workers[id].listener_error = None;
        self.publish(&workers);
    }

    pub fn stopped(&self, id: usize) {
        let mut workers = self.workers.lock().unwrap();
        workers[id] = WorkerStatus::default();
        self.publish(&workers);
    }

    fn publish(&self, workers: &[WorkerStatus]) {
        let desired = self.config.snapshot();
        let error = workers.iter().enumerate().find_map(|(id, worker)| {
            worker
                .apply_error
                .as_ref()
                .or(worker.listener_error.as_ref())
                .map(|error| format!("worker {id}: {error}"))
        });
        let bound = workers[0].bound.filter(|address| {
            desired.gateway.listener.is_some()
                && workers.iter().all(|worker| {
                    worker.requested == desired.gateway.listener && worker.bound == Some(*address)
                })
        });
        let applied = workers[0].applied.filter(|revision| {
            workers
                .iter()
                .all(|worker| worker.applied == Some(*revision))
        });
        let next = WorkerGroupState {
            published: desired.revision,
            applied,
            bound,
            error,
            workers: workers.to_vec(),
        };
        self.state.send_if_modified(|old| {
            if *old == next {
                false
            } else {
                *old = next;
                true
            }
        });
    }
}

fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    TcpListener::from_std(socket.into())
}
