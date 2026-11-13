//! process policy; connection budgets explicitly distinguish global and worker scope.
use anyhow::Context;
use std::{num::NonZeroUsize, time::Duration};

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub workers: NonZeroUsize,
    pub downstream_connections_global: NonZeroUsize,
    pub upstream_connections_per_worker: NonZeroUsize,
    pub upstream_idle_timeout: Duration,
    pub connection_drain_timeout: Duration,
    pub worker_drain_timeout: Duration,
    pub listener_retry_interval: Duration,
    pub http_max_headers: usize,
    pub http_max_buffer: usize,
}

impl RuntimeConfig {
    pub fn new(workers: NonZeroUsize) -> Self {
        Self {
            workers,
            downstream_connections_global: NonZeroUsize::new(1024).unwrap(),
            upstream_connections_per_worker: NonZeroUsize::new(1024).unwrap(),
            upstream_idle_timeout: Duration::from_secs(30),
            connection_drain_timeout: Duration::from_secs(5),
            worker_drain_timeout: Duration::from_secs(6),
            listener_retry_interval: Duration::from_millis(250),
            http_max_headers: 100,
            http_max_buffer: 32 * 1024,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let workers = match std::env::var("WEICHE_WORKERS") {
            Ok(value) => value
                .parse()
                .context("WEICHE_WORKERS must be a positive integer")?,
            Err(std::env::VarError::NotPresent) => std::thread::available_parallelism()?,
            Err(error) => return Err(error.into()),
        };
        // leave CPU placement to the scheduler; each worker owns one compio runtime.
        Ok(Self::new(workers))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.http_max_headers > 0 && self.http_max_buffer >= 8192,
            "invalid HTTP parser limits"
        );
        anyhow::ensure!(
            !self.listener_retry_interval.is_zero(),
            "listener retry interval must be positive"
        );
        anyhow::ensure!(
            self.worker_drain_timeout >= self.connection_drain_timeout,
            "worker drain must allow connection drain to finish"
        );
        Ok(())
    }
}
