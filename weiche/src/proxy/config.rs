//! configure the workers and publish immutable gateway snapshots.
use super::super::ir::{GatewaySnapshot, PublishedSnapshot, Revision};
use anyhow::{Context, ensure};
use std::{
    env::{self, VarError},
    num::NonZeroUsize,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

/// requested payload capacity in bytes for each direction of a listener socket.
#[derive(Clone, Copy, Debug, Default)]
pub struct SocketBuffers {
    pub recv: Option<usize>,
    pub send: Option<usize>,
}

/// listener queue and stream settings for TCP.
#[derive(Clone, Copy, Debug)]
pub struct TcpConfig {
    pub backlog: i32,
    pub nodelay: bool,
    pub buffers: SocketBuffers,
    pub connect_timeout: Duration,
}

/// downstream UDP socket sizing and per-peer flow lifetime.
#[derive(Clone, Debug)]
pub struct UdpConfig {
    pub buffers: SocketBuffers,
    pub idle_timeout: Duration,
}

/// downstream HTTP/3 advertisement and QUIC transport settings.
#[derive(Clone, Debug)]
pub struct QuicConfig {
    /// serve downstream HTTP/3 alongside terminating HTTPS listeners.
    pub enabled: bool,
    /// externally reachable UDP port when different from the listener port.
    pub advertised_port: Option<u16>,
    pub buffers: SocketBuffers,
    pub max_concurrent_bidi_streams: u32,
    pub idle_timeout: Duration,
}

/// process-wide limits and downstream listener policy.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub tcp: TcpConfig,
    pub udp: UdpConfig,
    pub quic: QuicConfig,
    /// number of worker runtimes sharing the listener sockets.
    pub workers: NonZeroUsize,
    /// maximum downstream connections and flows across workers.
    pub downstream_connections_global: NonZeroUsize,
    /// maximum backend connections on each worker.
    pub upstream_connections_per_worker: NonZeroUsize,
    /// idle timeout for pooled HTTP backends.
    pub upstream_idle_timeout: Duration,
    /// maximum time a downstream socket may occupy a permit before TLS completes.
    pub tls_handshake_timeout: Duration,
    /// grace period for requests on a closing connection.
    pub connection_drain_timeout: Duration,
    /// maximum time a worker waits for a listener to drain.
    pub worker_drain_timeout: Duration,
    /// delay before retrying an unavailable socket.
    pub listener_retry_interval: Duration,
    /// maximum HTTP/1 header count.
    pub http_max_headers: usize,
    /// maximum HTTP/1 buffer and multiplexed HTTP field section size.
    pub http_max_buffer: usize,
    /// maximum concurrent HTTP/2 streams.
    pub http2_max_concurrent_streams: u32,
}

impl RuntimeConfig {
    /// use the default connection limits and disable downstream HTTP/3.
    pub fn new(workers: NonZeroUsize) -> Self {
        Self {
            tcp: TcpConfig {
                backlog: 1024,
                nodelay: true,
                // leave both buffers unlocked for linux TCP auto-tuning.
                buffers: SocketBuffers::default(),
                connect_timeout: Duration::from_secs(10),
            },
            udp: UdpConfig {
                buffers: SocketBuffers {
                    recv: Some(2 * 1024 * 1024),
                    send: Some(2 * 1024 * 1024),
                },
                idle_timeout: Duration::from_secs(30),
            },
            quic: QuicConfig {
                enabled: false,
                advertised_port: None,
                buffers: SocketBuffers {
                    recv: Some(4 * 1024 * 1024),
                    send: Some(4 * 1024 * 1024),
                },
                max_concurrent_bidi_streams: 128,
                idle_timeout: Duration::from_secs(30),
            },
            workers,
            downstream_connections_global: NonZeroUsize::new(1024).unwrap(),
            upstream_connections_per_worker: NonZeroUsize::new(1024).unwrap(),
            upstream_idle_timeout: Duration::from_secs(30),
            tls_handshake_timeout: Duration::from_secs(10),
            connection_drain_timeout: Duration::from_secs(5),
            worker_drain_timeout: Duration::from_secs(6),
            listener_retry_interval: Duration::from_millis(250),
            http_max_headers: 100,
            http_max_buffer: 32 * 1024,
            http2_max_concurrent_streams: 128,
        }
    }

    /// load and validate process policy from the proxy environment.
    pub fn from_env() -> anyhow::Result<Self> {
        let workers =
            setting("WEICHE_WORKERS", "WEICHE_WORKERS must be a positive integer")?;
        // leave CPU placement to the scheduler. each worker owns one compio runtime.
        let mut config = Self::new(match workers {
            Some(count) => count,
            None => std::thread::available_parallelism()?,
        });
        config.quic.enabled = match env::var("WEICHE_HTTP3").as_deref() {
            Ok("true" | "1") => true,
            Ok("false" | "0") | Err(VarError::NotPresent) => false,
            _ => anyhow::bail!("WEICHE_HTTP3 must be true, 1, false or 0"),
        };
        config.quic.advertised_port =
            setting("WEICHE_HTTP3_PORT", "invalid WEICHE_HTTP3_PORT")?;
        config.validate()?;
        Ok(config)
    }

    /// check parser, listener and drain limits before starting the workers.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.quic.advertised_port != Some(0),
            "WEICHE_HTTP3_PORT must be positive"
        );
        ensure!(self.tcp.backlog > 0, "tcp backlog must be positive");
        ensure!(
            self.quic.max_concurrent_bidi_streams > 0,
            "quic stream limit must be positive"
        );
        ensure!(
            self.quic.idle_timeout.as_millis() > 0
                && compio::quic::IdleTimeout::try_from(self.quic.idle_timeout).is_ok(),
            "quic idle timeout must fit a positive QUIC millisecond interval"
        );
        for (protocol, buffers) in [
            ("tcp", self.tcp.buffers),
            ("udp", self.udp.buffers),
            ("quic", self.quic.buffers),
        ] {
            for (direction, size) in [("receive", buffers.recv), ("send", buffers.send)] {
                ensure!(
                    size.is_none_or(|n| (1..=i32::MAX as usize).contains(&n)),
                    "{protocol} {direction} buffer size must fit a positive socket option"
                );
            }
        }
        ensure!(
            self.http_max_headers > 0
                && (8192..=u32::MAX as usize).contains(&self.http_max_buffer)
                && self.http2_max_concurrent_streams > 0,
            "invalid HTTP parser limits"
        );
        for (timeout, message) in [
            (self.listener_retry_interval, "listener retry interval must be positive"),
            (self.tcp.connect_timeout, "tcp connect timeout must be positive"),
            (self.udp.idle_timeout, "udp idle timeout must be positive"),
        ] {
            ensure!(!timeout.is_zero(), message);
        }
        ensure!(
            self.worker_drain_timeout >= self.connection_drain_timeout,
            "worker drain must allow connection drain to finish"
        );
        Ok(())
    }
}

/// parse an optional numeric setting, preserving errors for non-unicode values.
fn setting<T: FromStr>(name: &str, invalid: &'static str) -> anyhow::Result<Option<T>>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => Ok(Some(value.parse().context(invalid)?)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// single-writer handle owned by the proxy watch thread.
pub struct Writer {
    /// publishes the current snapshot to all workers.
    tx: watch::Sender<Arc<PublishedSnapshot>>,
}

/// worker-side handle observing the latest published snapshot.
pub type Reader = watch::Receiver<Arc<PublishedSnapshot>>;

/// create a snapshot publication channel starting at revision zero.
pub fn channel(initial: GatewaySnapshot) -> (Writer, Reader) {
    let initial = Arc::new(PublishedSnapshot { revision: 0, gateway: initial });
    let (tx, rx) = watch::channel(initial);
    (Writer { tx }, rx)
}

impl Writer {
    /// publish a new revision unless the snapshot is identical to the current one.
    pub fn publish(&mut self, gateway: GatewaySnapshot) -> Revision {
        let current = self.tx.borrow();
        if current.gateway == gateway {
            return current.revision;
        }
        let next = current
            .revision
            .checked_add(1)
            .expect("configuration revision exhausted");
        drop(current);
        self.tx
            .send_replace(Arc::new(PublishedSnapshot { revision: next, gateway }));
        next
    }
}
