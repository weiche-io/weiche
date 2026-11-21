//! publish immutable routing snapshots through a watch channel.
use super::ir::{GatewaySnapshot, PublishedSnapshot, Revision};
use std::sync::Arc;
use tokio::sync::watch;

/// single-writer handle owned by the proxy watch thread.
pub struct Writer {
    tx: watch::Sender<Arc<PublishedSnapshot>>,
}

/// worker-side handle observing the latest published snapshot.
#[derive(Clone)]
pub struct Reader {
    rx: watch::Receiver<Arc<PublishedSnapshot>>,
}

/// create a snapshot publication channel starting at revision zero.
pub fn channel(initial: GatewaySnapshot) -> (Writer, Reader) {
    let initial = Arc::new(PublishedSnapshot { revision: 0, gateway: initial });
    let (tx, rx) = watch::channel(initial);
    (Writer { tx }, Reader { rx })
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

impl Reader {
    /// clone the latest published snapshot and mark it observed.
    pub fn snapshot(&mut self) -> Arc<PublishedSnapshot> {
        self.rx.borrow_and_update().clone()
    }

    /// wait until a newer snapshot is published or the writer closes.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.rx.changed().await
    }
}
