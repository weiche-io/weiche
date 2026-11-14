//! publish immutable routing snapshots through two independently locked slots.
use super::snapshot::{GatewaySnapshot, PublishedSnapshot, Revision};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::watch;

struct Shared {
    slots: [RwLock<Arc<PublishedSnapshot>>; 2],
    generation: AtomicUsize,
}

/// the control thread owns the only writer; publication requires mutable access.
pub struct Writer {
    shared: Arc<Shared>,
    notify: watch::Sender<()>,
}

#[derive(Clone)]
pub struct Reader {
    shared: Arc<Shared>,
    notify: watch::Receiver<()>,
}

pub fn channel(initial: GatewaySnapshot) -> (Writer, Reader) {
    let initial = Arc::new(PublishedSnapshot {
        revision: 0,
        gateway: initial,
    });
    let shared = Arc::new(Shared {
        slots: [RwLock::new(initial.clone()), RwLock::new(initial)],
        generation: AtomicUsize::new(0),
    });
    let (tx, rx) = watch::channel(());
    (
        Writer {
            shared: shared.clone(),
            notify: tx,
        },
        Reader { shared, notify: rx },
    )
}

impl Writer {
    pub fn publish(&mut self, gateway: GatewaySnapshot) -> Revision {
        let generation = self.shared.generation.load(Ordering::Relaxed);
        if self.shared.slots[generation & 1].read().unwrap().gateway == gateway {
            return generation;
        }
        let next = generation
            .checked_add(1)
            .expect("configuration revision exhausted");
        let table = Arc::new(PublishedSnapshot {
            revision: next,
            gateway,
        });
        *self.shared.slots[next & 1].write().unwrap() = table;
        // finish the inactive slot before making it visible to readers.
        self.shared.generation.store(next, Ordering::Release);
        self.notify.send_replace(());
        next
    }
}

impl Reader {
    pub fn snapshot(&self) -> Arc<PublishedSnapshot> {
        loop {
            let generation = self.shared.generation.load(Ordering::Acquire);
            let slot = self.shared.slots[generation & 1].read().unwrap();
            // a writer may have flipped twice while this reader waited for a lock.
            if self.shared.generation.load(Ordering::Acquire) == generation {
                return slot.clone();
            }
        }
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.notify.changed().await
    }
}
