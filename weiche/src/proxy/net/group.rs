//! coordinate UDP socket ownership across workers and protocol changes.
use super::{super::worker::Worker, bind_socket, cid};
use compio::net::UdpSocket;
use socket2::{Protocol, Socket, Type};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

/// live groups indexed by address, shared across all compio runtimes.
#[derive(Clone, Default)]
pub struct Groups(
    /// address registry locked across group creation and retirement.
    Arc<Mutex<BTreeMap<SocketAddr, Group>>>,
);

/// sockets in kernel bind order and the protocol that currently owns them.
struct Group {
    /// whether the group serves QUIC or explicit UDP routes.
    quic: bool,
    /// each socket keeps its kernel index until the last reader finishes draining.
    sockets: Vec<Slot>,
}

/// stable kernel socket and its worker-local reader reservation.
struct Slot {
    /// bound for the entire lifetime of this reuseport group.
    socket: Socket,
    /// whether a listener is reading or draining on this socket.
    busy: bool,
}

impl Group {
    /// bind all sockets before attaching the group-wide filter or admitting clients.
    fn bind(addr: SocketAddr, workers: usize, quic: bool) -> io::Result<Self> {
        let sockets = (0..workers)
            .map(|_| {
                bind_socket(addr, Type::DGRAM, Protocol::UDP)
                    .map(|socket| Slot { socket, busy: false })
            })
            .collect::<io::Result<Vec<_>>>()?;
        if quic
            && workers > 1
            && let Ok(count) = u16::try_from(workers)
            && let Err(error) = cid::attach(&sockets[0].socket, count)
        {
            tracing::warn!(%addr, %error, "quic reuseport steering unavailable");
        }
        Ok(Self { quic, sockets })
    }
}

/// reserve one worker's socket until the listener finishes draining.
pub struct Reader {
    /// registry shared with workers waiting to replace the listener.
    groups: Groups,
    /// local address of the reserved group.
    addr: SocketAddr,
    /// worker index in Linux bind order.
    id: usize,
}

impl Drop for Reader {
    /// release the worker reservation and retire a group after its last reader.
    fn drop(&mut self) {
        let mut groups = self.groups.0.lock().unwrap();
        let group = groups.get_mut(&self.addr).expect("registered socket group");
        group.sockets[self.id].busy = false;
        if !group.sockets.iter().any(|slot| slot.busy) {
            // close every slot under the bind lock before creating a replacement group.
            groups.remove(&self.addr);
        }
    }
}

impl Worker {
    /// reserve this worker's reader and a descriptor in the ordered UDP group.
    pub fn datagram(
        &self,
        addr: SocketAddr,
        quic: bool,
    ) -> io::Result<(UdpSocket, Reader)> {
        let registry = &self.group.datagrams;
        let id = self.id;
        let mut groups = registry.0.lock().unwrap();
        let group = match groups.entry(addr) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                entry.insert(Group::bind(addr, self.runtime.workers.get(), quic)?)
            }
        };
        let slot = &mut group.sockets[id];
        if group.quic != quic || slot.busy {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "previous UDP listener is draining",
            ));
        }
        let socket = slot.socket.try_clone();
        slot.busy = true;
        let reader = Reader { groups: registry.clone(), addr, id };
        drop(groups);
        Ok((UdpSocket::from_std(socket?.into())?, reader))
    }
}
