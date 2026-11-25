//! coordinate UDP socket ownership across workers and protocol changes.
use super::super::super::log;
use super::{super::config::SocketBuffers, bind_socket, cid};
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

/// protocol reading datagrams from a reuseport group.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DatagramMode {
    Udp,
    Quic,
}

/// sockets in kernel bind order and the protocol that currently owns them.
struct Group {
    mode: DatagramMode,
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
    fn bind(
        addr: SocketAddr,
        workers: usize,
        mode: DatagramMode,
        buffers: SocketBuffers,
    ) -> io::Result<Self> {
        let sockets = (0..workers)
            .map(|id| {
                let socket = bind_socket(addr, Type::DGRAM, Protocol::UDP, buffers, id)?;
                Ok(Slot { socket, busy: false })
            })
            .collect::<io::Result<Vec<_>>>()?;
        if mode == DatagramMode::Quic
            && workers > 1
            && let Ok(count) = u16::try_from(workers)
            && let Err(error) = cid::attach(&sockets[0].socket, count)
        {
            log!(
                warn,
                "quic reuseport steering unavailable",
                addr = addr.to_string(),
                error = error.to_string()
            );
        }
        Ok(Self { mode, sockets })
    }

    /// claim the slot only after attaching a descriptor to the worker runtime.
    fn acquire(&mut self, id: usize, mode: DatagramMode) -> io::Result<UdpSocket> {
        let slot = self.sockets.get_mut(id).ok_or(io::ErrorKind::InvalidInput)?;
        if self.mode != mode || slot.busy {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "previous udp listener is draining",
            ));
        }
        let socket = UdpSocket::from_std(slot.socket.try_clone()?.into())?;
        slot.busy = true;
        Ok(socket)
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

impl Groups {
    /// publish a new group only after its first worker has acquired a socket.
    pub fn acquire(
        &self,
        addr: SocketAddr,
        id: usize,
        workers: usize,
        mode: DatagramMode,
        buffers: SocketBuffers,
    ) -> io::Result<(UdpSocket, Reader)> {
        let mut groups = self.0.lock().unwrap();
        let socket = match groups.entry(addr) {
            Entry::Occupied(mut entry) => entry.get_mut().acquire(id, mode)?,
            Entry::Vacant(entry) => {
                let mut group = Group::bind(addr, workers, mode, buffers)?;
                let socket = group.acquire(id, mode)?;
                entry.insert(group);
                socket
            }
        };
        Ok((socket, Reader { groups: self.clone(), addr, id }))
    }
}
