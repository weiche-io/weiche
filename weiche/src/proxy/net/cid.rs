//! steer server-issued connection IDs to their issuing worker after peer migration.
use compio::quic::{ConnectionId, ConnectionIdGenerator};
use libc::{
    BPF_A, BPF_ABS, BPF_ALU, BPF_AND, BPF_B, BPF_H, BPF_JA, BPF_JEQ, BPF_JGE, BPF_JMP,
    BPF_JSET, BPF_LD, BPF_LEN, BPF_MOD, BPF_RET, BPF_W, sock_filter,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::{io, os::fd::AsRawFd, time::Duration};

/// eight-byte CID with a stable reuseport index and a fresh random suffix.
pub struct Generator(pub u16);

impl ConnectionIdGenerator for Generator {
    /// encode the worker in network byte order.
    fn generate_cid(&mut self) -> ConnectionId {
        let mut cid = [0; 8];
        cid[..2].copy_from_slice(&self.0.to_be_bytes());
        SystemRandom::new().fill(&mut cid[2..]).expect("system randomness");
        ConnectionId::new(&cid)
    }

    /// reserve two routing bytes and six random bytes.
    fn cid_len(&self) -> usize {
        8
    }

    /// allow the peer to retire its connection IDs normally.
    fn cid_lifetime(&self) -> Option<Duration> {
        None
    }
}

/// build a classic reuseport filter for QUIC v1. both Linux UDP lookup paths pass
/// sizeof(struct udphdr) to run_bpf_filter, which pulls it before running cBPF.
/// absolute offset zero therefore addresses the first QUIC byte, including on IPv6.
fn program(workers: u16) -> [sock_filter; 18] {
    let op = |code, k| sock_filter { code: code as u16, jt: 0, jf: 0, k };
    let jump = |code, k, jt, jf| sock_filter { code: (BPF_JMP | code) as u16, jt, jf, k };
    [
        op(BPF_LD | BPF_LEN, 0),
        jump(BPF_JGE, 3, 0, 13), // too short: hash
        op(BPF_LD | BPF_B | BPF_ABS, 0),
        jump(BPF_JSET, 0x80, 2, 0), // long header: inspect type
        op(BPF_LD | BPF_H | BPF_ABS, 1),
        op(BPF_JMP | BPF_JA, 10), // short header: reduce worker index
        op(BPF_ALU | BPF_AND, 0x30),
        jump(BPF_JEQ, 0x20, 1, 0), // handshake
        jump(BPF_JEQ, 0x10, 0, 6), // zero-rtt, otherwise hash
        op(BPF_LD | BPF_LEN, 0),
        jump(BPF_JGE, 8, 0, 4),
        op(BPF_LD | BPF_B | BPF_ABS, 5),
        jump(BPF_JEQ, 8, 0, 2),
        op(BPF_LD | BPF_H | BPF_ABS, 6),
        op(BPF_JMP | BPF_JA, 1),
        op(BPF_LD | BPF_W | BPF_ABS, (libc::SKF_AD_OFF + libc::SKF_AD_RXHASH) as u32),
        op(BPF_ALU | BPF_MOD, u32::from(workers)),
        op(BPF_RET | BPF_A, 0),
    ]
}

/// attach once to the bound group. Linux copies the program during setsockopt.
pub fn attach(socket: &socket2::Socket, workers: u16) -> io::Result<()> {
    let mut code = program(workers);
    let filter = libc::sock_fprog { len: code.len() as u16, filter: code.as_mut_ptr() };
    // safety: both the descriptor and the instruction array live through the call.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_CBPF,
            (&raw const filter).cast(),
            size_of_val(&filter) as libc::socklen_t,
        )
    };
    if result == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}
