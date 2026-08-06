use std::ffi::{c_char, c_int, c_void, CStr};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr;

const MAGIC: &[u8; 4] = b"ONIO";
const VERSION: u16 = 1;
const REQUEST_LEN: usize = 40;
const RESPONSE_LEN: usize = 72;
const BLOCK_SIZE: u32 = 4096;
const MAX_DEPTH: usize = 256;
const OP_HELLO: u16 = 1;
const OP_WRITE: u16 = 2;
const OP_READ: u16 = 3;
const OP_CLOSE: u16 = 4;
const DDIR_READ: c_int = 0;
const DDIR_WRITE: c_int = 1;
const FIO_Q_COMPLETED: c_int = 0;
const FIO_Q_QUEUED: c_int = 1;
const FIO_Q_BUSY: c_int = 2;

#[repr(C)]
pub struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

const POLLIN: i16 = 0x001;

#[repr(C)]
struct PollFd {
    fd: c_int,
    events: i16,
    revents: i16,
}

unsafe extern "C" {
    fn poll(fds: *mut PollFd, nfds: std::ffi::c_ulong, timeout: c_int) -> c_int;
}

/// Block until the socket is readable. `timeout_ms` follows `poll(2)`: negative
/// waits forever, `0` returns immediately. `Ok(false)` means the timeout expired.
///
/// This replaces an earlier `set_read_timeout` approach that was BOTH a wasted
/// `setsockopt` per loop iteration AND a latent bug: the "don't block" case
/// passed `Some(Duration::ZERO)`, which Rust rejects with `InvalidInput` (it
/// guards the POSIX footgun where `SO_RCVTIMEO = {0,0}` means "block forever").
/// The old code then returned `-EIO` and threw away every event it had already
/// collected. It never fired in the runs on file only because fio happened to
/// call with `min == max` there; other option combinations would hit it.
fn wait_readable(fd: c_int, timeout_ms: c_int) -> Result<bool, c_int> {
    let mut pfd = PollFd { fd, events: POLLIN, revents: 0 };
    loop {
        let rc = unsafe { poll(&mut pfd, 1, timeout_ms) };
        if rc > 0 {
            return Ok(true);
        }
        if rc == 0 {
            return Ok(false);
        }
        let code = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc_errno::EIO);
        if code != libc_errno::EINTR {
            return Err(code);
        }
    }
}

/// `poll(2)` millisecond timeout from fio's timespec. A sub-millisecond request
/// rounds UP to 1 ms: rounding down to 0 would turn fio's bounded wait into a
/// busy-spin.
fn timeout_ms(timeout: *const Timespec) -> c_int {
    if timeout.is_null() {
        return -1;
    }
    let t = unsafe { &*timeout };
    let ms = t.tv_sec.max(0).saturating_mul(1000) + t.tv_nsec.clamp(0, 999_999_999) / 1_000_000;
    if ms == 0 && (t.tv_sec > 0 || t.tv_nsec > 0) {
        return 1;
    }
    ms.min(c_int::MAX as i64) as c_int
}

#[derive(Clone, Copy, Default)]
struct Slot {
    id: u64,
    io_u: *mut c_void,
    buffer: *mut u8,
    len: u32,
    opcode: u16,
}

struct Client {
    stream: UnixStream,
    next_id: u64,
    depth: usize,
    slots: [Slot; MAX_DEPTH],
    completed: Vec<*mut c_void>,
    /// Requests staged by `queue` and flushed by `commit` as ONE write.
    ///
    /// Without this the engine paid two `write` syscalls per 4 KiB IO (header,
    /// then payload) with no batching at all, plus two more on the completion
    /// side — ~4 syscalls per IO, which is what capped the plugin at ~13 k IOPS
    /// regardless of iodepth and made it a worse load generator than ublk.
    /// Staging costs one 4 KiB memcpy per write, ~150 ns, against ~2-4 us for a
    /// syscall; a `writev` of per-slot iovecs would avoid even that, but the
    /// partial-write bookkeeping is easy to get silently wrong in a measurement
    /// tool, so the contiguous buffer + `write_all` is the deliberate choice.
    pending: Vec<u8>,
}

fn request(opcode: u16, payload_len: u32, id: u64, offset: u64, len: u32) -> [u8; REQUEST_LEN] {
    let mut out = [0; REQUEST_LEN];
    out[0..4].copy_from_slice(MAGIC);
    out[4..6].copy_from_slice(&VERSION.to_le_bytes());
    out[6..8].copy_from_slice(&opcode.to_le_bytes());
    out[12..16].copy_from_slice(&payload_len.to_le_bytes());
    out[16..24].copy_from_slice(&id.to_le_bytes());
    out[24..32].copy_from_slice(&offset.to_le_bytes());
    out[32..36].copy_from_slice(&len.to_le_bytes());
    out
}

fn u16_at(data: &[u8], at: usize) -> u16 { u16::from_le_bytes(data[at..at + 2].try_into().unwrap()) }
fn u32_at(data: &[u8], at: usize) -> u32 { u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) }
fn u64_at(data: &[u8], at: usize) -> u64 { u64::from_le_bytes(data[at..at + 8].try_into().unwrap()) }

fn errno(error: &io::Error) -> c_int { error.raw_os_error().unwrap_or(libc_errno::EIO) }

impl Client {
    fn connect(control: &str, volume: &str, depth: usize) -> io::Result<Self> {
        if depth == 0 || depth > MAX_DEPTH || volume.is_empty() || volume.len() > 255 {
            return Err(io::Error::from_raw_os_error(libc_errno::EINVAL));
        }
        let mut stream = UnixStream::connect(format!("{control}.io"))?;
        let hello = request(OP_HELLO, volume.len() as u32, 1, 0, 0);
        stream.write_all(&hello)?;
        stream.write_all(volume.as_bytes())?;
        let response = Self::read_header(&mut stream)?;
        let status = i32::from_le_bytes(response[8..12].try_into().unwrap());
        if u16_at(&response, 6) != OP_HELLO || status != 0 {
            return Err(io::Error::from_raw_os_error(if status < 0 { -status } else { libc_errno::EPROTO }));
        }
        Ok(Self {
            stream, next_id: 2, depth,
            slots: [Slot::default(); MAX_DEPTH],
            completed: Vec::with_capacity(depth),
            pending: Vec::with_capacity(depth * (REQUEST_LEN + BLOCK_SIZE as usize)),
        })
    }

    fn read_header(stream: &mut UnixStream) -> io::Result<[u8; RESPONSE_LEN]> {
        let mut response = [0; RESPONSE_LEN];
        stream.read_exact(&mut response)?;
        if &response[0..4] != MAGIC || u16_at(&response, 4) != VERSION {
            return Err(io::Error::from_raw_os_error(libc_errno::EPROTO));
        }
        Ok(response)
    }

    unsafe fn queue(&mut self, io_u: *mut c_void, ddir: c_int, offset: u64,
                    buffer: *mut u8, len: u32) -> Result<c_int, c_int> {
        let opcode = match ddir {
            DDIR_READ => OP_READ,
            DDIR_WRITE => OP_WRITE,
            _ => return Err(libc_errno::EOPNOTSUPP),
        };
        if len != BLOCK_SIZE || offset % BLOCK_SIZE as u64 != 0 || buffer.is_null() {
            return Err(libc_errno::EINVAL);
        }
        let id = self.next_id;
        let index = id as usize % self.depth;
        // FIO_Q_BUSY is fio's "no more room, call ->commit()", which is exactly
        // what a full slot ring means. It also bounds `pending` to
        // depth * (REQUEST_LEN + BLOCK_SIZE).
        if !self.slots[index].io_u.is_null() { return Ok(FIO_Q_BUSY); }
        let header = request(opcode, if opcode == OP_WRITE { len } else { 0 }, id, offset, len);
        self.pending.extend_from_slice(&header);
        if opcode == OP_WRITE {
            let payload = unsafe { std::slice::from_raw_parts(buffer, len as usize) };
            self.pending.extend_from_slice(payload);
        }
        self.next_id = self.next_id.wrapping_add(1);
        self.slots[index] = Slot { id, io_u, buffer, len, opcode };
        Ok(FIO_Q_QUEUED)
    }

    /// Flush every request staged since the last commit in ONE write.
    fn commit(&mut self) -> Result<(), c_int> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let result = self.stream.write_all(&self.pending).map_err(|e| errno(&e));
        // Clear either way: a short/failed write leaves the session
        // unrecoverable, and fio aborts the job on a commit error, so retrying
        // the same bytes would only desynchronise the protocol further.
        self.pending.clear();
        result
    }

    fn collect_one(&mut self) -> Result<(), c_int> {
        let response = Self::read_header(&mut self.stream).map_err(|e| errno(&e))?;
        let id = u64_at(&response, 16);
        let index = id as usize % self.depth;
        let slot = self.slots[index];
        if slot.io_u.is_null() || slot.id != id || u16_at(&response, 6) != slot.opcode {
            return Err(libc_errno::EPROTO);
        }
        let status = i32::from_le_bytes(response[8..12].try_into().unwrap());
        let bytes = u32_at(&response, 24);
        let payload_len = u32_at(&response, 28);
        if status < 0 || bytes != slot.len { return Err(if status < 0 { -status } else { libc_errno::EIO }); }
        if payload_len != 0 {
            if slot.opcode != OP_READ || payload_len != slot.len { return Err(libc_errno::EPROTO); }
            let payload = unsafe { std::slice::from_raw_parts_mut(slot.buffer, payload_len as usize) };
            self.stream.read_exact(payload).map_err(|e| errno(&e))?;
        }
        self.slots[index] = Slot::default();
        self.completed.push(slot.io_u);
        Ok(())
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_init(socket: *const c_char, volume: *const c_char,
                                       depth: u32, error: *mut c_int) -> *mut c_void {
    let result = (|| {
        if socket.is_null() || volume.is_null() { return Err(libc_errno::EINVAL); }
        let socket = unsafe { CStr::from_ptr(socket) }.to_str().map_err(|_| libc_errno::EINVAL)?;
        let volume = unsafe { CStr::from_ptr(volume) }.to_str().map_err(|_| libc_errno::EINVAL)?;
        Client::connect(socket, volume, depth as usize).map_err(|e| errno(&e))
    })();
    match result {
        Ok(client) => Box::into_raw(Box::new(client)).cast(),
        Err(code) => { if !error.is_null() { unsafe { *error = code; } } ptr::null_mut() }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_queue(client: *mut c_void, io_u: *mut c_void,
    ddir: c_int, offset: u64, buffer: *mut c_void, len: u32, error: *mut c_int) -> c_int {
    let client = unsafe { &mut *client.cast::<Client>() };
    match unsafe { client.queue(io_u, ddir, offset, buffer.cast(), len) } {
        Ok(status) => status,
        Err(code) => { if !error.is_null() { unsafe { *error = code; } } FIO_Q_COMPLETED }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_getevents(client: *mut c_void, min: u32, max: u32,
                                            timeout: *const Timespec) -> c_int {
    let client = unsafe { &mut *client.cast::<Client>() };
    client.completed.clear();
    let fd = client.stream.as_raw_fd();
    let block_ms = timeout_ms(timeout);
    while client.completed.len() < max as usize {
        // Below `min` we may wait out fio's timeout; at or above it we may only
        // reap what has already arrived and must never block.
        let wait = if client.completed.len() >= min as usize { 0 } else { block_ms };
        match wait_readable(fd, wait) {
            Ok(true) => {}
            Ok(false) => break,
            Err(code) => return -code,
        }
        match client.collect_one() {
            Ok(()) => {}
            Err(code) if code == libc_errno::EAGAIN || code == libc_errno::EWOULDBLOCK => break,
            Err(code) => return -code,
        }
    }
    client.completed.len() as c_int
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_commit(client: *mut c_void) -> c_int {
    let client = unsafe { &mut *client.cast::<Client>() };
    match client.commit() {
        Ok(()) => 0,
        Err(code) => -code,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_event(client: *mut c_void, event: c_int) -> *mut c_void {
    let client = unsafe { &mut *client.cast::<Client>() };
    client.completed.get(event as usize).copied().unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_cleanup(client: *mut c_void) {
    if client.is_null() { return; }
    let mut client = unsafe { Box::from_raw(client.cast::<Client>()) };
    let close = request(OP_CLOSE, 0, client.next_id, 0, 0);
    let _ = client.stream.write_all(&close);
}

mod libc_errno {
    pub const EINTR: i32 = 4;
    pub const EIO: i32 = 5;
    pub const EAGAIN: i32 = 11;
    pub const EWOULDBLOCK: i32 = EAGAIN;
    pub const EINVAL: i32 = 22;
    pub const EPROTO: i32 = 71;
    pub const EOPNOTSUPP: i32 = 95;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `poll(2)` semantics, and the reason `set_read_timeout` could not be used:
    /// Rust rejects a zero `Duration` outright, so the old "don't block" path
    /// errored and discarded already-collected events.
    #[test]
    fn timeout_ms_maps_poll_semantics() {
        assert_eq!(timeout_ms(ptr::null()), -1, "null timeout blocks forever");
        let zero = Timespec { tv_sec: 0, tv_nsec: 0 };
        assert_eq!(timeout_ms(&zero), 0, "an explicit zero must not block");
        let sub_ms = Timespec { tv_sec: 0, tv_nsec: 100_000 };
        assert_eq!(timeout_ms(&sub_ms), 1, "sub-ms rounds UP, never to a spin");
        let two_and_a_half = Timespec { tv_sec: 2, tv_nsec: 500_000_000 };
        assert_eq!(timeout_ms(&two_and_a_half), 2500);
        let negative = Timespec { tv_sec: -5, tv_nsec: -5 };
        assert_eq!(timeout_ms(&negative), 0, "negatives clamp, never wrap");

        // The exact call the old code made, kept as a guard so nobody
        // reintroduces it.
        let (a, _b) = UnixStream::pair().unwrap();
        assert!(a.set_read_timeout(Some(std::time::Duration::ZERO)).is_err());
    }

    /// `queue` must stage, not send: that is the whole point of the commit hook.
    #[test]
    fn queue_stages_header_then_payload_and_commit_drains() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = Client {
            stream: a,
            next_id: 2,
            depth: 4,
            slots: [Slot::default(); MAX_DEPTH],
            completed: Vec::new(),
            pending: Vec::new(),
        };
        let mut payload = [0xABu8; BLOCK_SIZE as usize];
        payload[0] = 0x5A;
        let io_u = 0x1000usize as *mut c_void;

        let status = unsafe {
            client.queue(io_u, DDIR_WRITE, 8192, payload.as_mut_ptr(), BLOCK_SIZE)
        };
        assert_eq!(status, Ok(FIO_Q_QUEUED));
        assert_eq!(
            client.pending.len(),
            REQUEST_LEN + BLOCK_SIZE as usize,
            "header and payload both staged"
        );
        assert_eq!(&client.pending[0..4], MAGIC);
        assert_eq!(u64_at(&client.pending, 24), 8192, "offset survives staging");
        assert_eq!(client.pending[REQUEST_LEN], 0x5A, "payload follows the header");

        client.commit().unwrap();
        assert!(client.pending.is_empty(), "commit drains the staging buffer");
        let mut got = vec![0u8; REQUEST_LEN + BLOCK_SIZE as usize];
        b.read_exact(&mut got).unwrap();
        assert_eq!(u16_at(&got, 6), OP_WRITE);
        assert_eq!(got[REQUEST_LEN], 0x5A);

        // A full slot ring is fio's cue to commit, not an error.
        for i in 0..3 {
            let st = unsafe {
                client.queue(
                    (0x2000 + i) as *mut c_void, DDIR_WRITE,
                    (i as u64 + 2) * 4096, payload.as_mut_ptr(), BLOCK_SIZE,
                )
            };
            assert_eq!(st, Ok(FIO_Q_QUEUED));
        }
        let busy = unsafe {
            client.queue(io_u, DDIR_WRITE, 4096 * 99, payload.as_mut_ptr(), BLOCK_SIZE)
        };
        assert_eq!(busy, Ok(FIO_Q_BUSY), "ring full => BUSY, bounding `pending`");
    }

    #[test]
    fn request_encoding_matches_protocol() {
        let encoded = request(OP_WRITE, 4096, 0x1122, 8192, 4096);
        assert_eq!(&encoded[0..4], b"ONIO");
        assert_eq!(u16_at(&encoded, 6), OP_WRITE);
        assert_eq!(u32_at(&encoded, 12), 4096);
        assert_eq!(u64_at(&encoded, 16), 0x1122);
        assert_eq!(u64_at(&encoded, 24), 8192);
        assert_eq!(u32_at(&encoded, 32), 4096);
        assert_eq!(&encoded[36..40], &[0; 4]);
    }
}
