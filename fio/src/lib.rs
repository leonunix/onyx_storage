use std::ffi::{c_char, c_int, c_void, CStr};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr;
use std::time::Instant;

const MAGIC: &[u8; 4] = b"ONIO";
const VERSION: u16 = 2;
const REQUEST_LEN: usize = 48;
const RESPONSE_LEN: usize = 96;
/// Offset of `RequestHeader::client_submit_ns`, patched into an already-staged
/// header by `commit()`. See `Client::commit`.
const REQUEST_SUBMIT_NS_AT: usize = 40;
/// Largest single response frame: header plus a full read payload.
const MAX_FRAME: usize = RESPONSE_LEN + BLOCK_SIZE as usize;
/// Receive buffer, sized so one `read(2)` can deliver many completions.
///
/// The reap loop used to cost THREE syscalls per completion — `poll`, then
/// `read_exact` for the 96-byte header, then `read_exact` for the 4 KiB
/// payload — and each fio job is a single thread doing that serially for every
/// response it has outstanding. That showed up in the protocol's own ledger as
/// `egress_ns` (server `write` -> client finished reading) becoming the largest
/// segment of the round trip once the server side stopped being the wall.
/// Reading into a buffer this size amortises the syscalls over up to 64
/// completions at the cost of one 4 KiB memcpy each, which is ~150 ns against
/// ~2 us for a syscall.
const RX_CAPACITY: usize = 64 * MAX_FRAME;
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

const CLOCK_MONOTONIC: c_int = 1;

unsafe extern "C" {
    fn poll(fds: *mut PollFd, nfds: std::ffi::c_ulong, timeout: c_int) -> c_int;
    fn clock_gettime(clk_id: c_int, tp: *mut Timespec) -> c_int;
}

/// Raw `CLOCK_MONOTONIC` nanoseconds — must match the server's
/// `onyx_storage::direct_io::monotonic_ns` bit for bit, because the whole
/// point is to difference a stamp taken here against one taken there.
///
/// `Instant` cannot do this: it is opaque and process-local. `CLOCK_MONOTONIC`
/// is system-wide on Linux, so the two processes share one timeline. This is
/// what turns the two socket-transit windows from "eliminated by proxy" into
/// "measured" — the previous round could rule out six hypotheses for the
/// ~10.6 ms unaccounted at QD1024 but could not see into either transit.
fn monotonic_ns() -> u64 {
    let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a live, exclusively borrowed timespec.
    if unsafe { clock_gettime(CLOCK_MONOTONIC, &mut ts) } != 0 {
        return 0;
    }
    (ts.tv_sec as u64).saturating_mul(1_000_000_000) + ts.tv_nsec as u64
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
    /// Set by `queue()`, read by `collect_one()` — the FULL client-observed
    /// round trip, to compare against `server_total_ns` (which only starts
    /// once the server has finished reading the request off the socket).
    /// Whatever gap remains is spent either staged in `pending` before a
    /// `commit()`, in transit, or after the response arrived but before
    /// `collect_one` got around to it.
    queued_at: Option<Instant>,
    /// Set by `commit()` — how long this request sat staged in `pending`
    /// before the client wrote it, i.e. `T(write) - T(queue)`. Carried per
    /// slot rather than only globally so the round-trip ledger can be closed
    /// per opcode; see `LatencyAccum::log`'s `accounted`.
    stage_delay_ns: u64,
}

/// One request staged in `pending` but not yet written: which slot owns it and
/// where its header sits in the staging buffer, so `commit()` can patch the
/// submit stamp in at the last possible moment.
#[derive(Clone, Copy)]
struct Staged {
    slot: usize,
    header_at: usize,
}

/// One IO's timings, laid out in the order the IO walks them.
///
/// `stage` / `intake` / `total` / `resp_queue` / `egress` are DISJOINT and
/// together span the whole round trip, so their sum is directly comparable
/// against `rtt` — that residual is the self-check (`accounted` below), the
/// same discipline `tools/lv2_epoch_delta.py` applies to the LV2 ledger.
/// `queue` / `engine` / `durable` / `dispatch` are breakdowns WITHIN `total`
/// and must not be added to the total again.
#[derive(Clone, Copy, Default)]
struct StageSample {
    client_rtt_ns: u64,
    stage_delay_ns: u64,
    intake_ns: u64,
    server_total_ns: u64,
    submit_queue_ns: u64,
    engine_submit_ns: u64,
    durable_wait_ns: u64,
    completion_dispatch_ns: u64,
    response_queue_ns: u64,
    egress_ns: u64,
}

/// Per-opcode accumulation of `StageSample`. Mean + max only (no
/// percentiles): enough to see which segment dominates without a histogram
/// allocation per response.
#[derive(Clone, Copy, Default)]
struct LatencyAccum {
    count: u64,
    sum: StageSample,
    max: StageSample,
}

impl LatencyAccum {
    fn record(&mut self, sample: StageSample) {
        self.count += 1;
        let s = &mut self.sum;
        let m = &mut self.max;
        s.client_rtt_ns += sample.client_rtt_ns;
        s.stage_delay_ns += sample.stage_delay_ns;
        s.intake_ns += sample.intake_ns;
        s.server_total_ns += sample.server_total_ns;
        s.submit_queue_ns += sample.submit_queue_ns;
        s.engine_submit_ns += sample.engine_submit_ns;
        s.durable_wait_ns += sample.durable_wait_ns;
        s.completion_dispatch_ns += sample.completion_dispatch_ns;
        s.response_queue_ns += sample.response_queue_ns;
        s.egress_ns += sample.egress_ns;
        m.client_rtt_ns = m.client_rtt_ns.max(sample.client_rtt_ns);
        m.stage_delay_ns = m.stage_delay_ns.max(sample.stage_delay_ns);
        m.intake_ns = m.intake_ns.max(sample.intake_ns);
        m.server_total_ns = m.server_total_ns.max(sample.server_total_ns);
        m.submit_queue_ns = m.submit_queue_ns.max(sample.submit_queue_ns);
        m.engine_submit_ns = m.engine_submit_ns.max(sample.engine_submit_ns);
        m.durable_wait_ns = m.durable_wait_ns.max(sample.durable_wait_ns);
        m.completion_dispatch_ns = m.completion_dispatch_ns.max(sample.completion_dispatch_ns);
        m.response_queue_ns = m.response_queue_ns.max(sample.response_queue_ns);
        m.egress_ns = m.egress_ns.max(sample.egress_ns);
    }

    /// `accounted` is the load-bearing number: it is the fraction of the
    /// client-observed round trip that the five disjoint segments explain.
    /// A previous round of work could account for only ~2-4% of an 11 ms
    /// round trip at QD1024, which is what motivated putting a client stamp
    /// on the wire in the first place. If this prints well under 100%, there
    /// is STILL a window with no instrument in it — do not attribute the
    /// remainder to whichever segment happens to be largest.
    fn log(&self, label: &str) {
        if self.count == 0 {
            return;
        }
        let n = self.count as f64;
        let s = &self.sum;
        let accounted = s.stage_delay_ns
            + s.intake_ns
            + s.server_total_ns
            + s.response_queue_ns
            + s.egress_ns;
        let pct = if s.client_rtt_ns > 0 {
            accounted as f64 / s.client_rtt_ns as f64 * 100.0
        } else {
            0.0
        };
        eprintln!(
            "onyx-stage {label} n={} accounted={pct:.1}% \
             rtt_ns avg={:.0} max={} stage_ns avg={:.0} max={} intake_ns avg={:.0} max={} \
             total_ns avg={:.0} max={} resp_queue_ns avg={:.0} max={} egress_ns avg={:.0} max={} \
             [within total] queue_ns avg={:.0} max={} engine_ns avg={:.0} max={} \
             durable_ns avg={:.0} max={} dispatch_ns avg={:.0} max={}",
            self.count,
            s.client_rtt_ns as f64 / n,
            self.max.client_rtt_ns,
            s.stage_delay_ns as f64 / n,
            self.max.stage_delay_ns,
            s.intake_ns as f64 / n,
            self.max.intake_ns,
            s.server_total_ns as f64 / n,
            self.max.server_total_ns,
            s.response_queue_ns as f64 / n,
            self.max.response_queue_ns,
            s.egress_ns as f64 / n,
            self.max.egress_ns,
            s.submit_queue_ns as f64 / n,
            self.max.submit_queue_ns,
            s.engine_submit_ns as f64 / n,
            self.max.engine_submit_ns,
            s.durable_wait_ns as f64 / n,
            self.max.durable_wait_ns,
            s.completion_dispatch_ns as f64 / n,
            self.max.completion_dispatch_ns,
        );
    }
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
    read_stats: LatencyAccum,
    write_stats: LatencyAccum,
    /// How many slots are actually occupied (queued-or-in-flight) each time
    /// `queue()` succeeds — diagnostic for whether fio is genuinely holding
    /// `iodepth` requests outstanding or something is quietly capping it far
    /// below the configured depth.
    depth_sum: u64,
    depth_samples: u64,
    depth_max: usize,
    /// Requests staged since the last `commit()` — stamped with the actual
    /// `write_all` time when `commit()` runs, so `stage_delay_ns` can isolate
    /// "sat in `pending` waiting for fio to call commit()" from everything
    /// downstream of the bytes actually leaving the client.
    staged: Vec<Staged>,
    /// Wall time spent inside `onyx_rs_getevents` (mostly blocked in
    /// `wait_readable`) vs the total lifetime of the client — splits "fio is
    /// genuinely blocked waiting for the socket to become readable" from
    /// "fio is off doing something else and hasn't called getevents yet".
    getevents_calls: u64,
    getevents_wall_ns: u64,
    lifetime_start: Option<Instant>,
    /// Responses read from the socket but not yet handed to fio. Persists
    /// ACROSS `getevents` calls, which is what lets a `read` that lands a
    /// partial frame simply return instead of blocking mid-frame.
    rx: Vec<u8>,
    rx_head: usize,
    rx_tail: usize,
    /// How many complete responses each `read(2)` delivered — the direct
    /// read-out of whether the buffering is doing anything.
    fills: u64,
    filled_frames: u64,
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
            read_stats: LatencyAccum::default(),
            write_stats: LatencyAccum::default(),
            depth_sum: 0,
            depth_samples: 0,
            depth_max: 0,
            staged: Vec::with_capacity(depth),
            getevents_calls: 0,
            getevents_wall_ns: 0,
            lifetime_start: Some(Instant::now()),
            rx: vec![0; RX_CAPACITY],
            rx_head: 0,
            rx_tail: 0,
            fills: 0,
            filled_frames: 0,
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
        let header_at = self.pending.len();
        self.pending.extend_from_slice(&header);
        if opcode == OP_WRITE {
            let payload = unsafe { std::slice::from_raw_parts(buffer, len as usize) };
            self.pending.extend_from_slice(payload);
        }
        self.next_id = self.next_id.wrapping_add(1);
        self.slots[index] = Slot {
            id,
            io_u,
            buffer,
            len,
            opcode,
            queued_at: Some(Instant::now()),
            stage_delay_ns: 0,
        };
        self.staged.push(Staged { slot: index, header_at });
        let outstanding = self.slots[..self.depth].iter().filter(|s| !s.io_u.is_null()).count();
        self.depth_sum += outstanding as u64;
        self.depth_samples += 1;
        self.depth_max = self.depth_max.max(outstanding);
        Ok(FIO_Q_QUEUED)
    }

    /// Flush every request staged since the last commit in ONE write.
    ///
    /// This is also where each staged header gets its `client_submit_ns`
    /// patched in — deliberately here and not in `queue()`, so the stamp
    /// measures transit rather than transit plus this client's own staging
    /// delay. One clock read serves the whole batch because a batch leaves in
    /// one `write_all`; if that `write_all` ever blocks on socket
    /// backpressure, the later requests in the batch absorb the block as
    /// `intake_ns`, which is signal (the socket is full) rather than noise.
    fn commit(&mut self) -> Result<(), c_int> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let sent_at = Instant::now();
        let submit_ns = monotonic_ns().to_le_bytes();
        for i in 0..self.staged.len() {
            let staged = self.staged[i];
            let slot = &mut self.slots[staged.slot];
            if let Some(queued_at) = slot.queued_at {
                slot.stage_delay_ns =
                    sent_at.saturating_duration_since(queued_at).as_nanos() as u64;
            }
            let at = staged.header_at + REQUEST_SUBMIT_NS_AT;
            self.pending[at..at + 8].copy_from_slice(&submit_ns);
        }
        self.staged.clear();
        let result = self.stream.write_all(&self.pending).map_err(|e| errno(&e));
        // Clear either way: a short/failed write leaves the session
        // unrecoverable, and fio aborts the job on a commit error, so retrying
        // the same bytes would only desynchronise the protocol further.
        self.pending.clear();
        result
    }

    /// Bytes read from the socket but not yet consumed.
    fn rx_len(&self) -> usize {
        self.rx_tail - self.rx_head
    }

    /// Total length of the response at the head of the buffer, once enough of
    /// it has arrived to know. `None` means "read more first".
    fn framed_len(&self) -> Option<usize> {
        if self.rx_len() < RESPONSE_LEN {
            return None;
        }
        let payload_len = u32_at(&self.rx[self.rx_head..], 28) as usize;
        let total = RESPONSE_LEN + payload_len;
        (self.rx_len() >= total).then_some(total)
    }

    /// One `read(2)` into the tail of the buffer. `Ok(false)` is EOF.
    ///
    /// Compaction keeps at least one whole frame of room at the tail, so a
    /// single read can never be starved into making no progress.
    fn fill(&mut self) -> Result<bool, c_int> {
        if self.rx_head == self.rx_tail {
            self.rx_head = 0;
            self.rx_tail = 0;
        } else if self.rx.len() - self.rx_tail < MAX_FRAME {
            self.rx.copy_within(self.rx_head..self.rx_tail, 0);
            self.rx_tail -= self.rx_head;
            self.rx_head = 0;
        }
        let read = self
            .stream
            .read(&mut self.rx[self.rx_tail..])
            .map_err(|e| errno(&e))?;
        if read == 0 {
            return Ok(false);
        }
        self.rx_tail += read;
        self.fills += 1;
        Ok(true)
    }

    /// Consumes exactly one complete response of `total` bytes from the head
    /// of the buffer.
    fn collect_framed(&mut self, total: usize) -> Result<(), c_int> {
        let at = self.rx_head;
        let mut response = [0u8; RESPONSE_LEN];
        response.copy_from_slice(&self.rx[at..at + RESPONSE_LEN]);
        if &response[0..4] != MAGIC || u16_at(&response, 4) != VERSION {
            return Err(libc_errno::EPROTO);
        }
        let id = u64_at(&response, 16);
        let index = id as usize % self.depth;
        let slot = self.slots[index];
        if slot.io_u.is_null() || slot.id != id || u16_at(&response, 6) != slot.opcode {
            return Err(libc_errno::EPROTO);
        }
        let status = i32::from_le_bytes(response[8..12].try_into().unwrap());
        let bytes = u32_at(&response, 24);
        let payload_len = u32_at(&response, 28) as usize;
        if status < 0 || bytes != slot.len {
            return Err(if status < 0 { -status } else { libc_errno::EIO });
        }
        if payload_len != 0 {
            if slot.opcode != OP_READ || payload_len != slot.len as usize {
                return Err(libc_errno::EPROTO);
            }
            // SAFETY: `slot.buffer` is fio's io_u buffer for this request and
            // is `slot.len` bytes, checked equal to `payload_len` above.
            let target = unsafe { std::slice::from_raw_parts_mut(slot.buffer, payload_len) };
            target.copy_from_slice(&self.rx[at + RESPONSE_LEN..at + total]);
        }
        self.rx_head += total;
        self.filled_frames += 1;

        // Taken AFTER the payload copy so `egress_ns` covers the whole return
        // trip the client actually waited on, header and body alike.
        let server_send_ns = u64_at(&response, 88);
        let sample = StageSample {
            client_rtt_ns: slot
                .queued_at
                .map(|start| start.elapsed().as_nanos() as u64)
                .unwrap_or(0),
            stage_delay_ns: slot.stage_delay_ns,
            intake_ns: u64_at(&response, 72),
            server_total_ns: u64_at(&response, 32),
            submit_queue_ns: u64_at(&response, 40),
            engine_submit_ns: u64_at(&response, 48),
            durable_wait_ns: u64_at(&response, 56),
            completion_dispatch_ns: u64_at(&response, 64),
            response_queue_ns: u64_at(&response, 80),
            egress_ns: if server_send_ns == 0 {
                0
            } else {
                monotonic_ns().saturating_sub(server_send_ns)
            },
        };
        let accum = match slot.opcode {
            OP_READ => &mut self.read_stats,
            OP_WRITE => &mut self.write_stats,
            _ => unreachable!("only read/write slots are tracked"),
        };
        accum.record(sample);
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
    let call_started = Instant::now();
    client.completed.clear();
    let fd = client.stream.as_raw_fd();
    let block_ms = timeout_ms(timeout);
    let mut failed = None;
    while client.completed.len() < max as usize {
        // Drain whatever is already buffered before touching the socket: one
        // `read` typically lands several completions, and paying `poll` per
        // completion is what made the reap loop the wall.
        if let Some(total) = client.framed_len() {
            if let Err(code) = client.collect_framed(total) {
                failed = Some(code);
                break;
            }
            continue;
        }
        // Below `min` we may wait out fio's timeout; at or above it we may only
        // reap what has already arrived and must never block. A partial frame
        // left in the buffer here is fine — it survives to the next call.
        let wait = if client.completed.len() >= min as usize { 0 } else { block_ms };
        match wait_readable(fd, wait) {
            Ok(true) => {}
            Ok(false) => break,
            Err(code) => {
                failed = Some(code);
                break;
            }
        }
        match client.fill() {
            Ok(true) => {}
            Ok(false) => {
                failed = Some(libc_errno::EIO);
                break;
            }
            Err(code) if code == libc_errno::EAGAIN || code == libc_errno::EWOULDBLOCK => break,
            Err(code) => {
                failed = Some(code);
                break;
            }
        }
    }
    client.getevents_calls += 1;
    client.getevents_wall_ns += call_started.elapsed().as_nanos() as u64;
    match failed {
        // Completions already reaped in this call must still be reported;
        // dropping them on the way out is how fio loses io_us.
        Some(code) if client.completed.is_empty() => -code,
        _ => client.completed.len() as c_int,
    }
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
    client.read_stats.log("read");
    client.write_stats.log("write");
    if client.depth_samples > 0 {
        eprintln!(
            "onyx-stage depth samples={} avg={:.2} max={}",
            client.depth_samples,
            client.depth_sum as f64 / client.depth_samples as f64,
            client.depth_max,
        );
    }
    if client.fills > 0 {
        eprintln!(
            "onyx-stage reap fills={} frames={} frames_per_read={:.2}",
            client.fills,
            client.filled_frames,
            client.filled_frames as f64 / client.fills as f64,
        );
    }
    if let Some(start) = client.lifetime_start {
        let lifetime_ns = start.elapsed().as_nanos() as u64;
        let pct = if lifetime_ns > 0 {
            client.getevents_wall_ns as f64 / lifetime_ns as f64 * 100.0
        } else {
            0.0
        };
        eprintln!(
            "onyx-stage getevents calls={} wall_ns={} lifetime_ns={} pct_blocked_in_getevents={:.1}%",
            client.getevents_calls, client.getevents_wall_ns, lifetime_ns, pct,
        );
    }
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

    fn test_client(stream: UnixStream, depth: usize) -> Client {
        Client {
            stream,
            next_id: 2,
            depth,
            slots: [Slot::default(); MAX_DEPTH],
            completed: Vec::new(),
            pending: Vec::new(),
            read_stats: LatencyAccum::default(),
            write_stats: LatencyAccum::default(),
            depth_sum: 0,
            depth_samples: 0,
            depth_max: 0,
            staged: Vec::new(),
            getevents_calls: 0,
            getevents_wall_ns: 0,
            lifetime_start: None,
            rx: vec![0; RX_CAPACITY],
            rx_head: 0,
            rx_tail: 0,
            fills: 0,
            filled_frames: 0,
        }
    }

    /// Minimal well-formed response frame, matching the server's encoding.
    fn response_frame(opcode: u16, id: u64, bytes: u32, payload_len: u32) -> Vec<u8> {
        let mut frame = vec![0u8; RESPONSE_LEN + payload_len as usize];
        frame[0..4].copy_from_slice(MAGIC);
        frame[4..6].copy_from_slice(&VERSION.to_le_bytes());
        frame[6..8].copy_from_slice(&opcode.to_le_bytes());
        frame[16..24].copy_from_slice(&id.to_le_bytes());
        frame[24..28].copy_from_slice(&bytes.to_le_bytes());
        frame[28..32].copy_from_slice(&payload_len.to_le_bytes());
        frame
    }

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
        let mut client = test_client(a, 4);
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
        assert_eq!(
            u64_at(&client.pending, REQUEST_SUBMIT_NS_AT), 0,
            "queue() must NOT stamp — a staged stamp would measure this client's \
             own staging delay as transit",
        );

        let before = monotonic_ns();
        client.commit().unwrap();
        assert!(client.pending.is_empty(), "commit drains the staging buffer");
        let mut got = vec![0u8; REQUEST_LEN + BLOCK_SIZE as usize];
        b.read_exact(&mut got).unwrap();
        assert_eq!(u16_at(&got, 6), OP_WRITE);
        assert_eq!(got[REQUEST_LEN], 0x5A);
        let stamp = u64_at(&got, REQUEST_SUBMIT_NS_AT);
        assert!(
            stamp >= before && stamp <= monotonic_ns(),
            "commit() stamps the wire with CLOCK_MONOTONIC: {stamp} outside [{before}, now]",
        );

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
        assert_eq!(u16_at(&encoded, 4), VERSION);
        assert_eq!(u16_at(&encoded, 6), OP_WRITE);
        assert_eq!(u32_at(&encoded, 12), 4096);
        assert_eq!(u64_at(&encoded, 16), 0x1122);
        assert_eq!(u64_at(&encoded, 24), 8192);
        assert_eq!(u32_at(&encoded, 32), 4096);
        assert_eq!(&encoded[36..40], &[0; 4], "reserved must stay zero");
        assert_eq!(u64_at(&encoded, REQUEST_SUBMIT_NS_AT), 0);
    }

    /// The point of the receive buffer: ONE `read(2)` must be able to deliver
    /// several completions. The old loop paid `poll` + two `read_exact` calls
    /// per completion, and with each fio job reaping serially for every request
    /// it has outstanding that cost showed up as `egress_ns`.
    #[test]
    fn one_read_reaps_every_response_the_server_batched() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = test_client(a, 8);

        // Three write completions the server wrote back to back.
        let mut wire = Vec::new();
        for id in 2u64..5 {
            let index = id as usize % client.depth;
            client.slots[index] = Slot {
                id,
                io_u: (0x1000 + id as usize) as *mut c_void,
                buffer: ptr::null_mut(),
                len: BLOCK_SIZE,
                opcode: OP_WRITE,
                queued_at: Some(Instant::now()),
                stage_delay_ns: 0,
            };
            wire.extend_from_slice(&response_frame(OP_WRITE, id, BLOCK_SIZE, 0));
        }
        b.write_all(&wire).unwrap();

        assert!(client.fill().unwrap(), "one read takes all three frames");
        assert_eq!(client.fills, 1);
        for _ in 0..3 {
            let total = client.framed_len().expect("a whole frame is buffered");
            client.collect_framed(total).unwrap();
        }
        assert_eq!(client.filled_frames, 3, "three completions from ONE read");
        assert_eq!(client.completed.len(), 3);
        assert_eq!(client.framed_len(), None, "buffer fully consumed");
        assert_eq!(client.rx_len(), 0);
    }

    /// A frame split across two reads must not be mistaken for a whole one,
    /// and the partial must survive to the next read.
    #[test]
    fn a_partial_frame_waits_for_the_rest() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = test_client(a, 8);
        let id = 2u64;
        client.slots[id as usize % client.depth] = Slot {
            id,
            io_u: 0x2000usize as *mut c_void,
            buffer: ptr::null_mut(),
            len: BLOCK_SIZE,
            opcode: OP_WRITE,
            queued_at: Some(Instant::now()),
            stage_delay_ns: 0,
        };
        let frame = response_frame(OP_WRITE, id, BLOCK_SIZE, 0);

        b.write_all(&frame[..RESPONSE_LEN - 8]).unwrap();
        client.fill().unwrap();
        assert_eq!(client.framed_len(), None, "a short header is not a frame");

        b.write_all(&frame[RESPONSE_LEN - 8..]).unwrap();
        client.fill().unwrap();
        let total = client.framed_len().expect("the rest arrived");
        assert_eq!(total, RESPONSE_LEN);
        client.collect_framed(total).unwrap();
        assert_eq!(client.completed.len(), 1);
    }

    /// The stamp is only comparable against the server's because both read the
    /// same system-wide clock. A wrong `clk_id` would still produce plausible
    /// nanoseconds, so pin it against the one clock `Instant` also uses.
    #[test]
    fn monotonic_ns_tracks_the_same_clock_as_instant() {
        let start_instant = Instant::now();
        let start = monotonic_ns();
        assert!(start > 0, "CLOCK_MONOTONIC must be readable");
        std::thread::sleep(std::time::Duration::from_millis(20));
        let elapsed_raw = monotonic_ns() - start;
        let elapsed_instant = start_instant.elapsed().as_nanos() as u64;
        assert!(elapsed_raw >= 20_000_000, "raw clock advanced {elapsed_raw} ns");
        assert!(
            elapsed_raw.abs_diff(elapsed_instant) < 5_000_000,
            "raw {elapsed_raw} ns and Instant {elapsed_instant} ns must be the same clock",
        );
    }
}
