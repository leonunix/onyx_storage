use std::ffi::{c_char, c_int, c_void, CStr};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr;
use std::time::Duration;
#[cfg(any(test, feature = "diagnostic-metrics"))]
use std::time::Instant;

#[cfg(any(test, feature = "diagnostic-metrics"))]
macro_rules! diagnostic_metrics {
    ($($body:tt)*) => {{ $($body)* }};
}

#[cfg(not(any(test, feature = "diagnostic-metrics")))]
macro_rules! diagnostic_metrics {
    ($($body:tt)*) => {{}};
}

const MAGIC: &[u8; 4] = b"ONIO";
const VERSION: u16 = 3;
const REQUEST_LEN: usize = 48;
const RESPONSE_LEN: usize = 96;
/// Offset of `RequestHeader::client_submit_ns`, patched into an already-staged
/// header by `commit()`. See `Client::commit`.
const REQUEST_SUBMIT_NS_AT: usize = 40;
/// Bytes of capability payload a successful `HELLO` response carries.
/// Must match the engine's `direct_io::HELLO_CAPABILITY_LEN`.
const HELLO_CAPABILITY_LEN: usize = 16;
const BLOCK_SIZE: u32 = 4096;
/// Largest single IO the engine accepts. Must match the engine's
/// `direct_io::MAX_DIRECT_IO_BYTES`; the `HELLO` capability payload is checked
/// against it at connect, so a disagreement is reported rather than assumed
/// away.
const MAX_IO_BYTES: u32 = 128 * 1024;
const MAX_DEPTH: usize = 256;
const OP_HELLO: u16 = 1;
const OP_WRITE: u16 = 2;
const OP_READ: u16 = 3;
const OP_CLOSE: u16 = 4;
const OP_TRIM: u16 = 5;

// Operation codes as `fio_bridge.c` hands them over. fio's own `enum fio_ddir`
// numbering is part of its private ABI, so the bridge — the only file that
// includes `fio.h` — translates into these, and nothing here depends on fio's
// values.
const ONYX_OP_READ: c_int = 0;
const ONYX_OP_WRITE: c_int = 1;
const ONYX_OP_TRIM: c_int = 2;
const ONYX_OP_SYNC: c_int = 3;

const FIO_Q_COMPLETED: c_int = 0;
const FIO_Q_QUEUED: c_int = 1;
const FIO_Q_BUSY: c_int = 2;

/// Largest single response frame for a job whose biggest IO is `max_bs`:
/// header plus a full read payload.
fn max_frame(max_bs: u32) -> usize {
    RESPONSE_LEN + max_bs as usize
}

/// Receive buffer size, so one `read(2)` can deliver many completions.
///
/// The reap loop used to cost THREE syscalls per completion — `poll`, then
/// `read_exact` for the 96-byte header, then `read_exact` for the payload —
/// and each fio job is a single thread doing that serially for every response
/// it has outstanding. That showed up in the protocol's own ledger as
/// `egress_ns` (server `write` -> client finished reading) becoming the
/// largest segment of the round trip once the server side stopped being the
/// wall. Reading into a buffer this size amortises the syscalls over ~64
/// small completions at the cost of one memcpy each, which is ~150 ns against
/// ~2 us for a syscall.
///
/// ⚠ The `max(.., 2 * max_frame)` term is a CORRECTNESS floor, not a
/// performance choice — see `Client::fill`. It is a floor rather than the
/// whole size because sizing purely off `max_frame` would make a `bs=128k`
/// job's buffer 8 MiB per job while delivering no more amortisation than
/// this, and a `bs=4k` job's buffer smaller than the batching it needs.
fn rx_capacity(max_bs: u32) -> usize {
    (64 * (RESPONSE_LEN + BLOCK_SIZE as usize)).max(2 * max_frame(max_bs))
}

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

#[cfg(any(test, feature = "diagnostic-metrics"))]
const CLOCK_MONOTONIC: c_int = 1;

unsafe extern "C" {
    fn poll(fds: *mut PollFd, nfds: std::ffi::c_ulong, timeout: c_int) -> c_int;
    #[cfg(any(test, feature = "diagnostic-metrics"))]
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
#[cfg(any(test, feature = "diagnostic-metrics"))]
fn monotonic_ns() -> u64 {
    let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `ts` is a live, exclusively borrowed timespec.
    if unsafe { clock_gettime(CLOCK_MONOTONIC, &mut ts) } != 0 {
        return 0;
    }
    (ts.tv_sec as u64).saturating_mul(1_000_000_000) + ts.tv_nsec as u64
}

#[cfg(any(test, feature = "diagnostic-metrics"))]
type MetricInstant = Instant;

#[cfg(not(any(test, feature = "diagnostic-metrics")))]
#[derive(Clone, Copy)]
struct MetricInstant;

#[inline(always)]
fn metric_now() -> MetricInstant {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return Instant::now();
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        MetricInstant
    }
}

#[inline(always)]
fn metric_elapsed(start: MetricInstant) -> Duration {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return start.elapsed();
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        let _ = start;
        Duration::ZERO
    }
}

#[inline(always)]
fn metric_duration_since(later: MetricInstant, earlier: MetricInstant) -> Duration {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return later.saturating_duration_since(earlier);
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        let _ = (later, earlier);
        Duration::ZERO
    }
}

#[inline(always)]
fn diagnostic_monotonic_ns() -> u64 {
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    {
        return monotonic_ns();
    }
    #[cfg(not(any(test, feature = "diagnostic-metrics")))]
    {
        0
    }
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
    queued_at: Option<MetricInstant>,
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
    #[cfg(any(test, feature = "diagnostic-metrics"))]
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
    /// Largest IO this job may issue — `max(td->o.max_bs[..])`, validated at
    /// connect against both `MAX_IO_BYTES` and what the engine reported. Every
    /// buffer below is sized from this rather than from the protocol ceiling,
    /// so a `bs=4k` job does not pay for a `bs=128k` job's frames.
    max_bs: u32,
    /// Volume size as the engine reported it at `HELLO`, surfaced to fio
    /// through `get_file_size` so a job file does not need `size=`.
    volume_size: u64,
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
    trim_stats: LatencyAccum,
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
    lifetime_start: Option<MetricInstant>,
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

/// The geometry the engine reports at `HELLO`. Mirrors the engine's
/// `direct_io::HelloCapability`.
#[derive(Clone, Copy)]
struct HelloCapability {
    capacity_bytes: u64,
    block_size: u32,
    max_io_bytes: u32,
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
    /// `max_bs` and `ba` come from the job's own options, so a bad block size
    /// or alignment fails HERE, with a message, instead of turning into an
    /// `EINVAL` on the first IO that fio reports as a device error.
    fn connect(control: &str, volume: &str, depth: usize, max_bs: u32, ba: u32)
               -> io::Result<Self> {
        if depth == 0 || depth > MAX_DEPTH || volume.is_empty() || volume.len() > 255 {
            return Err(io::Error::from_raw_os_error(libc_errno::EINVAL));
        }
        if max_bs == 0 || max_bs % BLOCK_SIZE != 0 || max_bs > MAX_IO_BYTES {
            return Err(io::Error::from_raw_os_error(libc_errno::EINVAL));
        }
        // fio defaults `ba` to `min_bs`, so `--bs=4k-32k` is already aligned;
        // an explicit `--ba` finer than a block is the case this catches.
        if ba == 0 || ba % BLOCK_SIZE != 0 {
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
        let capability = Self::read_hello_capability(&mut stream, &response, max_bs)?;
        Ok(Self {
            stream, next_id: 2, depth, max_bs,
            volume_size: capability.capacity_bytes,
            slots: [Slot::default(); MAX_DEPTH],
            completed: Vec::with_capacity(depth),
            pending: Vec::with_capacity(depth * (REQUEST_LEN + max_bs as usize)),
            read_stats: LatencyAccum::default(),
            write_stats: LatencyAccum::default(),
            trim_stats: LatencyAccum::default(),
            depth_sum: 0,
            depth_samples: 0,
            depth_max: 0,
            staged: Vec::with_capacity(depth),
            getevents_calls: 0,
            getevents_wall_ns: 0,
            lifetime_start: Some(metric_now()),
            rx: vec![0; rx_capacity(max_bs)],
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

    /// Consume the capability payload that follows a successful `HELLO`, and
    /// check the engine agrees with what this build assumes.
    ///
    /// Leaving those bytes on the socket would desynchronise every later
    /// response, so this runs before the first IO. It is also what softens
    /// the old "plugin and engine must come from the same tree" rule down to
    /// "same protocol version": a block size or IO ceiling this build cannot
    /// honour is now a named error at connect rather than a wrong number.
    fn read_hello_capability(stream: &mut UnixStream, response: &[u8; RESPONSE_LEN],
                             max_bs: u32) -> io::Result<HelloCapability> {
        if u32_at(response, 28) as usize != HELLO_CAPABILITY_LEN {
            return Err(io::Error::from_raw_os_error(libc_errno::EPROTO));
        }
        let mut payload = [0u8; HELLO_CAPABILITY_LEN];
        stream.read_exact(&mut payload)?;
        let capability = HelloCapability {
            capacity_bytes: u64_at(&payload, 0),
            block_size: u32_at(&payload, 8),
            max_io_bytes: u32_at(&payload, 12),
        };
        if capability.block_size != BLOCK_SIZE || capability.capacity_bytes == 0 {
            return Err(io::Error::from_raw_os_error(libc_errno::EPROTO));
        }
        if max_bs > capability.max_io_bytes {
            return Err(io::Error::from_raw_os_error(libc_errno::EINVAL));
        }
        Ok(capability)
    }

    unsafe fn queue(&mut self, io_u: *mut c_void, op: c_int, offset: u64,
                    buffer: *mut u8, len: u32) -> Result<c_int, c_int> {
        // A sync is satisfied without touching the wire, so it is handled
        // before any slot or framing bookkeeping.
        //
        // ⚠ This is correct for Onyx specifically, not a shortcut. The
        // foreground `append()` blocks until that sequence has completed its
        // LV2 `fdatasync`, so a write this client has already reaped is
        // already durable and there is nothing for a flush to push out. The
        // honest consequence: `--fsync=N` / `--fdatasync=N` measure NOTHING
        // on this engine, so do not read a number out of them.
        //
        // `FIO_Q_COMPLETED` accounts for the io_u right here in fio's queue
        // path — it must NOT also be pushed onto `completed`, or `event()`
        // would hand fio the same io_u a second time.
        if op == ONYX_OP_SYNC {
            let _ = io_u;
            return Ok(FIO_Q_COMPLETED);
        }
        let opcode = match op {
            ONYX_OP_READ => OP_READ,
            ONYX_OP_WRITE => OP_WRITE,
            ONYX_OP_TRIM => OP_TRIM,
            _ => return Err(libc_errno::EOPNOTSUPP),
        };
        // The engine takes any block-aligned length; `max_bs` is this job's own
        // ceiling, already checked against the engine's at connect.
        if len == 0
            || len % BLOCK_SIZE != 0
            || len > self.max_bs
            || offset % BLOCK_SIZE as u64 != 0
        {
            return Err(libc_errno::EINVAL);
        }
        // A trim carries no data and fio hands it no payload buffer.
        if buffer.is_null() && opcode != OP_TRIM {
            return Err(libc_errno::EINVAL);
        }
        let id = self.next_id;
        let index = id as usize % self.depth;
        // FIO_Q_BUSY is fio's "no more room, call ->commit()", which is exactly
        // what a full slot ring means. It also bounds `pending` to
        // depth * (REQUEST_LEN + max_bs).
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
            queued_at: Some(metric_now()),
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
        let sent_at = metric_now();
        let submit_ns = diagnostic_monotonic_ns().to_le_bytes();
        for i in 0..self.staged.len() {
            let staged = self.staged[i];
            let slot = &mut self.slots[staged.slot];
            if let Some(queued_at) = slot.queued_at {
                slot.stage_delay_ns = metric_duration_since(sent_at, queued_at).as_nanos() as u64;
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
    ///
    /// A `payload_len` above this job's own `max_bs` cannot be a frame this
    /// job asked for, and believing it would ask `fill` to wait for bytes the
    /// buffer has no room for — a hang instead of an error. Reporting the
    /// frame as complete at its header hands it straight to `collect_framed`,
    /// which rejects the mismatch as `EPROTO`.
    fn framed_len(&self) -> Option<usize> {
        if self.rx_len() < RESPONSE_LEN {
            return None;
        }
        let payload_len = u32_at(&self.rx[self.rx_head..], 28) as usize;
        if payload_len > self.max_bs as usize {
            return Some(RESPONSE_LEN);
        }
        let total = RESPONSE_LEN + payload_len;
        (self.rx_len() >= total).then_some(total)
    }

    /// One `read(2)` into the tail of the buffer. `Ok(false)` is EOF.
    ///
    /// Compaction keeps at least one whole frame of room at the tail, so a
    /// single read can never be starved into making no progress.
    ///
    /// ⚠ That guarantee rests on an invariant worth stating, because breaking
    /// it does not fail to compile — it produces a phantom `EIO`. Two facts
    /// combine:
    ///
    /// 1. `onyx_rs_getevents` consumes every COMPLETE frame before it ever
    ///    reaches `fill`, and the stream is ordered, so on entry the unread
    ///    bytes are a PREFIX of one incomplete frame: `rx_len() < max_frame`.
    /// 2. `rx_capacity` therefore only needs `2 * max_frame` for compaction to
    ///    leave `rx.len() - rx_len() > max_frame` free at the tail.
    ///
    /// Size `rx` below that and a large frame can compact to zero free room,
    /// which makes `read(&mut [])` return `Ok(0)` — indistinguishable here
    /// from a closed socket, so the job dies with an IO error while the
    /// engine is perfectly healthy.
    fn fill(&mut self) -> Result<bool, c_int> {
        debug_assert!(
            self.rx_len() < max_frame(self.max_bs),
            "fill() must only be reached with a partial frame buffered",
        );
        if self.rx_head == self.rx_tail {
            self.rx_head = 0;
            self.rx_tail = 0;
        } else if self.rx.len() - self.rx_tail < max_frame(self.max_bs) {
            self.rx.copy_within(self.rx_head..self.rx_tail, 0);
            self.rx_tail -= self.rx_head;
            self.rx_head = 0;
        }
        debug_assert!(
            self.rx_tail < self.rx.len(),
            "compaction must always leave room to read into",
        );
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
        diagnostic_metrics! {
            let server_send_ns = u64_at(&response, 88);
            let sample = StageSample {
                client_rtt_ns: slot
                    .queued_at
                    .map(|start| metric_elapsed(start).as_nanos() as u64)
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
                OP_TRIM => &mut self.trim_stats,
                _ => unreachable!("only read/write/trim slots are tracked"),
            };
            accum.record(sample);
        }
        self.slots[index] = Slot::default();
        self.completed.push(slot.io_u);
        Ok(())
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_init(socket: *const c_char, volume: *const c_char,
                                       depth: u32, max_bs: u32, ba: u32,
                                       error: *mut c_int) -> *mut c_void {
    let result = (|| {
        if socket.is_null() || volume.is_null() { return Err(libc_errno::EINVAL); }
        let socket = unsafe { CStr::from_ptr(socket) }.to_str().map_err(|_| libc_errno::EINVAL)?;
        let volume = unsafe { CStr::from_ptr(volume) }.to_str().map_err(|_| libc_errno::EINVAL)?;
        Client::connect(socket, volume, depth as usize, max_bs, ba).map_err(|e| errno(&e))
    })();
    match result {
        Ok(client) => Box::into_raw(Box::new(client)).cast(),
        Err(code) => { if !error.is_null() { unsafe { *error = code; } } ptr::null_mut() }
    }
}

/// Largest IO the protocol accepts, so the bridge can name the ceiling in its
/// own error message instead of leaving the operator with a bare `EINVAL`.
#[unsafe(no_mangle)]
pub extern "C" fn onyx_rs_max_io_bytes() -> u32 {
    MAX_IO_BYTES
}

/// Volume size as the engine reported it at `HELLO` — fio's `get_file_size`,
/// which is what makes `size=` optional in a job file.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_volume_size(client: *mut c_void) -> u64 {
    if client.is_null() { return 0; }
    unsafe { &*client.cast::<Client>() }.volume_size
}

/// Volume size via a throwaway session, for callers that have no client yet.
///
/// fio's `create_serialize` defaults to 1, which runs `setup_files` — and
/// therefore `->get_file_size` — in the parent BEFORE the job thread reaches
/// `->init`, so `io_ops_data` is still NULL there. Rather than depend on that
/// ordering (it differs with `create_serialize=0`), this opens a session,
/// reads the `HELLO` capability, and closes. One connection, no IO; the
/// server's 64-session limit is never contended because serialized setup
/// probes one job at a time.
///
/// Returns 0 on any failure, which just puts fio back to requiring `size=`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_probe_volume_size(socket: *const c_char,
                                                   volume: *const c_char) -> u64 {
    if socket.is_null() || volume.is_null() { return 0; }
    let Ok(socket) = unsafe { CStr::from_ptr(socket) }.to_str() else { return 0 };
    let Ok(volume) = unsafe { CStr::from_ptr(volume) }.to_str() else { return 0 };
    // Depth 1 and a single-block `max_bs`: this session issues no IO, so the
    // values only have to pass validation.
    match Client::connect(socket, volume, 1, BLOCK_SIZE, BLOCK_SIZE) {
        Ok(mut client) => {
            let size = client.volume_size;
            let close = request(OP_CLOSE, 0, client.next_id, 0, 0);
            let _ = client.stream.write_all(&close);
            size
        }
        Err(_) => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_queue(client: *mut c_void, io_u: *mut c_void,
    op: c_int, offset: u64, buffer: *mut c_void, len: u32, error: *mut c_int) -> c_int {
    let client = unsafe { &mut *client.cast::<Client>() };
    match unsafe { client.queue(io_u, op, offset, buffer.cast(), len) } {
        Ok(status) => status,
        Err(code) => { if !error.is_null() { unsafe { *error = code; } } FIO_Q_COMPLETED }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn onyx_rs_getevents(client: *mut c_void, min: u32, max: u32,
                                            timeout: *const Timespec) -> c_int {
    let client = unsafe { &mut *client.cast::<Client>() };
    let _call_started = metric_now();
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
    diagnostic_metrics! {
        client.getevents_calls += 1;
        client.getevents_wall_ns += metric_elapsed(_call_started).as_nanos() as u64;
    }
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
    client.trim_stats.log("trim");
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
        let lifetime_ns = metric_elapsed(start).as_nanos() as u64;
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

    /// A client with the buffers `connect` would have sized for a job whose
    /// largest IO is `max_bs` — including `rx_capacity`, so the framing tests
    /// exercise the real geometry rather than a generous test-only buffer.
    fn test_client_bs(stream: UnixStream, depth: usize, max_bs: u32) -> Client {
        Client {
            stream,
            next_id: 2,
            depth,
            max_bs,
            volume_size: 1 << 40,
            slots: [Slot::default(); MAX_DEPTH],
            completed: Vec::new(),
            pending: Vec::new(),
            read_stats: LatencyAccum::default(),
            write_stats: LatencyAccum::default(),
            trim_stats: LatencyAccum::default(),
            depth_sum: 0,
            depth_samples: 0,
            depth_max: 0,
            staged: Vec::new(),
            getevents_calls: 0,
            getevents_wall_ns: 0,
            lifetime_start: None,
            rx: vec![0; rx_capacity(max_bs)],
            rx_head: 0,
            rx_tail: 0,
            fills: 0,
            filled_frames: 0,
        }
    }

    fn test_client(stream: UnixStream, depth: usize) -> Client {
        test_client_bs(stream, depth, BLOCK_SIZE)
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
            client.queue(io_u, ONYX_OP_WRITE, 8192, payload.as_mut_ptr(), BLOCK_SIZE)
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
                    (0x2000 + i) as *mut c_void, ONYX_OP_WRITE,
                    (i as u64 + 2) * 4096, payload.as_mut_ptr(), BLOCK_SIZE,
                )
            };
            assert_eq!(st, Ok(FIO_Q_QUEUED));
        }
        let busy = unsafe {
            client.queue(io_u, ONYX_OP_WRITE, 4096 * 99, payload.as_mut_ptr(), BLOCK_SIZE)
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

    /// Multi-block IO is the whole point of protocol 3: the engine's aligned
    /// fast path already takes any `lba_count`, so the plugin must stop
    /// insisting on exactly one block.
    #[test]
    fn queue_accepts_a_multi_block_io_and_stages_the_whole_payload() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 4, 32 * 1024);
        let len = 32 * 1024u32;
        let mut payload = vec![0xC3u8; len as usize];
        payload[len as usize - 1] = 0x7E;

        let status = unsafe {
            client.queue(0x1000usize as *mut c_void, ONYX_OP_WRITE, 8192,
                         payload.as_mut_ptr(), len)
        };
        assert_eq!(status, Ok(FIO_Q_QUEUED));
        assert_eq!(client.pending.len(), REQUEST_LEN + len as usize);
        assert_eq!(u32_at(&client.pending, 12), len, "payload_len is the full IO");
        assert_eq!(u32_at(&client.pending, 32), len, "io_len is the full IO");
        assert_eq!(
            *client.pending.last().unwrap(), 0x7E,
            "the LAST payload byte must be staged, not just the first block",
        );
    }

    /// Both directions of the size gate. `max_bs` is the job's own ceiling,
    /// so a job configured for 32k must refuse 33k even though the protocol
    /// would accept it — the receive buffer was sized for 32k.
    #[test]
    fn queue_rejects_lengths_outside_the_jobs_block_size() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 4, 32 * 1024);
        let mut payload = vec![0u8; 64 * 1024];
        let io_u = 0x1000usize as *mut c_void;

        for bad in [0u32, 6 * 1024, BLOCK_SIZE + 1, 33 * 1024, 64 * 1024] {
            let status = unsafe {
                client.queue(io_u, ONYX_OP_WRITE, 0, payload.as_mut_ptr(), bad)
            };
            assert_eq!(status, Err(libc_errno::EINVAL), "len {bad} must be refused");
        }
        for good in [BLOCK_SIZE, 2 * BLOCK_SIZE, 32 * 1024] {
            let status = unsafe {
                client.queue(io_u, ONYX_OP_WRITE, 0, payload.as_mut_ptr(), good)
            };
            assert_eq!(status, Ok(FIO_Q_QUEUED), "len {good} must be accepted");
            client.pending.clear();
            client.staged.clear();
            client.slots = [Slot::default(); MAX_DEPTH];
        }
        // An unaligned offset stays refused whatever the length.
        let status = unsafe {
            client.queue(io_u, ONYX_OP_WRITE, 512, payload.as_mut_ptr(), BLOCK_SIZE)
        };
        assert_eq!(status, Err(libc_errno::EINVAL));
    }

    /// ⚠ THE regression this protocol change could introduce silently.
    ///
    /// `fill` compacts only when the tail has less than one whole frame free.
    /// If `rx` were sized off anything smaller than `2 * max_frame`, a large
    /// read frame could compact to ZERO free room, `read(&mut [])` would
    /// return `Ok(0)`, and `getevents` would report `EIO` on a perfectly
    /// healthy engine. This drives a 32 KiB read through the split-read path
    /// at a non-zero `rx_head` — the state that triggers compaction.
    #[test]
    fn a_large_read_frame_split_across_two_reads_still_completes() {
        let len = 32 * 1024u32;
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 8, len);
        assert!(
            client.rx.len() >= 2 * max_frame(len),
            "the correctness floor itself: rx {} vs 2*max_frame {}",
            client.rx.len(),
            2 * max_frame(len),
        );

        // Consume a small frame first so `rx_head` is non-zero and the next
        // fill has to compact rather than start clean.
        let mut warmup_target = vec![0u8; BLOCK_SIZE as usize];
        client.slots[2] = Slot {
            id: 2, io_u: 0x2000usize as *mut c_void,
            buffer: warmup_target.as_mut_ptr(), len: BLOCK_SIZE,
            opcode: OP_READ, queued_at: Some(Instant::now()), stage_delay_ns: 0,
        };
        let mut warmup = response_frame(OP_READ, 2, BLOCK_SIZE, BLOCK_SIZE);
        warmup[RESPONSE_LEN..].fill(0x11);
        b.write_all(&warmup).unwrap();
        client.fill().unwrap();
        let total = client.framed_len().unwrap();
        client.collect_framed(total).unwrap();
        assert!(client.rx_head > 0, "the next fill must have to compact");

        let id = 3u64;
        let mut target = vec![0u8; len as usize];
        client.slots[id as usize % client.depth] = Slot {
            id, io_u: 0x3000usize as *mut c_void,
            buffer: target.as_mut_ptr(), len,
            opcode: OP_READ, queued_at: Some(Instant::now()), stage_delay_ns: 0,
        };
        let mut frame = response_frame(OP_READ, id, len, len);
        frame[RESPONSE_LEN..].fill(0xA5);
        let split = RESPONSE_LEN + 1024;

        b.write_all(&frame[..split]).unwrap();
        assert!(client.fill().unwrap(), "first half must not look like EOF");
        assert_eq!(client.framed_len(), None, "a partial body is not a frame");

        b.write_all(&frame[split..]).unwrap();
        while client.framed_len().is_none() {
            assert!(client.fill().unwrap(), "the rest must arrive, not EOF");
        }
        let total = client.framed_len().unwrap();
        assert_eq!(total, RESPONSE_LEN + len as usize);
        client.collect_framed(total).unwrap();
        assert!(
            target.iter().all(|byte| *byte == 0xA5),
            "the whole 32 KiB body must land in fio's buffer",
        );
    }

    /// A `payload_len` this job could never have asked for must become an
    /// error, not a wait for bytes the buffer has no room to hold.
    #[test]
    fn an_oversized_payload_len_is_rejected_rather_than_waited_on() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 8, BLOCK_SIZE);
        let id = 2u64;
        client.slots[id as usize % client.depth] = Slot {
            id, io_u: 0x2000usize as *mut c_void,
            buffer: ptr::null_mut(), len: BLOCK_SIZE,
            opcode: OP_READ, queued_at: Some(Instant::now()), stage_delay_ns: 0,
        };
        // A 96-byte header claiming a body far larger than `max_bs`. Built by
        // patching the length field rather than by asking `response_frame` for
        // the body, because a real 1 MiB write would just block the socketpair.
        let mut header = response_frame(OP_READ, id, BLOCK_SIZE, 0);
        header[28..32].copy_from_slice(&(1u32 << 20).to_le_bytes());
        b.write_all(&header).unwrap();
        client.fill().unwrap();

        assert_eq!(
            client.framed_len(),
            Some(RESPONSE_LEN),
            "an impossible payload_len must be handed on, not waited for",
        );
        assert_eq!(client.collect_framed(RESPONSE_LEN), Err(libc_errno::EPROTO));
    }

    /// Different sizes in one batch must keep their own header/payload
    /// boundaries — the staging buffer is one contiguous write.
    #[test]
    fn mixed_sizes_in_one_commit_keep_their_boundaries() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 8, 32 * 1024);
        let sizes = [BLOCK_SIZE, 32 * 1024, 2 * BLOCK_SIZE];
        let mut payloads: Vec<Vec<u8>> = sizes
            .iter()
            .enumerate()
            .map(|(i, len)| vec![0xD0 + i as u8; *len as usize])
            .collect();

        for (i, len) in sizes.iter().enumerate() {
            let status = unsafe {
                client.queue((0x1000 + i) as *mut c_void, ONYX_OP_WRITE,
                             i as u64 * 64 * 1024, payloads[i].as_mut_ptr(), *len)
            };
            assert_eq!(status, Ok(FIO_Q_QUEUED));
        }
        client.commit().unwrap();

        let expected: usize = sizes.iter().map(|len| REQUEST_LEN + *len as usize).sum();
        let mut got = vec![0u8; expected];
        b.read_exact(&mut got).unwrap();
        let mut at = 0;
        for (i, len) in sizes.iter().enumerate() {
            assert_eq!(u16_at(&got, at + 6), OP_WRITE, "request {i} header");
            assert_eq!(u32_at(&got, at + 32), *len, "request {i} io_len");
            assert_eq!(u64_at(&got, at + 24), i as u64 * 64 * 1024, "request {i} offset");
            assert_eq!(
                got[at + REQUEST_LEN], 0xD0 + i as u8,
                "request {i}'s payload must start right after ITS header",
            );
            at += REQUEST_LEN + *len as usize;
        }
    }

    /// A sync must not reach the wire, and must not be double-reported.
    ///
    /// Onyx acks a write only after its LV2 `fdatasync`, so there is nothing
    /// for a flush to push out. `FIO_Q_COMPLETED` accounts for the io_u in
    /// fio's queue path, so pushing it onto `completed` as well would hand
    /// fio the same io_u a second time through `event()`.
    #[test]
    fn sync_completes_locally_without_touching_the_wire_or_completed() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client(a, 4);
        let status = unsafe {
            client.queue(0x9000usize as *mut c_void, ONYX_OP_SYNC, 0, ptr::null_mut(), 0)
        };
        assert_eq!(status, Ok(FIO_Q_COMPLETED));
        assert!(client.pending.is_empty(), "a sync must stage no bytes");
        assert!(client.staged.is_empty());
        assert!(
            client.completed.is_empty(),
            "FIO_Q_COMPLETED already accounted for it; event() must not see it again",
        );
        assert!(client.slots.iter().all(|s| s.io_u.is_null()), "no slot consumed");
    }

    /// A trim carries no payload but does carry a length, and fio hands it no
    /// buffer — so the null-buffer check must not reject it.
    #[test]
    fn trim_stages_a_header_only_request() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client_bs(a, 4, 32 * 1024);
        let len = 8 * BLOCK_SIZE;
        let status = unsafe {
            client.queue(0x4000usize as *mut c_void, ONYX_OP_TRIM, 4096,
                         ptr::null_mut(), len)
        };
        assert_eq!(status, Ok(FIO_Q_QUEUED));
        assert_eq!(client.pending.len(), REQUEST_LEN, "no payload follows a trim");
        assert_eq!(u16_at(&client.pending, 6), OP_TRIM);
        assert_eq!(u32_at(&client.pending, 12), 0, "payload_len must be 0");
        assert_eq!(u32_at(&client.pending, 32), len, "io_len is the trim range");
    }

    /// A read or write with no buffer is still a bug, even now that trim is
    /// allowed to pass a null one.
    #[test]
    fn a_data_op_without_a_buffer_is_still_refused() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client(a, 4);
        for op in [ONYX_OP_READ, ONYX_OP_WRITE] {
            let status = unsafe {
                client.queue(0x5000usize as *mut c_void, op, 0, ptr::null_mut(), BLOCK_SIZE)
            };
            assert_eq!(status, Err(libc_errno::EINVAL));
        }
    }

    /// An op code the bridge should never emit must be refused, not mapped to
    /// whatever opcode happens to sit at that index.
    #[test]
    fn an_unknown_op_is_refused() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = test_client(a, 4);
        let mut payload = [0u8; BLOCK_SIZE as usize];
        for op in [-1, 4, 99] {
            let status = unsafe {
                client.queue(0x6000usize as *mut c_void, op, 0,
                             payload.as_mut_ptr(), BLOCK_SIZE)
            };
            assert_eq!(status, Err(libc_errno::EOPNOTSUPP), "op {op}");
        }
    }

    /// `rx_capacity` has to satisfy BOTH jobs: batch small completions, and
    /// never fall below the compaction floor for large ones.
    #[test]
    fn rx_capacity_holds_the_compaction_floor_at_every_block_size() {
        for max_bs in [BLOCK_SIZE, 8 * 1024, 32 * 1024, 64 * 1024, MAX_IO_BYTES] {
            let capacity = rx_capacity(max_bs);
            assert!(
                capacity >= 2 * max_frame(max_bs),
                "max_bs {max_bs}: {capacity} is below the {} floor",
                2 * max_frame(max_bs),
            );
            assert!(
                capacity >= 64 * (RESPONSE_LEN + BLOCK_SIZE as usize),
                "max_bs {max_bs}: {capacity} gives up small-completion batching",
            );
        }
        assert_eq!(
            rx_capacity(BLOCK_SIZE),
            64 * (RESPONSE_LEN + BLOCK_SIZE as usize),
            "a 4k job must not pay for a large job's frames",
        );
    }

    /// The plugin's copies of the engine's protocol constants. A silent drift
    /// here is exactly what the `HELLO` capability check exists to catch at
    /// runtime, but catching it at build time is cheaper.
    #[test]
    fn protocol_constants_are_self_consistent() {
        assert_eq!(MAX_IO_BYTES % BLOCK_SIZE, 0);
        assert!(MAX_IO_BYTES >= BLOCK_SIZE);
        assert_eq!(HELLO_CAPABILITY_LEN, 16);
        assert_eq!(VERSION, 3, "the capability payload arrived with version 3");
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
