//! `BlockBackend` — the device seam shared by LV2 (write buffer) and LV3
//! (data) so they can sit on either a single OS file/block device or a
//! chunklet `LogicalDisk` (RAID over many PDs) without either layer caring
//! which.
//!
//! # Why this exists
//!
//! Today LV2/LV3 take a concrete [`RawDevice`] and, on Linux, drive their own
//! `io_uring` directly against its fd (`RawDevice::as_raw_fd`). A chunklet LD
//! is striped across multiple PDs, so there is no single fd to hand to
//! `io_uring` — and chunklet already owns the cross-PD `io_uring` batching
//! internally. The integration contract is therefore: **onyx hands buffers to
//! the backend; the backend owns whatever device-level batching exists.**
//!
//! `uring_target` is the discriminator. A file/blockdev backend returns
//! `Some((fd, base_off))` so the existing onyx `io_uring` hot path keeps
//! working with zero regression; a chunklet backend returns `None`, and the
//! caller falls back to the synchronous batched [`BlockBackend::read_many_at`]
//! / [`BlockBackend::write_many_at`] + [`BlockBackend::flush`] path (chunklet
//! parallelises the batch across PDs under the hood).
//!
//! # Concurrency
//!
//! Implementations are `Send + Sync` and every method takes `&self`: the
//! backend is shared behind an `Arc` across all LV2 sync threads / LV3
//! ReadPool workers. `RawDevice` is internally fd-per-call-safe (`pread`/
//! `pwrite` are positional), and a chunklet LD serialises only at the stripe
//! level, so concurrent callers do not need external coordination here.

use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use onyx_chunklet::io::{with_io_class, IoClass as ChunkletIoClass};
use onyx_chunklet::ld::LogicalDisk;
use onyx_chunklet::ChunkletError;
use parking_lot::{Condvar, Mutex};
use serde::Serialize;

use crate::error::{OnyxError, OnyxResult};
use crate::io::device::RawDevice;

/// IO service class used by the Onyx-side chunklet admission scheduler.
///
/// The scheduler counts synchronous chunklet batch calls, not their internal
/// PD operations. Keeping LV3 and Meta separate matters: a MetaDB checkpoint
/// can fan out many calls and must not strand the LV3 drain path behind one
/// shared background FIFO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IoClass {
    /// LV2 commit-log writes that gate foreground acknowledgement.
    Foreground,
    /// LV3 payload writes that drain the durable LV2 ring.
    Lv3,
    /// MetaDB WAL, apply, and checkpoint writes.
    Meta,
}

impl IoClass {
    const ALL: [Self; 3] = [Self::Foreground, Self::Lv3, Self::Meta];

    const fn index(self) -> usize {
        match self {
            Self::Foreground => 0,
            Self::Lv3 => 1,
            Self::Meta => 2,
        }
    }

    const fn chunklet(self) -> ChunkletIoClass {
        match self {
            Self::Foreground => ChunkletIoClass::Foreground,
            Self::Lv3 => ChunkletIoClass::DrainData,
            Self::Meta => ChunkletIoClass::DrainMeta,
        }
    }
}

/// Per-class scheduler counters captured under the scheduler mutex.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ChunkletIoClassSnapshot {
    pub reserved: u32,
    pub active: u32,
    pub waiters: u32,
    pub max_active: u32,
    pub max_waiters: u32,
    pub admissions: u64,
    pub wait_ns: u64,
    pub wait_max_ns: u64,
    pub borrowed_admissions: u64,
    /// Time for this class to regain service after it queued behind another
    /// class holding more than its reservation.
    pub reclaim_max_ns: u64,
    pub reclaim_current_ns: u64,
    pub reclaim_events: u64,
    pub reclaim_in_progress: bool,
}

/// Point-in-time scheduler state and lifetime high-water counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ChunkletIoSchedulerSnapshot {
    pub total_limit: u32,
    pub foreground_reserved: u32,
    pub lv3_reserved: u32,
    pub meta_reserved: u32,
    pub total_active: u32,
    pub total_max_active: u32,
    pub foreground: ChunkletIoClassSnapshot,
    pub lv3: ChunkletIoClassSnapshot,
    pub meta: ChunkletIoClassSnapshot,
    /// Aggregate maximum/current reclaim delay across all three classes.
    pub reclaim_max_ns: u64,
    pub reclaim_current_ns: u64,
    pub reclaim_events: u64,
    pub reclaim_in_progress: bool,
}

#[derive(Debug)]
struct SchedulerWaiter {
    cv: Condvar,
}

#[derive(Default)]
struct SchedulerClassState {
    active: u32,
    max_active: u32,
    max_waiters: u32,
    admissions: u64,
    wait_ns: u64,
    wait_max_ns: u64,
    borrowed_admissions: u64,
    /// Explicit nodes avoid a numeric ticket that could wrap after a long run.
    queue: VecDeque<Arc<SchedulerWaiter>>,
    reclaim_started: Option<Instant>,
    reclaim_max_ns: u64,
    reclaim_events: u64,
}

struct SchedulerState {
    total_active: u32,
    total_max_active: u32,
    next_class: usize,
    classes: [SchedulerClassState; 3],
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            total_active: 0,
            total_max_active: 0,
            next_class: 0,
            classes: std::array::from_fn(|_| SchedulerClassState::default()),
        }
    }
}

impl SchedulerState {
    fn class(&self, class: IoClass) -> &SchedulerClassState {
        &self.classes[class.index()]
    }

    fn class_mut(&mut self, class: IoClass) -> &mut SchedulerClassState {
        &mut self.classes[class.index()]
    }
}

/// Bounded three-class admission for synchronous chunklet write batches.
///
/// Each class owns a non-zero guaranteed reservation. Deficit classes are
/// served first; once every waiting class has its reservation, any queued class
/// may borrow idle shares up to `total_limit`. Existing borrowers are not
/// preempted and retire at the next batch completion. A FIFO queue per class
/// prevents same-class starvation.
pub(crate) struct ChunkletIoScheduler {
    total_limit: u32,
    reserved: [u32; 3],
    state: Mutex<SchedulerState>,
}

impl ChunkletIoScheduler {
    /// Build a scheduler with protected `[Foreground, LV3, Meta]` shares.
    /// No production default is embedded here so box experiments can sweep the
    /// total and contention split independently.
    pub(crate) fn new(total_limit: u32, reserved: [u32; 3]) -> OnyxResult<Self> {
        let reserved_sum = reserved
            .into_iter()
            .try_fold(0u32, |sum, share| sum.checked_add(share));
        if total_limit == 0
            || reserved.into_iter().any(|share| share == 0)
            || !reserved_sum.is_some_and(|sum| sum <= total_limit)
        {
            return Err(OnyxError::Config(
                "chunklet IO scheduler requires three non-zero reservations whose checked sum does not exceed total"
                    .into(),
            ));
        }
        Ok(Self {
            total_limit,
            reserved,
            state: Mutex::new(SchedulerState::default()),
        })
    }

    fn reserved(&self, class: IoClass) -> u32 {
        self.reserved[class.index()]
    }

    fn next_candidate(&self, state: &SchedulerState) -> Option<IoClass> {
        let has_deficit = IoClass::ALL.into_iter().any(|class| {
            !state.class(class).queue.is_empty() && state.class(class).active < self.reserved(class)
        });
        (0..IoClass::ALL.len()).find_map(|offset| {
            let class = IoClass::ALL[(state.next_class + offset) % IoClass::ALL.len()];
            let own = state.class(class);
            (!own.queue.is_empty() && (!has_deficit || own.active < self.reserved(class)))
                .then_some(class)
        })
    }

    fn candidate_waiter(&self, state: &SchedulerState) -> Option<Arc<SchedulerWaiter>> {
        self.next_candidate(state)
            .and_then(|class| state.class(class).queue.front().cloned())
    }

    fn elapsed_ns(started: Instant) -> u64 {
        started.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn other_class_is_borrowing(&self, state: &SchedulerState, class: IoClass) -> bool {
        IoClass::ALL
            .into_iter()
            .any(|other| other != class && state.class(other).active > self.reserved(other))
    }

    fn maybe_start_reclaim(&self, state: &mut SchedulerState, class: IoClass) {
        if state.class(class).active < self.reserved(class)
            && state.class(class).reclaim_started.is_none()
            && self.other_class_is_borrowing(state, class)
        {
            let own = state.class_mut(class);
            own.reclaim_started = Some(Instant::now());
            own.reclaim_events = own.reclaim_events.saturating_add(1);
        }
    }

    fn finish_reclaim(state: &mut SchedulerState, class: IoClass) {
        let own = state.class_mut(class);
        if let Some(started) = own.reclaim_started.take() {
            let elapsed = Self::elapsed_ns(started);
            own.reclaim_max_ns = own.reclaim_max_ns.max(elapsed);
        }
    }

    fn record_admission(&self, state: &mut SchedulerState, class: IoClass, wait_ns: u64) {
        let reserved = self.reserved(class);
        {
            let own = state.class_mut(class);
            let borrowed = own.active >= reserved;
            own.active = own
                .active
                .checked_add(1)
                .expect("scheduler class active count overflow");
            own.max_active = own.max_active.max(own.active);
            own.admissions = own.admissions.saturating_add(1);
            own.wait_ns = own.wait_ns.saturating_add(wait_ns);
            own.wait_max_ns = own.wait_max_ns.max(wait_ns);
            if borrowed {
                own.borrowed_admissions = own.borrowed_admissions.saturating_add(1);
            }
        }
        state.total_active = state
            .total_active
            .checked_add(1)
            .expect("scheduler total active count overflow");
        debug_assert!(state.total_active <= self.total_limit);
        state.total_max_active = state.total_max_active.max(state.total_active);
        state.next_class = (class.index() + 1) % IoClass::ALL.len();
        Self::finish_reclaim(state, class);
    }

    pub(crate) fn acquire(self: &Arc<Self>, class: IoClass) -> ChunkletIoPermit {
        let mut state = self.state.lock();
        if state.total_active < self.total_limit
            && IoClass::ALL
                .into_iter()
                .all(|queued| state.class(queued).queue.is_empty())
        {
            self.record_admission(&mut state, class, 0);
            drop(state);
            return ChunkletIoPermit {
                scheduler: Arc::clone(self),
                class,
            };
        }
        drop(state);

        let waiter = Arc::new(SchedulerWaiter { cv: Condvar::new() });
        let wait_started = Instant::now();
        let mut state = self.state.lock();
        {
            let own = state.class_mut(class);
            own.queue.push_back(Arc::clone(&waiter));
            own.max_waiters = own
                .max_waiters
                .max(u32::try_from(own.queue.len()).unwrap_or(u32::MAX));
        }
        loop {
            let is_head = state
                .class(class)
                .queue
                .front()
                .is_some_and(|head| Arc::ptr_eq(head, &waiter));
            if is_head
                && self.next_candidate(&state) == Some(class)
                && state.total_active < self.total_limit
            {
                break;
            }
            if is_head {
                self.maybe_start_reclaim(&mut state, class);
            }
            waiter.cv.wait(&mut state);
        }

        let wait_ns = Self::elapsed_ns(wait_started);
        {
            let own = state.class_mut(class);
            let head = own.queue.pop_front().expect("scheduler waiter disappeared");
            debug_assert!(Arc::ptr_eq(&head, &waiter));
        }
        self.record_admission(&mut state, class, wait_ns);
        let next = (state.total_active < self.total_limit)
            .then(|| self.candidate_waiter(&state))
            .flatten();
        drop(state);
        if let Some(waiter) = next {
            waiter.cv.notify_one();
        }

        ChunkletIoPermit {
            scheduler: Arc::clone(self),
            class,
        }
    }

    fn release(&self, class: IoClass) {
        let mut state = self.state.lock();
        let own = state.class_mut(class);
        debug_assert!(own.active > 0, "scheduler permit released twice");
        own.active = own.active.saturating_sub(1);
        debug_assert!(state.total_active > 0, "scheduler total underflow");
        state.total_active = state.total_active.saturating_sub(1);
        let next = self.candidate_waiter(&state);
        drop(state);
        if let Some(waiter) = next {
            waiter.cv.notify_one();
        }
    }

    pub(crate) fn snapshot(&self) -> ChunkletIoSchedulerSnapshot {
        let state = self.state.lock();
        let class_snapshot = |class: IoClass| {
            let state = state.class(class);
            let reclaim_current_ns = state.reclaim_started.map(Self::elapsed_ns).unwrap_or(0);
            ChunkletIoClassSnapshot {
                reserved: self.reserved(class),
                active: state.active,
                waiters: u32::try_from(state.queue.len()).unwrap_or(u32::MAX),
                max_active: state.max_active,
                max_waiters: state.max_waiters,
                admissions: state.admissions,
                wait_ns: state.wait_ns,
                wait_max_ns: state.wait_max_ns,
                borrowed_admissions: state.borrowed_admissions,
                reclaim_max_ns: state.reclaim_max_ns.max(reclaim_current_ns),
                reclaim_current_ns,
                reclaim_events: state.reclaim_events,
                reclaim_in_progress: state.reclaim_started.is_some(),
            }
        };
        let foreground = class_snapshot(IoClass::Foreground);
        let lv3 = class_snapshot(IoClass::Lv3);
        let meta = class_snapshot(IoClass::Meta);
        let reclaim_max_ns = foreground
            .reclaim_max_ns
            .max(lv3.reclaim_max_ns)
            .max(meta.reclaim_max_ns);
        let reclaim_current_ns = foreground
            .reclaim_current_ns
            .max(lv3.reclaim_current_ns)
            .max(meta.reclaim_current_ns);
        ChunkletIoSchedulerSnapshot {
            total_limit: self.total_limit,
            foreground_reserved: self.reserved(IoClass::Foreground),
            lv3_reserved: self.reserved(IoClass::Lv3),
            meta_reserved: self.reserved(IoClass::Meta),
            total_active: state.total_active,
            total_max_active: state.total_max_active,
            foreground,
            lv3,
            meta,
            reclaim_max_ns,
            reclaim_current_ns,
            reclaim_events: foreground
                .reclaim_events
                .saturating_add(lv3.reclaim_events)
                .saturating_add(meta.reclaim_events),
            reclaim_in_progress: foreground.reclaim_in_progress
                || lv3.reclaim_in_progress
                || meta.reclaim_in_progress,
        }
    }
}

/// Releases one scheduler admission on every return path, including errors and
/// stale-handle retry failures.
#[must_use = "dropping the permit releases the scheduler admission"]
pub(crate) struct ChunkletIoPermit {
    scheduler: Arc<ChunkletIoScheduler>,
    class: IoClass,
}

impl Drop for ChunkletIoPermit {
    fn drop(&mut self) {
        self.scheduler.release(self.class);
    }
}

/// Linear block device exposed to LV2/LV3. Offsets and lengths are byte
/// quantities; alignment to the underlying block size is the implementation's
/// concern (`RawDevice` bounce-buffers unaligned IO, a chunklet LD requires
/// 4 KiB alignment from the caller).
pub trait BlockBackend: Send + Sync {
    /// Read exactly `buf.len()` bytes starting at `offset`.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()>;

    /// Write exactly `buf.len()` bytes starting at `offset`. The write is not
    /// guaranteed crash-durable until a subsequent [`BlockBackend::flush`].
    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()>;

    /// Read several independent `(offset, buf)` pairs. The default loops
    /// `read_at`; a chunklet backend overrides this to fan the batch across
    /// PDs in one submit.
    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        for (offset, buf) in ops.iter_mut() {
            self.read_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Write several independent `(offset, buf)` pairs. The default loops
    /// `write_at`; a chunklet backend overrides this to fan the batch across
    /// PDs in one submit. Durability still requires a following `flush`.
    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        for (offset, buf) in ops {
            self.write_at(buf, *offset)?;
        }
        Ok(())
    }

    /// Make every prior `write_at` / `write_many_at` crash-durable. This is
    /// the persistence barrier LV2's ack-after-durable gate waits on.
    fn flush(&self) -> OnyxResult<()>;

    /// Total addressable bytes. For a chunklet LD this is `capacity_bytes()`
    /// (already net of per-chunklet headers and parity).
    fn size(&self) -> u64;

    /// Whether the backend requires block-aligned offsets/lengths and uses the
    /// O_DIRECT-style on-disk encoding. Drives the buffer commit-log's compact
    /// "flushed" marker format ([`crate::buffer`]'s `mark_entry_flushed`): a
    /// direct backend writes a full aligned block, a buffered one a sub-block
    /// header. Defaults to `true` (aligned) — the safe choice for block devices
    /// and a chunklet LD (which is always 4 KiB-aligned). `RawDevice` overrides
    /// it with its actual `O_DIRECT` status (regular-file test backends fall
    /// back to buffered IO).
    fn direct_io(&self) -> bool {
        true
    }

    /// `Some((fd, base_offset))` when this backend is a single OS file / block
    /// device whose fd can be handed to `io_uring` SQE construction; `None`
    /// for striped/RAID backends (chunklet), which forces callers onto the
    /// synchronous `*_many_at` + `flush` path.
    fn uring_target(&self) -> Option<(RawFd, u64)> {
        None
    }

    /// RAID full-stripe width in **blocks** — the write-alignment granularity
    /// the flush writer targets to hit a parity backend's zero-RMW full-stripe
    /// path. `1` (the default) means "no stripe constraint": single files, block
    /// devices, and non-parity chunklet LDs (mirror/plain/raid0) impose no
    /// alignment beyond one block, so the writer's stripe padding is a no-op.
    /// A chunklet RAID5/6 LD overrides this with `full_stripe_bytes / block_size`
    /// (e.g. 6 for a 6+2 RAID6 at a 4 KiB strip).
    fn stripe_blocks(&self) -> u32 {
        1
    }

    /// Human-readable identifier for error/log messages (a device path for a
    /// `RawDevice`, an LD label for chunklet).
    fn label(&self) -> String {
        "block-device".to_string()
    }
}

/// `RawDevice` *is* a `BlockBackend`: a single OS file or block device. This
/// preserves today's behaviour for non-chunklet deployments (sparse-file
/// tests, single-disk dev, kernel md/LVM) and lets `&RawDevice` coerce to
/// `&dyn BlockBackend` at the many superblock / startup call sites. Because it
/// is a single fd, `uring_target` returns `Some`, keeping the existing onyx
/// `io_uring` LV2/LV3 hot paths untouched.
impl BlockBackend for RawDevice {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        RawDevice::read_at(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        RawDevice::write_at(self, buf, offset)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.sync()
    }

    fn size(&self) -> u64 {
        RawDevice::size(self)
    }

    fn uring_target(&self) -> Option<(RawFd, u64)> {
        Some((self.as_raw_fd(), self.base_offset()))
    }

    fn direct_io(&self) -> bool {
        RawDevice::is_direct_io(self)
    }

    fn label(&self) -> String {
        self.path().display().to_string()
    }
}

/// A fixed `[base, base+len)` window over another `BlockBackend`, translating
/// every offset into the inner backend's address space. This replaces
/// [`RawDevice::slice`] for the device-seam abstraction: the LV2 buffer pool
/// carves per-shard sub-views (data area + checkpoint block) out of one root
/// backend, which may be a single file/blockdev *or* one chunklet `LogicalDisk`
/// shared by every shard.
///
/// `uring_target` adds the window base to the inner fd's base offset, so a
/// `RawDevice`-backed slice reports exactly the `(fd, shard_offset)` the old
/// `RawDevice::slice` did — keeping the io_uring LV2 hot path byte-for-byte
/// identical. A chunklet-backed slice inherits `None` and forces the
/// synchronous `*_many_at` + `flush` path.
pub struct BackendSlice {
    inner: Arc<dyn BlockBackend>,
    base: u64,
    /// Window length. Atomic so a meta-LD page window can be widened online
    /// (after the inner `ChunkletBackend` swaps in the extended LD) via
    /// [`Self::grow_len`], while concurrent IO bounds-checks read it lock-free.
    len: AtomicU64,
}

impl BackendSlice {
    /// Build a window over `inner`. Errors if `base + len` overflows or exceeds
    /// the inner backend's size (mirrors `RawDevice::slice` bounds checks).
    pub fn new(inner: Arc<dyn BlockBackend>, base: u64, len: u64) -> OnyxResult<Self> {
        let end = base.checked_add(len).ok_or_else(|| {
            crate::error::OnyxError::Config(format!(
                "backend slice overflow: base={base} len={len}"
            ))
        })?;
        if end > inner.size() {
            return Err(crate::error::OnyxError::Config(format!(
                "backend slice out of bounds: base={base} len={len} inner_size={}",
                inner.size()
            )));
        }
        Ok(Self {
            inner,
            base,
            len: AtomicU64::new(len),
        })
    }

    /// Widen the window to `new_len` after the inner backend has grown (meta-LD
    /// `extend_ld` → `ChunkletBackend::swap_ld`). Grow-only, and bounded by the
    /// (now larger) inner size so the window can never address past the device.
    pub fn grow_len(&self, new_len: u64) -> OnyxResult<()> {
        let end = self.base.checked_add(new_len).ok_or_else(|| {
            crate::error::OnyxError::Config(format!(
                "backend slice grow overflow: base={} new_len={new_len}",
                self.base
            ))
        })?;
        if end > self.inner.size() {
            return Err(crate::error::OnyxError::Config(format!(
                "backend slice grow out of bounds: base={} new_len={new_len} inner_size={}",
                self.base,
                self.inner.size()
            )));
        }
        let cur = self.len.load(Ordering::Relaxed);
        if new_len < cur {
            return Err(crate::error::OnyxError::Config(format!(
                "backend slice grow would shrink window {cur} -> {new_len}"
            )));
        }
        self.len.store(new_len, Ordering::Release);
        Ok(())
    }

    fn translate(&self, offset: u64, len: usize) -> OnyxResult<u64> {
        let end = offset.checked_add(len as u64).ok_or_else(|| {
            crate::error::OnyxError::Config(format!(
                "backend slice IO overflow: offset={offset} len={len}"
            ))
        })?;
        let window = self.len.load(Ordering::Relaxed);
        if end > window {
            return Err(crate::error::OnyxError::Config(format!(
                "out-of-bounds slice IO: offset={offset} len={len} window={window}"
            )));
        }
        Ok(self.base + offset)
    }
}

impl BlockBackend for BackendSlice {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        let abs = self.translate(offset, buf.len())?;
        self.inner.read_at(buf, abs)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        let abs = self.translate(offset, buf.len())?;
        self.inner.write_at(buf, abs)
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        // Translate each window-relative offset to the inner address space,
        // then fan the whole batch through the inner backend in one call so a
        // chunklet inner still parallelises across PDs.
        let mut abs_ops: Vec<(u64, &mut [u8])> = Vec::with_capacity(ops.len());
        for (offset, buf) in ops.iter_mut() {
            let abs = self.translate(*offset, buf.len())?;
            abs_ops.push((abs, &mut **buf));
        }
        self.inner.read_many_at(&mut abs_ops)
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        let mut abs_ops: Vec<(u64, &[u8])> = Vec::with_capacity(ops.len());
        for (offset, buf) in ops {
            let abs = self.translate(*offset, buf.len())?;
            abs_ops.push((abs, *buf));
        }
        self.inner.write_many_at(&abs_ops)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.inner.flush()
    }

    fn size(&self) -> u64 {
        self.len.load(Ordering::Relaxed)
    }

    fn uring_target(&self) -> Option<(RawFd, u64)> {
        self.inner
            .uring_target()
            .map(|(fd, inner_base)| (fd, inner_base + self.base))
    }

    fn stripe_blocks(&self) -> u32 {
        self.inner.stripe_blocks()
    }

    fn direct_io(&self) -> bool {
        self.inner.direct_io()
    }

    fn label(&self) -> String {
        format!(
            "{}[+{}:{}]",
            self.inner.label(),
            self.base,
            self.len.load(Ordering::Relaxed)
        )
    }
}

/// Convenience constructor: a windowed sub-view as an `Arc<dyn BlockBackend>`,
/// the form every per-shard slice consumer wants.
pub fn slice_backend(
    inner: Arc<dyn BlockBackend>,
    base: u64,
    len: u64,
) -> OnyxResult<Arc<dyn BlockBackend>> {
    Ok(Arc::new(BackendSlice::new(inner, base, len)?))
}

/// `BlockBackend` over a chunklet [`LogicalDisk`] — the production RAID path.
///
/// All io_uring lives inside chunklet (its `UringBackend` fans a batch across
/// the LD's member PDs), so this backend exposes no fd: `uring_target` returns
/// `None`, steering LV2/LV3 onto the synchronous `*_many_at` + `flush` path.
/// `read_many_at` / `write_many_at` forward the whole batch to the LD so
/// chunklet can parallelise it in one submit. `size` is the LD's
/// `capacity_bytes` (already net of per-chunklet headers + parity).
///
/// # Stale-handle refresh on failover / rebuild
///
/// A `Pool::mark_pd_failed` (disk pull → PD-health watchdog) and a rebuild
/// commit both bump the chunklet LD **runtime epoch**, which forces every
/// handle opened at the old epoch to fail its next IO with a `handle is stale:
/// runtime epoch advanced` invariant so reads switch to reconstruct and writes
/// write-forward onto the surviving PDs. This backend is the single seam every
/// LV2 commit-log write, LV3 data write, and meta IO funnels through (directly
/// or via a [`BackendSlice`]), so it centralises the recovery: on that error it
/// re-`open_ld`s the LD onto the new epoch, swaps the fresh handle into the
/// [`ArcSwap`] cell, and retries — transparently to every caller. Before this,
/// the LV2 sync writer spin-retried the same stale handle tens of thousands of
/// times and wedged the flush path on a live disk pull (2026-07-06 box).
pub struct ChunkletBackend {
    /// Swappable LD handle. `extend_ld` does NOT bump the runtime epoch, so the
    /// handle opened at startup keeps the OLD capacity forever; growing online
    /// means re-`open_ld` and atomically swapping the handle in here. In-flight
    /// IO holds a `load()` guard and completes against the old `Arc`; new IO
    /// picks up the new handle. See [`Self::swap_ld`].
    ///
    /// `arc_swap::ArcSwap<T>` requires `T: Sized`, so the trait object is stored
    /// behind an extra `Arc` (the standard trait-object workaround); reads stay
    /// lock-free at the cost of one extra pointer hop.
    ld: ArcSwap<Arc<dyn LogicalDisk>>,
    /// LD capacity in bytes, published AFTER a `swap_ld` installs a larger
    /// handle (grow-only) so a reader never sees a bigger capacity paired with
    /// a handle that would reject IO past the old top.
    capacity: AtomicU64,
    /// Keeps the owning `Pool` alive for the backend's lifetime (the LD holds
    /// its member PD `Arc`s, but the Pool owns background/management state and
    /// is the handle the Phase-4 ops surface needs). `None` when the caller
    /// manages the pool lifetime itself (tests). Also the handle
    /// [`Self::reopen_after_stale`] re-`open_ld`s through on an epoch advance —
    /// with no pool there is nothing to re-open against, so the stale error just
    /// surfaces.
    pool: Option<Arc<onyx_chunklet::Pool>>,
    /// Serialises stale-handle re-opens so a herd of concurrent IO threads all
    /// hitting the same epoch advance re-`open_ld`s the LD once, not once per
    /// thread. Threads that lose the race find the handle already swapped and
    /// simply retry against it.
    reopen_lock: Mutex<()>,
    /// Physical-IO class passed into chunklet for every write, independent of
    /// whether the legacy Onyx-side batch admission scheduler is enabled.
    nested_io_class: ChunkletIoClass,
    /// Optional Onyx-side write admission. Disabled for legacy constructors
    /// and callers that have not explicitly wired all LD roles to one shared
    /// scheduler.
    io_scheduler: Option<(Arc<ChunkletIoScheduler>, IoClass)>,
}

impl ChunkletBackend {
    pub fn new(ld: Arc<dyn LogicalDisk>) -> Self {
        let capacity = ld.capacity_bytes();
        Self {
            ld: ArcSwap::from_pointee(ld),
            capacity: AtomicU64::new(capacity),
            pool: None,
            reopen_lock: Mutex::new(()),
            nested_io_class: ChunkletIoClass::Foreground,
            io_scheduler: None,
        }
    }

    /// Build a backend that also keeps its owning `Pool` alive.
    pub fn with_pool(ld: Arc<dyn LogicalDisk>, pool: Arc<onyx_chunklet::Pool>) -> Self {
        let mut b = Self::new(ld);
        b.pool = Some(pool);
        b
    }

    /// Build an unscheduled role backend while preserving its nested physical
    /// IO class. This is the production path when legacy batch admission is off.
    pub(crate) fn with_pool_and_class(
        ld: Arc<dyn LogicalDisk>,
        pool: Arc<onyx_chunklet::Pool>,
        class: IoClass,
    ) -> Self {
        let mut backend = Self::with_pool(ld, pool);
        backend.nested_io_class = class.chunklet();
        backend
    }

    /// Build a scheduled backend without an owning Pool (primarily tests and
    /// callers that manage the Pool lifetime separately).
    pub(crate) fn new_scheduled(
        ld: Arc<dyn LogicalDisk>,
        class: IoClass,
        scheduler: Arc<ChunkletIoScheduler>,
    ) -> Self {
        let mut backend = Self::new(ld);
        backend.nested_io_class = class.chunklet();
        backend.io_scheduler = Some((scheduler, class));
        backend
    }

    /// Production constructor for one role-specific backend. LV2, LV3, and
    /// MetaDB must receive the same scheduler Arc for reservations to apply
    /// across the shared physical pool.
    pub(crate) fn with_pool_and_scheduler(
        ld: Arc<dyn LogicalDisk>,
        pool: Arc<onyx_chunklet::Pool>,
        class: IoClass,
        scheduler: Arc<ChunkletIoScheduler>,
    ) -> Self {
        let mut backend = Self::with_pool_and_class(ld, pool, class);
        backend.io_scheduler = Some((scheduler, class));
        backend
    }

    fn acquire_write_permit(&self) -> Option<ChunkletIoPermit> {
        self.io_scheduler
            .as_ref()
            .map(|(scheduler, class)| scheduler.acquire(*class))
    }

    /// The current underlying logical disk (e.g. to read `strip_size()` for
    /// packer alignment). Clones the inner `Arc<dyn LogicalDisk>` out of the
    /// double-`Arc` cell.
    pub fn logical_disk(&self) -> Arc<dyn LogicalDisk> {
        (**self.ld.load()).clone()
    }

    /// The owning pool, when this backend keeps it alive. The Phase-4 ops
    /// surface (status / rebuild / scrub / extend / replace-disk) routes
    /// through here.
    pub fn pool(&self) -> Option<&Arc<onyx_chunklet::Pool>> {
        self.pool.as_ref()
    }

    pub(crate) fn io_scheduler(&self) -> Option<Arc<ChunkletIoScheduler>> {
        self.io_scheduler
            .as_ref()
            .map(|(scheduler, _)| scheduler.clone())
    }

    /// Install a freshly-opened LD handle after an online `extend_ld`. Grow-only
    /// (extend is additive): store the new (larger) handle FIRST, then publish
    /// the new capacity, so a concurrent reader that observes the new capacity
    /// is guaranteed to also see the new handle that can service the extra
    /// range. In-flight IO already holding a `load()` guard finishes on the old
    /// `Arc`. `new_ld` must refer to the SAME LD (a re-`open_ld` of the extended
    /// disk); capacity must not shrink.
    pub fn swap_ld(&self, new_ld: Arc<dyn LogicalDisk>) {
        let new_cap = new_ld.capacity_bytes();
        self.ld.store(Arc::new(new_ld));
        self.capacity.store(new_cap, Ordering::Release);
    }

    /// Max transparent handle refreshes for a single IO before the stale error
    /// surfaces. A failover/rebuild epoch bump needs one refresh; the small
    /// ceiling tolerates a couple of back-to-back bumps (a `mark_pd_failed`
    /// immediately followed by a rebuild commit) without ever hot-spinning.
    /// Past it the error propagates and the caller's own retry loop (e.g. the
    /// LV2 sync loop's backoff) takes over.
    const MAX_STALE_REFRESH: u32 = 32;

    /// True for the "runtime epoch advanced" invariant chunklet raises when a
    /// `mark_pd_failed` / rebuild-commit bumps the LD runtime epoch out from
    /// under a handle opened at the old epoch. Deliberately NOT true for the
    /// "LD was dropped" invariant (which shares the `handle is stale:` prefix) —
    /// a dropped LD never comes back, so re-opening is pointless and the error
    /// must surface.
    fn is_epoch_advanced(err: &ChunkletError) -> bool {
        matches!(err, ChunkletError::Invariant(msg) if msg.contains("runtime epoch advanced"))
    }

    /// Re-open the LD onto the current runtime epoch and swap the fresh handle
    /// into the cell, so subsequent IO reconstructs off / write-forwards onto
    /// the surviving PDs. Deduplicated via `reopen_lock`: only the first thread
    /// to observe a given stale handle re-opens; a thread that finds the handle
    /// already swapped (pointer changed) just returns so the caller retries
    /// against the new handle. Capacity is unchanged by a failover/rebuild
    /// (no rows added), so it is left as-is — the online-extend path owns
    /// capacity growth via [`Self::swap_ld`].
    fn reopen_after_stale(&self, failed: &Arc<dyn LogicalDisk>) -> OnyxResult<()> {
        let Some(pool) = self.pool.as_ref() else {
            // No owning Pool (a bare `new` test backend): nothing to re-open
            // against, so let the original stale error surface to the caller.
            return Ok(());
        };
        let _g = self.reopen_lock.lock();
        // If another thread already installed a fresh handle, don't re-open
        // again — the caller will retry against the newly-swapped handle.
        let current = self.ld.load();
        let current_ref: &Arc<dyn LogicalDisk> = &current;
        if !Arc::ptr_eq(current_ref, failed) {
            return Ok(());
        }
        let ld_id = failed.id();
        let fresh = pool.open_ld(ld_id)?;
        self.ld.store(Arc::new(fresh));
        Ok(())
    }

    /// Run `op` against the current LD handle, transparently re-opening and
    /// retrying on a stale-epoch error. This is the failover/rebuild handle
    /// refresh that keeps the LV2 commit-log writer, LV3 writer, and meta IO
    /// from spinning on a stale handle after a disk pull bumps the LD epoch.
    fn with_stale_retry<T>(
        &self,
        operation: &'static str,
        mut op: impl FnMut(&Arc<dyn LogicalDisk>) -> Result<T, ChunkletError>,
    ) -> OnyxResult<T> {
        let total_started = Instant::now();
        let mut load_elapsed = Duration::ZERO;
        let mut operation_elapsed = Duration::ZERO;
        let mut refresh_elapsed = Duration::ZERO;
        let mut refreshes = 0u32;
        loop {
            let load_started = Instant::now();
            let guard = self.ld.load();
            load_elapsed += load_started.elapsed();
            let ld: &Arc<dyn LogicalDisk> = &guard;
            let operation_started = Instant::now();
            let result = op(ld);
            operation_elapsed += operation_started.elapsed();
            match result {
                Ok(v) => {
                    let total_elapsed = total_started.elapsed();
                    if total_elapsed >= Duration::from_millis(10) {
                        tracing::warn!(
                            operation,
                            ld = %ld.id(),
                            refreshes,
                            load_us = load_elapsed.as_micros() as u64,
                            operation_us = operation_elapsed.as_micros() as u64,
                            refresh_us = refresh_elapsed.as_micros() as u64,
                            total_us = total_elapsed.as_micros() as u64,
                            "slow chunklet backend operation"
                        );
                    }
                    return Ok(v);
                }
                Err(err) => {
                    if Self::is_epoch_advanced(&err)
                        && self.pool.is_some()
                        && refreshes < Self::MAX_STALE_REFRESH
                    {
                        refreshes += 1;
                        let refresh_started = Instant::now();
                        // Back off only AFTER the first refresh: a single
                        // failover bump has already settled, so the common case
                        // pays no sleep. Repeated staleness (an in-flight
                        // rebuild still bumping) gets a small bounded backoff
                        // instead of a hot spin.
                        if refreshes > 1 {
                            std::thread::sleep(Duration::from_millis(u64::from(refreshes).min(8)));
                        }
                        self.reopen_after_stale(ld)?;
                        refresh_elapsed += refresh_started.elapsed();
                        continue;
                    }
                    let total_elapsed = total_started.elapsed();
                    if total_elapsed >= Duration::from_millis(10) {
                        tracing::warn!(
                            operation,
                            ld = %ld.id(),
                            refreshes,
                            load_us = load_elapsed.as_micros() as u64,
                            operation_us = operation_elapsed.as_micros() as u64,
                            refresh_us = refresh_elapsed.as_micros() as u64,
                            total_us = total_elapsed.as_micros() as u64,
                            error = %err,
                            "slow failed chunklet backend operation"
                        );
                    }
                    return Err(err.into());
                }
            }
        }
    }
}

impl BlockBackend for ChunkletBackend {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        // chunklet's argument order is (offset, buf); ours is (buf, offset).
        self.with_stale_retry("read_at", |ld| ld.read_at(offset, buf))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        let _permit = self.acquire_write_permit();
        with_io_class(self.nested_io_class, || {
            self.with_stale_retry("write_at", |ld| ld.write_at(offset, buf))
        })
    }

    fn read_many_at(&self, ops: &mut [(u64, &mut [u8])]) -> OnyxResult<()> {
        // Signature matches chunklet's `LogicalDisk::read_many_at` exactly.
        self.with_stale_retry("read_many_at", |ld| ld.read_many_at(ops))
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        let _permit = self.acquire_write_permit();
        with_io_class(self.nested_io_class, || {
            self.with_stale_retry("write_many_at", |ld| ld.write_many_at(ops))
        })
    }

    fn flush(&self) -> OnyxResult<()> {
        // A barrier must never wait behind writes whose durability it may need
        // to establish. Current write-through PDs skip it, but preserving this
        // bypass also keeps future write-back devices deadlock-free.
        with_io_class(self.nested_io_class, || {
            self.with_stale_retry("flush", |ld| ld.flush())
        })
    }

    fn size(&self) -> u64 {
        self.capacity.load(Ordering::Acquire)
    }
    // uring_target defaults to None: striped across PDs, no single fd.

    fn stripe_blocks(&self) -> u32 {
        // `LdRaid5/6::strip_size()` already returns full_stripe_bytes (data
        // strips × strip), so full_stripe_blocks = strip_size / block_size.
        // Mirror/plain/raid0 report their block/strip size => 1..N with no
        // parity, and their `write_full_stripe` has no RMW to avoid, so the
        // writer's stripe padding on them is harmless (usually just 1).
        let ld = self.ld.load();
        let bs = ld.block_size() as u64;
        if bs == 0 {
            return 1;
        }
        let strip = ld.strip_size() as u64;
        u32::try_from((strip / bs).max(1)).unwrap_or(1)
    }

    fn label(&self) -> String {
        format!("chunklet-ld:{:?}", self.ld.load().id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    use onyx_chunklet::types::LdId;
    use tempfile::NamedTempFile;

    struct FailingLogicalDisk;

    impl LogicalDisk for FailingLogicalDisk {
        fn id(&self) -> LdId {
            LdId::nil()
        }

        fn capacity_bytes(&self) -> u64 {
            4096
        }

        fn block_size(&self) -> usize {
            4096
        }

        fn strip_size(&self) -> usize {
            4096
        }

        fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<(), ChunkletError> {
            Err(ChunkletError::Invariant("injected read failure".into()))
        }

        fn write_at(&self, _offset: u64, _buf: &[u8]) -> Result<(), ChunkletError> {
            Err(ChunkletError::Invariant("injected write failure".into()))
        }

        fn flush(&self) -> Result<(), ChunkletError> {
            Ok(())
        }
    }

    fn wait_for_scheduler(
        scheduler: &ChunkletIoScheduler,
        predicate: impl Fn(&ChunkletIoSchedulerSnapshot) -> bool,
    ) -> ChunkletIoSchedulerSnapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = scheduler.snapshot();
            if predicate(&snapshot) {
                return snapshot;
            }
            assert!(
                Instant::now() < deadline,
                "scheduler state did not converge: {snapshot:?}"
            );
            thread::yield_now();
        }
    }

    fn spawn_permit_holder(
        scheduler: Arc<ChunkletIoScheduler>,
        class: IoClass,
        id: u32,
        acquired: mpsc::Sender<u32>,
        release: mpsc::Receiver<()>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let _permit = scheduler.acquire(class);
            acquired.send(id).unwrap();
            release.recv().unwrap();
        })
    }

    #[test]
    fn chunklet_io_scheduler_validates_share() {
        assert!(ChunkletIoScheduler::new(0, [0, 0, 0]).is_err());
        assert!(ChunkletIoScheduler::new(4, [2, 2, 0]).is_err());
        assert!(ChunkletIoScheduler::new(4, [2, 1, 2]).is_err());
        assert!(ChunkletIoScheduler::new(u32::MAX, [u32::MAX, 1, 1]).is_err());
        let headroom = ChunkletIoScheduler::new(40, [9, 7, 9]).unwrap();
        assert_eq!(headroom.snapshot().total_limit, 40);
        let scheduler = ChunkletIoScheduler::new(40, [9, 7, 24]).unwrap();
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.total_limit, 40);
        assert_eq!(snapshot.foreground_reserved, 9);
        assert_eq!(snapshot.lv3_reserved, 7);
        assert_eq!(snapshot.meta_reserved, 24);
        assert_eq!(snapshot.foreground.reserved, 9);
        assert_eq!(snapshot.lv3.reserved, 7);
        assert_eq!(snapshot.meta.reserved, 24);
    }

    #[test]
    fn chunklet_io_scheduler_borrows_but_never_exceeds_total() {
        let scheduler = Arc::new(ChunkletIoScheduler::new(4, [1, 1, 2]).unwrap());
        let mut permits = Vec::new();
        for _ in 0..4 {
            permits.push(scheduler.acquire(IoClass::Meta));
        }
        let full = scheduler.snapshot();
        assert_eq!(full.total_active, 4);
        assert_eq!(full.total_max_active, 4);
        assert_eq!(full.meta.active, 4);
        assert_eq!(full.meta.borrowed_admissions, 2);

        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let waiter = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Meta,
            1,
            acquired_tx,
            release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.meta.waiters == 1);
        assert_eq!(scheduler.snapshot().total_active, 4);

        drop(permits.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        assert_eq!(scheduler.snapshot().total_active, 4);
        release_tx.send(()).unwrap();
        waiter.join().unwrap();
        drop(permits);

        let drained = scheduler.snapshot();
        assert_eq!(drained.total_active, 0);
        assert_eq!(drained.total_max_active, 4);
        assert_eq!(drained.meta.borrowed_admissions, 3);
    }

    #[test]
    fn chunklet_io_scheduler_is_fifo_within_each_class() {
        let scheduler = Arc::new(ChunkletIoScheduler::new(3, [1, 1, 1]).unwrap());
        let mut borrowed = vec![
            scheduler.acquire(IoClass::Meta),
            scheduler.acquire(IoClass::Meta),
            scheduler.acquire(IoClass::Meta),
        ];
        let (acquired_tx, acquired_rx) = mpsc::channel();

        let (first_release_tx, first_release_rx) = mpsc::channel();
        let first = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Lv3,
            1,
            acquired_tx.clone(),
            first_release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.lv3.waiters == 1);

        let (second_release_tx, second_release_rx) = mpsc::channel();
        let second = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Lv3,
            2,
            acquired_tx,
            second_release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.lv3.waiters == 2);

        drop(borrowed.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        drop(borrowed.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 2);

        first_release_tx.send(()).unwrap();
        second_release_tx.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        drop(borrowed);

        let drained = scheduler.snapshot();
        assert_eq!(drained.total_active, 0);
        assert_eq!(drained.total_max_active, 3);
        assert_eq!(drained.lv3.max_waiters, 2);
        assert_eq!(drained.lv3.admissions, 2);
        assert_eq!(drained.meta.admissions, 3);
        assert!(drained.lv3.wait_max_ns > 0);
        assert!(drained.reclaim_events >= 1);
        assert!(drained.reclaim_max_ns > 0);
        assert!(!drained.reclaim_in_progress);
    }

    #[test]
    fn chunklet_io_scheduler_meta_saturation_preserves_lv3_share() {
        // A checkpoint can occupy every admission while nobody else waits.
        // Once LV3 queues, new Meta calls stop at Meta's share and completions
        // are reclaimed until LV3 owns its protected two slots.
        let scheduler = Arc::new(ChunkletIoScheduler::new(6, [2, 2, 2]).unwrap());
        let mut meta_borrowers = (0..6)
            .map(|_| scheduler.acquire(IoClass::Meta))
            .collect::<Vec<_>>();
        let (acquired_tx, acquired_rx) = mpsc::channel();

        let (lv3_first_release_tx, lv3_first_release_rx) = mpsc::channel();
        let lv3_first = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Lv3,
            1,
            acquired_tx.clone(),
            lv3_first_release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.lv3.waiters == 1);

        let (lv3_second_release_tx, lv3_second_release_rx) = mpsc::channel();
        let lv3_second = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Lv3,
            2,
            acquired_tx.clone(),
            lv3_second_release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.lv3.waiters == 2);

        let (meta_waiter_release_tx, meta_waiter_release_rx) = mpsc::channel();
        let meta_waiter = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Meta,
            3,
            acquired_tx,
            meta_waiter_release_rx,
        );
        wait_for_scheduler(&scheduler, |snapshot| snapshot.meta.waiters == 1);

        drop(meta_borrowers.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        drop(meta_borrowers.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 2);

        let protected = scheduler.snapshot();
        assert_eq!(protected.total_active, 6);
        assert_eq!(protected.meta.active, 4);
        assert_eq!(protected.lv3.active, 2);
        assert_eq!(protected.meta.waiters, 1);
        assert_eq!(protected.lv3.waiters, 0);

        // After protected LV3 demand has entered, an exiting LV3 call leaves
        // genuinely idle capacity that the queued Meta caller may borrow.
        lv3_first_release_tx.send(()).unwrap();
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 3);

        lv3_second_release_tx.send(()).unwrap();
        meta_waiter_release_tx.send(()).unwrap();
        lv3_first.join().unwrap();
        lv3_second.join().unwrap();
        meta_waiter.join().unwrap();
        drop(meta_borrowers);

        let drained = scheduler.snapshot();
        assert_eq!(drained.total_active, 0);
        assert_eq!(drained.total_max_active, 6);
        assert_eq!(drained.lv3.admissions, 2);
        assert_eq!(drained.meta.admissions, 7);
        assert!(drained.lv3.reclaim_events >= 1);
        assert!(drained.lv3.reclaim_max_ns > 0);
    }

    #[test]
    fn chunklet_io_scheduler_is_work_conserving_when_meta_is_idle() {
        let scheduler = Arc::new(ChunkletIoScheduler::new(5, [2, 1, 2]).unwrap());
        let mut foreground = (0..5)
            .map(|_| scheduler.acquire(IoClass::Foreground))
            .collect::<Vec<_>>();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let mut releases = Vec::new();
        let mut waiters = Vec::new();

        for (id, class) in [
            (1, IoClass::Lv3),
            (2, IoClass::Lv3),
            (3, IoClass::Lv3),
            (4, IoClass::Foreground),
            (5, IoClass::Foreground),
        ] {
            let (release_tx, release_rx) = mpsc::channel();
            waiters.push(spawn_permit_holder(
                Arc::clone(&scheduler),
                class,
                id,
                acquired_tx.clone(),
                release_rx,
            ));
            releases.push(release_tx);
        }
        wait_for_scheduler(&scheduler, |snapshot| {
            snapshot.foreground.waiters == 2 && snapshot.lv3.waiters == 3
        });

        for _ in 0..5 {
            drop(foreground.pop());
            acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }

        let full = scheduler.snapshot();
        assert_eq!(full.total_active, 5);
        assert_eq!(full.foreground.active, 2);
        assert_eq!(full.lv3.active, 3);
        assert_eq!(full.meta.active, 0);
        assert!(full.lv3.borrowed_admissions > 0);

        for release in releases {
            release.send(()).unwrap();
        }
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert_eq!(scheduler.snapshot().total_active, 0);
    }

    #[test]
    fn chunklet_io_scheduler_reclaim_tracks_actual_block_to_service() {
        let scheduler = Arc::new(ChunkletIoScheduler::new(3, [1, 1, 1]).unwrap());
        let mut meta = vec![
            scheduler.acquire(IoClass::Meta),
            scheduler.acquire(IoClass::Meta),
            scheduler.acquire(IoClass::Meta),
        ];
        let healthy_borrow = scheduler.snapshot();
        assert_eq!(healthy_borrow.meta.borrowed_admissions, 2);
        assert_eq!(healthy_borrow.reclaim_events, 0);

        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let lv3 = spawn_permit_holder(
            Arc::clone(&scheduler),
            IoClass::Lv3,
            1,
            acquired_tx,
            release_rx,
        );
        let blocked = wait_for_scheduler(&scheduler, |snapshot| {
            snapshot.lv3.waiters == 1 && snapshot.lv3.reclaim_in_progress
        });
        assert_eq!(blocked.lv3.reclaim_events, 1);

        drop(meta.pop());
        assert_eq!(acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        let served = scheduler.snapshot();
        assert!(!served.lv3.reclaim_in_progress);
        assert!(served.lv3.reclaim_max_ns > 0);

        release_tx.send(()).unwrap();
        lv3.join().unwrap();
        drop(meta);
        assert_eq!(scheduler.snapshot().total_active, 0);
    }

    #[test]
    fn chunklet_io_scheduler_error_path_releases_permit() {
        let scheduler = Arc::new(ChunkletIoScheduler::new(3, [1, 1, 1]).unwrap());
        let backend = ChunkletBackend::new_scheduled(
            Arc::new(FailingLogicalDisk),
            IoClass::Foreground,
            Arc::clone(&scheduler),
        );
        assert!(backend.write_at(&vec![0; 4096], 0).is_err());
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.total_active, 0);
        assert_eq!(snapshot.foreground.active, 0);
        assert_eq!(snapshot.foreground.admissions, 1);
    }

    #[test]
    fn chunklet_io_classes_map_to_nested_block_classes() {
        assert_eq!(IoClass::Foreground.chunklet(), ChunkletIoClass::Foreground);
        assert_eq!(IoClass::Lv3.chunklet(), ChunkletIoClass::DrainData);
        assert_eq!(IoClass::Meta.chunklet(), ChunkletIoClass::DrainMeta);
    }

    fn backend(size: u64) -> (RawDevice, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        (dev, tmp)
    }

    #[test]
    fn round_trip_single() {
        let (b, _tmp) = backend(64 * 1024);
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        b.write_at(&payload, 8192).unwrap();
        b.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        b.read_at(&mut got, 8192).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn round_trip_many() {
        let (b, _tmp) = backend(64 * 1024);
        let a: Vec<u8> = vec![0xaa; 4096];
        let c: Vec<u8> = vec![0x55; 4096];
        b.write_many_at(&[(0, &a), (4096, &c)]).unwrap();
        b.flush().unwrap();
        let mut ga = vec![0u8; 4096];
        let mut gc = vec![0u8; 4096];
        {
            let mut ops: Vec<(u64, &mut [u8])> = vec![(0, &mut ga), (4096, &mut gc)];
            b.read_many_at(&mut ops).unwrap();
        }
        assert_eq!(ga, a);
        assert_eq!(gc, c);
    }

    #[test]
    fn size_and_uring_target() {
        let (b, _tmp) = backend(64 * 1024);
        assert_eq!(b.size(), 64 * 1024);
        // A file/blockdev backend exposes its fd for the io_uring hot path.
        assert!(b.uring_target().is_some());
    }

    #[test]
    fn backend_slice_windows_and_forwards_uring_base() {
        let (b, _tmp) = backend(64 * 1024);
        let root_base = b.uring_target().unwrap().1;
        let root: Arc<dyn BlockBackend> = Arc::new(b);

        // A 4 KiB window starting at offset 8192.
        let win = BackendSlice::new(root.clone(), 8192, 4096).unwrap();
        assert_eq!(win.size(), 4096);
        // uring_target forwards the inner fd with the window base folded in, so
        // the io_uring path builds the same absolute SQE offset RawDevice::slice
        // would have.
        let (root_fd, _) = root.uring_target().unwrap();
        let (win_fd, win_base) = win.uring_target().unwrap();
        assert_eq!(win_fd, root_fd);
        assert_eq!(win_base, root_base + 8192);

        // Writes land at the translated absolute offset.
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        win.write_at(&payload, 0).unwrap();
        win.flush().unwrap();
        let mut via_root = vec![0u8; 4096];
        root.read_at(&mut via_root, 8192).unwrap();
        assert_eq!(via_root, payload);

        // Out-of-window IO is rejected.
        let mut overflow = vec![0u8; 4096];
        assert!(win.read_at(&mut overflow, 1).is_err());
        assert!(BackendSlice::new(root, 63 * 1024, 4096).is_err());
    }

    /// End-to-end through the production RAID6 path: a real chunklet Pool over
    /// sparse files, an `LdRaid6` LD wrapped in `ChunkletBackend`. Proves the
    /// onyx → chunklet seam (arg-order flip, error mapping, flush, no fd).
    #[test]
    fn chunklet_backend_round_trip_raid6() {
        use onyx_chunklet::io::RawDevice as CkRaw;
        use onyx_chunklet::pool::LdSpec;
        use onyx_chunklet::{Pool, PoolConfig};

        let dir = tempfile::tempdir().unwrap();
        let mut raws = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("pd{i}"));
            raws.push(CkRaw::open_or_create(&p, 4 << 30).unwrap());
        }
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
        let ld = pool.open_ld(ld_id).unwrap();

        let backend = ChunkletBackend::new(ld);
        assert!(
            backend.uring_target().is_none(),
            "RAID LD must not expose a fd"
        );
        assert!(backend.size() > 0);

        let payload: Vec<u8> = (0..(64 << 10)).map(|i| (i % 251) as u8).collect();
        backend.write_at(&payload, 0).unwrap();
        backend.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        backend.read_at(&mut got, 0).unwrap();
        assert_eq!(got, payload);
    }

    /// stripe_blocks() derives the RAID6 full-stripe width (in blocks) from the
    /// LD geometry: 3 data + 2 parity at a 4 KiB strip => 3-block (12 KiB)
    /// stripe. This is the alignment granularity the flush writer targets.
    #[test]
    fn chunklet_backend_stripe_blocks_raid6() {
        use onyx_chunklet::io::RawDevice as CkRaw;
        use onyx_chunklet::pool::LdSpec;
        use onyx_chunklet::{Pool, PoolConfig};

        let dir = tempfile::tempdir().unwrap();
        let mut raws = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("pd{i}"));
            raws.push(CkRaw::open_or_create(&p, 4 << 30).unwrap());
        }
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
        let ld = pool.open_ld(ld_id).unwrap();
        let backend = ChunkletBackend::new(ld);
        assert_eq!(backend.stripe_blocks(), 3);
    }

    /// A single file/blockdev is not striped => stripe_blocks() is 1, so the
    /// writer's stripe logic is a no-op off-chunklet.
    #[test]
    fn raw_device_stripe_blocks_is_one() {
        let (b, _tmp) = backend(64 * 1024);
        assert_eq!(b.stripe_blocks(), 1);
    }

    /// Regression for the 2026-07-06 box bug: a `mark_pd_failed` bumps the LD
    /// runtime epoch, staling the handle the backend cached at open. Before the
    /// refresh-on-stale fix, every subsequent write/flush on that handle failed
    /// with `handle is stale: runtime epoch advanced` (the LV2 commit-log writer
    /// spun 45k times and wedged the flush). With the fix the backend re-opens
    /// the LD onto the new epoch transparently, so writes/reads keep succeeding
    /// on the now-degraded (reconstruct) set.
    #[test]
    fn chunklet_backend_refreshes_handle_after_epoch_bump() {
        use onyx_chunklet::io::RawDevice as CkRaw;
        use onyx_chunklet::pool::LdSpec;
        use onyx_chunklet::{Pool, PoolConfig};

        let dir = tempfile::tempdir().unwrap();
        let mut raws = Vec::new();
        for i in 0..5 {
            let p = dir.path().join(format!("pd{i}"));
            raws.push(CkRaw::open_or_create(&p, 4 << 30).unwrap());
        }
        let pool = Pool::create(
            raws,
            PoolConfig {
                spare_pct: 0,
                ..Default::default()
            },
        )
        .unwrap();
        // set_size = 3 + 2 = 5 => the single set spans all 5 PDs, so failing any
        // member bumps this LD's epoch; RAID6 tolerates 2 failures, so 1 pulled
        // PD leaves the set writable (write-forward) and readable (reconstruct).
        let ld_id = pool.create_ld(LdSpec::raid6(3, 1, 1, 0)).unwrap();
        let ld = pool.open_ld(ld_id).unwrap();
        // `with_pool` is what wires the re-open path (a bare `new` backend has no
        // pool to re-open against).
        let backend = ChunkletBackend::with_pool(ld, pool.clone());

        // Baseline: write + flush + read-back on the healthy set.
        let payload: Vec<u8> = (0..(64 << 10)).map(|i| (i % 251) as u8).collect();
        backend.write_at(&payload, 0).unwrap();
        backend.flush().unwrap();
        let mut got = vec![0u8; payload.len()];
        backend.read_at(&mut got, 0).unwrap();
        assert_eq!(got, payload);

        // Fail a member PD -> bumps the LD runtime epoch, staling the handle the
        // backend is holding.
        let member_pd = pool.find_ld(ld_id).unwrap().members[0].pd;
        pool.mark_pd_failed(member_pd).unwrap();

        // These would each fail with a stale-handle invariant pre-fix; with the
        // fix the backend re-opens onto the new (degraded) epoch and retries.
        let payload2: Vec<u8> = (0..(64 << 10)).map(|i| ((i * 7 + 3) % 251) as u8).collect();
        backend
            .write_at(&payload2, 0)
            .expect("write must refresh the stale handle, not error");
        backend
            .flush()
            .expect("flush must refresh the stale handle, not error");
        let mut got2 = vec![0u8; payload2.len()];
        backend
            .read_at(&mut got2, 0)
            .expect("read must refresh the stale handle, not error");
        assert_eq!(
            got2, payload2,
            "degraded reconstruct read must return the new data"
        );

        // A second failover bump is likewise absorbed (covers the mark_pd_failed
        // -> rebuild-commit double bump).
        let member_pd2 = pool.find_ld(ld_id).unwrap().members[1].pd;
        if member_pd2 != member_pd {
            pool.mark_pd_failed(member_pd2).unwrap();
            let mut got3 = vec![0u8; payload2.len()];
            backend
                .read_at(&mut got3, 0)
                .expect("read must survive a second epoch bump");
            assert_eq!(got3, payload2);
        }
    }
}
