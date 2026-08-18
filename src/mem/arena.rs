//! Per-lane slab arena for O_DIRECT buffers.
//!
//! # The measurement this replaces
//!
//! Box, 2026-08-17, RWMIX=0, 25,438 writer cycles: the LV3 flush writer spends
//! **53.3 %** of its time in `io`, and only 47.6 % of *that* is the device. Two
//! legs of the rest are pure overhead:
//!
//! - `bufalloc` **10.58 ms/cycle** — the thread-local parking lot in
//!   [`crate::io::aligned`] holds `THREAD_POOL_MAX_BUFFERS = 8` buffers against
//!   **301 buffers per writer cycle**, a 2.7 % hit-rate ceiling, so nearly every
//!   24 KiB buffer went to `alloc_zeroed` at 35-50 µs each;
//! - `bufzero` **10.56 ms/cycle** — a second full `fill(0)` on top of
//!   `alloc_zeroed`, 100 % overwritten on the next line.
//!
//! # Why an arena and not a bigger free list
//!
//! - A bigger parking lot is *worse*: `take_from_pool` is
//!   `iter().filter(fits).min_by_key(size)`, a full scan per take. At cap 512 and
//!   301 takes that is 154 k iterations per cycle.
//! - A pure bump allocator with a per-cycle reset has the right *lifetime* shape
//!   (`flush_writer_batch.cycles` 25,438 vs `lv3_batch.wait_calls` 25,783 ⇒ ~1.01
//!   blocking submit per cycle) but the wrong *ownership* shape: the LV3 batch
//!   executor's reply is `let _ = request.done.send(..)`, so when a producer has
//!   already left (early return from `submit_many`, panic) the slabs are dropped
//!   **on the executor thread**. A bump reset assumes every slot comes back, in
//!   order, on the owning thread. None of the three is guaranteed here.
//! - A lock-free stack is unnecessary by three orders of magnitude: ~601
//!   take+release per cycle at ~20 ns for an uncontended `parking_lot::Mutex` is
//!   **12 µs/cycle** against the 21,100 µs/cycle being removed. This project's
//!   rule is not to optimise what has not been measured.
//!
//! So: one arena per writer lane, split into size classes, one free stack per
//! class, slots pre-faulted and **never returned to the OS**. `take` is a pop,
//! release is a push, and because the handle carries an `Arc<SlabArena>` the
//! release is correct on any thread.
//!
//! # Invariants
//!
//! 1. **A slot is never handed out twice.** The handle owns it exclusively; under
//!    `debug_assertions` the arena also keeps an outstanding-address set and
//!    asserts take/release pairing. Double-allocation is exactly the shape of the
//!    `defrag_stripe_publish_crc_p0` class of bug.
//! 2. **Class rounding never reaches the device.** [`AlignedBuf::len`] is the
//!    requested aligned size, never the slot width, because the run path uses
//!    `buffer.len()` as its device op length — writing one block more would land
//!    on a neighbouring PBA.
//! 3. **Slots come back dirty.** Callers must cover every byte they submit; see
//!    [`crate::mem::SlabFill`].
//! 4. **Nothing is ever madvised or munmapped in steady state.** The
//!    `MADV_DONTNEED` → `flush_tlb_mm_range` cross-CPU IPI storm that this
//!    replaces is documented in [`crate::io::aligned`] (7.86 % of system CPU) and
//!    in the compress worker's scratch-buffer comment (~40 % of that thread).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::classes::ClassTable;
use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::{round_up, AlignedBuf};
use crate::metrics::EngineMetrics;
use crate::types::BLOCK_SIZE;

/// Bytes a single growth step tries to map. Sized so a warm writer lane reaches
/// its measured 7.2 MiB working set in two growth steps and then never grows
/// again, and so the region is large enough for `MADV_HUGEPAGE` to be meaningful
/// when that knob is on.
const CHUNK_TARGET_BYTES: usize = 4 * 1024 * 1024;

struct ClassPool {
    slot_bytes: usize,
    free: Mutex<Vec<usize>>,
}

struct Region {
    addr: usize,
    bytes: usize,
}

/// A lane's slab arena. Cheap to clone as an `Arc`; every handed-out buffer holds
/// one clone so the arena outlives its buffers on any thread.
pub struct SlabArena {
    table: ClassTable,
    classes: Box<[ClassPool]>,
    regions: Mutex<Vec<Region>>,
    resident_bytes: AtomicUsize,
    cap_bytes: usize,
    hugepage: bool,
    metrics: Option<Arc<EngineMetrics>>,
    /// Releases seen on a thread other than the one that built the arena. Purely
    /// diagnostic — the release path is correct either way — so it is only
    /// tracked where it is free.
    #[cfg(debug_assertions)]
    foreign_releases: AtomicU64,
    #[cfg(debug_assertions)]
    owner: std::thread::ThreadId,
    #[cfg(debug_assertions)]
    outstanding: Mutex<std::collections::HashSet<usize>>,
    /// Slots handed out right now, across all classes.
    live_slots: AtomicU64,
}

impl SlabArena {
    /// Build an arena capped at `cap_bytes` resident, serving requests up to
    /// `max_class_blocks` blocks. Call this **on the owning thread and after
    /// `affinity::bind_current`** so the pre-faulted pages land NUMA-local.
    pub fn new(
        cap_bytes: usize,
        max_class_blocks: u32,
        hugepage: bool,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Arc<Self> {
        let table = ClassTable::new(max_class_blocks);
        let bs = BLOCK_SIZE as usize;
        let classes: Vec<ClassPool> = (0..table.class_count())
            .map(|c| ClassPool {
                slot_bytes: table.slot_blocks(c as u16) as usize * bs,
                free: Mutex::new(Vec::new()),
            })
            .collect();
        Arc::new(Self {
            table,
            classes: classes.into_boxed_slice(),
            regions: Mutex::new(Vec::new()),
            resident_bytes: AtomicUsize::new(0),
            cap_bytes,
            hugepage,
            metrics,
            #[cfg(debug_assertions)]
            foreign_releases: AtomicU64::new(0),
            #[cfg(debug_assertions)]
            owner: std::thread::current().id(),
            #[cfg(debug_assertions)]
            outstanding: Mutex::new(std::collections::HashSet::new()),
            live_slots: AtomicU64::new(0),
        })
    }

    /// Take a buffer of at least `size` bytes. Never fails because of the arena
    /// itself: an out-of-range size or an arena at its cap falls back to a heap
    /// [`AlignedBuf`] and bumps `mem_arena.overflow`.
    ///
    /// The returned buffer's `len` is `round_up(size, BLOCK_SIZE)` — the slot may
    /// be wider, and that width must never be visible to the device.
    pub fn take(self: &Arc<Self>, size: usize) -> OnyxResult<AlignedBuf> {
        let aligned = round_up(size, BLOCK_SIZE as usize);
        if aligned == 0 {
            return Err(OnyxError::Config("cannot allocate zero-size buffer".into()));
        }
        if let Some(metrics) = &self.metrics {
            metrics.mem_arena_takes.fetch_add(1, Ordering::Relaxed);
        }
        let blocks = (aligned / BLOCK_SIZE as usize) as u32;
        let Some(class) = self.table.class_of(blocks) else {
            return self.overflow(aligned);
        };

        // Bind the pop so the class lock is released before `hand_out` runs; the
        // only lock order this arena has is regions -> free, and nothing should
        // hold a free stack across other work.
        let recycled = self.classes[class as usize].free.lock().pop();
        if let Some(addr) = recycled {
            return Ok(self.hand_out(addr, aligned, class, true));
        }
        match self.grow_class(class) {
            Some(addr) => Ok(self.hand_out(addr, aligned, class, false)),
            None => self.overflow(aligned),
        }
    }

    fn hand_out(self: &Arc<Self>, addr: usize, len: usize, class: u16, hit: bool) -> AlignedBuf {
        debug_assert_eq!(
            addr % BLOCK_SIZE as usize,
            0,
            "slot must be O_DIRECT aligned"
        );
        debug_assert!(
            len <= self.classes[class as usize].slot_bytes,
            "requested {} bytes from a {}-byte slot",
            len,
            self.classes[class as usize].slot_bytes
        );
        #[cfg(debug_assertions)]
        assert!(
            self.outstanding.lock().insert(addr),
            "slab arena handed out {:#x} twice",
            addr
        );
        self.live_slots.fetch_add(1, Ordering::Relaxed);
        if hit {
            if let Some(metrics) = &self.metrics {
                metrics.mem_arena_hits.fetch_add(1, Ordering::Relaxed);
            }
        }
        // SAFETY: `addr` is a slot inside a region this arena mapped and keeps
        // mapped for its whole life, it is `slot_bytes >= len` long, and it is not
        // reachable from anywhere else until the handle releases it.
        unsafe { AlignedBuf::from_arena_slot(addr as *mut u8, len, self.clone(), class) }
    }

    /// Return a slot. Called from `AlignedBuf::drop` on whichever thread happens
    /// to own the buffer at that point.
    pub(crate) fn release(&self, addr: usize, class: u16) {
        #[cfg(debug_assertions)]
        {
            assert!(
                self.outstanding.lock().remove(&addr),
                "slab arena released {:#x} which it did not hand out",
                addr
            );
            if std::thread::current().id() != self.owner {
                self.foreign_releases.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.live_slots.fetch_sub(1, Ordering::Relaxed);
        // Deliberately no zeroing, no madvise, no munmap. See invariant 4.
        self.classes[class as usize].free.lock().push(addr);
    }

    /// Map one more chunk of `class`-sized slots and return one of them.
    /// `None` = at the cap; the caller falls back to the heap.
    fn grow_class(&self, class: u16) -> Option<usize> {
        let slot_bytes = self.classes[class as usize].slot_bytes;
        let mut regions = self.regions.lock();
        // Re-check under the region lock: another thread may have grown this
        // class while we were waiting, in which case its slots are already free.
        let recycled = self.classes[class as usize].free.lock().pop();
        if let Some(addr) = recycled {
            return Some(addr);
        }
        let resident = self.resident_bytes.load(Ordering::Relaxed);
        let headroom = self.cap_bytes.saturating_sub(resident);
        if headroom < slot_bytes {
            return None;
        }
        let slots = (CHUNK_TARGET_BYTES / slot_bytes).clamp(1, headroom / slot_bytes);
        let bytes = slots * slot_bytes;
        let addr = match map_region(bytes, self.hugepage) {
            Ok(addr) => addr,
            Err(e) => {
                tracing::warn!(
                    bytes,
                    error = %e,
                    "slab arena growth failed; falling back to heap buffers"
                );
                return None;
            }
        };
        regions.push(Region { addr, bytes });
        self.resident_bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Some(metrics) = &self.metrics {
            metrics.mem_arena_grows.fetch_add(1, Ordering::Relaxed);
            metrics
                .mem_arena_grow_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
        // Hand out the first slot directly and park the rest.
        {
            let mut free = self.classes[class as usize].free.lock();
            free.reserve(slots);
            for i in 1..slots {
                free.push(addr + i * slot_bytes);
            }
        }
        Some(addr)
    }

    fn overflow(&self, aligned: usize) -> OnyxResult<AlignedBuf> {
        if let Some(metrics) = &self.metrics {
            metrics.mem_arena_overflow.fetch_add(1, Ordering::Relaxed);
        }
        AlignedBuf::new(aligned, false)
    }

    /// Slot width of `class`, in bytes. This is the buffer's *capacity*; its
    /// logical length stays whatever was requested (invariant 2).
    pub(crate) fn slot_bytes(&self, class: u16) -> usize {
        self.classes[class as usize].slot_bytes
    }

    /// Bytes mapped by this arena. Never decreases while the arena lives.
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// Slots currently handed out. Zero between writer cycles.
    pub fn live_slots(&self) -> u64 {
        self.live_slots.load(Ordering::Relaxed)
    }

    #[cfg(debug_assertions)]
    pub fn foreign_releases(&self) -> u64 {
        self.foreign_releases.load(Ordering::Relaxed)
    }
}

impl Drop for SlabArena {
    fn drop(&mut self) {
        // Every handle holds an `Arc<SlabArena>`, so reaching Drop proves no slot
        // is still outstanding. Unmapping here is safe and keeps test processes
        // from accumulating arenas.
        debug_assert_eq!(
            self.live_slots(),
            0,
            "slab arena dropped with live slots outstanding"
        );
        for region in self.regions.lock().drain(..) {
            unmap_region(region.addr, region.bytes);
        }
    }
}

/// Map `bytes` of pre-faulted anonymous memory. Pre-faulting is the point: it
/// moves the page-fault cost of the whole arena to one startup event instead of
/// paying it per buffer, and it happens on the calling thread so first touch is
/// NUMA-local when the caller has already bound itself.
fn map_region(bytes: usize, hugepage: bool) -> OnyxResult<usize> {
    use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
    use std::num::NonZeroUsize;

    let len = NonZeroUsize::new(bytes)
        .ok_or_else(|| OnyxError::Config("cannot map a zero-byte arena region".into()))?;
    // `MAP_POPULATE` is the pre-fault: the crate is Linux-only (see the
    // `compile_error!` in `lib.rs`), so there is no portable fallback to carry.
    let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_POPULATE;
    let ptr = unsafe {
        mmap_anonymous(
            None,
            len,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
            flags,
        )
    }
    .map_err(|e| OnyxError::Io(std::io::Error::from_raw_os_error(e as i32)))?;
    let addr = ptr.as_ptr() as usize;

    if hugepage {
        // Advisory: a failure here costs TLB entries, not correctness.
        if let Err(e) = unsafe {
            nix::sys::mman::madvise(ptr, bytes, nix::sys::mman::MmapAdvise::MADV_HUGEPAGE)
        } {
            tracing::warn!(error = %e, "MADV_HUGEPAGE on slab arena region failed");
        }
    }
    Ok(addr)
}

fn unmap_region(addr: usize, bytes: usize) {
    if let Some(ptr) = std::ptr::NonNull::new(addr as *mut std::ffi::c_void) {
        let _ = unsafe { nix::sys::mman::munmap(ptr, bytes) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arena(cap: usize) -> Arc<SlabArena> {
        SlabArena::new(cap, 64, false, None)
    }

    #[test]
    fn take_reuses_the_same_slot_after_release() {
        let arena = arena(4 * 1024 * 1024);
        let first = arena.take(24 * 1024).unwrap();
        let addr = first.as_ptr() as usize;
        drop(first);
        assert_eq!(arena.live_slots(), 0);
        let second = arena.take(24 * 1024).unwrap();
        assert_eq!(
            second.as_ptr() as usize,
            addr,
            "release must recycle in place"
        );
        // One growth step served both takes: a chunk is whole slots, so it is at
        // most the target and never zero.
        let resident = arena.resident_bytes();
        assert!(resident > 0 && resident <= CHUNK_TARGET_BYTES, "{resident}");
    }

    #[test]
    fn steady_state_never_grows_again() {
        let arena = arena(64 * 1024 * 1024);
        let mut after_first = 0usize;
        for cycle in 0..4 {
            let bufs: Vec<AlignedBuf> = (0..64).map(|_| arena.take(24 * 1024).unwrap()).collect();
            assert_eq!(arena.live_slots(), 64);
            drop(bufs);
            if cycle == 0 {
                after_first = arena.resident_bytes();
                assert!(after_first > 0);
            } else {
                assert_eq!(
                    arena.resident_bytes(),
                    after_first,
                    "steady state must not map more memory"
                );
            }
        }
    }

    /// Invariant 2: a request served by a wider class must still report the
    /// requested length, because the run path uses `len()` as the device op size.
    #[test]
    fn wider_slot_does_not_change_the_reported_length() {
        let arena = arena(8 * 1024 * 1024);
        let bs = BLOCK_SIZE as usize;
        // 17 blocks has no exact class; the 24-block class serves it.
        let buf = arena.take(17 * bs).unwrap();
        assert_eq!(buf.len(), 17 * bs);
        assert_eq!(buf.as_slice().len(), 17 * bs);
    }

    /// Invariant 3: recycled slots are dirty, so callers must cover what they
    /// submit. This pins the property that makes `SlabFill` mandatory.
    #[test]
    fn recycled_slot_keeps_previous_content() {
        let arena = arena(4 * 1024 * 1024);
        let mut first = arena.take(8192).unwrap();
        first.as_mut_slice().fill(0xA5);
        drop(first);
        let second = arena.take(8192).unwrap();
        assert!(
            second.as_slice().iter().any(|&b| b == 0xA5),
            "arena must not pay to zero a recycled slot"
        );
    }

    #[test]
    fn release_on_another_thread_returns_the_slot() {
        let arena = arena(4 * 1024 * 1024);
        let buf = arena.take(24 * 1024).unwrap();
        let addr = buf.as_ptr() as usize;
        std::thread::spawn(move || drop(buf)).join().unwrap();
        assert_eq!(arena.live_slots(), 0);
        #[cfg(debug_assertions)]
        assert_eq!(arena.foreign_releases(), 1);
        let again = arena.take(24 * 1024).unwrap();
        assert_eq!(again.as_ptr() as usize, addr);
    }

    #[test]
    fn out_of_range_size_falls_back_to_the_heap() {
        let arena = arena(4 * 1024 * 1024);
        let bs = BLOCK_SIZE as usize;
        // 65 blocks is past `max_class_blocks = 64`.
        let buf = arena.take(65 * bs).unwrap();
        assert_eq!(buf.len(), 65 * bs);
        assert_eq!(arena.resident_bytes(), 0, "no region mapped for a fallback");
        assert_eq!(arena.live_slots(), 0, "fallback is not an arena slot");
    }

    #[test]
    fn cap_is_a_ceiling_not_a_failure() {
        // Room for exactly one 24 KiB slot.
        let arena = arena(24 * 1024);
        let held: Vec<AlignedBuf> = (0..4).map(|_| arena.take(24 * 1024).unwrap()).collect();
        assert_eq!(held.len(), 4);
        assert!(arena.resident_bytes() <= 24 * 1024);
        assert_eq!(arena.live_slots(), 1, "one slot from the arena, three heap");
        for buf in &held {
            assert_eq!(buf.len(), 24 * 1024);
        }
    }

    #[test]
    fn zero_size_take_is_rejected() {
        let arena = arena(4 * 1024 * 1024);
        assert!(arena.take(0).is_err());
    }

    #[test]
    fn distinct_classes_do_not_share_slots() {
        let arena = arena(16 * 1024 * 1024);
        let bs = BLOCK_SIZE as usize;
        let a = arena.take(bs).unwrap();
        let b = arena.take(6 * bs).unwrap();
        assert_ne!(a.as_ptr(), b.as_ptr());
        // Distinct classes each map their own region.
        assert!(arena.resident_bytes() > CHUNK_TARGET_BYTES);
        assert!(arena.resident_bytes() <= 2 * CHUNK_TARGET_BYTES);
    }
}
