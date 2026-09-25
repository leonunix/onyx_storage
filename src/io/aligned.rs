use std::alloc::{self, Layout};
use std::cell::RefCell;
use std::sync::Arc;

use crate::error::{OnyxError, OnyxResult};
use crate::mem::SlabArena;
use crate::types::BLOCK_SIZE;

thread_local! {
    /// Per-thread parking lot for hot-path AlignedBuf allocations.
    /// `AlignedBuf::new` checks this first; `AlignedBuf::Drop` returns
    /// buffers here instead of calling `dealloc`. Bounds the resident
    /// pool at `THREAD_POOL_MAX_BUFFERS` per thread; excess are freed.
    ///
    /// Why: at high LV2 fdatasync rate (16 buffer shards × ~100/s
    /// fsync cycles, each cycle allocating one or more aligned bufs),
    /// jemalloc's large-allocation path returns pages via
    /// `madvise(MADV_DONTNEED)` which triggers cross-core TLB
    /// invalidation IPIs. perf 2026-05-28 measured 7.86% of system
    /// CPU in `smp_call_function_many_cond` → `flush_tlb_mm_range`
    /// from the `persistent-slot` (LV2 sync) thread. Pooling the
    /// allocation keeps the pages resident and skips the madvise.
    ///
    /// ⛔ **This pool does NOT solve that problem, measured.** A 2026-09-24
    /// in-engine `perf` profile caught `persistent-slot` still issuing ~12.9k
    /// madvise/s — 62 % of the box's 20.7k/s — because the hit condition is
    /// `parked.layout.size() >= size` and an LV2 span's length is a wide random
    /// variable under a 4k-32k workload, so misses come from SIZE mismatch at
    /// any rate. The real fix is [`crate::mem::SlabArena`], whose module doc
    /// explains why a *bigger* parking lot is worse (take is a full scan).
    /// This pool is kept only for the many cold callers that do not have an
    /// arena. See memory `perf_inside_engine_first_cpu_ledger`.
    static ALIGNED_BUF_POOL: RefCell<Vec<AlignedBuf>> = const { RefCell::new(Vec::new()) };
}

const THREAD_POOL_MAX_BUFFERS: usize = 8;

/// Where an [`AlignedBuf`]'s memory came from, and therefore how it must be
/// returned. Provenance is carried in the handle rather than inferred, because
/// the LV3 write path hands buffers across threads: the batch executor's reply is
/// `let _ = request.done.send(..)`, so a buffer can be dropped on an executor
/// thread when its producer has already left. A thread-local pool silently loses
/// such a buffer; an `Arc<SlabArena>` cannot.
enum Backing {
    /// `alloc_zeroed`; may be parked in the thread-local pool on drop.
    Heap,
    /// `mmap(MAP_HUGETLB)`; munmapped on drop, never pooled.
    Hugepage,
    /// One slot of a [`SlabArena`]; the slot goes back to that arena on drop,
    /// from whichever thread holds the buffer at the time.
    Arena { arena: Arc<SlabArena>, class: u16 },
}

/// 4KB-aligned buffer for O_DIRECT IO.
/// Optionally backed by hugepages or by a [`SlabArena`] slot.
pub struct AlignedBuf {
    ptr: *mut u8,
    /// Logical length: what callers see and — critically — what the LV3 run path
    /// passes to the device as its op length. **Never** the slot/allocation
    /// capacity: an arena slot may be wider than the request, and writing the
    /// extra blocks would land on a neighbouring PBA.
    len: usize,
    /// Capacity descriptor. `layout.size() >= len` for every backing.
    layout: Layout,
    backing: Backing,
}

// SAFETY: AlignedBuf owns its memory and the pointer is not shared
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate an aligned buffer of the given size.
    /// Size will be rounded up to a multiple of BLOCK_SIZE.
    ///
    /// ⚠ **The contents are UNDEFINED.** The caller must write every byte it
    /// then submits to a device. This has always been the actual behaviour —
    /// the per-thread pool below hands a parked buffer back without zeroing it,
    /// and an arena slot comes back dirty by design
    /// ([`crate::mem::SlabArena`] invariant 3) — but it used to be documented
    /// as "already zeroed" because the *miss* path happens to call
    /// `alloc_zeroed`. Sites that genuinely need zeros must say so:
    /// [`AlignedBuf::new_zeroed`].
    ///
    /// Hot-path callers (LV2 sync, LV3 writer) hit a per-thread
    /// allocation pool first: a parked buf whose capacity already
    /// fits is reused in place, skipping jemalloc and the
    /// large-allocation madvise/TLB-IPI cost. Hugepage-backed bufs
    /// are not pooled (the mmap region cannot be safely resized;
    /// hugepage callers are infrequent enough that fresh mmap is
    /// fine).
    pub fn new(size: usize, use_hugepages: bool) -> OnyxResult<Self> {
        let aligned_size = round_up(size, BLOCK_SIZE as usize);
        if aligned_size == 0 {
            return Err(OnyxError::Config("cannot allocate zero-size buffer".into()));
        }

        if use_hugepages {
            return Self::alloc_hugepage(aligned_size);
        }

        if let Some(buf) = Self::take_from_pool(aligned_size) {
            return Ok(buf);
        }

        Self::alloc_regular(aligned_size)
    }

    /// Allocate an aligned buffer whose `[0..len)` is guaranteed to be zero.
    ///
    /// For the sites that write only part of a block and then submit the whole
    /// block — a superblock's reserved tail, a checkpoint page past its
    /// payload, a compact header's padding. [`AlignedBuf::new`] cannot serve
    /// them: it may hand back a recycled buffer, in which case the tail is
    /// whatever the previous user left there and that is what reaches the disk.
    ///
    /// Not a hot path by construction — anything hot enough to care should be
    /// covering every byte it submits instead.
    pub fn new_zeroed(size: usize, use_hugepages: bool) -> OnyxResult<Self> {
        let mut buf = Self::new(size, use_hugepages)?;
        // `alloc_zeroed`/fresh `mmap` already gives zeros, but a pooled or
        // arena-backed buffer does not, and the caller asked for the guarantee
        // rather than for whichever path it happened to take.
        buf.as_mut_slice().fill(0);
        Ok(buf)
    }

    /// Try to reuse a parked buffer whose capacity is >= `size`.
    /// Returns `None` if the thread-local pool is empty, all parked
    /// bufs are too small, or the pool's RefCell is concurrently
    /// borrowed (the latter cannot happen in single-thread use but
    /// we degrade gracefully).
    fn take_from_pool(size: usize) -> Option<Self> {
        ALIGNED_BUF_POOL.with(|cell| {
            let mut pool = cell.try_borrow_mut().ok()?;
            // Find the smallest parked buf that still fits, so the
            // pool keeps an even distribution of sizes available.
            let idx = pool
                .iter()
                .enumerate()
                .filter(|(_, b)| b.layout.size() >= size)
                .min_by_key(|(_, b)| b.layout.size())
                .map(|(i, _)| i)?;
            let mut buf = pool.swap_remove(idx);
            buf.len = size;
            Some(buf)
        })
    }

    /// Wrap one [`SlabArena`] slot.
    ///
    /// `len` is the caller's requested (block-aligned) size and stays the buffer's
    /// logical length even when the slot is wider — see the field comment on
    /// [`AlignedBuf::len`].
    ///
    /// # Safety
    ///
    /// `ptr` must be the start of a `class`-sized slot of `arena`, at least `len`
    /// bytes long, BLOCK_SIZE-aligned, and not reachable from anywhere else until
    /// this buffer is dropped.
    pub(crate) unsafe fn from_arena_slot(
        ptr: *mut u8,
        len: usize,
        arena: Arc<SlabArena>,
        class: u16,
    ) -> Self {
        let capacity = arena.slot_bytes(class);
        debug_assert!(capacity >= len);
        let layout = Layout::from_size_align(capacity, BLOCK_SIZE as usize)
            .expect("arena slot capacity is a BLOCK_SIZE multiple");
        Self {
            ptr,
            len,
            layout,
            backing: Backing::Arena { arena, class },
        }
    }

    /// Try to park `self`'s allocation in the thread-local pool.
    /// Returns `true` iff the pool took ownership of the allocation;
    /// in that case the caller MUST clear `self.ptr` so the Drop
    /// tail does not double-free. Hugepage-backed bufs and a pool
    /// that's already at `THREAD_POOL_MAX_BUFFERS` both decline.
    fn try_park_alloc(&mut self) -> bool {
        if !matches!(self.backing, Backing::Heap) || self.ptr.is_null() {
            return false;
        }
        ALIGNED_BUF_POOL
            .try_with(|cell| {
                let mut pool = match cell.try_borrow_mut() {
                    Ok(p) => p,
                    Err(_) => return false,
                };
                if pool.len() >= THREAD_POOL_MAX_BUFFERS {
                    return false;
                }
                // Take ownership without running Drop on `self` —
                // the pool entry holds the same ptr/layout, and the
                // caller will null `self.ptr` before Drop's dealloc.
                let parked = AlignedBuf {
                    ptr: self.ptr,
                    len: self.layout.size(),
                    layout: self.layout,
                    backing: Backing::Heap,
                };
                pool.push(parked);
                true
            })
            .unwrap_or(false)
    }

    fn alloc_regular(size: usize) -> OnyxResult<Self> {
        let layout = Layout::from_size_align(size, BLOCK_SIZE as usize)
            .map_err(|e| OnyxError::Config(format!("invalid layout: {}", e)))?;

        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(OnyxError::Io(std::io::Error::from(
                std::io::ErrorKind::OutOfMemory,
            )));
        }

        Ok(Self {
            ptr,
            len: size,
            layout,
            backing: Backing::Heap,
        })
    }

    fn alloc_hugepage(size: usize) -> OnyxResult<Self> {
        use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
        use std::num::NonZeroUsize;

        let nz_size = NonZeroUsize::new(size)
            .ok_or_else(|| OnyxError::Config("cannot mmap zero bytes".into()))?;

        let ptr = unsafe {
            mmap_anonymous(
                None,
                nz_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_HUGETLB,
            )
        };

        match ptr {
            Ok(p) => {
                let layout = Layout::from_size_align(size, BLOCK_SIZE as usize).unwrap();
                Ok(Self {
                    ptr: p.as_ptr() as *mut u8,
                    len: size,
                    layout,
                    backing: Backing::Hugepage,
                })
            }
            Err(_) => {
                // Fallback to regular allocation
                tracing::warn!("hugepage allocation failed, falling back to regular memory");
                Self::alloc_regular(size)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

/// Single-thread grow-only AlignedBuf pool. Each sync / writer thread
/// owns one of these; calls to [`AlignedBufPool::take`] either return
/// a parked buffer that already fits or grow the parked allocation
/// in place. The goal is to avoid the jemalloc large-allocation churn
/// path: at high fdatasync rate on a 16-shard buffer, repeated
/// `AlignedBuf::new` / drop cycles burn 7-10% of system CPU in
/// `madvise(MADV_DONTNEED)` → `flush_tlb_mm_range` cross-CPU IPI
/// (observed via perf 2026-05-28 on the nvme-box smoke).
///
/// Concurrency: this is **not** thread-safe. It's intended to live in
/// a single thread's stack (one per sync_loop, one per writer batch
/// loop, etc.). The buffers it hands out are `Send` and can be moved
/// to io_uring SQE submission temporarily; the pool just expects each
/// `take` to be paired with a `put` from the same thread.
pub struct AlignedBufPool {
    /// Buffers currently parked, sorted by capacity ascending. Most
    /// callers ask for the same size or grow monotonically, so the
    /// linear scan is O(small).
    parked: Vec<AlignedBuf>,
    use_hugepages: bool,
}

impl AlignedBufPool {
    pub fn new(use_hugepages: bool) -> Self {
        Self {
            parked: Vec::new(),
            use_hugepages,
        }
    }

    /// Take a buffer of at least `size` bytes. Returns the smallest
    /// parked buffer whose capacity (rounded-up `len`) fits, or
    /// allocates a fresh one. The returned buffer's `len` is set to
    /// `aligned_size = round_up(size, BLOCK_SIZE)`.
    pub fn take(&mut self, size: usize) -> OnyxResult<AlignedBuf> {
        let aligned_size = round_up(size, BLOCK_SIZE as usize);
        // Find first parked buffer with capacity >= aligned_size.
        let pick = self
            .parked
            .iter()
            .position(|b| b.layout.size() >= aligned_size);
        if let Some(idx) = pick {
            let mut buf = self.parked.swap_remove(idx);
            // The pool stores buffers with `len == layout.size()`; the
            // caller sees a logical len matching the requested size.
            buf.len = aligned_size;
            return Ok(buf);
        }
        AlignedBuf::new(aligned_size, self.use_hugepages)
    }

    /// Return a buffer to the pool. The buffer's allocation stays
    /// resident — the next `take` for a fitting size reuses it
    /// without entering jemalloc. The pool caps itself at `max_keep`
    /// resident buffers to bound steady-state memory; excess buffers
    /// are dropped (and their backing allocation freed) on return.
    pub fn put(&mut self, mut buf: AlignedBuf, max_keep: usize) {
        // Arena-backed buffers belong to their arena, not to this pool: dropping
        // hands the slot straight back. Parking one here would also corrupt its
        // logical length, which the LV3 write path uses as a device op length.
        if !matches!(buf.backing, Backing::Heap) {
            return;
        }
        // Restore len to layout-size so `take`'s capacity check is
        // accurate next time.
        buf.len = buf.layout.size();
        if self.parked.len() >= max_keep {
            // Replace the smallest parked buffer if `buf` is larger;
            // otherwise drop. Keeps the pool's distribution biased
            // toward larger sizes that are more expensive to reallocate.
            let smallest_idx = self
                .parked
                .iter()
                .enumerate()
                .min_by_key(|(_, b)| b.layout.size())
                .map(|(i, _)| i);
            if let Some(idx) = smallest_idx {
                if self.parked[idx].layout.size() < buf.layout.size() {
                    self.parked[idx] = buf;
                }
            }
            return;
        }
        self.parked.push(buf);
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        match &self.backing {
            Backing::Hugepage => {
                unsafe {
                    let _ = nix::sys::mman::munmap(
                        std::ptr::NonNull::new(self.ptr as *mut _).unwrap(),
                        self.len,
                    );
                }
                return;
            }
            Backing::Arena { arena, class } => {
                // Correct on any thread: the handle owns an `Arc` to its own
                // arena. Nothing is zeroed, madvised or unmapped here — the slot
                // is simply available again.
                arena.release(self.ptr as usize, *class);
                self.ptr = std::ptr::null_mut();
                return;
            }
            Backing::Heap => {}
        }
        if self.try_park_alloc() {
            // The pool now owns our allocation. Null the ptr so the
            // tail below is a no-op (dealloc on null is UB).
            self.ptr = std::ptr::null_mut();
            return;
        }
        if !self.ptr.is_null() {
            unsafe {
                alloc::dealloc(self.ptr, self.layout);
            }
        }
    }
}

pub fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}
