//! Per-PBA "referenced" bitmap for Stage-5 precision orphaned-dedup reclaim.
//!
//! § Background. §6 orphan reclaim ([`crate::dedup::scanner`]) demotes orphaned
//! dedup-index entries (delete entry → membership refcount drops to 0 → retire
//! the PBA → the GC confirm scan `referenced_extents` frees it iff truly
//! unreferenced). §6's *selector* is the 1 MiB heat region ([`super::heatmap`]):
//! it only demotes entries whose whole 1 MiB region went cold. The dense
//! allocator reuses freed PBAs for new live writes, so most overwrite-orphans
//! co-locate with live data in the same region (`region_count > 0`) and §6
//! conservatively skips them.
//!
//! § What this is. A per-PBA (1 bit / physical block) "was this PBA referenced
//! by any live L2P mapping during a full sweep?" bitmap, filled for free by the
//! GC heat-refresh sweep's existing live-L2P walk. Stage-5 swaps §6's
//! region-granularity selector for this per-PBA one, so orphans *interleaved*
//! with live data get reclaimed too.
//!
//! § Polarity / safety. Unlike the heat map (a count that only ever *delays*
//! reclaim when stale), this bitmap's polarity is inverted: a `0` bit means
//! "no live mapping to this PBA was seen" — which is true *either* because the
//! PBA is genuinely unreferenced *or* because the sweep simply did not visit the
//! referring LBA. So a `0` bit is only trustworthy over a **complete cover**.
//! That is why publication is gated on the lap-barrier (see below) and the
//! selector requires `0` across the last `k` completed barriers. Even then the
//! bitmap is only a *selector*: the actual free still goes through the GC
//! Gate-2 `referenced_extents` exact scan, so a wrong bit can only ever cost a
//! re-promote, never lose data.
//!
//! § Lap-barrier publication. The heat "sweep"/epoch is only *approximate*
//! coverage (random-phase laps, the epoch boundary can land mid-lap — see
//! [`super::runner`]). So this bitmap is **not** published on the heat epoch
//! tick. The GC thread fills one buffer and publishes it as a completed
//! snapshot only when **every live volume has completed ≥1 full lap into that
//! buffer** (a true cover). The lap-barrier bookkeeping lives on the GC thread
//! ([`super::runner::HeatCursor`]); this type just owns the published snapshots.
//!
//! § Concurrency. Single writer: the GC refresh thread owns the in-progress
//! fill buffer (not stored here) and calls [`RefBitmap::publish`] to hand it
//! over. Readers (the dedup scanner) call [`RefBitmap::unreferenced_in_recent`],
//! which `load`s the published snapshots through `ArcSwap`. Published buffers
//! are immutable, so reads never tear. A buffer is only ever recycled for the
//! next fill via `Arc::try_unwrap` of the *evicted* (oldest) snapshot — never a
//! buffer still inside the window or still held by a reader — so the writer can
//! never mutate a buffer a reader is reading.

use std::collections::VecDeque;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::types::Pba;

/// Bits per machine word of the bitmap.
const BITS_PER_WORD: u64 = 64;

/// A single published completed-sweep snapshot: `bits[i>>6] & (1<<(i&63))` is
/// set iff PBA `i` had a live L2P reference during the lap-barrier that produced
/// this snapshot. Immutable once published.
struct Snapshot {
    bits: Box<[u64]>,
}

/// The published window of the most recent completed snapshots (newest at the
/// back), plus a monotonically increasing generation counter. Swapped atomically
/// as a unit via `ArcSwap` so a reader always sees a consistent window.
struct Snapshots {
    snaps: VecDeque<Arc<Snapshot>>,
    /// Increments on every `publish`; the dedup scanner keys its
    /// `iter_dedup_entries` cache on this so it rebuilds once per new barrier.
    barrier_gen: u64,
}

struct RefInner {
    /// PBA range the bitmap covers (`allocator.total_block_count()`).
    total_pbas: u64,
    /// Words per snapshot buffer (`ceil(total_pbas / 64)`).
    n_words: usize,
    /// Number of completed snapshots to retain (the "clean sweeps" K).
    k: usize,
    snapshots: ArcSwap<Snapshots>,
}

/// Shared handle to the per-PBA referenced bitmap; cheap to clone (mirrors
/// [`super::heatmap::HeatMap`]). The in-progress fill buffer is **not** held
/// here — it lives on the GC thread and is handed in via [`Self::publish`].
#[derive(Clone)]
pub struct RefBitmap {
    inner: Arc<RefInner>,
}

impl RefBitmap {
    /// Build a bitmap covering `0..total_pbas`, retaining the last `k` completed
    /// snapshots (`k` is clamped to at least 1). No snapshots exist yet, so
    /// [`Self::unreferenced_in_recent`] returns `None` until `k` barriers have
    /// been published.
    pub fn new(total_pbas: u64, k: usize) -> Self {
        let k = k.max(1);
        let n_words = total_pbas.div_ceil(BITS_PER_WORD) as usize;
        Self {
            inner: Arc::new(RefInner {
                total_pbas,
                n_words,
                k,
                snapshots: ArcSwap::from_pointee(Snapshots {
                    snaps: VecDeque::with_capacity(k + 1),
                    barrier_gen: 0,
                }),
            }),
        }
    }

    /// Total PBA range the bitmap was sized for.
    pub fn total_pbas(&self) -> u64 {
        self.inner.total_pbas
    }

    /// Number of completed snapshots required ("clean sweeps" K).
    pub fn k(&self) -> usize {
        self.inner.k
    }

    /// Words per snapshot buffer.
    pub fn n_words(&self) -> usize {
        self.inner.n_words
    }

    /// Allocate a fresh zeroed fill buffer sized for one snapshot. The GC thread
    /// holds this buffer, marks bits into it during a sweep, then hands it back
    /// via [`Self::publish`].
    pub fn fresh_fill_buffer(&self) -> Box<[u64]> {
        vec![0u64; self.inner.n_words].into_boxed_slice()
    }

    /// Set the bit for `pba` in a GC-thread-local fill buffer. Out-of-range PBAs
    /// (≥ `total_pbas`) are ignored — never aliased into a valid slot. `buf` must
    /// be a buffer produced by [`Self::fresh_fill_buffer`] / [`Self::publish`].
    #[inline]
    pub fn mark(buf: &mut [u64], pba: Pba) {
        let i = pba.0;
        let word = (i / BITS_PER_WORD) as usize;
        if word < buf.len() {
            buf[word] |= 1u64 << (i % BITS_PER_WORD);
        }
    }

    /// Publish `filled` as the newest completed snapshot and return a recycled,
    /// zeroed buffer for the next fill. Single-writer (GC thread).
    ///
    /// Recycling rule (the concurrency invariant): a buffer may be reused for
    /// the next fill **only** by reclaiming the *evicted* oldest snapshot's
    /// buffer via `Arc::try_unwrap`. If that snapshot is still held by a reader,
    /// `try_unwrap` fails and a fresh buffer is allocated instead — so the writer
    /// never mutates a buffer any reader (or any in-window snapshot) can see.
    pub fn publish(&self, filled: Box<[u64]>) -> Box<[u64]> {
        debug_assert_eq!(
            filled.len(),
            self.inner.n_words,
            "publish: fill buffer wrong length"
        );
        let prev = self.inner.snapshots.load_full();
        let mut snaps = prev.snaps.clone();
        snaps.push_back(Arc::new(Snapshot { bits: filled }));

        // Evict the oldest if the window now exceeds K, and try to recycle its
        // buffer for the next fill.
        let mut recycled: Option<Box<[u64]>> = None;
        while snaps.len() > self.inner.k {
            if let Some(evicted) = snaps.pop_front() {
                if recycled.is_none() {
                    if let Some(snap) = Arc::into_inner(evicted) {
                        let mut buf = snap.bits;
                        buf.fill(0);
                        debug_assert_eq!(buf.len(), self.inner.n_words);
                        recycled = Some(buf);
                    }
                    // try_unwrap failed (a reader still holds it) → leave it to
                    // drop when the reader releases; fall through to fresh alloc.
                }
            }
        }

        self.inner.snapshots.store(Arc::new(Snapshots {
            snaps,
            barrier_gen: prev.barrier_gen + 1,
        }));

        recycled.unwrap_or_else(|| self.fresh_fill_buffer())
    }

    /// Monotonic generation counter, incremented on every [`Self::publish`].
    /// The dedup scanner keys its `iter_dedup_entries` cache on this.
    pub fn barrier_gen(&self) -> u64 {
        self.inner.snapshots.load().barrier_gen
    }

    /// Number of completed snapshots currently retained (0..=K).
    pub fn published_count(&self) -> usize {
        self.inner.snapshots.load().snaps.len()
    }

    /// Heap footprint of the retained snapshots in bytes (excludes the GC
    /// thread's in-progress fill buffer).
    pub fn memory_bytes(&self) -> usize {
        self.inner.snapshots.load().snaps.len() * self.inner.n_words * std::mem::size_of::<u64>()
    }

    /// Worst-case resident bytes once the window is full and a fill is in flight:
    /// `(K + 1) * snapshot_bytes`. Used for the startup memory log.
    pub fn projected_resident_bytes(&self) -> usize {
        (self.inner.k + 1) * self.inner.n_words * std::mem::size_of::<u64>()
    }

    /// Has `pba` been **unreferenced** (bit == 0) across **all** of the `k` most
    /// recent completed snapshots?
    ///
    /// - `None` — fewer than `k` snapshots have been published (not converged);
    ///   the caller skips (treats as not-yet-decidable).
    /// - `Some(true)` — bit `0` in every one of the last `k` snapshots → a
    ///   Stage-5 orphan candidate (still subject to the GC Gate-2 confirm scan).
    /// - `Some(false)` — referenced in at least one of the last `k` snapshots, or
    ///   `pba` is out of range → skip (conservative).
    pub fn unreferenced_in_recent(&self, pba: Pba, k: usize) -> Option<bool> {
        let snapshots = self.inner.snapshots.load();
        let have = snapshots.snaps.len();
        if k == 0 || have < k {
            return None;
        }
        let i = pba.0;
        if i >= self.inner.total_pbas {
            // Out of range — never a dedup PBA; treat as "referenced" so it is
            // never selected for demotion.
            return Some(false);
        }
        let word = (i / BITS_PER_WORD) as usize;
        let bit = 1u64 << (i % BITS_PER_WORD);
        // Walk only the k most-recent snapshots (the back of the deque).
        for snap in snapshots.snaps.iter().skip(have - k) {
            if snap.bits[word] & bit != 0 {
                return Some(false);
            }
        }
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marked_buffer(rb: &RefBitmap, pbas: &[u64]) -> Box<[u64]> {
        let mut buf = rb.fresh_fill_buffer();
        for &p in pbas {
            RefBitmap::mark(&mut buf, Pba(p));
        }
        buf
    }

    #[test]
    fn mark_and_read_roundtrip() {
        let rb = RefBitmap::new(4096, 1);
        // One published snapshot with PBA 10 and 200 referenced.
        let _ = rb.publish(marked_buffer(&rb, &[10, 200]));
        assert_eq!(rb.published_count(), 1);
        assert_eq!(rb.unreferenced_in_recent(Pba(10), 1), Some(false));
        assert_eq!(rb.unreferenced_in_recent(Pba(200), 1), Some(false));
        assert_eq!(rb.unreferenced_in_recent(Pba(11), 1), Some(true));
        assert_eq!(rb.unreferenced_in_recent(Pba(4095), 1), Some(true));
    }

    #[test]
    fn returns_none_below_k() {
        let rb = RefBitmap::new(4096, 2);
        assert_eq!(rb.unreferenced_in_recent(Pba(0), 2), None);
        let _ = rb.publish(marked_buffer(&rb, &[]));
        // Only one barrier so far; need 2.
        assert_eq!(rb.unreferenced_in_recent(Pba(0), 2), None);
        let _ = rb.publish(marked_buffer(&rb, &[]));
        assert_eq!(rb.unreferenced_in_recent(Pba(0), 2), Some(true));
    }

    #[test]
    fn some_true_only_when_zero_across_all_k() {
        let rb = RefBitmap::new(4096, 2);
        // Barrier 1: PBA 42 referenced. Barrier 2: PBA 42 NOT referenced.
        let _ = rb.publish(marked_buffer(&rb, &[42]));
        let _ = rb.publish(marked_buffer(&rb, &[]));
        // Referenced in one of the last 2 → not an orphan.
        assert_eq!(rb.unreferenced_in_recent(Pba(42), 2), Some(false));
        // A PBA never referenced in either → orphan candidate.
        assert_eq!(rb.unreferenced_in_recent(Pba(7), 2), Some(true));
        // Barrier 3: still not referenced. Now the last 2 (b2,b3) are both 0.
        let _ = rb.publish(marked_buffer(&rb, &[]));
        assert_eq!(rb.unreferenced_in_recent(Pba(42), 2), Some(true));
    }

    #[test]
    fn eviction_keeps_exactly_k() {
        let rb = RefBitmap::new(4096, 2);
        for _ in 0..5 {
            let _ = rb.publish(marked_buffer(&rb, &[]));
        }
        assert_eq!(rb.published_count(), 2);
        assert_eq!(rb.barrier_gen(), 5);
    }

    #[test]
    fn recycle_only_evicted_unheld_buffer() {
        let rb = RefBitmap::new(4096, 1);
        let _ = rb.publish(marked_buffer(&rb, &[1]));
        // Hold an Arc to the (only / oldest) snapshot via a reader load.
        let held = rb.inner.snapshots.load_full();
        let oldest_ptr = held.snaps.front().unwrap().as_ref() as *const Snapshot;
        // Publishing now evicts the held snapshot; it cannot be recycled because
        // `held` still references it → a fresh buffer must come back.
        let recycled = rb.publish(marked_buffer(&rb, &[2]));
        let recycled_ptr = recycled.as_ptr();
        // The recycled buffer must NOT be the bits of the still-held snapshot.
        let held_bits_ptr = held.snaps.front().unwrap().bits.as_ptr();
        assert_eq!(
            held.snaps.front().unwrap().as_ref() as *const Snapshot,
            oldest_ptr
        );
        assert_ne!(recycled_ptr, held_bits_ptr);
        // Drop the reader, publish again: now the evicted buffer is unheld and
        // can be recycled (we don't assert pointer equality — allocator is free
        // to do either — only that no aliasing occurred while held).
        drop(held);
        let _ = rb.publish(marked_buffer(&rb, &[3]));
    }

    #[test]
    fn out_of_range_pba_is_referenced_never_panics() {
        let rb = RefBitmap::new(4096, 1);
        let _ = rb.publish(marked_buffer(&rb, &[]));
        // total_pbas = 4096; PBA 4096 and beyond are out of range.
        assert_eq!(rb.unreferenced_in_recent(Pba(4096), 1), Some(false));
        assert_eq!(rb.unreferenced_in_recent(Pba(u64::MAX), 1), Some(false));
        // mark() on an out-of-range PBA is a silent no-op (no panic / no alias).
        let mut buf = rb.fresh_fill_buffer();
        RefBitmap::mark(&mut buf, Pba(4096));
        RefBitmap::mark(&mut buf, Pba(u64::MAX));
        assert!(buf.iter().all(|&w| w == 0));
    }

    #[test]
    fn multi_block_unit_all_marked() {
        let rb = RefBitmap::new(4096, 1);
        // A 4-block compression unit at PBA 100 → blocks 100..104 all referenced.
        let _ = rb.publish(marked_buffer(&rb, &[100, 101, 102, 103]));
        for p in 100..104 {
            assert_eq!(rb.unreferenced_in_recent(Pba(p), 1), Some(false));
        }
        assert_eq!(rb.unreferenced_in_recent(Pba(104), 1), Some(true));
        assert_eq!(rb.unreferenced_in_recent(Pba(99), 1), Some(true));
    }
}
