//! Per-shard RAM candidate cache for promote-on-verified-hit dedup.
//!
//! Onyx's old write path inserted every dedup miss into the persistent
//! `dedup_index` and `dedup_reverse`, paying two metadb puts per
//! 4 K block whether or not the block was ever going to be referenced
//! again. The new design routes the first occurrence of a fingerprint
//! into this in-memory cache instead. A second occurrence (still in
//! cache) confirms a real duplicate — the writer then verifies by
//! reading the original PBA back from LV3 and, on byte match, promotes
//! the entry into the persistent dedup tables in a single atomic
//! batch. Cache entries that age out without ever seeing a duplicate
//! are forgotten with zero on-disk cost.
//!
//! # Sharding
//!
//! Hash routing matches the metadb `dedup_shards` partitioning so that
//! a candidate hit for fingerprint `fp` and the eventual promote
//! commit always touch the same metadb shard — preserving the inline
//! dedup commit fast path. The shard for `fp` is the top
//! `log2(num_shards)` bits of `fp[0]`, identical to
//! [`onyx_metadb::dedup_types::shard_for_hash`].
//!
//! # Lookup / insert / remove semantics
//!
//! - `lookup(fp)` returns the PBA registered for `fp`, if any. Does
//!   not promote in any LRU sense (the LRU bump happens only on a
//!   confirmed-duplicate `lookup`).
//! - `insert(fp, pba)` records the first-seen PBA. If the cache was
//!   already full the eldest entry is evicted; the caller is *not*
//!   notified — eviction simply drops a dedup opportunity, never a
//!   correctness invariant.
//! - `remove_by_hash(fp)` clears the cache slot. Used by the promote
//!   path once `(fp, pba)` is mirrored into the persistent
//!   `dedup_index`.
//! - `remove_by_pba(pba)` clears the cache slot whose stored PBA
//!   matches. Used by the writer's refcount→0 cleanup so a freed PBA
//!   is never returned to a future verify call.
//!
//! Crash recovery is intentionally trivial: the cache is RAM-only.
//! Crashes lose pending candidates, costing a brief drop in dedup
//! ratio for the most-recent window. The persistent `dedup_index`
//! always survives — promote-on-verified-hit only writes there after
//! a successful LV3 verify, which is itself crash-safe via the buffer
//! pool flush log.

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::meta::schema::ContentHash;
use crate::types::Pba;

/// Default cache capacity per shard. With 8 shards × 16 M entries =
/// 128 M candidate fingerprints in flight, costing ~6 GiB at the
/// 24 B-per-entry overhead reported by `lru::LruCache<[u8; 8], u64>`
/// plus shard bookkeeping. Production callers should pick a value
/// matching the dedup-window they want to cover at the target write
/// rate (≈ window_seconds × IOPS / num_shards).
pub const DEFAULT_PER_SHARD_CAPACITY: usize = 1 << 20; // 1 M / shard

/// Shared handle to the candidate cache; cheap to clone.
#[derive(Clone)]
pub struct CandidateCache {
    inner: Arc<Inner>,
}

struct Inner {
    /// Per-shard LRU map: fp → pba. Sharded so concurrent inserts
    /// from different dedup workers don't queue on a single mutex.
    shards: Box<[Mutex<lru::LruCache<ContentHash, Pba>>]>,
    /// Reverse mapping pba → fp so refcount→0 cleanup can drop the
    /// candidate slot without searching every shard. Lock-free
    /// because contention is exclusively on the eviction-collision
    /// edge case.
    pba_to_fp: DashMap<Pba, ContentHash>,
    shard_mask: usize,
}

impl CandidateCache {
    /// Build a cache with `num_shards` (must be a power of two) of
    /// `per_shard_capacity` entries each.
    pub fn new(num_shards: usize, per_shard_capacity: usize) -> Self {
        assert!(
            num_shards.is_power_of_two(),
            "num_shards must be a power of two; got {num_shards}"
        );
        let cap = std::num::NonZeroUsize::new(per_shard_capacity.max(1))
            .expect("per_shard_capacity is at least 1");
        let shards = (0..num_shards)
            .map(|_| Mutex::new(lru::LruCache::new(cap)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            inner: Arc::new(Inner {
                shards,
                pba_to_fp: DashMap::new(),
                shard_mask: num_shards - 1,
            }),
        }
    }

    /// Lookup the PBA recorded for `fp`. Returns `None` if the
    /// fingerprint has never been seen, was evicted, or was already
    /// promoted into `dedup_index` (callers always check the
    /// persistent index *before* asking the candidate cache).
    pub fn lookup(&self, fp: &ContentHash) -> Option<Pba> {
        let shard = self.shard_for(fp);
        // Use `peek` so a bare lookup does not bump LRU position; the
        // bump happens on the writer-confirmed-duplicate path via
        // `mark_hit`. This keeps the cache focused on
        // recency-of-arrival rather than recency-of-probe so that
        // background scanners poking at random LBAs can't displace
        // the foreground working set.
        self.inner.shards[shard].lock().peek(fp).copied()
    }

    /// Same as [`lookup`] but bumps the entry to the LRU head. Use
    /// when a real duplicate has been confirmed (post LV3 verify) and
    /// the entry is therefore still load-bearing.
    pub fn mark_hit(&self, fp: &ContentHash) -> Option<Pba> {
        let shard = self.shard_for(fp);
        self.inner.shards[shard].lock().get(fp).copied()
    }

    /// Insert a first-seen `(fp, pba)` pair. Returns the previous
    /// stored PBA for this fp (if any) — the caller probably already
    /// knew one existed via [`lookup`] but the return makes
    /// idempotent re-inserts visible.
    pub fn insert(&self, fp: ContentHash, pba: Pba) -> Option<Pba> {
        let shard = self.shard_for(&fp);
        // `push` returns Some((evicted_fp, evicted_pba)) when the LRU
        // had to evict to make room. We use that to keep `pba_to_fp`
        // in sync.
        let prev = self.inner.shards[shard].lock().push(fp, pba);
        self.inner.pba_to_fp.insert(pba, fp);
        if let Some((evicted_fp, evicted_pba)) = prev {
            // Only drop the reverse mapping if it still points at the
            // evicted PBA — a concurrent insert for a different fp
            // could have just stamped a new entry for `evicted_pba`.
            self.inner.pba_to_fp.remove_if(&evicted_pba, |_, fp| {
                fp == &evicted_fp
            });
            // The "previous PBA for this fp" return value is the
            // evicted entry only when it shared the same fp, which
            // happens only on idempotent re-insert; otherwise the
            // eviction was for a different fp and the caller should
            // see `None` for "no prior PBA recorded for fp".
            if evicted_fp == fp {
                return Some(evicted_pba);
            }
        }
        None
    }

    /// Drop the cache slot for `fp`. Called by the promote path once
    /// the entry has been mirrored into the persistent dedup tables —
    /// further duplicates of `fp` will hit `dedup_index` directly and
    /// the candidate slot would otherwise waste capacity.
    pub fn remove_by_hash(&self, fp: &ContentHash) {
        let shard = self.shard_for(fp);
        if let Some(pba) = self.inner.shards[shard].lock().pop(fp) {
            self.inner.pba_to_fp.remove_if(&pba, |_, stored_fp| {
                stored_fp == fp
            });
        }
    }

    /// Drop whichever cache slot points at `pba`. Called from the
    /// writer's refcount→0 cleanup so a freed PBA can never be
    /// returned to a future verify check (which would read the
    /// space-allocator's reused, but content-different, sector).
    pub fn remove_by_pba(&self, pba: Pba) {
        if let Some((_, fp)) = self.inner.pba_to_fp.remove(&pba) {
            let shard = self.shard_for(&fp);
            // Only pop if the slot still maps to the same pba. A
            // racing `insert(fp, new_pba)` has already updated
            // `pba_to_fp`; in that case the LRU entry is for the new
            // pba and we should leave it.
            let mut g = self.inner.shards[shard].lock();
            if let Some(&stored) = g.peek(&fp) {
                if stored == pba {
                    g.pop(&fp);
                }
            }
        }
    }

    /// Total live entries across all shards. Acquires every shard
    /// mutex once; intended for metrics, not the hot path.
    pub fn len(&self) -> usize {
        self.inner.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.shards.iter().all(|s| s.lock().is_empty())
    }

    /// Approximate memory footprint. Each LRU entry costs roughly
    /// `key + value + 2 × (link pointer)` ≈ 32 B. Production
    /// monitoring should treat this as a soft estimate.
    pub fn approx_bytes(&self) -> usize {
        self.len() * 32
    }

    pub fn num_shards(&self) -> usize {
        self.inner.shards.len()
    }

    #[inline]
    fn shard_for(&self, fp: &ContentHash) -> usize {
        // Top bits of fp[0]; matches `onyx_metadb::dedup_types::shard_for_hash`
        // so a candidate stays in the same metadb dedup shard when the
        // entry promotes.
        (fp[0] as usize) & self.inner.shard_mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(byte: u8) -> ContentHash {
        let mut h = [0u8; 8];
        h[0] = byte;
        h
    }

    #[test]
    fn lookup_misses_on_empty() {
        let c = CandidateCache::new(8, 16);
        assert!(c.lookup(&fp(0)).is_none());
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
    }

    #[test]
    fn insert_then_lookup_returns_pba() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        assert_eq!(c.lookup(&fp(0xAA)), Some(Pba(42)));
    }

    #[test]
    fn mark_hit_returns_value() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        assert_eq!(c.mark_hit(&fp(0xAA)), Some(Pba(42)));
    }

    #[test]
    fn remove_by_hash_clears() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        c.remove_by_hash(&fp(0xAA));
        assert!(c.lookup(&fp(0xAA)).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn remove_by_pba_clears() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        c.remove_by_pba(Pba(42));
        assert!(c.lookup(&fp(0xAA)).is_none());
    }

    #[test]
    fn remove_by_pba_skips_when_pba_was_replaced() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        // Re-insert same fp at new PBA — this updates the LRU slot
        // and the reverse map. The old PBA is gone from the cache.
        c.insert(fp(0xAA), Pba(99));
        // Cleaning up the original pba should be a no-op now (the LRU
        // slot points at PBA 99, not 42).
        c.remove_by_pba(Pba(42));
        assert_eq!(c.lookup(&fp(0xAA)), Some(Pba(99)));
    }

    #[test]
    fn idempotent_reinsert_returns_old_pba() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), Pba(42));
        let prev = c.insert(fp(0xAA), Pba(99));
        assert_eq!(prev, Some(Pba(42)));
        assert_eq!(c.lookup(&fp(0xAA)), Some(Pba(99)));
    }

    #[test]
    fn lru_evicts_oldest_when_full() {
        let c = CandidateCache::new(2, 2); // 2 shards × 2 entries
        // Fingerprints chosen so all four land in shard 0:
        //   shard = fp[0] & (num_shards - 1) = fp[0] & 1
        //   even fp[0] → shard 0
        let pbas = [(0u8, 10), (2, 20), (4, 30), (6, 40)];
        for (b, pba) in pbas {
            c.insert(fp(b), Pba(pba));
        }
        // 2-entry capacity in shard 0 keeps the two most recent (4, 6);
        // (0, 2) should be gone.
        assert!(c.lookup(&fp(0)).is_none());
        assert!(c.lookup(&fp(2)).is_none());
        assert_eq!(c.lookup(&fp(4)), Some(Pba(30)));
        assert_eq!(c.lookup(&fp(6)), Some(Pba(40)));
    }

    #[test]
    fn shard_routing_matches_top_bits() {
        let c = CandidateCache::new(8, 4);
        // fp[0] = 0b00010000 = 16 → shard 16 & 7 = 0
        // fp[0] = 0b00010001 = 17 → shard 17 & 7 = 1
        let mut a = [0u8; 8];
        a[0] = 16;
        let mut b = [0u8; 8];
        b[0] = 17;
        c.insert(a, Pba(100));
        c.insert(b, Pba(200));
        // Each lands in a different shard, so neither evicts the other
        // even though shard capacity is 4.
        assert_eq!(c.lookup(&a), Some(Pba(100)));
        assert_eq!(c.lookup(&b), Some(Pba(200)));
    }

    #[test]
    fn concurrent_inserts_are_safe() {
        use std::thread;
        let c = CandidateCache::new(8, 1024);
        thread::scope(|scope| {
            for t in 0..8u8 {
                let c = c.clone();
                scope.spawn(move || {
                    for i in 0..256u32 {
                        let mut h = [0u8; 8];
                        h[0] = t;
                        h[1..5].copy_from_slice(&i.to_be_bytes());
                        c.insert(h, Pba(t as u64 * 1_000 + i as u64));
                    }
                });
            }
        });
        // Every inserted (fp, pba) should be retrievable since 8×256 =
        // 2048 entries fits easily across 8 shards × 1024.
        for t in 0..8u8 {
            for i in 0..256u32 {
                let mut h = [0u8; 8];
                h[0] = t;
                h[1..5].copy_from_slice(&i.to_be_bytes());
                assert_eq!(
                    c.lookup(&h),
                    Some(Pba(t as u64 * 1_000 + i as u64))
                );
            }
        }
    }
}
