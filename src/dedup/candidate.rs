//! Per-shard RAM candidate cache for promote-on-verified-hit dedup.
//!
//! Onyx's old write path inserted every dedup miss into the persistent
//! `dedup_index` and `dedup_reverse`, paying two metadb puts per
//! 4 KiB block whether or not the block was ever going to be referenced
//! again. The new design routes the first occurrence of a fingerprint
//! into this in-memory cache instead. A second occurrence (still in
//! cache) confirms a real duplicate — the writer then verifies by
//! reading the original fragment back from LV3 and, on byte match,
//! promotes the entry into the persistent dedup tables in a single
//! atomic batch. Cache entries that age out without ever seeing a
//! duplicate are forgotten with zero on-disk cost.
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
//! # Stored value
//!
//! Each cache slot is `(ContentHash → BlockmapValue)`. The value is
//! the full block-map metadata for the *original* (first-seen) write,
//! including PBA, slot offset, compression algorithm, unit sizes,
//! offset within the unit, and CRC. That is everything the LV3 read
//! pool needs to decode and serve the original 4 KiB block back to
//! the verifier; storing it eliminates a metadb get on the verify
//! hot path and means the cache is robust against blockmap remaps
//! after the candidate was inserted.
//!
//! # Lookup / insert / remove semantics
//!
//! - `lookup(fp)` returns the [`BlockmapValue`] registered for `fp`,
//!   if any. Does not promote in any LRU sense (the LRU bump happens
//!   only on a confirmed-duplicate `mark_hit`).
//! - `insert(fp, value)` records the first-seen entry. If the cache
//!   was already full the eldest entry is evicted; the caller is
//!   *not* notified — eviction simply drops a dedup opportunity,
//!   never a correctness invariant.
//! - `remove_by_hash(fp)` clears the cache slot. Used by the promote
//!   path once `(fp, value)` is mirrored into the persistent
//!   `dedup_index`.
//! - `remove_by_pba(pba)` clears every cache slot whose stored
//!   BlockmapValue refers to `pba`. Used by the writer's
//!   refcount→0 cleanup so a freed PBA is never returned to a future
//!   verify call (which would otherwise read a sector the allocator
//!   has handed to a new owner).
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

use crate::meta::schema::{BlockmapValue, ContentHash};
use crate::types::Pba;

/// Default cache capacity per shard. With 8 shards × 1 M entries =
/// 8 M candidate fingerprints in flight, costing ~400 MiB at the
/// ~50 B per-entry overhead (8 B hash + 28 B BlockmapValue + LRU
/// linkage). Production callers should pick a value matching the
/// dedup-window they want to cover at the target write rate
/// (≈ window_seconds × IOPS / num_shards).
pub const DEFAULT_PER_SHARD_CAPACITY: usize = 1 << 20; // 1 M / shard

/// Shared handle to the candidate cache; cheap to clone.
#[derive(Clone)]
pub struct CandidateCache {
    inner: Arc<Inner>,
}

struct Inner {
    /// Per-shard LRU map: fp → BlockmapValue. Sharded so concurrent
    /// inserts from different dedup workers don't queue on a single
    /// mutex.
    shards: Box<[Mutex<lru::LruCache<ContentHash, BlockmapValue>>]>,
    /// Reverse mapping pba → list of hashes whose stored
    /// BlockmapValue points at this pba. Used by the writer's
    /// refcount→0 cleanup to drop every candidate slot referring to
    /// the freed PBA.
    ///
    /// A packed slot can carry several distinct fingerprints (one per
    /// LBA inside the slot), all sharing the same PBA, so the value
    /// is `Vec<ContentHash>`. The Vec lives behind the per-entry
    /// DashMap lock; entries that are stale (LRU-evicted but still
    /// listed here) are tolerated — `remove_by_pba` simply finds no
    /// matching shard slot for those and moves on.
    pba_to_hashes: DashMap<Pba, Vec<ContentHash>>,
    shard_mask: usize,
}

impl CandidateCache {
    /// Build a cache with `num_shards` (rounded up to a power of two)
    /// of `per_shard_capacity` entries each.
    pub fn new(num_shards: usize, per_shard_capacity: usize) -> Self {
        let num_shards = num_shards.max(1).next_power_of_two();
        let cap = std::num::NonZeroUsize::new(per_shard_capacity.max(1))
            .expect("per_shard_capacity is at least 1");
        let shards = (0..num_shards)
            .map(|_| Mutex::new(lru::LruCache::new(cap)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            inner: Arc::new(Inner {
                shards,
                pba_to_hashes: DashMap::new(),
                shard_mask: num_shards - 1,
            }),
        }
    }

    /// Lookup the [`BlockmapValue`] recorded for `fp`. Returns `None`
    /// if the fingerprint has never been seen, was evicted, or was
    /// already promoted into `dedup_index` (callers always check the
    /// persistent index *before* asking the candidate cache).
    pub fn lookup(&self, fp: &ContentHash) -> Option<BlockmapValue> {
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
    pub fn mark_hit(&self, fp: &ContentHash) -> Option<BlockmapValue> {
        let shard = self.shard_for(fp);
        self.inner.shards[shard].lock().get(fp).copied()
    }

    /// Insert a first-seen `(fp, value)` pair. Returns the previous
    /// stored value for this fp (if any) so callers can reason about
    /// idempotent re-inserts.
    pub fn insert(&self, fp: ContentHash, value: BlockmapValue) -> Option<BlockmapValue> {
        let shard = self.shard_for(&fp);
        let pba = value.pba;
        let mut prev_for_fp: Option<BlockmapValue> = None;
        let mut evicted_pair: Option<(ContentHash, BlockmapValue)> = None;
        {
            let mut g = self.inner.shards[shard].lock();
            // Capture the existing value for this fp (if any) before
            // overwriting; `lru::LruCache::push` returns the *evicted*
            // entry which may be a different fp.
            prev_for_fp = g.peek(&fp).copied();
            evicted_pair = g.push(fp, value);
        }
        // Update the reverse index. This is split from the LRU mutex
        // critical section so the DashMap entry write does not
        // contend with foreground lookups on the LRU shard.
        self.inner
            .pba_to_hashes
            .entry(pba)
            .or_default()
            .push(fp);
        if let Some((evicted_fp, evicted_value)) = evicted_pair {
            // Same-fp eviction means we re-inserted; reverse index
            // already covers the new (pba, fp) tuple, just remove the
            // old (evicted_value.pba, fp) pair.
            self.drop_from_reverse(evicted_value.pba, &evicted_fp);
        }
        prev_for_fp
    }

    /// Drop the cache slot for `fp`. Called by the promote path once
    /// the entry has been mirrored into the persistent dedup tables —
    /// further duplicates of `fp` will hit `dedup_index` directly and
    /// the candidate slot would otherwise waste capacity.
    pub fn remove_by_hash(&self, fp: &ContentHash) {
        let shard = self.shard_for(fp);
        let evicted = self.inner.shards[shard].lock().pop(fp);
        if let Some(value) = evicted {
            self.drop_from_reverse(value.pba, fp);
        }
    }

    /// Drop every cache slot whose stored BlockmapValue points at
    /// `pba`. Called from the writer's refcount→0 cleanup so a freed
    /// PBA can never be returned to a future verify check.
    pub fn remove_by_pba(&self, pba: Pba) {
        let entry = self.inner.pba_to_hashes.remove(&pba);
        if let Some((_, hashes)) = entry {
            for fp in hashes {
                let shard = self.shard_for(&fp);
                let mut g = self.inner.shards[shard].lock();
                // Pop only when the slot still points at the freed
                // PBA. A racing `insert(fp, new_value)` for a *new*
                // PBA may have stamped a fresh entry for the same fp;
                // we must leave that alone.
                if let Some(stored) = g.peek(&fp) {
                    if stored.pba == pba {
                        g.pop(&fp);
                    }
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
    /// `key (8 B) + value (28 B) + 2 link pointers (16 B)` ≈ 52 B
    /// plus the per-PBA reverse entry overhead. Production
    /// monitoring should treat this as a soft estimate.
    pub fn approx_bytes(&self) -> usize {
        self.len() * 52
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

    /// Helper: remove `fp` from the reverse list for `pba`, dropping
    /// the whole entry if the list becomes empty.
    fn drop_from_reverse(&self, pba: Pba, fp: &ContentHash) {
        let mut empty = false;
        if let Some(mut entry) = self.inner.pba_to_hashes.get_mut(&pba) {
            if let Some(pos) = entry.iter().position(|h| h == fp) {
                entry.swap_remove(pos);
            }
            empty = entry.is_empty();
        }
        if empty {
            // Re-check under the dashmap shard lock so we don't drop a
            // concurrently-repopulated entry.
            self.inner
                .pba_to_hashes
                .remove_if(&pba, |_, hashes| hashes.is_empty());
        }
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

    fn bv(pba: u64) -> BlockmapValue {
        BlockmapValue {
            pba: Pba(pba),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        }
    }

    fn bv_with_offset(pba: u64, offset_in_unit: u16) -> BlockmapValue {
        let mut v = bv(pba);
        v.offset_in_unit = offset_in_unit;
        v
    }

    #[test]
    fn lookup_misses_on_empty() {
        let c = CandidateCache::new(8, 16);
        assert!(c.lookup(&fp(0)).is_none());
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
    }

    #[test]
    fn insert_then_lookup_returns_value() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        assert_eq!(c.lookup(&fp(0xAA)), Some(bv(42)));
    }

    #[test]
    fn mark_hit_returns_value() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        assert_eq!(c.mark_hit(&fp(0xAA)), Some(bv(42)));
    }

    #[test]
    fn remove_by_hash_clears() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        c.remove_by_hash(&fp(0xAA));
        assert!(c.lookup(&fp(0xAA)).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn remove_by_pba_clears_single_entry() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        c.remove_by_pba(Pba(42));
        assert!(c.lookup(&fp(0xAA)).is_none());
    }

    #[test]
    fn remove_by_pba_clears_all_fragments_in_packed_slot() {
        // A packed slot at pba=100 may carry several different
        // fingerprints (one per LBA inside the slot). Cleaning up the
        // PBA must drop every candidate entry that referenced it.
        let c = CandidateCache::new(8, 64);
        c.insert(fp(1), bv_with_offset(100, 0));
        c.insert(fp(2), bv_with_offset(100, 1));
        c.insert(fp(3), bv_with_offset(100, 2));
        c.insert(fp(4), bv(200)); // different PBA — should survive

        c.remove_by_pba(Pba(100));

        assert!(c.lookup(&fp(1)).is_none());
        assert!(c.lookup(&fp(2)).is_none());
        assert!(c.lookup(&fp(3)).is_none());
        assert_eq!(c.lookup(&fp(4)), Some(bv(200)));
    }

    #[test]
    fn remove_by_pba_skips_when_pba_was_replaced() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        // Re-insert same fp at new PBA.
        c.insert(fp(0xAA), bv(99));
        // Cleaning up the original pba should be a no-op now.
        c.remove_by_pba(Pba(42));
        assert_eq!(c.lookup(&fp(0xAA)), Some(bv(99)));
    }

    #[test]
    fn idempotent_reinsert_returns_old_value() {
        let c = CandidateCache::new(8, 16);
        c.insert(fp(0xAA), bv(42));
        let prev = c.insert(fp(0xAA), bv(99));
        assert_eq!(prev, Some(bv(42)));
        assert_eq!(c.lookup(&fp(0xAA)), Some(bv(99)));
    }

    #[test]
    fn lru_evicts_oldest_when_full() {
        let c = CandidateCache::new(2, 2);
        // fp[0] even → shard 0 (mask 1). All four collide on shard 0.
        let pairs = [(0u8, 10), (2, 20), (4, 30), (6, 40)];
        for (b, pba) in pairs {
            c.insert(fp(b), bv(pba));
        }
        // 2-entry capacity in shard 0 keeps the two most recent (4, 6);
        // (0, 2) should be gone.
        assert!(c.lookup(&fp(0)).is_none());
        assert!(c.lookup(&fp(2)).is_none());
        assert_eq!(c.lookup(&fp(4)), Some(bv(30)));
        assert_eq!(c.lookup(&fp(6)), Some(bv(40)));
    }

    #[test]
    fn shard_routing_matches_top_bits() {
        let c = CandidateCache::new(8, 4);
        let mut a = [0u8; 8];
        a[0] = 16;
        let mut b = [0u8; 8];
        b[0] = 17;
        c.insert(a, bv(100));
        c.insert(b, bv(200));
        // Each lands in a different shard.
        assert_eq!(c.lookup(&a), Some(bv(100)));
        assert_eq!(c.lookup(&b), Some(bv(200)));
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
                        c.insert(h, bv(t as u64 * 1_000 + i as u64));
                    }
                });
            }
        });
        for t in 0..8u8 {
            for i in 0..256u32 {
                let mut h = [0u8; 8];
                h[0] = t;
                h[1..5].copy_from_slice(&i.to_be_bytes());
                assert_eq!(
                    c.lookup(&h),
                    Some(bv(t as u64 * 1_000 + i as u64))
                );
            }
        }
    }

    #[test]
    fn rounds_shard_count_up_to_power_of_two() {
        let c = CandidateCache::new(5, 16);
        // 5 → 8.
        assert_eq!(c.num_shards(), 8);
    }
}
