//! Per-shard RAM candidate cache for promote-on-verified-hit dedup.
//!
//! Onyx's old write path inserted every dedup miss into the persistent
//! `dedup_index`, paying a metadb put per 4 KiB block whether or not
//! the block was ever going to be referenced again. The current design
//! routes the first occurrence of a fingerprint
//! into this in-memory cache instead. A second occurrence (still in
//! cache) confirms a real duplicate — the writer then verifies by
//! reading the original fragment back from LV3 and, on byte match,
//! promotes the entry into the persistent dedup table in a single
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

use std::collections::HashMap;
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
    /// In-flight promote gate: content hashes currently being promoted
    /// (`DedupPut`) by some dedup worker. Serialises same-hash promotes
    /// across the per-lane workers so two concurrent promotes never both
    /// resolve + decref the same old PBA — which underflowed the global
    /// refcount (nvme-box dedup_drainer A/B 2026-06-01; the race is the
    /// one documented in metadb `tx.rs::resolve_dedup_old_pbas`, fixed
    /// here on the onyx side where dedup serialization belongs).
    /// Membership-only.
    promote_inflight: dashmap::DashSet<ContentHash>,
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
                promote_inflight: dashmap::DashSet::new(),
                shard_mask: num_shards - 1,
            }),
        }
    }

    /// Try to claim `hash` for an in-flight promote. Returns `true` if
    /// this caller now owns the in-flight slot (it MUST call
    /// [`Self::release_promote`] after its commit, success or failure),
    /// or `false` if another worker is already promoting this hash — in
    /// which case the caller must drop the `DedupPut` for `hash` from
    /// its batch. The rc-neutral `L2pRemap` for the hit is kept
    /// regardless (it lands once the owning promote has incref'd the
    /// target PBA, or self-heals to a miss via its rc guard otherwise),
    /// so dropping the duplicate `DedupPut` costs at most a momentary
    /// dedup-ratio dip, never correctness. This is the onyx-side
    /// serialization the metadb `resolve_dedup_old_pbas` note calls for:
    /// two concurrent same-hash promotes would otherwise both capture
    /// and decref the same old PBA → rc underflow.
    pub fn try_claim_promote(&self, hash: &ContentHash) -> bool {
        self.inner.promote_inflight.insert(*hash)
    }

    /// Release a promote claim taken by [`Self::try_claim_promote`].
    pub fn release_promote(&self, hash: &ContentHash) {
        self.inner.promote_inflight.remove(hash);
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

    /// Insert a first-seen `(fp, value)` pair. Returns the previous
    /// stored value for this fp (if any) so callers can reason about
    /// idempotent re-inserts.
    pub fn insert(&self, fp: ContentHash, value: BlockmapValue) -> Option<BlockmapValue> {
        let shard = self.shard_for(&fp);
        let pba = value.pba;
        let prev_for_fp: Option<BlockmapValue>;
        let evicted_pair: Option<(ContentHash, BlockmapValue)>;
        {
            let mut g = self.inner.shards[shard].lock();
            // `lru::LruCache::push` returns the *evicted* entry, which
            // may be a different fp. Capture the existing value for
            // this fp first so we can return it as the "prior PBA"
            // signal.
            prev_for_fp = g.peek(&fp).copied();
            evicted_pair = g.push(fp, value);
        }
        // Reverse index update is split from the LRU critical section
        // so the DashMap write does not contend with foreground
        // lookups on the LRU shard.
        self.inner.pba_to_hashes.entry(pba).or_default().push(fp);
        if let Some((evicted_fp, evicted_value)) = evicted_pair {
            self.drop_from_reverse(evicted_value.pba, &evicted_fp);
        }
        prev_for_fp
    }

    /// Bulk insert. Buckets `pairs` by LRU shard so the per-shard
    /// mutex is acquired once per shard, not once per pair. Cuts
    /// mutex traffic dramatically on the writer post-commit path
    /// (one packed slot can carry hundreds of fragments).
    pub fn insert_many(&self, pairs: &[(ContentHash, BlockmapValue)]) {
        if pairs.is_empty() {
            return;
        }
        // Bucket by shard.
        let n = self.inner.shards.len();
        let mut by_shard: Vec<Vec<(ContentHash, BlockmapValue)>> =
            (0..n).map(|_| Vec::new()).collect();
        for &(fp, value) in pairs {
            by_shard[self.shard_for(&fp)].push((fp, value));
        }
        for (shard_idx, shard_pairs) in by_shard.into_iter().enumerate() {
            if shard_pairs.is_empty() {
                continue;
            }
            // Capture evictions under one lock, drain after release.
            let mut evicted: Vec<(ContentHash, BlockmapValue)> =
                Vec::with_capacity(shard_pairs.len());
            {
                let mut g = self.inner.shards[shard_idx].lock();
                for (fp, value) in &shard_pairs {
                    if let Some((ef, ev)) = g.push(*fp, *value) {
                        evicted.push((ef, ev));
                    }
                }
            }
            // Reverse-map updates outside the LRU lock. Group by PBA so a
            // packed slot with many fingerprints updates one DashMap entry
            // once instead of once per LBA.
            let mut reverse_by_pba: HashMap<Pba, Vec<ContentHash>> = HashMap::new();
            for (fp, value) in &shard_pairs {
                reverse_by_pba.entry(value.pba).or_default().push(*fp);
            }
            for (pba, fps) in reverse_by_pba {
                self.inner.pba_to_hashes.entry(pba).or_default().extend(fps);
            }
            for (efp, evalue) in evicted {
                self.drop_from_reverse(evalue.pba, &efp);
            }
        }
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
        self.remove_by_pbas(std::iter::once(pba));
    }

    /// Batch form of [`Self::remove_by_pba`]. Cleanup commonly retires tens of
    /// thousands of raw 4 KiB PBAs at once. Probing the reverse DashMap and
    /// allocating an LRU-shard bucket vector once per PBA made an empty cache
    /// expensive and made a populated cache repeatedly lock the same shards.
    pub fn remove_by_pbas<I>(&self, pbas: I)
    where
        I: IntoIterator<Item = Pba>,
    {
        if self.inner.pba_to_hashes.is_empty() {
            return;
        }

        // Group every matching fingerprint by LRU shard across the whole
        // cleanup batch, so each shard mutex is acquired at most once.
        let n = self.inner.shards.len();
        let mut by_shard: Vec<Vec<(ContentHash, Pba)>> = (0..n).map(|_| Vec::new()).collect();
        for pba in pbas {
            let Some((_, hashes)) = self.inner.pba_to_hashes.remove(&pba) else {
                continue;
            };
            for fp in hashes {
                by_shard[self.shard_for(&fp)].push((fp, pba));
            }
        }
        for (shard_idx, entries) in by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let mut g = self.inner.shards[shard_idx].lock();
            for (fp, pba) in entries {
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

    /// True if the cache currently holds at least one fingerprint
    /// pointing at `pba`. Cheap (one DashMap probe, no LRU lock); used
    /// by the cold-tail rescan to skip warming entries the writer or a
    /// previous cycle already cached.
    pub fn has_pba(&self, pba: Pba) -> bool {
        self.inner
            .pba_to_hashes
            .get(&pba)
            .map(|entry| !entry.is_empty())
            .unwrap_or(false)
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

    #[test]
    fn promote_gate_serialises_same_hash() {
        let c = CandidateCache::new(8, 64);
        let h = fp(0xAB);
        // First claim wins; a concurrent claim of the same hash is
        // rejected (its DedupPut must be dropped by the caller).
        assert!(c.try_claim_promote(&h), "first claim must win");
        assert!(
            !c.try_claim_promote(&h),
            "second concurrent claim must be rejected"
        );
        // A different hash is independent.
        assert!(
            c.try_claim_promote(&fp(0xCD)),
            "distinct hash claims independently"
        );
        // After release, the hash is claimable again.
        c.release_promote(&h);
        assert!(c.try_claim_promote(&h), "claimable again after release");
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
    fn remove_by_pbas_batches_across_reverse_and_lru_shards() {
        let c = CandidateCache::new(8, 64);
        for i in 0..32u8 {
            c.insert(fp(i), bv(100 + u64::from(i % 4)));
        }
        c.insert(fp(200), bv(999));

        c.remove_by_pbas((100..104).map(Pba));

        for i in 0..32u8 {
            assert!(c.lookup(&fp(i)).is_none());
        }
        assert_eq!(c.lookup(&fp(200)), Some(bv(999)));
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
                assert_eq!(c.lookup(&h), Some(bv(t as u64 * 1_000 + i as u64)));
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
