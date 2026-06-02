# Adaptive reclaim + background heat-map refcount

Status: **design / not implemented.** Foundation landed: batched retired-extent
reclaim scan (`0c792a9 gc: batch retired-extent reclaim scan`). This doc is the
next step on top of it.

> **Guiding principle.** Dedup is a feature that *earns* space and speed — it must
> never become a write-path tax. All of dedup's accounting and reclaim stays **off
> the hot path**; the system grows its hot/cold understanding only in idle
> background time, and front-end IO never pays a cycle for it. Every design choice
> below is in service of that: dedup should be a delight, not a drag.

## Context

Physical-block reclaim must answer "is PBA P still referenced?" before returning
it to the allocator. onyx has no persistent `PBA → referrers` reverse index, so
the answer comes from a forward scan of every volume's L2P
(`MetaStore::referenced_extents` → `Db::scan_range_unordered_chunked`, called from
`GcRunner::reclaim_retired_extents` in [src/gc/runner.rs](../src/gc/runner.rs)).
The batch-scan change collapsed that from `O(retired × all_L2P)` (one full scan
per retired extent) to **one scan per GC cycle**.

We considered two ways to remove the scan entirely and rejected both:

- **Per-LBA physical refcount (revert Phase 5 rc-neutral).** Makes `rc==0` an O(1)
  reclaim gate, but either (a) `take_snapshot` must `+1` every referenced block →
  O(volume) burst, or (b) rc counts only live refs → `rc==0` is not a sufficient
  gate (snapshot pins) → still needs a snapshot-pin check. Both horns hurt; also
  re-adds hot-path write amplification Phase 5 deliberately removed.
- **`PBA → {VolumeId}` reverse set (vidset).** A per-PBA structure whose per-entry
  size grows with sharing degree; at many-volume scale the widely-shared (hot)
  blocks blow it up. Heavy regardless of per-entry encoding.

## Core idea

The batched reclaim scan **already visits every live `(volume, lba) → PBA`
mapping**. Accumulate a reference statistic *during that walk*. The statistic is
then:

1. **100 % off the hot path** — the write path emits zero refcount ops (preserves
   the Phase 5 benefit), and
2. **snapshot-free** — we only count live forward mappings; `take_snapshot` stays
   O(1) (it shares COW pages, creates no new mappings).

This is "pay-on-reclaim refcount", made free because the scan exists anyway. It is
the clean landing of the [[metadata_reverse_dimension_reclaim]] theme: the count is
maintained by a low-load background scan, not by the hot path, and the system's
hot/cold model **converges over idle time** — the longer it runs, the better it
knows what to skip and what to chase.

## Data model

Two tiers; pick per use, they can coexist:

- **Heat histogram (coarse, cheap, always affordable).** Partition the PBA space
  into fixed regions (e.g. one bucket per strip, or per N MiB). Each bucket holds a
  live-mapping count (`u32`) and a `last_scanned` cycle stamp. For a 21 TiB LV3 at
  1 MiB buckets that's ~22 M buckets × ~8 B ≈ 176 MiB; at strip (e.g. 512 KiB)
  granularity larger — tune the bucket size to the memory budget. This is a
  *prior* (region density), not an exact per-PBA count.
- **Exact per-PBA count (optional, heavy).** Equivalent to rebuilding the refcount
  array each scan (what open-time `space allocator rebuilt from metadata` already
  does). Only worth it if reclaim wants to read an exact `count==0` oracle rather
  than confirm-by-scan. Memory ≈ the rc array; likely skip unless measured.

Default plan: **heat histogram** for cadence control + keep the existing
confirm-scan for the actual free decision (see Safety). The exact-count tier is a
later option.

## Scan side (accumulation)

In the all-volume L2P walk (`referenced_extents` today; generalize to a
`scan_and_account` pass): for each non-zero decoded `BlockmapValue`, for each PBA
in `value.physical_pbas(BLOCK_SIZE)`, bump `heat[bucket(pba)].count` and set
`last_scanned = cycle`. Cost: O(1) per mapping on a walk that already happens.

Reuse this single pass for everything that needs to see live L2P:
- retired-extent reference check (current),
- dedup **cold-tail** warming (today a separate per-volume cursor scan in
  [src/dedup/scanner.rs](../src/dedup/scanner.rs)) — fold cold blocks into candidate
  cache in the same walk,
- the heat histogram.

## Reclaim side (consumption)

Per retired candidate extent, read `heat[bucket]` from the last refresh:
- bucket count `> 0` (region has live refs) → **defer** (likely still referenced);
- bucket count `== 0` and `last_scanned` is fresh → confirm reclaimable.

The heat read is a *filter/prior*, not proof: a cold bucket can still contain a
referenced PBA. The actual free still goes through the confirm path + the existing
retire→reclaim gate. The win is that we stop spending a full scan per cycle on
regions the heat map says are hot.

## Adaptive cadence

- **Hot regions** (high count, stable across refreshes): reclaim yield ≈ 0 →
  **lengthen re-scan interval**, spend little scan budget there.
- **Cold regions** (sparse / many dead, changing): high reclaim yield + dedup
  cold-tail value → **scan more often**.

This turns the per-cycle *full* scan into a per-cycle *adaptive partial* scan — a
further cut on top of batching. A scan budget per cycle (like
`cold_tail_max_per_cycle`) bounds the work; the heat map decides where to spend it.

## Decoupling

Today the scan only runs when there are retired candidates (`reclaim_retired_extents`
early-returns otherwise). To keep the heat map current as a standing model, split:

- **refresh** (build/update heat + warm cold-tail): runs on its own adaptive
  cadence, even with no pending reclaim;
- **reclaim** (consume heat + confirm + free): runs when there are retired
  candidates.

Both ride the same scan machinery; keep them one pass when both are due.

## Safety invariants

- **Never premature free.** The heat/count is a *lagging snapshot*. A retired
  `count==0` (or cold-bucket) PBA cannot acquire a new live reference in the
  window: fresh writes never allocate a retired PBA, and a dedup-hit remap onto it
  carries `guard rc>=1` which is rejected at `rc==0`. So staleness only ever
  **delays** reclaim, never causes a wrong free. (Same over-approximation property
  as the rejected vidset design — superset of true referrers.)
- **Staleness floor.** Hot regions are scanned rarely, but enforce a "every N
  cycles do a full-coverage pass" floor so a region that flips hot→cold (e.g. mass
  discard) is not starved of reclaim. No silent caps — `log()`/metric what was
  skipped.
- **Reclaim decision unchanged at the free point.** A block is freed iff (confirmed
  no live ref) AND (rc/retire gate) — identical to today; the heat map only changes
  *when/whether we bother to check*, not the correctness of the check.

## Metrics

Extend the GC metrics added with the batch-scan
(`gc_reclaim_blockmap_scans` in [src/metrics/mod.rs](../src/metrics/mod.rs)) with:
heat refresh passes, buckets skipped-as-hot per cycle, cold buckets scanned,
reclaim candidates filtered-by-heat vs confirmed, staleness-floor full passes. The
skipped counts are the "no silent truncation" signal.

## Phasing

1. **(landed)** Batch the reclaim scan — `0c792a9`.
2. Heat histogram built during the reclaim scan + surfaced in status/metrics
   (observe-only; no behavior change). Proves the model converges and is cheap.
3. Adaptive cadence: use the heat map to make the scan partial (skip hot regions,
   prioritize cold), with the staleness-floor.
4. Decouple refresh from reclaim; fold dedup cold-tail into the same pass.
5. (optional) exact per-PBA count tier if a true O(1) `count==0` reclaim oracle is
   wanted over confirm-by-scan.

Each phase is independently A/B-able and onyx-side only (no metadb change, no
hot-path/snapshot semantics touched), so none triggers the metadb soak gate.
