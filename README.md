# Onyx Storage Engine

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**Userspace all-flash block storage engine with inline compression, content-addressable dedup, and RAID-aware space management.**

Onyx is a high-performance block storage engine inspired by Red Hat VDO. It uses the in-tree onyx-metadb engine for metadata management, O_DIRECT for data I/O, and exposes block devices via Linux ublk. Designed for NVMe SSD arrays behind dm-raid / LVM.

> **Early Technology Preview** &mdash; This project is in early development for learning and research purposes. Core functionality (compression, dedup, GC, packer) is implemented and tested, but it is NOT production-ready. Do not use in production environments.

For the current functional audit and PBA/dedup mechanism map, see [docs/onyx-functional-audit.md](docs/onyx-functional-audit.md) and [docs/onyx-mechanism-map.svg](docs/onyx-mechanism-map.svg).

## Features

- **Inline compression** &mdash; LZ4 / ZSTD with coalesced multi-block compression units for high ratio
- **Content-addressable dedup** &mdash; xxh3_64 fingerprinting, RAM candidate cache for first-occurrence misses, LV3 byte-verified promote on duplicate sighting, sharded apply lanes, cuckoo-filter L0, cold-tail background rescan to recover ratio after restart
- **Fragment packing** &mdash; VDO-style bin-packing of sub-4KB compressed fragments into shared physical slots
- **Garbage collection** &mdash; background dead-block scanner and rewriter with back-pressure control
- **Purpose-built metadata engine** &mdash; in-tree [onyx-metadb](metadb/) (paged COW radix L2P + paged-array refcount + cuckoo dedup_index) sharing one WAL with group commit
- **Crash consistency** &mdash; LV2 write-buffer sync-before-ack; metadb commits publish L2P remaps and verified dedup promotes atomically, with checkpoint/watermark based ring release
- **High-performance write path** &mdash; staging channel + write thread batch (encode/CRC off hot path), jemalloc, DashMap 256-shard indices, per-shard backpressure
- **Retired PBA reclaim** &mdash; committed dead PBAs are retired first, then reclaimed only after refcount, hazard, and blockmap/l2p-buffer confirmation gates
- **Zone-based parallelism** &mdash; LBA space partitioned into zones, each served by a dedicated worker thread
- **ublk frontend** (Linux) &mdash; expose volumes as `/dev/ublkbN` block devices with 512B sector alignment
- **Service mode** &mdash; multi-volume serving in a single process, Unix socket IPC for online management and graceful shutdown

## Architecture

```text
ublk (Linux)
  |
ZoneManager --> ZoneWorker x N  (per-zone single-thread, crossbeam channel)
  |
WriteBufferPool  (staging channel + write thread batch, ring log on LV2, jemalloc)
  |  background BufferFlusher (per-shard lanes)
  v
Dedup Workers --> Compress Workers --> Commit Workers
  |
IoEngine (O_DIRECT --> LV3) + MetaStore (metadb tx: L2P remap + verified DedupPut/repair)
  |
SpaceAllocator (free -> allocated -> retired -> reclaimed)
  |
dm-raid + LVM --> NVMe SSD x N

onyx-metadb (in-tree)
  paged COW radix L2P (per-volume, snapshot-able)
  paged-array refcount + per-shard delta (dedup membership / lineage aware)
  cuckoo dedup_index (4 slots/bucket) + cuckoo-filter L0 + L1 hot cache
  shared WAL + group commit + per-shard apply lanes
```

## Repository Layout

```text
.
├── src/           Rust storage engine
├── config/        Engine configuration
├── tests/         Rust integration / correctness tests
└── dashboard/     Control plane subproject
    ├── backend/   Go API, RBAC, audit, Onyx/dm/LVM adapters
    ├── frontend/  Vue 3 + Bootstrap management UI
    └── docs/      Architecture, RBAC, roadmap
```

`dashboard/` is tracked from this repository as a Git submodule. It is the control-plane subproject for Onyx, versioned independently but mounted inside the main repository workflow.

## Quick Start

### Prerequisites

- Rust 1.75+ (2021 edition)
- No external metadata database dependency; onyx-metadb is built from this workspace
- Linux 6.0+ for ublk frontend (macOS supported for development via stdin frontend)

### Build

```bash
cargo build --release
```

Or use the top-level helper targets:

```bash
make
make all
make engine-build
make engine-test
```

`make` and `make all` build the Rust storage engine only. They do not build the dashboard submodule by default.

Build dashboard only when you explicitly need it:

```bash
make dashboard-backend
make dashboard-frontend
make dashboard-backend-build
make dashboard-frontend-build
make dashboard-build
```

If you cloned the repository fresh, initialize the submodule first:

```bash
git submodule update --init --recursive
```

### Configure

Edit `config/default.toml` (a fully tuned NVMe profile is in `config/nvme-detailed.toml`):

```toml
[meta]
path = "/data/onyx/metadb"
# wal_dir = "/data/onyx/wal"        # optional: separate WAL device
block_cache_mb = 256                # metadb page cache budget
index_pin_mb = 256                  # pin L2P index pages so point gets never miss
checkpoint_interval_ms = 5000
group_commit_timeout_us = 50        # WAL group-commit window (1 = aggressive batching)
dedup_shards = 8                    # per-shard dedup apply lanes (default 8) — part of on-disk layout
dedup_cuckoo_buckets = 4000000      # size by unique-4K hash working set / (4 * 0.5~0.7)
dedup_l1_cache_entries = 64000      # hot dedup_index entries kept in RAM

[storage]
data_device = "/dev/vg0/onyx-data"
block_size = 4096
default_compression = "Lz4"
io_backend = "uring"                # uring | psync
read_pool_workers = 8

[buffer]
device = "/dev/vg0/onyx-buffer"
capacity_mb = 16384
flush_watermark_pct = 80
group_commit_wait_us = 500          # batching window for buffer write thread
shards = 4                          # ring shards (1 flush lane per shard)

[flush]
compress_workers = 2                # per flush lane
packed_meta_batch_max_lbas = 1024   # packed metadata commit ceiling (NVMe sweet spot)

[dedup]
enabled = true
workers = 2                         # per buffer shard (shards x workers = total foreground dedup workers)
buffer_skip_threshold_pct = 90      # per-shard fill% triggering DEDUP_SKIPPED
rescan_interval_ms = 30000
max_rescan_per_cycle = 256          # DEDUP_SKIPPED blocks drained per cycle
cold_tail_max_per_cycle = 256       # live blockmap entries hashed into the candidate cache per cycle (0 disables)
# candidate_shards = 8              # candidate-cache shard count (defaults to dedup_shards)
# candidate_per_shard_capacity =    # entries per shard (defaults to ~1M; total RAM = shards x cap x ~52B)

[ublk]
nr_queues = 4
queue_depth = 128

[service]
socket_path = "/var/run/onyx-storage.sock"  # IPC socket for stop/create/delete
```

### Usage

```bash
# Create a volume (1 GB, LZ4 compression)
onyx-storage -c config/default.toml create-volume -n myvolume -s 1073741824 --compression lz4

# List volumes
onyx-storage -c config/default.toml list-volumes

# Start serving all volumes via ublk (each volume gets its own /dev/ublkbN)
onyx-storage -c config/default.toml start

# Start specific volumes only
onyx-storage -c config/default.toml start -v vol1 -v vol2

# While running: create/delete/list volumes via IPC (another terminal)
onyx-storage -c config/default.toml create-volume -n newvol -s 1073741824 --compression lz4
onyx-storage -c config/default.toml list-volumes
onyx-storage -c config/default.toml delete-volume -n newvol

# Graceful stop (via Unix socket, or Ctrl+C / SIGTERM)
onyx-storage -c config/default.toml stop
```

## Dashboard Subproject

The dashboard subproject lives under [dashboard/README.md](dashboard/README.md) and covers:

- device / dm / LVM topology discovery
- Onyx volume lifecycle management
- engine status and metrics views
- RBAC, login, and audit log foundations

Run it from the main repository:

```bash
make dashboard-backend
make dashboard-frontend
```

The dashboard is optional and is not part of the default storage-engine build artifact.

Or manually:

```bash
cd dashboard/backend && go run ./cmd/dashboardd
cd dashboard/frontend && npm install && npm run dev
```

## Design Highlights

### Write Path

1. User I/O arrives at ZoneWorker
2. `append()`: ring reserve (~50ns) + DashMap inserts + staging channel send &rarr; **~3&micro;s total, zero disk I/O**
3. Write thread: batch encode + CRC + pwrite + fdatasync &rarr; ack to user via ready channel
4. Background flusher (per-shard lane): coalesce contiguous LBAs &rarr; dedup workers (4KB xxh3_64 fingerprint, `Db::multi_get_dedup` + RAM candidate-cache lookup + LV3 batched-io_uring byte verify) &rarr; compress merged unit &rarr; packer bin-pack &rarr; commit workers publish L2P remaps and verified dedup promotes through metadb

User-perceived latency = ring lock + memcpy + channel send. Encoding, CRC, disk I/O, compression, and dedup are fully off the hot path.

### Read Path

1. Check in-memory buffer index (O(1) HashMap) &rarr; hit = return immediately
2. Query L2P via metadb `Db::multi_get` &rarr; IoEngine reads physical slot (with slot_offset for packed fragments)
3. CRC32 verify &rarr; decompress &rarr; extract 4KB at offset_in_unit

### Dedup

- 4 KiB is the dedup granularity (fixed-size fingerprinting); compression granularity is much larger (up to 128 KiB coalesced units).
- **xxh3_64 fingerprints (8 B)**, not crypto-strength. Per-pair collision probability ~1.5e-8, so byte verify is *correctness*, not optimisation &mdash; every prospective hit re-reads the original fragment from LV3 through the engine's batched io_uring `ReadPool` and compares against the new write's source bytes before it is allowed to dedup.
- **Promote-on-verified-hit**: a first-occurrence miss does **not** publish to the persistent `dedup_index` &mdash; instead it lands in a sharded RAM `CandidateCache` (per-shard LRU, ~1 M slots/shard by default). Only the second sighting of the same fingerprint, after LV3 byte-verify confirms a real duplicate, promotes the entry into `dedup_index` atomically with the LBA remap (`atomic_batch_dedup_hits_with_promote`). Singleton blocks therefore cost **zero metadb writes** for their dedup metadata.
- **Two-stage L0**: `dedup_index` lookups go through L1 hot cache &rarr; cuckoo filter (16-bit fingerprint, 4 slots/bucket, packed u64) &rarr; on-disk cuckoo. The filter avoids reading cold cuckoo pages on every miss; FPR ~0.006%, lossless degradation when saturated.
- **Cold-tail rescan** (in `DedupScanner`): a per-volume LBA cursor walks live blockmap entries one chunk per cycle, fans LV3 reads through the `ReadPool`, computes xxh3, and warms the candidate cache. This recovers dedup ratio after process restart (the cache is RAM-only) and on long-running engines whose dedup window has moved past entries the writer originally cached. Tunable via `dedup.cold_tail_max_per_cycle`.
- Under per-shard buffer pressure (>90 %), foreground dedup is skipped and blocks are flagged `DEDUP_SKIPPED`; the same scanner drains them later, hashing in the background and warming the candidate cache (still no direct dedup_index write &mdash; promote stays gated on a verified second sighting).
- Dedup index cleanup avoids a persistent reverse table. Post-commit cleanup removes candidate-cache entries for dead PBAs and retires committed physical space; persistent forward-index maintenance is handled by verify-mismatch compare-put repair, bounded dedup-index scrub, and orphan reclaim/demote. A retired PBA is not reusable until GC confirms `refcount == 0`, waits out hazards, and verifies that neither folded L2P nor the metadb L2P buffer references it.
- `dedup_shards` (default 8) drives per-shard apply lanes inside metadb so concurrent flush lanes don't serialise on a single dedup hot lock; the candidate cache shards on the same routing so a hit and the eventual promote land in the same metadb shard.

### Garbage Collection

- Background scanner identifies compression units with high dead-block ratio (>25% by default)
- Rewriter extracts live blocks, writes them back through the buffer (reusing the normal write path)
- Old committed PBAs are retired first; GC reclaim then checks refcount, waits for in-flight readers/promotes, scans blockmap/l2p-buffer for references, and only then returns space to the allocator
- Back-pressure: GC pauses when buffer utilization exceeds 80%

## metadb Metadata Tables

Onyx's logical "tables" map onto four purpose-built structures inside [onyx-metadb](metadb/), all sharing one WAL and committing through a single `tx` per writer batch:

| Logical table   | metadb backing                                       | Key                          | Value               | Purpose                                  |
|-----------------|------------------------------------------------------|------------------------------|---------------------|------------------------------------------|
| volume catalog  | manifest `VolumeEntry` + ordinal cache               | `VolumeOrdinal`              | shard roots, flags  | Volume registry; onyx caches `VolumeId` &harr; `VolumeOrdinal` |
| L2P (blockmap)  | per-volume paged COW radix tree                      | `Lba(u64 BE)`                | 28 B `L2pValue`     | LBA &rarr; PBA + compression metadata; supports per-volume snapshots / clones |
| refcount        | global paged-array + per-shard delta                 | `Pba(u64 BE)`                | `u32` count         | Physical block reference counts (no snapshots) |
| dedup_index     | global cuckoo (4 slots/bucket, 28 buckets/page) + cuckoo-filter L0 + L1 hot cache | `Hash8` (xxh3_64) | 27 B `DedupValue`   | Content hash &rarr; PBA fast lookup       |

Cross-table atomicity comes from metadb transactions: writer commits publish L2P remaps, and verified candidate promotes add `DedupPut` in the same transaction as their LBA remap (`atomic_batch_dedup_hits_with_promote`). First-occurrence misses live in the RAM `CandidateCache` and never touch the persistent dedup table. There is no RocksDB, no column families, and no `WriteBatch` &mdash; the engine no longer has a `rocksdb` dependency at all.

Committed PBA reclamation is intentionally two-stage. Remap/delete/dedup demote paths retire dead physical space after removing candidate-cache references; `GcRunner::reclaim_retired_extents` later releases it only after refcount, hazard, and exact blockmap/l2p-buffer checks. Volume deletion follows the same retired-PBA model.

## Roadmap

- [x] MVP: ublk + metadata engine + compression + space management
- [x] Packer + GC: fragment bin-packing, GC scanner/rewriter, back-pressure, hole-map reuse
- [x] Dedup: worker pool, dedup_index, tiered skip strategy, DEDUP_SKIPPED rescan, RAM candidate cache + LV3 byte-verified promote on duplicate sighting, candidate-before-retire cleanup, scrub/orphan maintenance, cold-tail blockmap rescan, cuckoo-filter L0
- [x] Performance (frontend): staging buffer, write thread batch, jemalloc, DashMap 256-shard indices, ring backpressure
- [x] Performance (backend): batched writer (drain 32 units per metadb tx), multi_get for old mappings, batched dedup cleanup, sharded dedup apply lanes, balanced read pool dispatch
- [x] Metadata engine swap: replaced RocksDB with in-tree onyx-metadb (paged COW radix L2P, paged-array refcount + delta, cuckoo dedup_index, shared WAL with group commit)
- [x] Service mode: multi-volume start, Unix socket IPC (stop/create/delete/list), signal handling (SIGTERM/SIGINT)
- [ ] RAID-aware: strip-aligned writes, strip-granularity allocation
- [ ] Production hardening: iSCSI frontend, HA (active-standby dual controller), Prometheus metrics
- [ ] High performance: NVMe-oF over RDMA

## License

Licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

---

[中文文档](README_CN.md)
