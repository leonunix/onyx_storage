# Onyx Storage Engine

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**Userspace all-flash block storage engine with inline compression, content-addressable dedup, and RAID-aware space management.**

Onyx is a high-performance block storage engine inspired by Red Hat VDO. It uses the in-tree onyx-metadb engine for metadata management, O_DIRECT for data I/O, and exposes block devices via Linux ublk. Designed for NVMe SSD arrays behind dm-raid / LVM.

> **Early Technology Preview** &mdash; This project is in early development for learning and research purposes. Core functionality (compression, dedup, GC, packer) is implemented and tested, but it is NOT production-ready. Do not use in production environments.

## Features

- **Inline compression** &mdash; LZ4 / ZSTD with coalesced multi-block compression units for high ratio
- **Content-addressable dedup** &mdash; SHA-256 fingerprinting, sharded apply lanes, inline (writer-path) hash registration, background rescan for pressure-skipped blocks
- **Fragment packing** &mdash; VDO-style bin-packing of sub-4KB compressed fragments into shared physical slots
- **Garbage collection** &mdash; background dead-block scanner and rewriter with back-pressure control
- **Purpose-built metadata engine** &mdash; in-tree [onyx-metadb](metadb/) (paged COW radix L2P + paged-array refcount + cuckoo dedup_index + paged dedup_reverse) sharing one WAL with group commit
- **Crash consistency** &mdash; one metadb tx per writer batch (L2P + refcount + dedup atomic under one fsync); write-buffer sync-before-ack
- **High-performance write path** &mdash; staging channel + write thread batch (encode/CRC off hot path), jemalloc, DashMap 256-shard indices, per-shard backpressure
- **Batched backend** &mdash; writer drains up to 32 units per batch: one metadb tx, `Db::multi_get` for old mappings, batched dedup cleanup via `cleanup_dedup_for_pbas_batch`
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
Dedup Workers --> Compress Workers --> Batch Writer (drain up to 32 units)
  |
IoEngine (O_DIRECT --> LV3) + MetaStore (one metadb tx: L2P + refcount + dedup, single fsync)
  |
SpaceAllocator (BTreeSet free list, strip-aligned allocation)
  |
dm-raid + LVM --> NVMe SSD x N

onyx-metadb (in-tree)
  paged COW radix L2P (per-volume, snapshot-able)
  paged-array refcount + per-shard delta
  cuckoo dedup_index + L0/L1 hot cache
  paged dedup_reverse (PBA-prefix scan)
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
memtable_budget_mb = 256            # dedup_index L0 memtable budget
index_pin_mb = 256                  # pin L2P index pages so point gets never miss
checkpoint_interval_ms = 5000
group_commit_timeout_us = 50        # WAL group-commit window (1 = aggressive batching)
dedup_shards = 8                    # per-shard dedup apply lanes (default 8)
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
4. Background flusher (per-shard lane): coalesce contiguous LBAs &rarr; dedup workers (4KB SHA-256, `Db::multi_get_dedup`) &rarr; compress merged unit &rarr; packer bin-pack &rarr; batch writer (drain up to 32 units &rarr; one metadb `tx.commit()`, one WAL group-commit fsync)

User-perceived latency = ring lock + memcpy + channel send. Encoding, CRC, disk I/O, compression, and dedup are fully off the hot path.

### Read Path

1. Check in-memory buffer index (O(1) HashMap) &rarr; hit = return immediately
2. Query L2P via metadb `Db::multi_get` &rarr; IoEngine reads physical slot (with slot_offset for packed fragments)
3. CRC32 verify &rarr; decompress &rarr; extract 4KB at offset_in_unit

### Dedup

- 4KB is the dedup granularity (fixed-size fingerprinting); compression granularity is much larger (up to 128KB coalesced units)
- Under per-shard buffer pressure (>90%), dedup is skipped and blocks are flagged `DEDUP_SKIPPED`; a background DedupScanner rescans them later
- Inline registration: writer commits `dedup_index` (cuckoo) + `dedup_reverse` (paged radix) inside the same packed/unit metadata transaction &mdash; no separate post-write register thread
- Dedup index cleanup is atomic: when refcount hits zero, `paged_reverse` PBA-prefix scan removes stale `dedup_index` entries in the same metadb tx
- `dedup_shards` (default 8) drives per-shard apply lanes inside metadb so concurrent flush lanes don't serialize on a single dedup hot lock

### Garbage Collection

- Background scanner identifies compression units with high dead-block ratio (>25% by default)
- Rewriter extracts live blocks, writes them back through the buffer (reusing the normal write path)
- Old PBA refcounts naturally reach zero &rarr; space reclaimed
- Back-pressure: GC pauses when buffer utilization exceeds 80%

## metadb Metadata Tables

Onyx's logical "tables" map onto four purpose-built structures inside [onyx-metadb](metadb/), all sharing one WAL and committing through a single `tx` per writer batch:

| Logical table   | metadb backing                           | Key                | Value               | Purpose                                  |
|-----------------|------------------------------------------|--------------------|---------------------|------------------------------------------|
| volume catalog  | manifest `VolumeEntry` + ordinal cache   | `VolumeOrdinal`    | shard roots, flags  | Volume registry; onyx caches `VolumeId` &harr; `VolumeOrdinal` |
| L2P (blockmap)  | per-volume paged COW radix tree          | `Lba(u64 BE)`      | 28B `L2pValue`      | LBA &rarr; PBA + compression metadata; supports per-volume snapshots / clones |
| refcount        | global paged-array + per-shard delta     | `Pba(u64 BE)`      | `u32` count         | Physical block reference counts (no snapshots) |
| dedup_index     | global cuckoo (4 slots/bucket) + L0/L1   | `sha256(32B)`      | 27B `DedupValue`    | Content hash &rarr; PBA fast lookup       |
| dedup_reverse   | global paged radix + overflow chains     | `Pba` prefix + hash| (empty)             | Prefix-scan-by-PBA &rarr; cleanup on refcount=0 |

Cross-table atomicity comes from a single metadb transaction: writer drains up to 32 units, accumulates all L2P insert/delete + refcount incref/decref + dedup_index put + dedup_reverse register into one `tx`, and `tx.commit()` lands them under one WAL group-commit fsync. There is no RocksDB, no column families, and no `WriteBatch` &mdash; the engine no longer has a `rocksdb` dependency at all.

Volume deletion goes through `Db::drop_volume`: metadb walks the per-volume L2P shards, batches PBA decrefs, triggers dedup_reverse cleanup on PBAs whose refcount hits zero, and frees the shard pages.

## Roadmap

- [x] MVP: ublk + metadata engine + compression + space management
- [x] Packer + GC: fragment bin-packing, GC scanner/rewriter, back-pressure, hole-map reuse
- [x] Dedup: worker pool, dedup_index/dedup_reverse, tiered skip strategy, background rescan, inline writer-path registration
- [x] Performance (frontend): staging buffer, write thread batch, jemalloc, DashMap 256-shard indices, ring backpressure
- [x] Performance (backend): batched writer (drain 32 units per metadb tx), multi_get for old mappings, batched dedup cleanup, sharded dedup apply lanes, balanced read pool dispatch
- [x] Metadata engine swap: replaced RocksDB with in-tree onyx-metadb (paged COW radix L2P, paged-array refcount + delta, cuckoo dedup_index, paged dedup_reverse, shared WAL with group commit)
- [x] Service mode: multi-volume start, Unix socket IPC (stop/create/delete/list), signal handling (SIGTERM/SIGINT)
- [ ] RAID-aware: strip-aligned writes, strip-granularity allocation
- [ ] Production hardening: iSCSI frontend, HA (active-standby dual controller), Prometheus metrics
- [ ] High performance: NVMe-oF over RDMA

## License

Licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

---

[中文文档](README_CN.md)
