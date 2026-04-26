# Onyx Storage Engine

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**Userspace all-flash block storage engine with inline compression, content-addressable dedup, and RAID-aware space management.**

Onyx is a high-performance block storage engine inspired by Red Hat VDO. It uses the in-tree onyx-metadb engine for metadata management, O_DIRECT for data I/O, and exposes block devices via Linux ublk. Designed for NVMe SSD arrays behind dm-raid / LVM.

> **Early Technology Preview** &mdash; This project is in early development for learning and research purposes. Core functionality (compression, dedup, GC, packer) is implemented and tested, but it is NOT production-ready. Do not use in production environments.

## Features

- **Inline compression** &mdash; LZ4 / ZSTD with coalesced multi-block compression units for high ratio
- **Content-addressable dedup** &mdash; SHA-256 fingerprinting, reference counting, background rescan for skipped blocks
- **Fragment packing** &mdash; VDO-style bin-packing of sub-4KB compressed fragments into shared physical slots
- **Garbage collection** &mdash; background dead-block scanner and rewriter with back-pressure control
- **Crash consistency** &mdash; metadb atomic commits; write-buffer sync-before-ack
- **High-performance write path** &mdash; staging channel + write thread batch (encode/CRC off hot path), jemalloc, DashMap 256-shard indices, per-shard backpressure
- **Batched backend** &mdash; writer drains up to 32 units per batch: one metadata commit, multi_get for old mappings, batched dedup cleanup
- **Zone-based parallelism** &mdash; LBA space partitioned into zones, each served by a dedicated worker thread
- **ublk frontend** (Linux) &mdash; expose volumes as `/dev/ublkbN` block devices with 512B sector alignment
- **Service mode** &mdash; multi-volume serving in a single process, Unix socket IPC for online management and graceful shutdown

## Architecture

```text
ublk (Linux) / stdin (macOS dev)
  |
ZoneManager --> ZoneWorker x N  (per-zone single-thread, crossbeam channel)
  |
WriteBufferPool  (staging channel + write thread batch, ring log on LV2, jemalloc)
  |  background BufferFlusher (per-shard lanes)
  v
Dedup Workers --> Compress Workers --> Batch Writer (drain N units)
  |
IoEngine (O_DIRECT --> LV3) + MetaStore (metadb multi_get + batch commit)
  |
SpaceAllocator (BTreeSet free list, strip-aligned allocation)
  |
dm-raid + LVM --> NVMe SSD x N
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

Edit `config/default.toml`:

```toml
[meta]
path = "/data/onyx/metadb"
block_cache_mb = 256

[storage]
data_device = "/dev/vg0/onyx-data"
block_size = 4096
default_compression = "Lz4"

[buffer]
device = "/dev/vg0/onyx-buffer"
capacity_mb = 16384
flush_watermark_pct = 80
group_commit_wait_us = 500    # batching window for group commit
shards = 4                   # ring shards (1 flush lane per shard)

[flush]
compress_workers = 2          # per flush lane

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
4. Background flusher (per-shard lane): coalesce contiguous LBAs &rarr; dedup (4KB SHA-256) &rarr; compress merged unit &rarr; packer bin-pack &rarr; batch writer (drain up to 32 units &rarr; one WriteBatch)

User-perceived latency = ring lock + memcpy + channel send. Encoding, CRC, disk I/O, compression, and dedup are fully off the hot path.

### Read Path

1. Check in-memory buffer index (O(1) HashMap) &rarr; hit = return immediately
2. Query blockmap &rarr; IoEngine reads physical slot (with slot_offset for packed fragments)
3. CRC32 verify &rarr; decompress &rarr; extract 4KB at offset_in_unit

### Dedup

- 4KB is the dedup granularity (fixed-size fingerprinting); compression granularity is much larger (up to 128KB coalesced units)
- Under buffer pressure (>90%), dedup is skipped and blocks are flagged `DEDUP_SKIPPED`; a background DedupScanner rescans them later
- Dedup index cleanup is atomic: when refcount hits zero, dedup_reverse prefix scan removes stale entries in the same WriteBatch

### Garbage Collection

- Background scanner identifies compression units with high dead-block ratio (>25% by default)
- Rewriter extracts live blocks, writes them back through the buffer (reusing the normal write path)
- Old PBA refcounts naturally reach zero &rarr; space reclaimed
- Back-pressure: GC pauses when buffer utilization exceeds 80%

## metadb Metadata Tables

Global metadata:

| CF              | Key                     | Value               | Purpose                        |
|-----------------|-------------------------|----------------------|--------------------------------|
| `volumes`       | `vol-{id}`              | bincode VolumeConfig | Volume registry                |
| `refcount`      | `pba(BE)`               | `count(BE)`          | Physical block reference counts|
| `dedup_index`   | `sha256(32B)`           | DedupEntry(27B)      | Content hash &rarr; PBA        |
| `dedup_reverse` | `pba(BE) + sha256(32B)` | empty                | Reverse lookup for cleanup     |

Per-volume blockmap:

| Table                    | Key        | Value            | Purpose                |
|--------------------------|------------|------------------|------------------------|
| `blockmap:{volume_id}`   | `lba(BE)`  | 28B BlockmapValue| LBA &rarr; PBA mapping |

Each volume gets its own metadb L2P namespace. Volume deletion drops the namespace and decrements refcounts; dedup reverse entries are cleaned when the target PBA reaches zero.

## Roadmap

- [x] MVP: ublk + metadb + compression + space management
- [x] Packer + GC: fragment bin-packing, GC scanner/rewriter, back-pressure, hole-map reuse
- [x] Dedup: worker pool, dedup_index/dedup_reverse, tiered skip strategy, background rescan
- [x] Performance: staging buffer, write thread batch, jemalloc, batched backend writer, multi_get, batched dedup cleanup
- [x] Service mode: multi-volume start, Unix socket IPC (stop/create/delete/list), signal handling (SIGTERM/SIGINT)
- [ ] RAID-aware: strip-aligned writes, strip-granularity allocation
- [ ] Production hardening: iSCSI frontend, HA (active-standby dual controller), Prometheus metrics
- [ ] High performance: NVMe-oF over RDMA

## License

Licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

---

[中文文档](README_CN.md)
