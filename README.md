# Onyx Storage Engine

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**Userspace all-flash block storage engine with inline compression, content-addressable dedup, and RAID-aware space management.**

Onyx is a high-performance block storage engine inspired by Red Hat VDO. It uses RocksDB for metadata management, O_DIRECT for data I/O, and exposes block devices via Linux ublk. Designed for NVMe SSD arrays behind dm-raid / LVM.

> **Status: Technology Preview** &mdash; core functionality (compression, dedup, GC, packer) is implemented and tested. Not yet recommended for production workloads.

## Features

- **Inline compression** &mdash; LZ4 / ZSTD with coalesced multi-block compression units for high ratio
- **Content-addressable dedup** &mdash; SHA-256 fingerprinting, reference counting, background rescan for skipped blocks
- **Fragment packing** &mdash; VDO-style bin-packing of sub-4KB compressed fragments into shared physical slots
- **Garbage collection** &mdash; background dead-block scanner and rewriter with back-pressure control
- **Crash consistency** &mdash; RocksDB WriteBatch atomic updates; write-buffer sync-before-ack
- **Zone-based parallelism** &mdash; LBA space partitioned into zones, each served by a dedicated worker thread
- **ublk frontend** (Linux) &mdash; expose volumes as `/dev/ublkbN` block devices with 512B sector alignment

## Architecture

```text
ublk (Linux) / stdin (macOS dev)
  |
ZoneManager --> ZoneWorker x N  (per-zone single-thread, crossbeam channel)
  |
WriteBufferPool  (O_DIRECT ring log on LV2, 8KB slots, sync-before-ack)
  |  background BufferFlusher
  v
Dedup Workers --> Compress Workers --> Packer (bin-pack fragments)
  |
IoEngine (O_DIRECT --> LV3) + MetaStore (RocksDB WriteBatch)
  |
SpaceAllocator (BTreeSet free list, strip-aligned allocation)
  |
dm-raid + LVM --> NVMe SSD x N
```

## Quick Start

### Prerequisites

- Rust 1.75+ (2021 edition)
- RocksDB system library (bundled via `rocksdb` crate)
- Linux 6.0+ for ublk frontend (macOS supported for development via stdin frontend)

### Build

```bash
cargo build --release
```

### Configure

Edit `config/default.toml`:

```toml
[meta]
rocksdb_path = "/data/onyx/rocksdb"
block_cache_mb = 256

[storage]
data_device = "/dev/vg0/onyx-data"
block_size = 4096
default_compression = "Lz4"

[buffer]
device = "/dev/vg0/onyx-buffer"
capacity_mb = 16384
flush_watermark_pct = 80

[ublk]
nr_queues = 4
queue_depth = 128
```

### Usage

```bash
# Create a volume (1 GB, LZ4 compression)
onyx-storage -c config/default.toml create-volume -n myvolume -s 1073741824 --compression lz4

# List volumes
onyx-storage -c config/default.toml list-volumes

# Start serving a volume via ublk
onyx-storage -c config/default.toml start -v myvolume

# Delete a volume
onyx-storage -c config/default.toml delete-volume -n myvolume
```

## Design Highlights

### Write Path

1. User I/O arrives at ZoneWorker
2. Raw (uncompressed) data appended to WriteBufferPool &rarr; sync &rarr; **ack to user**
3. Background flusher drains buffer: coalesce contiguous LBAs &rarr; dedup (4KB SHA-256) &rarr; compress merged unit &rarr; packer bin-pack &rarr; O_DIRECT write to LV3
4. RocksDB WriteBatch atomically updates blockmap + refcount

User-perceived latency = buffer write + sync. Compression and dedup are fully off the hot path.

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

## RocksDB Column Families

| CF              | Key                              | Value               | Purpose                        |
|-----------------|----------------------------------|----------------------|--------------------------------|
| `volumes`       | `vol-{id}`                       | bincode VolumeConfig | Volume registry                |
| `blockmap`      | `vol_id_len + vol_id + lba(BE)`  | 28B BlockmapValue    | LBA &rarr; PBA mapping         |
| `refcount`      | `pba(BE)`                        | `count(BE)`          | Physical block reference counts|
| `dedup_index`   | `sha256(32B)`                    | DedupEntry(27B)      | Content hash &rarr; PBA        |
| `dedup_reverse` | `pba(BE) + sha256(32B)`          | empty                | Reverse lookup for cleanup     |

## Roadmap

- [x] MVP: ublk + RocksDB + compression + space management
- [x] Packer + GC: fragment bin-packing, GC scanner/rewriter, back-pressure, hole-map reuse
- [x] Dedup: worker pool, dedup_index/dedup_reverse, tiered skip strategy, background rescan
- [ ] RAID-aware: strip-aligned writes, strip-granularity allocation
- [ ] Production hardening: iSCSI frontend, HA (active-standby dual controller), Prometheus metrics
- [ ] High performance: NVMe-oF over RDMA

## License

Licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

---

[中文文档](README_CN.md)
