# Onyx 存储引擎

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**用户态全闪块存储引擎，支持内联压缩、内容寻址去重和 RAID 感知空间管理。**

Onyx 是一个高性能块存储引擎，设计灵感来自 Red Hat VDO。使用仓库内的 onyx-metadb 管理元数据，O_DIRECT 进行数据 I/O，通过 Linux ublk 对外暴露块设备。面向 dm-raid / LVM 之上的 NVMe SSD 阵列。

> **早期技术预览** &mdash; 本项目处于早期开发阶段，用于学习和研究存储引擎内部原理。核心功能（压缩、去重、GC、Packer）已实现并通过测试，但尚未达到生产级别，请勿用于生产环境。

当前功能审计与 PBA/dedup 机制图见 [docs/onyx-functional-audit.md](docs/onyx-functional-audit.md) 和 [docs/onyx-mechanism-map.svg](docs/onyx-mechanism-map.svg)。

## 特性

- **内联压缩** &mdash; LZ4 / ZSTD，合并多块压缩单元以提高压缩比
- **内容寻址去重** &mdash; xxh3_64 指纹、首次出现的 miss 进 RAM 候选缓存（CandidateCache）、第二次命中走 LV3 字节比对验证后再 promote 到 dedup_index、cuckoo-filter L0、后台 DEDUP_SKIPPED 补扫 + cold-tail 扫描恢复重启后的去重率
- **Fragment 打包** &mdash; VDO 风格 bin-packing，多个 < 4KB 压缩 fragment 共享物理 slot
- **垃圾回收** &mdash; 后台 dead block 扫描与回写，带背压控制
- **崩溃一致性** &mdash; metadb 原子提交；写缓冲 sync 后才 ack
- **Zone 并发** &mdash; LBA 空间分区为多个 zone，每个 zone 由独立工作线程服务
- **ublk 前端**（仅 Linux）&mdash; 将卷暴露为 `/dev/ublkbN` 块设备，512B 扇区对齐

## 架构

```text
ublk (Linux) / stdin (macOS 开发)
  |
ZoneManager --> ZoneWorker x N（每 zone 单线程，crossbeam channel 调度）
  |
WriteBufferPool（LV2 上 O_DIRECT 环形日志，8KB slot，sync 后 ack）
  |  后台 BufferFlusher
  v
Dedup Workers --> Compress Workers --> Packer（bin-pack fragments）
  |
IoEngine（O_DIRECT --> LV3）+ MetaStore（metadb 原子提交）
  |
SpaceAllocator（BTreeSet 空闲链表，strip 对齐分配）
  |
dm-raid + LVM --> NVMe SSD x N
```

## 快速开始

### 前置依赖

- Rust 1.75+（2021 edition）
- 无外部元数据数据库依赖；onyx-metadb 随工作区构建
- Linux 6.0+ 用于 ublk 前端（macOS 通过 stdin 前端支持开发调试）

### 构建

```bash
cargo build --release
```

### 配置

编辑 `config/default.toml`：

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

[ublk]
nr_queues = 4
queue_depth = 128
```

### 使用

```bash
# 创建卷（1 GB，LZ4 压缩）
onyx-storage -c config/default.toml create-volume -n myvolume -s 1073741824 --compression lz4

# 列出卷
onyx-storage -c config/default.toml list-volumes

# 启动卷服务（通过 ublk 暴露块设备）
onyx-storage -c config/default.toml start -v myvolume

# 删除卷
onyx-storage -c config/default.toml delete-volume -n myvolume
```

## 设计要点

### 写路径

1. 用户 I/O 到达 ZoneWorker
2. 原始数据（未压缩）追加到 WriteBufferPool &rarr; sync &rarr; **ack 返回用户**
3. 后台 flusher 排空缓冲：合并连续 LBA &rarr; dedup workers（4KB xxh3_64 + dedup_index 查询 + 候选缓存查询 + LV3 批量 io_uring 字节验证）&rarr; 压缩合并单元 &rarr; Packer 打包 &rarr; O_DIRECT 写入 LV3
4. metadb 原子提交 L2P remap；verified hit 同事务 promote `(hash, blockmap)` 到 dedup_index

用户感知延迟 = 缓冲写入 + sync。压缩、去重、verify 全部不在热路径上。

### 读路径

1. 查内存缓冲索引（O(1) HashMap）&rarr; 命中则直接返回
2. 查 blockmap &rarr; IoEngine 读物理 slot（slot_offset 定位 packed fragment）
3. CRC32 校验 &rarr; 解压 &rarr; 按 offset_in_unit 提取 4KB

### 去重

- 4KB 是去重粒度（固定大小指纹）；压缩粒度远大于此（最大 128KB 合并单元）。
- **xxh3_64 指纹（8 字节）**，不是 crypto-strength 哈希。pair 碰撞率约 1.5e-8，所以**字节验证是正确性而不是优化**：每个候选 hit 都会通过 `ReadPool` 把原始 fragment 从 LV3 重新读回来，与新写入的源数据 byte-compare 之后才允许 dedup。
- **Promote-on-verified-hit**：首次出现的 miss **不**写 `dedup_index`，只进 sharded 的 RAM `CandidateCache`（默认每 shard ~1M slot 的 LRU）。第二次见到同一指纹、且 LV3 字节验证通过之后，才在 LBA remap 同事务里把 `(hash, blockmap)` promote 到 `dedup_index`（`atomic_batch_dedup_hits_with_promote`）。只出现一次的块在 dedup 元数据上**零写入**。
- **两级 L0**：`dedup_index` 查询走 L1 hot cache &rarr; cuckoo filter（16-bit fp, 4 slots/bucket, packed u64）&rarr; on-disk cuckoo。filter 让 cold miss 不必触盘；FPR 约 0.006%，饱和后无损降级（contains 永远返回 true）。
- **Cold-tail 扫描**（在 `DedupScanner` 内）：每 cycle 用 per-volume LBA 游标扫一段 live blockmap，通过 `ReadPool` 批量读 LV3，xxh3 后插入候选缓存。这能在进程重启后（candidate 是 RAM-only）和长跑场景下恢复 dedup 率。`dedup.cold_tail_max_per_cycle` 控制 cycle 预算。
- per-shard buffer > 90% 时前台 dedup 跳过、标记 `DEDUP_SKIPPED`；同一个 scanner 后续把这些块拉出来 hash 后塞进候选缓存（仍**不**写 dedup_index，promote 永远靠 verified second sighting）。
- dedup index 清理不再依赖持久 reverse 表：post-commit cleanup 会先清除指向 dead PBA 的 candidate-cache 条目，再把 committed PBA 放入 retired 集合；持久 forward index 由 verify mismatch 的 compare-put repair、后台 scrub、orphan reclaim/demote 维护。retired PBA 只有在 GC 确认 `refcount == 0`、hazard 清空、folded L2P 与 l2p_buffer 都无引用后才会真正回到 allocator。
- `dedup_shards`（默认 8）驱动 metadb 内每 shard 的 apply lane；候选缓存复用同一套 shard 路由，hit 与 promote commit 永远落在同一个 metadb shard。

### 垃圾回收

- 后台扫描识别 dead block 比例高的压缩单元（默认阈值 25%）
- 回写器提取有效块，通过缓冲重新写入（复用正常写路径）
- 旧 committed PBA 先进入 retired；GC reclaim 再检查 refcount、等待 hazard、扫描 blockmap/l2p_buffer，无引用后才释放空间
- 背压：缓冲利用率超过 80% 时暂停 GC

## metadb 元数据表

全局元数据（固定）：

| 表              | Key                          | Value               | 用途                    |
|-----------------|------------------------------|---------------------|-------------------------|
| `volumes`       | `VolumeOrdinal`              | manifest entry      | 卷注册表（ordinal 缓存） |
| `refcount`      | `pba(BE)`                    | `u32` 计数          | 物理块引用计数          |
| `dedup_index`   | `Hash8` (xxh3_64)            | DedupEntry(27B)     | 内容哈希 &rarr; PBA（cuckoo + filter L0 + L1 hot cache）|

Per-volume blockmap（每个 volume 一个命名空间）：

| 表                     | Key       | Value            | 用途                  |
|------------------------|-----------|------------------|-----------------------|
| `blockmap:{volume_id}` | `lba(BE)` | 28B BlockmapValue| LBA &rarr; PBA 映射   |

每个 volume 独立 L2P 命名空间。删卷会删除该命名空间并让 dead PBA 进入 cleanup/retire 路径；持久 dedup_index 由 repair、scrub 和 orphan reclaim 维护，真正释放空间统一走 retired PBA 的 GC confirm scan。

## 演进路线

- [x] MVP：ublk + metadb + 压缩 + 空间管理
- [x] Packer + GC：fragment bin-packing、GC 扫描/回写、背压控制、hole map 复用
- [x] 去重：工作线程池、dedup_index、分级跳过策略、DEDUP_SKIPPED 补扫、RAM 候选缓存 + LV3 字节验证 promote、candidate-before-retire cleanup、scrub/orphan 维护、cold-tail blockmap 扫描、cuckoo-filter L0
- [ ] RAID 感知：strip 对齐写出、strip 粒度分配
- [ ] 生产化：iSCSI 前端、HA（双控 active-standby）、Prometheus 监控
- [ ] 高性能：NVMe-oF over RDMA

## 许可证

基于 GNU Affero 通用公共许可证 v3.0 发布。详见 [LICENSE](LICENSE)。

---

[English](README.md)
