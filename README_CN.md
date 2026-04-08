# Onyx 存储引擎

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Community Driven](https://img.shields.io/badge/Community-Driven-green.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

**用户态全闪块存储引擎，支持内联压缩、内容寻址去重和 RAID 感知空间管理。**

Onyx 是一个高性能块存储引擎，设计灵感来自 Red Hat VDO。使用 RocksDB 管理元数据，O_DIRECT 进行数据 I/O，通过 Linux ublk 对外暴露块设备。面向 dm-raid / LVM 之上的 NVMe SSD 阵列。

> **早期技术预览** &mdash; 本项目处于早期开发阶段，用于学习和研究存储引擎内部原理。核心功能（压缩、去重、GC、Packer）已实现并通过测试，但尚未达到生产级别，请勿用于生产环境。

## 特性

- **内联压缩** &mdash; LZ4 / ZSTD，合并多块压缩单元以提高压缩比
- **内容寻址去重** &mdash; SHA-256 指纹识别、引用计数、后台补扫跳过的块
- **Fragment 打包** &mdash; VDO 风格 bin-packing，多个 < 4KB 压缩 fragment 共享物理 slot
- **垃圾回收** &mdash; 后台 dead block 扫描与回写，带背压控制
- **崩溃一致性** &mdash; RocksDB WriteBatch 原子更新；写缓冲 sync 后才 ack
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
IoEngine（O_DIRECT --> LV3）+ MetaStore（RocksDB WriteBatch）
  |
SpaceAllocator（BTreeSet 空闲链表，strip 对齐分配）
  |
dm-raid + LVM --> NVMe SSD x N
```

## 快速开始

### 前置依赖

- Rust 1.75+（2021 edition）
- RocksDB 系统库（通过 `rocksdb` crate 自动构建）
- Linux 6.0+ 用于 ublk 前端（macOS 通过 stdin 前端支持开发调试）

### 构建

```bash
cargo build --release
```

### 配置

编辑 `config/default.toml`：

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
3. 后台 flusher 排空缓冲：合并连续 LBA &rarr; 去重（4KB SHA-256）&rarr; 压缩合并单元 &rarr; Packer 打包 &rarr; O_DIRECT 写入 LV3
4. RocksDB WriteBatch 原子更新 blockmap + refcount

用户感知延迟 = 缓冲写入 + sync。压缩和去重完全不在热路径上。

### 读路径

1. 查内存缓冲索引（O(1) HashMap）&rarr; 命中则直接返回
2. 查 blockmap &rarr; IoEngine 读物理 slot（slot_offset 定位 packed fragment）
3. CRC32 校验 &rarr; 解压 &rarr; 按 offset_in_unit 提取 4KB

### 去重

- 4KB 是去重粒度（固定大小指纹）；压缩粒度远大于此（最大 128KB 合并单元）
- 缓冲压力 > 90% 时跳过去重，标记 `DEDUP_SKIPPED`；后台 DedupScanner 补扫
- 去重索引清理是原子的：refcount 归零时，前缀扫描 dedup_reverse 清理过期条目，在同一 WriteBatch 中提交

### 垃圾回收

- 后台扫描识别 dead block 比例高的压缩单元（默认阈值 25%）
- 回写器提取有效块，通过缓冲重新写入（复用正常写路径）
- 旧 PBA refcount 自然归零 &rarr; 空间回收
- 背压：缓冲利用率超过 80% 时暂停 GC

## RocksDB Column Families

| CF              | Key                              | Value               | 用途                    |
|-----------------|----------------------------------|----------------------|-------------------------|
| `volumes`       | `vol-{id}`                       | bincode VolumeConfig | 卷注册表                |
| `blockmap`      | `vol_id_len + vol_id + lba(BE)`  | 28B BlockmapValue    | LBA &rarr; PBA 映射     |
| `refcount`      | `pba(BE)`                        | `count(BE)`          | 物理块引用计数          |
| `dedup_index`   | `sha256(32B)`                    | DedupEntry(27B)      | 内容哈希 &rarr; PBA     |
| `dedup_reverse` | `pba(BE) + sha256(32B)`          | empty                | 反向查找用于清理        |

## 演进路线

- [x] MVP：ublk + RocksDB + 压缩 + 空间管理
- [x] Packer + GC：fragment bin-packing、GC 扫描/回写、背压控制、hole map 复用
- [x] 去重：工作线程池、dedup_index/dedup_reverse、分级跳过策略、后台补扫
- [ ] RAID 感知：strip 对齐写出、strip 粒度分配
- [ ] 生产化：iSCSI 前端、HA（双控 active-standby）、Prometheus 监控
- [ ] 高性能：NVMe-oF over RDMA

## 许可证

基于 GNU Affero 通用公共许可证 v3.0 发布。详见 [LICENSE](LICENSE)。

---

[English](README.md)
