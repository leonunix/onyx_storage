# Onyx NUMA 感知与隔离设计

> 状态:设计稿(2026-06-11),待另一 session 实施。
> 目标读者:实施 session 的 Claude / 开发者。本文给出可直接开工的模块拆分、
> 配置 schema、分阶段退出判据。背景数据与教训全部来自 nvme-box 实测。

## 1. 背景:为什么要做

nvme-box(2× Xeon Gold 5318Y,96 逻辑核,**node0=偶数核 0-94,node1=奇数核 1-95,
交错编号**)上的既有结论:

1. **跨 socket 散布是单卷吞吐 bimodal(113-578 MiB/s 抖动)的根因**。所有
   `*_cpus` 配置写的是连续区间,在交错编号下每个角色都被 50/50 撕到两个 socket;
   大共享 buffer 的 first-touch 落在随机 node。单节点 confine
   (`numactl --cpunodebind=0 --membind=0` + `threading.enabled=false`)后稳定
   ~1080 MiB/s(3/3,~1% stdev),用了一半的核。
2. **单节点 confine 同时是单卷天花板**:dedup drainer、write thread、flusher 在
   一个 node 的 48 个逻辑核里互抢 CPU(async-drainer A/B 中 drainer 偷走前台 20%
   吞吐即为证据)。要往 2 GB/s 走必须把第二个 socket 用起来,但必须是**分区**而
   不是**散布**。
3. **ublk 队列线程会逃逸 numactl**:libublk 按内核建议的 per-queue 亲和性自行
   sched_setaffinity(`threading.enabled=false` 时 onyx 不覆盖),实测逃到 node1
   且每线程 ~86% CPU。2026-06-11 的 fio-pinning A/B 证明:把负载发生器钉到 node1
   会与这些逃逸线程同核竞争,前端 p99 16ms→135ms。
4. **隔离缺失**:soak 期间 node0 被打到 <10% idle,SSH/交互 shell 全卡。需要给
   OS/运维保留核,并把这套布局变成引擎自管,而不是依赖外部 numactl + 手写核表。

设计目标按优先级:

- **T1(正确分区)**:双 socket 同时工作、零跨 socket 散布,单卷吞吐
  显著超过单节点 1080 MiB/s 基线(目标 ~1.8-2 GB/s,受盘上限约束)。
- **T2(自管与可移植)**:引擎启动时自己发现拓扑、自己算布局,不再要求用户跑
  numactl、不再手抄 96 核的核号表;单 socket 机器/虚拟机自动退化为 no-op。
- **T3(隔离)**:OS/交互保留核;明确 IRQ 与负载发生器的摆放纪律(运维文档)。

## 2. 现状盘点(实施前请校对 file:line,代码会漂移)

### 2.1 线程清单(与 shard 数的缩放关系)

| 角色 | 数量公式(默认) | 现绑定 ThreadRole | spawn 位置 |
|------|------------------|--------------------|------------|
| ublk queue worker | `ublk.nr_queues × queue_workers`(32×4=128) | `Ublk`, ordinal=qid+worker | `src/frontend/ublk.rs:392` |
| buffer sync(LV2 ring fsync) | `buffer.shards`(16) | `BufferSync`, ordinal=shard | `src/buffer/commit_log/pool/open.rs:359` |
| flusher coalesce | `buffer.shards` | `FlusherCoalesce` | `src/buffer/flush/runtime.rs:185` |
| flusher dedup | `shards × dedup.workers`(32) | `FlusherDedup` | `runtime.rs:220` |
| flusher compress | `shards × compress_workers`(32) | `FlusherCompress` | `runtime.rs:259` |
| flusher writer(LV3 写+发起 commit) | `buffer.shards` | `FlusherWriter` | `runtime.rs:290` |
| flusher cleanup / post-commit | `shards` + 16 | `FlusherCleanup` | `runtime.rs:338/422` |
| commit worker | **固定 16**,`hash(vol_id)%16` 路由 | `CommitWorker` | `runtime.rs:379` |
| read pool(LV3 读) | `storage.read_pool_workers`(16),每 worker 独立 ring | `ReadPool` | `src/io/read_pool.rs:144` |
| GC runner / dedup scanner | 各 1 | `Background` | `src/gc/runner.rs:100`, `src/dedup/scanner.rs:116` |
| metadb apply lane | `16 shards × 4 lane 类`=64 | `L2pApply/RefcountApply/DedupApply` | `metadb/src/db.rs:574` |
| metadb txg-sync / quiesce | 各 1 | `TxgSync` ordinal 0/1 | `metadb/src/db/txg_sync.rs:203` |
| metadb WAL writer | 1 | `Wal` | wal/ |
| metadb dedup drainer | `dedup_shards`(8) | `DedupDrainer` | `metadb/src/dedup/index.rs:524` |
| metadb io-submitter / writeback / async-reclaim / lineage-gc / page-read | 1+1+1+1+N | 部分无绑定 | 各模块 |

关键事实:

- **单卷场景 commit worker 退化为单线程**(`hash(vol_id)%16` → 一个 worker 扛全部
  commit),它和 metadb apply/WAL 的 cache-line 往返是热路径(见
  `src/affinity.rs:15-21` 注释:跨 socket 每 commit 多 ~0.5-1ms)。
- 写路径上 ublk worker **直接**做 append(zone 是虚拟路由),append 后 park 在
  shard 的 `lv2_durability` waiter 上;buffer shard 由 LBA 决定 → **任意 ublk
  线程会写任意 shard,跨 node append 不可消除**(只能让 shard 之后的流水线全程
  node 内)。
- flusher writer 在绑核**之后**创建自己的 io_uring ring(`runtime.rs:294-308`),
  这是现存唯一的 first-touch-after-bind 实践,新设计要推广到所有 per-shard 内存。

### 2.2 大内存

| 对象 | 大小来源 | 粒度 | 现状 |
|------|----------|------|------|
| LV2 staging/pending(`buffer_payload_memory_limit` 32G) | buffer 配置 | per-shard | first-touch 随机 |
| metadb block cache(64G)+ index pin(8G) | `meta.block_cache_mb` | 全局 16 内部 shard | first-touch 随机 |
| metadb memtable/l2p_buffer(8G) | `meta.memtable_budget_mb` | per-shard | 随 apply lane |
| CandidateCache(~400MB-数GB) | `dedup.candidate_*` | 8 shard LRU,hash 路由 | 全局 |
| read pool / writer 的 io_uring + AlignedBuf | 各自 | per-worker | writer 已 local |

### 2.3 现有机制的四个缺陷(设计要逐一消除)

1. `CpuSet::parse` 接受连续区间 + `pick(ordinal)=cpus[ordinal%len]` 轮转 →
   交错编号下把同角色线程**均匀撕到两个 socket**(`src/affinity.rs:157`)。
2. `threading.enabled=false` 是"信任 numactl"模式,但 libublk 的 per-queue 绑定
   不受 numactl 管 → ublk 逃逸。
3. 配置是 18 个手写核号字符串,换机器/换核数全部重抄,且没人校验它们与 NUMA
   拓扑的关系。
4. 没有任何 mempolicy 控制:共享大缓存(block cache 64G)整块落在启动线程所在
   node,挤压该 node 的本地内存(`--membind=0` 下 144G 预算 vs 125G 节点容量的
   风险已在审计中指出)。

## 3. 设计

### 3.1 核心模型:NUMA pod(按 buffer shard 分区)+ home node(控制面)

```
                  node0 (pod 0)                      node1 (pod 1)
  ublk queues   qid 0..15  → 绑 node0 核          qid 16..31 → 绑 node1 核
  buffer shards shard 0..7                         shard 8..15
    └ 每 shard: sync + coalesce + dedup×2          (同构)
                + compress×2 + writer + cleanup
  metadb lanes  L2P/RC/Dedup lane 0..7             lane 8..15
  metadb dedup  drainer 0..3                       drainer 4..7
  read pool     worker 0..7                        worker 8..15

  home node = node0(可配):
    commit workers(16,单卷时实际 1 个热)
    metadb WAL writer、txg-sync、quiesce、io-submitter、
    manifest/checkpoint、writeback、async-reclaim、lineage-gc
    GC runner、dedup scanner、heartbeat、IPC
  reserve:每 node 预留 N 个物理核给 OS/IRQ/交互(默认 2)
```

原则:

- **数据面按 shard 分区**:一个 shard 的整条 lane(staging ring → coalesce →
  dedup → compress → writer → cleanup)+ 它对应的 metadb L2P/RC/Dedup apply lane
  全部钉在同一 node。shard→node 映射:`node = shard_idx * nodes / shards`
  (前半后半,不取模——避免奇偶交错把相邻 shard 拆开)。
- **控制面单例集中 home node**:WAL fsync 流、txg-sync fold、manifest swap 是全局
  串行点,拆不开;集中一处使"singleton 间"通信零跨界,代价是另一个 pod 的 lane
  与 home node 的通信跨 socket(channel send + WAL 提交),这是模型里**预算内**
  的跨界(消息级,不是 cache-line 级乒乓)。
- **不可消除的跨界只保留三类**:① ublk append 的 payload memcpy(写入哪个 shard
  由 LBA 决定);② 远端 pod → home node 的 commit/WAL 消息;③ 共享缓存
  (block cache、CandidateCache)的远端命中。其余一切角色间交互都应 node 内。

### 3.2 新模块 `src/numa.rs`:拓扑发现

```rust
pub struct NumaTopology {
    pub nodes: Vec<NumaNode>,          // 解析 /sys/devices/system/node/node*/cpulist
}
pub struct NumaNode {
    pub id: usize,
    pub cpus: Vec<usize>,              // 该 node 全部逻辑核
    pub cores: Vec<[usize; 2]>,        // HT 兄弟对(thread_siblings_list),
                                       // 分配时"先铺物理核、后用兄弟"
    pub mem_total_bytes: u64,          // node*/meminfo,用于内存预算校验
}
impl NumaTopology {
    pub fn detect() -> Self;           // 非 Linux / 单 node → 单 node 退化
}
```

- 不引入 `libnuma` 依赖,直接读 sysfs(与现有 nix/libc 风格一致)。
- 解析失败/容器内不可见 → 视为 1 个 node 包含全部在线核,所有 NUMA 逻辑退化为
  no-op(macOS 编译路径同样退化)。

### 3.3 配置 schema(新 `[numa]` 节,取代手写核表)

```toml
[numa]
# off    = 现状(threading 节继续生效,外部 numactl 自理)
# confine = 单 node 收纳:全部角色 + 内存绑到 numa.home_node(引擎内实现
#           numactl --cpunodebind --membind 的等价物,并且覆盖 ublk 逃逸)
# partition = 双(多)node pod 分区,即第 3.1 节模型
mode = "partition"
home_node = 0           # 控制面单例所在 node;confine 模式即收纳目标
data_nodes = [0, 1]     # 参与数据面分区的 node(默认 = 全部)
reserve_cores_per_node = 2   # 每 node 预留的物理核数(OS/IRQ/交互),
                             # 从核分配池中剔除,取每 node 编号最大的物理核
cold_cache_policy = "auto"   # Tier B 大缓存放置:auto(预算驱动,见 3.5)
                             # | home | interleave
allow_overcommit = false     # 内存计划表超出 node 容量时是否仍允许启动
```

兼容性规则:

- `numa.mode != "off"` 时,`[threading]` 的 `*_cpus` 字符串**忽略并打 warn**
  (避免两套机制叠加——这是 nvme-numa0.toml 头注释里那个坑的制度化)。
  保留 `threading.enabled` 作废弃别名:`enabled=true 且 numa 未配` 时维持旧行为。
- `mode="confine"` 取代"numactl + threading.enabled=false"姿势:好处是 ublk
  队列线程也被收进来(onyx 的 bind 在 libublk 之后执行,能覆盖),登录卡顿和
  逃逸问题一并解决;`config/nvme-numa0.toml` 迁移到 `mode="confine"`。

### 3.4 布局计算与绑定改造(`src/affinity.rs` 重构)

1. `AffinityLayout` 由 `NumaTopology + NumaConfig + 各角色线程数` **推导**,
   不再解析核号字符串。推导算法:
   - 每 node 取 `cpus - reserve`,按"物理核优先、HT 兄弟靠后"排序成分配池。
   - sharded 角色(表 2.1 中数量随 shards 缩放的):`bind(role, shard_idx)` →
     该 shard 所属 pod 的池,池内按角色固定次序圆排(同 shard 的 writer/sync
     等高 CPU 角色优先拿独立物理核,dedup/compress 可与兄弟共核)。
   - singleton 角色 → home node 池。
   - ublk:`qid → node = qid * nodes / nr_queues`,worker 绑该 node 池。
2. **绑定语义从"钉单核"放宽为"钉核集"**:`set_current_cpu(cpu)` 改为
   `set_current_cpuset(&[usize])`。除少数确需独核的角色(metadb WAL、txg-sync
   维持现状),其余角色绑到"本 pod 该角色的核子集"——保留调度弹性,避免
   128 个 ublk 线程 1:1 钉死导致的排队不均。这同时消除 parallel-drain v1 的
   "继承单核 pin"事故类别:scope.spawn 的子线程改为 `bind_pod(node)`(绑全
   pod)而不是 `unbind_current()`(绑全机)——drain worker 就近 fold 本 node
   的 shard。
3. metadb `AffinityConfig` 从 7 个核串改为传 `Vec<NodeCpus>` + `shard→node` 映射
   (onyx 在 `src/meta/backend/metadb.rs` 组装);metadb 内 `bind_current(role,
   shard_idx)` 据此选 node 池。metadb 独立使用(无 onyx)时该配置缺省 → 全部
   no-op,不破坏 standalone 测试。

### 3.5 内存放置:显式分层 + 启动期容量预算(本设计的核心难点)

**容量矛盾必须正面解决**:nvme-box 单 node 容量 ~125G,而引擎预算
block cache 64G + index pin 8G + memtable 8G + buffer payload 32G +
CandidateCache + cuckoo page cache + jemalloc/OS 开销 ≈ **144G+,单 node 放不下**。
单纯 "bind + first-touch" 在 confine 模式下结局只有两种:strict bind →
本地 direct-reclaim 风暴/OOM;preferred → **内核随机选哪些页溢出到远端**
(谁后 fault 谁过界,热 staging ring 可能在远端、冷 cache 页反而在本地)。
失控溢出比受控远端访问坏得多。所以原则是:**每一块大内存的 node 归属由引擎
显式决定,绝不把"过界"留给内核;放不下时由我们选"谁过界"(冷容量),
不是内核选。** partition 模式同时也是容量问题的解(预算摊到两 node,每边
~70G,不再过界)——这是双 socket 分区的第二动机,与吞吐并列。

#### 内存分层

| Tier | 内容 | 体量 | 放置策略 |
|------|------|------|----------|
| **A 热路径** | LV2 staging ring + `PendingEntry` payload 池、io_uring ring、AlignedBuf、metadb memtable / l2p_buffer / rc delta slot / per-shard PageBuf pool、index pin(8G) | ~50G | **严格 node-local**:在已绑核的 owner 线程内分配 first-touch;关键大段显式 `mbind(MPOL_BIND, pod_node)`。预算上优先保障 |
| **B 容量缓存** | metadb block cache(64G)、cuckoo page cache、CandidateCache | ~100G | **命中延迟不敏感**(远端命中 +~100ns,相对 4K 解码可忽略):partition 模式 `MPOL_INTERLEAVE(data_nodes)`;confine 模式按预算决定 home / interleave / 定向放远端 |
| **C 内核侧** | meta XFS 的 page cache、WAL 文件缓存 | 内核管 | 运维:`vm.zone_reclaim_mode=0`;Tier B interleave 后两 node 余量对称,内核自取 |

实现要点:

1. Tier A/B 的大段分配统一走显式 mmap 路径(AlignedBuf 已是),以便施加
   `mbind`;jemalloc 只对小对象负责,其 NUMA 局部性靠"线程已绑核 + first-touch"
   近似即可,不做 per-arena 控制。新增 `numa::bind_region(ptr,len,node)` /
   `numa::interleave_region(ptr,len,nodes)`(Linux-only,失败 warn 不 fail)。
2. **启动期内存计划表**:numa 模块汇总各 Tier 的配置预算,生成 per-node 计划
   (Tier A 本地需求 + Tier B 份额 + `reserve` + 安全余量 vs `node.mem_total`),
   INFO 打印整表;**超出即把 Tier B 策略降级**(home→interleave→偏远端)并
   WARN,仍不够则拒绝启动(可 `numa.allow_overcommit=true` 强行放行)。
   "过界"从运行时事故变成构造上不可能。
3. `mode="confine"` 的明确语义:**CPU 收纳单 node;内存只对 Tier A 严格
   home,Tier B 按计划表外溢**(`numa.cold_cache_policy = "auto"|"home"|
   "interleave"`,默认 auto=预算驱动)。不再无脑 `set_mempolicy(MPOL_BIND)`
   全进程——那正是 144G vs 125G 的事故源;进程默认策略用
   `MPOL_PREFERRED(home)`,大段再按 Tier 精确覆盖。
4. **per-shard first-touch 迁移点**(Tier A 的落地清单):LV2 staging ring 与
   recovery 重建按 shard 派发到本 pod 线程;metadb per-shard 结构在该 shard 的
   apply lane 线程内首次 touch;read pool / writer ring 已 in-thread,保持。
5. **CandidateCache**:hash 路由与 pod 无关,保持全局 + Tier B 策略。可选后续:
   按 pod 分桶(收益未证,Phase 2 之后再议)。
6. 验收手段:`numastat -p`、`/proc/<pid>/numa_maps` 抽查大段归属与计划表一致;
   长 soak 中 `numastat` 监控两 node free 不被单边耗尽。

### 3.6 ublk 前端与 IRQ(隔离)

- onyx 的 `bind_current(Ublk, ...)` 在 libublk 启动队列线程后执行,直接覆盖
  其内核建议亲和性 → 逃逸问题在 `mode!=off` 下自动消失。
- qid→node 对半分。注意:**ublk worker 与它要写的 buffer shard 没有相关性**
  (LBA 决定 shard),所以对半分的意义是"前端算力均匀 + 完成路径本地",不是
  消除 append 跨界;不要试图按 LBA 给 queue 分区(块层不保证 LBA→queue 映射)。
- 运维文档(不进引擎):NVMe/网卡 IRQ 用 irqbalance ban + 手工 steering 到各
  node 的 reserve 核;负载发生器(fio)摆放纪律——**绝不钉到与 ublk 队列线程
  同核**(2026-06-11 实测 p99 16ms→135ms),基准测试推荐 fio 占用 pod 内
  reserve 之外的指定核或独立机器。

### 3.7 明确不做的(本设计范围外)

- 不按 node 拆 WAL / manifest(架构级改动,先看 partition 后 WAL 是否成为新瓶颈)。
- 不做 zone→node 的 LBA 静态分区(前端到 shard 的跨界 memcpy 是预算内成本)。
- 不引入 libnuma / hwloc 依赖。
- 不动 metadb shard 数、dedup shard 数(manifest 固化,迁移成本高)。

## 4. 分阶段实施(每阶段独立可合、有退出判据)

### Phase 0:拓扑模块 + confine 模式(低风险,先吃掉运维痛点)— ✅ 已实现(2026-06-11)

实现与验证记录:`src/numa.rs`(拓扑 + 内存计划 + mempolicy helpers + confine
enforcer)、`src/config.rs` `[numa]` 节、`src/affinity.rs` WholeSet 布局、
`config/nvme-numa0.toml` 已切 `mode="confine"`。nvme-box 验证:无 numactl 12-min
soak 与 numactl 基线同负载对比 IOPS 持平(20k 限速双双打满)、p99/p999 反而更优
(16/31ms → 13/22ms)、crc/commit_err=0;内存计划表正确判定 Interleave
(Tier A 48G + Tier B 64.4G ×1.1 > 124.8G-4G);reserve 2 物理核(44/46/92/94)
正确剔除;**libublk 自钉逃逸由常驻 `numa-confine` enforcer 线程(5s 周期扫
`/proc/self/task`)拉回——实测 17 个 stray 线程全部重绑**(逃逸机制 = libublk
per-queue daemon 在我们 bind 之后自行 sched_setaffinity 内核建议掩码,继承+
单次覆盖都防不住,必须 sweep)。剩余:≥2h 长 soak 填满 block cache 验证
Tier B 受控外溢(`pgscan_direct` 不增长)。

1. `src/numa.rs` 拓扑发现 + 单测(伪 sysfs 目录注入)。
2. `[numa]` 配置解析 + 与 `[threading]` 的互斥/告警逻辑。
3. `mode="confine"`:全角色绑 home node 核集 + 进程默认 `MPOL_PREFERRED(home)`
   + 覆盖 ublk;**内存计划表 + Tier B `cold_cache_policy` 降级逻辑 +
   `numa::bind_region/interleave_region`** 一并落地(confine 在 nvme-box 上
   预算 144G > 125G,没有受控外溢就是带病上线);`nvme-numa0.toml` 切到此模式。
4. **退出判据**:box 上不再需要 numactl 前缀,12-min dedupe-compress soak 吞吐
   与 numactl 基线(~1080 MiB/s 档)持平 ±5%,`ps -o psr` 抽查无线程逃逸出
   node0(尤其 ublk-q*),crc=0;启动日志能看到完整内存计划表,且一轮**长 soak
   (≥2h,把 block cache 填满)**中 `numastat` 显示 Tier B 按计划外溢、node0
   free 不被耗尽、无 direct-reclaim 风暴(`/proc/vmstat` 的
   `pgscan_direct` 不持续增长)。

### Phase 1:partition 模式(数据面分区 + 控制面 home)

> **⚠ 实测修正(2026-06-11,六轮迭代,详见 memory
> `numa_phase1_iterations_and_rcauth_bug`)**:本节原设想的 pod-per-shard 全
> 分区被推翻。实测边界:md 共享设备写提交(fsync 64µs→334-540µs)、metadb
> LSN 串行 apply 链(跨界 +70µs/LSN → ~14k 排空天花板)、元数据内存
> interleave(us/remap 20→400-700µs 累积退化)、dedup worker(指针追逐 home
> 元数据)、远端读(5-8ms vs 1.65ms)——**前端+元数据+设备+dedup 构成不可
> 跨 socket 的延迟域**。最终落地模型 = home 延迟域 + **仅 compress 外移**
> 非 home pod(`affinity.rs PartitionTopo::pod_index`),内存与 confine 同款
> BIND(home)+缩缓存。该模型在 rate-capped 20k 下稳定 26min。另:此前各轮
> "5 分钟排空塌方"的真凶是 rc-neutral reclaim stall(配置漏开
> `rc_authoritative_reclaim`),非 NUMA;而 rc_authoritative=true 首次真负载
> 暴露 metadb invalid-page-magic 正确性 bug(P0,Phase 1 gate 的前置)。

1. `affinity.rs` 布局推导重构(3.4 节 1-2);metadb `AffinityConfig` 改 node 化
   (3.4 节 3)。
2. shard→pod 映射接入所有 spawn 点(表 2.1);commit worker / 单例归 home node。
3. per-shard first-touch 迁移(3.5 节 1)中 onyx 侧的两处(staging ring、
   recovery 重建)。
4. **退出判据**:
   - 单测/集成全绿;`mode="off"` 行为 bit-for-bit 不变(默认 off,本阶段不改默认)。
   - box partition vs confine A/B(各 ≥3 轮防 bimodal):partition 吞吐 >
     confine 基线 **+40%** 视为达标(2 GB/s 是盘上限内的理想值,不是 gate);
     任何一轮出现 confine 时代不存在的多秒级前台 p99 退化即视为散布回归,失败。
   - `mpstat` 中两 node busy 比例相称;`numastat -p` 显示 per-shard 大段各归其 pod。

### Phase 2:内存精修 + 残余角色

1. metadb per-shard 结构(memtable / l2p_buffer / rc delta / PageBuf pool)的
   first-touch 迁移到 apply lane 线程(3.5 节 4 的 metadb 部分;onyx 部分在 P1)。
2. read pool 按 pod 拆分 + 请求按提交者 node 就近路由;dedup drainer 4+4。
3. reserve 核与 IRQ 运维手册(docs/ 下新增 ops 段落)。
4. **退出判据**:`/proc/<pid>/numa_maps` 抽查与内存计划表一致;Tier B
   interleave 开/关 A/B 不出现读 p99 回归;长 soak 无单 node 内存耗尽。

### Phase 3:验证矩阵收口

- confine(node0)/ confine(node1)/ partition 三态各 3 轮 12-min soak +
  1 轮 2h soak,记录:IOPS、p99/p999、`commit_apply_max`、flush `lock_max`、
  buffer fill、crc/commit_errors(必须全 0)。
- 用 `.dev/schedstat_mon.sh`(box 已有)+ `mpstat` 旁路采样佐证无抢核;
  fio 钉核遵守 3.6 纪律。
- 通过后讨论 partition 是否设为 NUMA 多 node 机器的默认。

## 5. 风险与已知坑(实施时强制阅读)

| # | 坑 | 来源 | 对策 |
|---|----|------|------|
| 1 | 交错核编号:连续区间=跨 socket 散布 | bimodal 根因实测 | 布局只从拓扑推导,禁手写区间 |
| 2 | 子线程继承父线程单核 pin | parallel-drain v1 3-4× 回归 | 一律 `bind_pod`,禁 unbind-to-all/继承 |
| 3 | libublk 自行绑核无视 numactl | ublk 逃逸实测 | onyx 在其后覆盖绑定(Phase 0 即修) |
| 4 | fio 钉到 ublk 线程同核 | 2026-06-11 A/B,p99 8× | 运维纪律,不进引擎 |
| 5 | NUMA bimodality 污染 A/B | 历史上多个"neutral"误判 | 每个判决 ≥3 轮 + stdev 报告 |
| 6 | node 内存容量 < 配置预算(confine 必现) | 144G vs 125G,内核随机选页溢出远端 | **启动期内存计划表 + Tier 分层显式放置**(3.5):热路径严格本地,冷容量受控外溢;超预算降级/拒启 |
| 6b | **confine 进程策略用 PREFERRED = 85 分钟定时炸弹**(2026-06-11 p0-2h 实测) | WAL/XFS 页缓存 ~55G 持续填满 home node;PREFERRED 满后"溢出不回收"→ 之后所有热分配落远端 → flush 吞吐 20k→9k remap/s 塌方 → 32G payload 积压螺旋。`pgscan_direct` 全程平直即罪证 | confine 必须用 **MPOL_BIND**(= numactl --membind,8-12h soak 实证):满了就地回收冷页缓存,保热分配本地。PREFERRED 只用于 partition 的 per-pod 线程 |
| 7 | commit worker 单卷退化单线程 | `hash(vol)%16` | 归 home node 与 metadb 同侧;后续如证明是瓶颈,再议 per-shard commit 路由(范围外) |
| 8 | 远 pod→home WAL 跨界延迟 | 模型固有 | 预算内;Phase 1 A/B 中观测 `commit_*_wait`,若主导再立项 per-node WAL |
| 9 | 性能判据必须 release + 清盘 | CLAUDE.md | 同既有纪律;dedup schema 改动后 `rm -rf /mnt/onyx-meta/*` |

## 6. 与既有文档/记忆的关系

- 机制图:`docs/onyx-mechanism-map.svg`;功能审计:`docs/onyx-functional-audit.md`。
- 实测背景(memory 索引):`numa_root_cause_bimodal_confirmed`(bimodal 根因与
  Tier0/Tier1 提法)、`cross_numa_partition_todo`(用户指令)、
  `ublk_numa_scatter_and_lazy_compaction_soak`(逃逸)、
  `l2p_drain_spike_is_real_fio_ab`(fio 摆放纪律与采样方法)。
- 本设计即原 "Tier 1" 的落地稿;Phase 0 的 confine 模式同时取代 "Tier 0" 的
  numactl 姿势。
