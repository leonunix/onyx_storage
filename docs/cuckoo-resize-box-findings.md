# Cuckoo dedup 在线扩容 —— 上机验证问题汇总（2026-07-10）

## 背景

metadb 的 on-disk cuckoo dedup 索引，其桶数（模数）原先在 `chunklet-init` 时定死、无法在线改。
本次工作让模数**随真实 unique-hash 工作集在线翻倍、零停机**（两表增量迁移方案）。代码此前已过全部
自动化测试（metadb 单元/集成 + 12× 并发迁移压力测试），进入**上机（nvme-box，10×NVMe，纯 chunklet
后端）验收**阶段。

上机验证挖出了 **3 个自动化测试没覆盖的 resize bug** + 1 个迁移写放大问题，并进一步定位出两个
**与本功能无关的 onyx LV2 ring 生命周期 bug**。以下逐一说明。

---

## Bug A（P0）—— 饱和在恢复/staged 提交路径上抛硬错，会失败提交、可能卡死恢复

**现象**：低 dedup 大流量下，cuckoo 表在下一次扩容触发前被填到接近满（load≈0.99），
verified-hit promote 的提交撞 `MAX_CUCKOO_CHAIN=64` 抛 `Corruption`，导致**整条 metadb 提交失败**
（该提交同时携带 L2P 映射）。实测 `commit_errors` 从 10 一路涨到 1097。

**根因**：早先的"饱和降级为丢弃、不失败提交"的兜底**只修了 lane 提交路径**，漏了
**bare/staged 提交路径**（`apply_op_bare → apply_dedup_put_with_rc → stage_put → 硬错版 put`）。
而 onyx 的 promote（`atomic_batch_dedup_hits_with_promote`）走的正是 staged 路径（小批 <8 op 走
bare apply）。更严重的是**这条路径也是 WAL 回放恢复路径** —— 一个饱和的 DedupPut 若进了 WAL，
回放会反复撞同一个硬错，有卡死恢复的风险。

**修复**：bare/staged/回放路径的 dedup put 改为**饱和即丢弃**（返回 placed=false、跳过 rc、计数），
与 lane 路径完全一致。丢弃一次 promote = 未来一次 dedup miss，不碰正确性、不失败提交、不卡恢复。

**验证**：上机 `commit_errors=0`（4100 万次提交零错误）；新增回归测试覆盖 staged 路径饱和 + 回放不卡死。

---

## Bug B（P1，确定性、每次迁移必现）—— 迁移完成时 OLD 表整批页泄漏

**现象**：每次扩容 swap 完成，日志都报 `page_store: duplicate free of page N in one batch`，
`dedup_resize_finish` 释放 OLD 表页失败（错误被吞成 WARN）→ **OLD 表的页永久不回收**（onyx 出于开机
速度关掉了开池时的孤儿回收）→ high_water 只涨不降。

**根因**：`CuckooHash::referenced_page_ids()` 把 meta head 页**算了两次**（显式 push 一次，又
extend 了 `meta_chain`，而 `meta_chain[0]` 恒等于 head）。该列表喂给 `free_many` 时，批内出现重复
页 id → 整批被拒 → OLD 表页全部泄漏。

**关联**：这正是此前观察到的"metadb high_water 只涨不回收"现象的一个成因。

**修复**：`referenced_page_ids` 去掉重复的 head push（meta_chain 已含 head，不变量由 read_chain 测试
保证）。**验证**：上机 0 条 duplicate-free、swap 干净、OLD 页正常回收；新增回归测试断言该列表无重复页。

---

## Issue C（P1，可用性）—— begin_grow 在提交门下重建 L0，卡住整条提交管道数秒

**现象**：每次扩容开始时，单次 apply 停顿高达 **3.4 秒**，前台 IO 冻结。

**根因**：`begin_grow` 在持有 `apply_gate.write`（+ 索引状态写锁）的临界区内，遍历整个即将成为 OLD
的表来重建 L0 布隆过滤器。表越大（~百万条目）遍历越久，期间所有提交被挡住。

**修复**：begin_grow 不再遍历重建 —— 保留原有 sketch（cuckoo filter 无假阴性，撑满后降级为
pass-through，读仍正确），把 L0 重建挪到**释放 apply_gate 之后**的 off-gate 步骤执行。
**验证**：上机 apply 停顿从 3.4s 降到 168ms；新增回归测试覆盖三种子状态读正确性。

---

## Issue D（迁移写放大）—— 迁移逐条目重写整页，形成对 meta LD 的写风暴

**现象**：迁移大表时对 chunklet meta LD 产生巨量同步小写（perf 实锤 scanner 大量时间在
`io_uring → io_write → direct_IO`），迁移单次耗时数分钟并加重前台竞争。

**根因**：`migrate_page_into` 逐 slot 调 `put_if_absent`，每个条目用新模数 rehash 落到不同 NEW 页，
且 cuckoo 页是 eager 写 → 迁移一个 OLD 页最多触发 112 次 NEW 页同步写。

**修复**：新增批量原语 `put_if_absent_many_grouped` —— 一个 OLD 页内的条目按目标页分组、一次
`write_sealed_page_runs` 合并提交（保持 OLD-before-NEW 锁序以守住 delete-vs-copy 竞态安全）。
**验证**：同尺寸迁移耗时 ~381s → ~142s（**~2.7×**）；新增回归测试 + 既有并发迁移压力测试全过。

---

## fio 冻结 —— LV2 ring 物理占用被隐藏，**与本次 cuckoo resize 无关**

**现象**：上机 fio 出现秒级（实测 2–5s）写吞吐归零的冻结。

**第一层定位（仍成立）**：冻结在 **cuckoo 模数已到上限、无迁移运行的 steady 状态下照样发生**；
`metadb_flush`（checkpoint）实测最长 **22.9 秒**、持 `apply_gate.write` 最长 **4.3 秒**。因此它与
cuckoo resize 正交，checkpoint 延迟也确实会放大写路径尾延迟。

**进一步定位（修正原结论）**：`buffer_pending_entries` / `buffer_fill_pct` 表示的是尚未 apply 的
**逻辑工作量**，不是 LV2 ring 的**物理占用**。entry apply 完成后会从 pending/index 消失，但在对应
metadb checkpoint durable 之前仍不能推进 ring head、仍占着 slot。因此观察到 `pending=51` 甚至
`work=0%` 时，某个 shard 的 ring 实际可能已经接近满；随后少量 append 就会进入硬 backpressure。
原 throttle 同样读取逻辑 fill，因而无法提前保护真正承压的 shard。

**另一个确定性 bug**：正常 shutdown 只推进了内存 ring head，checkpoint 原先依赖
`WriteBufferPool::Drop` 持久化。服务停止时仍有 `Arc` owner，可能先把 LV3 标记 clean 并退出，导致下次
启动从旧 checkpoint 重放已经 apply 的 entry。实测一次 clean restart 错误恢复了 **224611** 条 entry。

**修复**：

- 新增 `buffer_physical_fill_pct`（各 shard 物理 fill 的最大值），TUI 同时显示 `work` 与 `ring`。
- soft throttle 改为按目标 shard 的物理 fill 独立节流，避免一个热点 shard 串行化所有 producer。
- 修复 throttle 的陈旧时间戳和陈旧高水位缓存，避免重复 sleep 及 ring 已清空后的幽灵限流。
- shutdown 在写 clean LV3 superblock 前显式持久化所有 LV2 shard checkpoint 并 flush；失败则不写 clean 标记。

**上机验证**：修复后重启时 16/16 shard 均 `pending=0` 且 `head==tail`，没有后台 replay。最终 90 秒
fio（8 jobs、iodepth=16、4K randrw）期间物理 ring 采样峰值 20%，结束回到 0%；
`backpressure_events=0`、`throttle_count=0`，没有再出现 2–5 秒冻结。读/写 p99 为 **6.72/6.85ms**，
最大延迟为 **54/33ms**。剩余毫秒级尾延迟仍需沿线程调度和大 L2P checkpoint 分开优化。

**结论**：本次冻结与 cuckoo resize 无关这一点不变，但原先“仅由大 L2P checkpoint 长持
`apply_gate.write` 直接造成”的定性不完整。`pending` 与物理 ring 占用混淆、throttle 取错压力信号，
才解释了“pending 很低却几秒内突然背压”；shutdown checkpoint 漏持久化还会在重启后额外制造陈旧工作量。

> 备注（诚实记录）：排查初期曾因监控脚本的单位 bug（把计数字段当字节、整除后恒为 0）+ 3 秒平均，
> 一度误判为"迁移导致的持续冻结"。用正确解析 + 1 秒粒度 + steady 状态对照后，才定性为预存 checkpoint
> stall。冻结本身真实，根因不在本次扩容工作；后续 ring 指标和重启对照补齐后，才得到上述完整定性。

---

## Checkpoint / 调度尾延迟继续拆解

ring 生命周期修复后，又对 steady 状态下剩余的毫秒级尾延迟做了阶段级计时和同参数 A/B。这里的所有
`backpressure` / `throttle` 数字均为测试窗口前后累计计数的 **delta**；`buffer_fill_pct` 仍只表示逻辑
work，物理压力只看 `buffer_physical_fill_pct`。

### 锁边界和阶段计时结论

checkpoint 现在分别记录：sample 锁等待、L2P/page walk、dirty snapshot、L2P fold 的 plan/tree wait/
apply/publish/finish、page seal/write、fsync、manifest stage、`apply_gate.write` 等待/持有、最终 4K publish
以及 publish 后 cleanup。实测确认：

- dirty L2P snapshot 通过 freeze/swap 取得；prefold、page 构建、page write、manifest stage 均在 gate 外。
- checkpoint publish 先在 gate 外等待 PageStore IO barrier，再取得 `apply_gate.write`；gate 内只做最终
  manifest 4K publish + fsync，不做全量 L2P 扫描、大批复制或普通 page write。
- 被替换 manifest/catalog 页的遍历和 `free_many` 已移到 gate 释放之后。V8 后 publish/gate 单次峰值由
  约 142ms 降到约 19ms；V12 正式 90 秒窗口 gate 等待累计仅 **3us**，持有累计 **44.1ms**。
- V12 正式窗口的主要 gate 外成本是 L2P fold **21.61s**（其中 plan 2.56s、tree wait 0.09s、
  apply 18.11s、publish 2.09s）和 page write **34.19s**。这说明剩余结构成本是随机 L2P leaf COW/
  page 构建和写回，不再是 write gate 临界区。

### 调度和并行 drain 反证

对 `metadb-bfg-sync`、`metadb-l2p-prefold`、async reclaim 等线程采集了 `sched_switch`、off-CPU、
runqueue delay 和 CPU profile。典型 runqueue delay 是微秒级：诊断窗口中 bfg-sync CPU 37.95s/
runqueue 91.6ms、prefold 15.30s/28.4ms、async reclaim 40.26s/54.2ms。没有证据支持同 NUMA 节点
CPU 争用造成 0.6–10ms 抖动；profile 也没有单一锁热点，主要是 page COW、HashMap、memmove、CRC、
TLB shootdown 和 IO 的组合。

`parallel_l2p_drain` 不是未经验证的建议。旧的全 shard fan-out 已知会放大竞争，本轮又专门试了有界
2-worker 版本（V10）：60 秒窗口虽然吞吐上升，但 physical ring 峰值 **97%**、pending **575843**、
`throttle +634563`、throttle wait **+306.4s**，write p99 **16.91ms**。因此该实验已回退，当前
`parallel_l2p_drain_enabled=false`；它不能解决这个结构问题。

### async reclaim 的 O(n) 放大和 V12 修复

profile 进一步找到一个独立但会与 fold 争抢内存带宽的放大项：async reclaim 每处理约 4096 页就对
累计数百万页的 `free_list` 做 `sort_unstable + dedup`，随后尾部截断又线性扫描并再次排序。因而一次
checkpoint 产生的 reclaim 成本接近 `O(reclaim_cycles * total_free_pages)`。

V12 把 PageStore free list 改为无序 LIFO stack，并在同一把 metadata mutex 下增量维护 authoritative
free bitmap：普通单页/批量分配继续 O(1) pop，reclaim 只检查 bitmap 后 append；只有少见的
`allocate_run` 才按需排序，只有真的截断 free tail 才线性过滤一次。失败的 Free-stamp IO 会清除
`in_flight` 并重试，不会提前把页加入 free stack。

V11 profile 窗口处理 157.4 万页耗时 51.96s（约 33.0us/page）；V12 warm 窗口处理 214.4 万页耗时
27.19s（约 12.7us/page），单位页成本下降约 **2.6x**。V12 正式窗口处理 335.5 万页耗时 43.47s
（约 13.0us/page），改进保持稳定。

### 同参数 fio A/B

统一参数：`/dev/ublkb1`、4K `randrw`、30% read、8 jobs、iodepth 16、200G working set、90 秒，
相同 dedupe/compress 数据模型。V12 正式运行在 nvme-box UTC 14:28:50–14:30:22，engine PID 1667756、
fio PID 1670515。

| 90s run | read p50 / p99 / p99.9 / max | write p50 / p99 / p99.9 / max | physical peak | throttle delta |
|---------|-------------------------------|--------------------------------|---------------|----------------|
| baseline | 0.946 / 7.832 / 19.268 / 144.690ms | 1.044 / 7.897 / 18.481 / 122.867ms | - | - |
| V9（仅 fold 拆分） | 1.106 / 9.110 / 39.059 / 620.483ms | 1.286 / 9.896 / 40.108 / 476.508ms | 96% | +586112 |
| V12（free stack） | 0.971 / 7.111 / 9.634 / 43.103ms | 1.073 / 7.176 / 9.109 / 27.456ms | 12% | 0 |

V12 相对 baseline 的 p99 约下降 9%，p99.9 约下降 50%，max 下降 70–78%；总 IO 数 373.1 万，
相对 baseline 381.2 万低约 2.1%，没有用吞吐骤降换尾延迟。结束后 ring 排空回 0%，日志无 error。
它解决了本轮可定位的 reclaim 放大和 gate 边界问题，但不会消除随机大 L2P 的根本写放大；长期结构解
仍是 persistent L2P delta runs/LSM overlay + 多 generation merge，而不是重新打开并行 shard drain。

---

## 总体状态

| 项 | 状态 |
|----|------|
| Bug A / B / C | 已修复 + 上机验证 + 回归测试 |
| Issue D（迁移批量化） | 已修复 + 回归测试，迁移提速 ~2.7× |
| 在线扩容功能本身 | **正确**：阶梯扩容、4100 万次提交零错误、0 CRC、0 dedup 校验失配、无数据丢失 |
| 门禁 | ring 修复前完整 `cargo test`：675 passed、0 failed、5 ignored；checkpoint 改动的完整门禁待本轮收尾更新 |
| fio 冻结 | LV2 ring 生命周期和 checkpoint gate 边界已修复；V12 90 秒无背压/节流，max 43/27ms |
| 待办 | persistent L2P delta/merge 设计；多小时 metadb-soak（参照模型 oracle + 重启 + fault 注入） |

**一句话**：上机验收挖出并修掉了 3 个 resize bug（其中 Bug A 是会失败前台提交、可能卡恢复的 P0）、
1 个迁移写放大问题、2 个 onyx LV2 ring 生命周期 bug，以及 checkpoint/reclaim 的结构性放大；在线扩容
功能已验证正确，fio 秒级冻结与扩容无关，V12 正式对照中无背压/节流且尾延迟明显收敛。
