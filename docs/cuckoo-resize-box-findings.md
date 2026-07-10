# Cuckoo dedup 在线扩容 —— 上机验证问题汇总（2026-07-10）

## 背景

metadb 的 on-disk cuckoo dedup 索引，其桶数（模数）原先在 `chunklet-init` 时定死、无法在线改。
本次工作让模数**随真实 unique-hash 工作集在线翻倍、零停机**（两表增量迁移方案）。代码此前已过全部
自动化测试（metadb 单元/集成 + 12× 并发迁移压力测试），进入**上机（nvme-box，10×NVMe，纯 chunklet
后端）验收**阶段。

上机验证挖出了 **3 个自动化测试没覆盖的真实 bug** + 1 个迁移写放大问题，并澄清了一个**与本功能无关
的预存性能问题**。以下逐一说明。

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

## fio 冻结 —— 定性为 onyx 预存的 checkpoint stall，**与本次 cuckoo resize 无关**

**现象**：上机 fio 出现秒级（实测 2–5s）写吞吐归零的冻结。

**关键定位**：这些冻结在 **cuckoo 模数已到上限、无任何迁移在跑的 steady 状态下照样发生**；同时
`metadb_flush`（checkpoint）实测最长 **22.9 秒**、持 `apply_gate.write` 最长 **4.3 秒** —— 即
checkpoint 阶段把整条提交管道挡住，导致前台 append 冻结。该 stall 由**近满卷（253GiB/256GiB）的
超大 L2P**驱动，与 cuckoo dedup 索引扩容正交。

**结论**：fio 冻结是 onyx 写管道**已知的预存问题**（大 L2P 上的 checkpoint / 4-shard 并发提交
stall），迁移只是恰好同时在跑。本次 cuckoo resize 的三个 bug 修复与批量化均正确且不引入该冻结。
建议作为**独立课题**单独立项排查（大 L2P flush 为何长持 apply_gate.write 数秒）。

> 备注（诚实记录）：排查初期曾因监控脚本的单位 bug（把计数字段当字节、整除后恒为 0）+ 3 秒平均，
> 一度误判为"迁移导致的持续冻结"。用正确解析 + 1 秒粒度 + steady 状态对照后，才定性为预存 checkpoint
> stall。冻结本身真实，但根因不在本次扩容工作。

---

## 总体状态

| 项 | 状态 |
|----|------|
| Bug A / B / C | 已修复 + 上机验证 + 回归测试 |
| Issue D（迁移批量化） | 已修复 + 回归测试，迁移提速 ~2.7× |
| 在线扩容功能本身 | **正确**：阶梯扩容、4100 万次提交零错误、0 CRC、0 dedup 校验失配、无数据丢失 |
| 门禁 | metadb 库测 733/0、onyx 库测 311/0 全绿 |
| fio 冻结 | 预存 onyx checkpoint stall，与本功能无关，单独立项 |
| 待办 | 多小时 metadb-soak（参照模型 oracle + 重启 + fault 注入）通过后提交 v25 |

**一句话**：上机验收挖出并修掉了 3 个单元/压力测试没覆盖的真实 bug（其中 Bug A 是会失败前台提交、
可能卡恢复的 P0），在线扩容功能已验证正确；观察到的 fio 冻结经定位是 onyx 预存的大-L2P checkpoint
stall，与本次扩容无关。
