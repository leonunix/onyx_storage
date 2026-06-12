# Onyx 主仓库功能审计

审计范围：`src/` 主引擎代码、`metadb/` 对外语义、现有测试。未跟踪目录 `chunklet/`、`dashboard/` 未纳入主线结论。

配套机制图：[`docs/onyx-mechanism-map.svg`](./onyx-mechanism-map.svg)

## 总体判断

当前代码已经从 AGENTS.md 里的 RocksDB 设计演进成 `onyx_metadb` 设计：L2P、refcount、dedup index 都由 metadb 承担，写入热路径不再按每个 LBA 直接维护传统 refcount。PBA 回收主线分成两套语义，并有一个 lineage 专用例外：

- **事务失败/未提交新 PBA**：直接 `allocator.free_one/free_extent`。
- **已经进入元数据可见集的 PBA**：应先 `retire`，再由 GC 的 Gate 1/Gate 2 确认后 `reclaim`。
- **metadb Lineage GC 的 FreePbas**：这是 proof-carrying 例外，不是普通 committed PBA。
  metadb 只在 dead-list segment 已越过 snapshot / descendant branch pin，且 refcount ledger
  为 0 后发出；`apply_free_pbas()` 只 surface exclusive `rc==0`，或 shared 分支 decrement 后刚好
  归零的 PBA。这个来源可以 direct free，但不能推广给普通 cleanup。

所以本次审计里最需要收敛的不是“metadb 为什么会 surface 这些 PBA”——这部分设计是有
proof 的；真正要盯住的是 Onyx 消费端什么时候能把 proof 转换成 allocator 可复用空间。
“exclusive/final-zero”只证明 durable metadata 不再需要它，不自动证明 RAM candidate cache、
in-flight promote、replay 幂等都已经被处理。


## 当前主线

### 写入与 flush

1. LV2 buffer append + sync 后前台 ack。
2. flusher coalesce，dedup worker 做 hash/lookup/verify。
3. miss 进入 compress/packer/writer；hit 直接 metadb remap。
4. commit worker 负责 LV3 IO 已完成后的 metadb commit、candidate cache publish、post_commit mark_applied。

关键代码：

- `src/buffer/flush/stages/dedup.rs`
- `src/buffer/flush/writer/commit_worker/passthrough.rs`
- `src/buffer/flush/writer/commit_worker/packed.rs`
- `src/meta/backend/metadb.rs`

### PBA 生命周期

推荐把所有 PBA 都按下面状态机理解：

```text
Free
  -> AllocatedTransient      // allocator 分配，LV3 可能已写，但 L2P 未提交
  -> CommittedLive           // blockmap/dedup_index/lineage 可见
  -> Retired                 // 元数据认为可疑 dead，但仍不可复用
  -> ReclaimedFree           // rc==0 + blockmap confirm 无引用 + hazard clear

CommittedLive
  -> LineageGcFreePbasProven // metadb dead-list + snap/branch pin + rc ledger proof
  -> ReclaimedFree           // lineage 专用 direct-free 例外
```

直接 `free_one/free_extent` 默认只适合 `AllocatedTransient`。metadb `FreePbas` 携带
lineage GC proof，是 committed PBA direct-free 的唯一候选例外；但在 Onyx 侧仍需要满足
“释放前清候选缓存、等待 hazard、重复 FreePbas 幂等”这三个消费端条件。

当前主要路径：

- rollback、discarded、seq_guard 全拒绝的新分配 PBA 直接 free。
- remap/delete/dedup demote 产生的 dead PBA 走 cleanup `retire_dead_pbas()`。
- GC `reclaim_retired_extents()` 执行 Gate 1 refcount、hazard wait、Gate 2 blockmap/l2p_buffer confirm，再 reclaim。
- lineage drain 对 metadb `FreePbas` 直接 free；来源是 exclusive/final-zero proof 路径，
  但当前消费端没有走普通 cleanup 的 candidate-cache remove-before-retire，也没有集合式幂等。

### PBA 释放决策表

| 来源 | durable metadata 证明 | Onyx 当前动作 | 可复用前还需要什么 |
|---|---|---|---|
| LV3 写失败 / metadata commit 失败 / seq_guard 全拒绝 | 新 PBA 从未提交进 L2P/dedup/lineage | `allocator.free_*` | allocator overlap + hazard wait 足够 |
| 普通 overwrite/delete/dedup demote | old PBA 已不再是当前映射，但可能仍被 dedup/snapshot/l2p_buffer 引用 | `candidate.remove_by_pba()` → `allocator.retire_*` | GC Gate 1 rc==0、hazard barrier、Gate 2 folded L2P + l2p_buffer confirm |
| Dedup orphan/scrub demote | dedup_index 条目 stale/orphan，删除后 rc 可能归零 | `candidate.remove_by_pba()` → `allocator.retire_*` | 同上，`referenced_extents()` 是最终 authority |
| Lineage GC `FreePbas` | dead-list segment 已过 snapshot/descendant pin，rc ledger 为 0；`apply_free_pbas()` 只 surface exclusive 或 decrement-to-zero PBA | `allocator.free_*` | 当前只依赖 allocator hazard wait；缺 candidate-cache 清理和 replay 幂等语义 |

## Dedup 功能审计

### 正确性边界

Dedup 不是“hash 命中即共享”，当前热路径做了几层保护：

- `xxh3_64` 只作为快速 fingerprint，命中后通过 ReadPool byte-verify。
- candidate-cache promote 在 lookup 时 pin PBA，verify 时继续 pin，避免 promote 目标在提交前被回收。
- persistent dedup_index hit 通过 metadb `L2pRemap` guard 检查 target rc。
- candidate promote 因同一 tx 内 DedupPut 和 L2pRemap 分 lane 执行，remap 不带 rc guard，安全性依赖 candidate cache 的 remove-before-retire 和 hazard pin。

这套逻辑是能自洽的，但边界很窄，必须保留测试覆盖。

### 四个后台职责混在 `DedupScanner`

`DedupScanner` 现在同时做：

- DEDUP_SKIPPED 补扫。
- cold-tail warming/remap。
- dedup_index scrub。
- orphan reclaim/demote。

这些职责共享一个 loop 和一组配置门限，但安全属性不同：补扫影响 dedup ratio，scrub 修 stale index，orphan reclaim 会触发 PBA retire。建议拆成至少三个内部 worker/模块：`SkippedRescan`、`ColdTailWarm`、`DedupIndexMaintenance`，共享 scan cursor 和 budget 即可。

## 主要风险

### P1：lineage FreePbas 来源合法，但消费端 direct-free 还缺两个证明

位置：`src/engine/lineage.rs`

`LineageFreedPbaDrainHandle::drain_once()` 对 metadb 发出的 `FreePbas` 直接调用 allocator
`free_one/free_extent`。这不是“任意已提交 PBA 直接释放”：metadb Lineage GC 的 plan 阶段会确认
dead-list head segment 内记录不被 active snapshot pin、不被 descendant branch pin，并且 global
refcount ledger 为 0；`apply_free_pbas()` 也只 surface exclusive `rc==0`，或 shared 分支 decrement
后刚好归零的 PBA。

因此“FreePbas 只 free 独享/最终归零 PBA”这点成立。但 free PBA 是两段式证明：

1. **metadb 证明这个 PBA 在 durable lineage/refcount 语义上可释放。**
2. **Onyx 证明这个 PBA 可以马上回到 allocator free list。**

当前第 1 段比较完整，第 2 段还不够完整：

- 普通 cleanup 在释放前一定 `candidate.remove_by_pba(pba)`，防止 RAM candidate-cache 以后把已释放 PBA 交给 verify/promote。lineage drain 现在没有 candidate cache 句柄，直接 free，没有执行这步。
- `allocator.free_*` 会等待已有 hazard，并在等待后复查 free/retired overlap；但它不会像 GC retired reclaim 那样先等 hazard、再扫 folded L2P + l2p_buffer。lineage 依赖 metadb dead-list proof 替代 Gate 2，这可以接受，但需要明确“proof 覆盖的是 metadata，不覆盖 candidate-cache”。
- metadb 注释仍写 `FreePbas` duplicate surface 由 “onyx retire is a set” 保证幂等；实际 Onyx 现在 direct-free。需要区分两种重复：
  - **同一批 drain 内的重复**已经被 `coalesce_free_pbas_to_extents()` 的 `sorted.dedup()`（`src/meta/backend/metadb/values.rs`，附带 `coalesce_free_pbas_sorts_and_dedups_unsorted_input` 测试）吸收，不会到达 allocator。这条路径是安全的。
  - 真正缺的是**跨 drain 周期 / 跨 restart 的重复 surface**：第一批已经 free（甚至已被复用）后，第二批再 surface 同一 PBA，`dedup()` 覆盖不到，会落到 allocator overlap error + warn。通常不会错误复用，但语义不是注释说的“集合幂等”，最坏会让已 surface 但未消费的 PBA 等到 restart 才靠 allocator 从 metadata 重建 free list 时释放。

建议：

- 给 lineage drain 接入 `CandidateCache`，先 `remove_by_pba()` 再 free，补齐普通 cleanup 的缓存失效不变量。
- 把 direct-free 包成专用 API，例如 `free_lineage_gc_proven_pbas()` 或
  `reclaim_with_proof(LineageGcFreePbas)`；禁止普通业务路径直接调用 allocator `free_*`。
- 修正 metadb / Onyx 注释：如果保留 direct-free，就不要再说 “Onyx-side retire is a set”；要么消费端做已释放集合去重，要么把 duplicate FreePbas 视为可观测 warn 并测试它不会导致复用错误。
- 增加 crash/replay、snapshot/clone pin、candidate-cache stale/hazard 的端到端测试。这里不建议盲目把 lineage 全部改 retire；更好的修法是补齐 direct-free 消费端的 proof。

### P1：dedup 开启但 ReadPool 关闭会进入 trust-hash 模式

位置：`src/buffer/flush/stages/dedup.rs`

注释明确说 ReadPool 为 `None` 时不做 verify。由于当前 hash 是 `xxh3_64`，这不是可忽略风险。生产配置里如果 `storage.read_pool_workers=0` 且 dedup enabled，理论上 hash collision 会导致错误共享。

建议：dedup enabled 时强制 ReadPool > 0；或者把无 verify 模式改成显式危险配置。

### P1：dirty startup 的“refcount rebuild”是误导性 no-op

位置：`src/engine.rs`、`src/meta/store.rs`

dirty startup 分支打印 rebuilding refcount，但 `MetaStore::rebuild_refcount_from_blockmap()` 当前返回 default summary。若 metadb recovery 是唯一正确来源，日志和 API 应改名；若不是，当前缺少真实 rebuild。

建议：改成 `meta.recover_or_validate_refcount()`，并在日志里说明由 metadb WAL/TXG recovery 负责；不要显示 “rebuilt”。

### P2：cleanup retire 失败无重试

位置：`src/buffer/flush/cleanup.rs`

`retire_dead_pbas()` 失败只 warn（`cleanup.rs` 里注释直接写 `continuing without retry`），长运行会造成空间不可复用。注意这里的“重启修复”靠的是 **allocator 重启时从 metadata 重建 free list**，不是上面 P1 说的 refcount rebuild（那条是 no-op）；两者是不同的 rebuild，不要混为一谈。建议给 cleanup batch 加 retry/backoff 和 `pba_reclaim_stuck` 指标，不要把一致性押在重启上。

### P2：新旧 writer 路径并存造成认知噪音

`src/buffer/flush/writer/passthrough.rs`、`packed.rs` 与 `commit_worker/*` 并存。稳态热路径走 commit worker（`write_units_batch` / `write_packed_slots_batch` 发 `CommitJob` 给 worker），但旧路径**不是纯死代码或只剩测试痕迹**：`write_unit()` 仍是停机 drain（`handle_compressed_unit`，shutdown 时调用）的活路径，`write_packed_slot()` 还作为 commit_worker 通道为空时的 defensive fallback（`defer_retry`，防 seq 被 orphan）。`write_packed_slot()` 本身确实只剩测试在调。建议把这两条旧入口**显式标注成 shutdown-drain / degraded-fallback path**（而不是当 test-only 删掉），其余确认无引用的旧逻辑再用 cfg/test-only 收拢。

## 建议的拆分边界

1. 新建 `pba_lifecycle` 层，统一暴露：
   - `rollback_uncommitted(extent)`
   - `retire_committed(reason, extent)`
   - `free_lineage_gc_proven(extent)`
   - `confirm_and_reclaim(limit)`
   - 禁止业务路径直接调用 allocator `free_*`；lineage 专用 API 内部负责 candidate-cache 清理、hazard wait、重复 surface 处理。
2. Dedup 分层：
   - hot path: hash/lookup/verify/promote/remap。
   - index maintenance: scrub stale forward entry。
   - reclaim policy: orphan selector only，最终 free 仍由 PBA lifecycle 决定。
3. Metadata adapter 文档化：
   - `L2pRemap` 是否 bump rc。
   - `DedupPut/Delete` 对 rc 的含义。
   - `FreePbas` 的强契约：metadb proof 覆盖 snapshot/descendant/refcount，不覆盖 Onyx RAM candidate-cache。
4. 配置 guard：
   - `dedup.enabled && read_pool_workers == 0` 默认拒绝启动。
   - lineage direct-free 只接受 proof enum / 专用 API，不暴露成普通 cleanup 能调用的 allocator free。

## 建议补测

- lineage FreePbas 的 snapshot pin / descendant branch pin / refcount ledger proof 测试。
- lineage FreePbas crash replay 幂等测试：重复 surface 后不能 double-free 已复用空间；若保留 direct-free，重复项要被消费端去重或明确被 allocator overlap 安全拒绝。
- lineage FreePbas 释放前清 candidate-cache 的测试：同 PBA 的 candidate entry 必须在 allocator free 前消失。
- lineage FreePbas 与 candidate promote/hazard 并发的端到端测试。
- dedup enabled + ReadPool disabled 的启动配置测试。
- cleanup retire failure retry 测试。
- dirty startup 日志/API 行为测试。

## 最短整改路线

1. 先把 lineage direct-free 改成显式 proof API/命名，并让它先清 candidate-cache、处理 duplicate surface。
2. 给 dedup + no ReadPool 加启动拒绝。
3. 更新 AGENTS.md 和架构文档，删除 RocksDB/dedup_reverse/SHA-256 的旧描述，并写清楚 lineage FreePbas 例外。
4. 把 `DedupScanner` 拆职责，不急着改行为，先改命名和指标归属。
5. 统一 PBA lifecycle API，再收紧 allocator 直接 free 的调用点。
