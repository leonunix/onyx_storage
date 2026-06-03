# Onyx 回收架构现状梳理 (2026-06-03)

> 目的：把"一个 PBA 从写入 → 覆盖/discard → 真正释放"的当前真实路径，
> 跨 onyx + metadb 钉清楚（带 file:line）。本文只描述**代码现状**，并标出
> 几个仍存疑/可能过期的点。

## 0. 一句话

普通写路径已经 **rc-neutral（Phase 5 live）**：写/覆盖 **L2pRemap 不动 refcount**。
后果不是"`rc` 绝不下降"，而是：**普通覆盖 / RangeDelete / DropSnapshot 都不会为旧映射
decref**，所以 dedup/shared PBA 的 `rc` 可能变成 stale over-count。`DedupDelete` /
`DedupPut` 替换 / `FreePbas` 仍然会 decref。

⇒ **凡是 gate 在 `rc==0` 上的回收闸（Lineage GC §3b、retired reclaim §3c Gate1），
都放不了 stale-positive 的 dedup PBA。** 真正决定一个 **dedup PBA 是否还被活 L2P 引用**，
只能靠扫 L2P 重建"真实 referenced 集"；但当前 §3c 在扫之前仍有 `rc==0` Gate1，所以这条
能力还没有完整接到 stale-positive dedup 回收上（见 §6）。

判"还被引用吗"有两个正交维度：
- **dedup 共享** —— 别的活 LBA 是否还指向同一个 PBA。`rc` 只能作为保守信号：
  `rc==0` 足以继续做确认，`rc>0` 可能是真共享，也可能是普通覆盖留下的 stale over-count。
- **snapshot 钉住** —— 某个快照/clone 的 L2P 是否还能看到这个旧 PBA。由 dead-list 的
  `[birth,death)` vs 快照 LSN 判（§3b，这部分 rc 无关）。

## 1. 写路径是 rc-neutral（Phase 5）

`apply_l2p_remap`（覆盖）只做一件回收相关的事：把旧映射 push 进 **per-volume
dead-list**，**不 decref refcount**。

- `record_dead()` → `volume.dead_list.push(DeadRecord{ pba, birth_lsn, death_lsn })`
  —— [metadb `db/apply/l2p.rs:53`](../metadb/src/db/apply/l2p.rs#L53)
- 明文规则 [l2p.rs:66-74](../metadb/src/db/apply/l2p.rs#L66)：
  > "Phase 5 rule: hot-path L2P remaps are **rc-neutral**. The only events that bump
  > global refcounts are PromotionChunk (clone), FreePbas (Lineage GC), and the
  > DedupPut/DedupDelete family."
- Phase 5 是强制 live：`lineage_gc_emit_freepbas` 默认 **true**，且 metadb
  `create/open` 会拒绝显式 `false`（[open.rs](../metadb/src/db/lifecycle/open.rs#L752)）。
  这堵死了退回 Phase 3 "只截 dead-list、不发 FreePbas" 的路径。

**推论**：refcount 实际追踪 **dedup/promote/FreePbas 事件账**，**不追踪普通写覆盖**。
普通覆盖不 decref ⇒ 对一个被 dedup 共享的 PBA，覆盖其一个引用 LBA 时 `rc` 不会因为
这次覆盖下降 ⇒ **rc 对 dedup 可能过计数(stale over-count)**。这一点是理解 §6 缺口的关键。

## 2. 两个"还被引用"维度

| 维度 | 信号 | 判定成本 | 谁判 |
|------|------|----------|------|
| dedup 共享 | refcount：`rc>0` 表示曾被 dedup/promote/FreePbas 账引用；可能 stale | O(1) 查 rc —— 只能保守过滤，不能证明真实 live L2P 引用 | lineage GC / retired reclaim 会看 rc；RangeDelete 不再改 rc |
| snapshot 钉住 | dead-list record 的 `[birth_lsn, death_lsn)` 是否被任何活 snapshot 的 `created_lsn` 或后代 clone 的 `branched_at_lsn` 覆盖 | 走 dead-list 链 + snapshot 列表 | lineage GC §3b |

## 3. 当前并存的三条回收路径

### 3a. discard/delete-volume 的 RangeDelete cleanup
- `apply_l2p_range_delete`（[l2p.rs](../metadb/src/db/apply/l2p.rs#L349)）：
  discard 一段 LBA，只删除 captured L2P entries，**不查 refcount、不 decref PBA、
  不返回 freed_pbas**。这是 Phase 5 必须语义：普通 PBA rc 不是 per-live-LBA counter，
  对 captured LBA 一条条 decref 会把仍被其它 volume/LBA 引用的 PBA 错误打到 0。
- onyx metadb adapter 另外会在 `range_delete` 提交后，对旧映射聚合出的 PBA 再查
  `multi_get_refcounts`；当前 refcount 为 0 的 cleanup 标记 `pba_freed=true`
  （[src/meta/backend/metadb.rs:586-595](../src/meta/backend/metadb.rs#L586)）。
- 输出 cleanup 后，onyx `cleanup_dead_pbas_batch → retire_dead_pbas` 把 `pba_freed`
  的 PBA 放入 allocator retired 集，后续由 §3c confirm scan 真正 reclaim。

> 关键点：RangeDelete 现在和普通覆盖一样是 PBA rc-neutral。它能删除逻辑映射，但不能靠
> per-LBA decref 证明物理 PBA 可释放；物理释放必须走 Lineage GC / retired confirm scan。

### 3b. Lineage GC —— snapshot/clone pin + rc==0 head-segment 推进
- metadb `db/async_reclaim.rs` `gc_plan_head_advance`：per-volume 走 dead-list head
  段，逐条查：**(a) 不被活 snapshot `snap_lsns` pin、(b) 不被后代 clone
  `branched_at_lsn` pin、(c) `rc==0`**（[async_reclaim.rs:370-390](../metadb/src/db/async_reclaim.rs#L370)）。
  整段全过 → 推进 `head_pid`、PBA 进 `dead_pbas`。任一条不过 → 整段留到下个 cycle 重判。
- `emit_freepbas=true` → `commit_free_pbas` → `dispatch_freed_pbas_outcomes`
  （[db.rs:1488](../metadb/src/db.rs#L1488)）→ 经 `FreedPbasSink` channel 给 onyx。
- onyx `LineageFreedPbaDrainHandle`（[src/engine/lineage.rs:58](../src/engine/lineage.rs#L58)）：
  `drain_lineage_freed_pbas()` → `allocator.free_one/free_extent` **直接进 free list**。

因为 plan 阶段要求 `rc==0`，Lineage GC 只能推进已经不被 rc 账挡住的 dead-list segment；
stale-positive dedup PBA 会卡住 head segment。

### 3c. reclaim_retired_extents + heat map —— 全卷 L2P 扫描
- onyx `GcRunner::reclaim_retired_extents`（[src/gc/runner.rs:291](../src/gc/runner.rs#L291)）：
  `allocator.retired_candidates` → **Gate1**（`multi_get_refcounts` 只留 rc==0，
  对非 dedup PBA 因 rc 缺省=0 而 **vacuous**）→ **Gate2**（`referenced_extents`
  **全卷 L2P 扫描**，[src/meta/store.rs:535](../src/meta/store.rs#L535)）→ free。
- **heat map（Stage A/B）优化的就是 Gate2 这个全卷扫描**：热区(大概率还被引用)跳过扫描，
  冷区才扫。
- retired 集的喂入：discard/delete-volume/metadata cleanup 报回的 `pba_freed`（§3a）经
  `cleanup_dead_pbas_batch → retire_dead_pbas`（[src/buffer/flush/cleanup.rs:62](../src/buffer/flush/cleanup.rs#L62)）。

**为什么 Phase 5 仍需要全卷 L2P 扫描（核心理解）**：
因为 §1 里 rc 对 dedup **过计数**（覆盖不 decref）——单凭 rc 判不出一个 dedup PBA
是否真死了。要找出"rc 还 >0 但其实已无任何活 LBA 引用"的 PBA，只能**扫 L2P 重建
真实 referenced 集**。这就是 [adaptive-reclaim-heatmap.md](adaptive-reclaim-heatmap.md)
说的"no persistent PBA→referrers reverse index, so … forward scan of every volume's L2P"。
heat map 让这个昂贵扫描不必每 cycle 全做。

但当前实现里，§3c 在 Gate2 扫描前仍有 `rc==0` Gate1，所以它只确认 rc 已经归零的 retired
extent。**stale-positive dedup PBA 还没有一条完整的 reclaim 接线**（见 §6）。

## 4. refcount 在哪些地方被维护（汇总）

| 事件 | 对 rc 的影响 |
|------|-------------|
| 普通写 / 覆盖（L2pRemap）| ❌ 不动（rc-neutral）|
| RangeDelete / DropSnapshot | ❌ 不动（只删 L2P / metadata page refs）|
| DedupPut / DedupDelete | DedupPut incref new PBA，替换旧 entry 时可能 decref old PBA；DedupDelete decref old PBA |
| PromotionChunk（clone walker）| 把父卷共享 PBA 的 rc 补上 |
| FreePbas（lineage GC）| 释放时 rc 收尾 |

## 5. DedupScanner ≠ 回收
onyx `src/dedup/scanner.rs`：DEDUP_SKIPPED 补扫 + cold-tail 补扫，都是给
**candidate cache 充水**（恢复 dedup 率），**不做 PBA 回收**。
它的 scrub 会 `delete_dedup_index_if_matches` 清 stale forward dedup entry，但不会把
物理 PBA 判死并交还 allocator。

## 6. 最大疑点：dedup PBA 的回收到底接在哪？（动手前必须定）

把上面的逻辑推到底，出现一个**关键矛盾**，需要你定夺：

- dedup PBA：普通覆盖不会 decref，所以可能出现 `rc>0` 但 live L2P 已经无引用的 stale-positive 状态。
- 但 §3c 的 `reclaim_retired_extents` **Gate 1 也是 `multi_get_refcounts` 只留 rc==0**
  （[runner.rs](../src/gc/runner.rs#L314)）⇒ **它同样会把 dedup PBA 挡在门外**。

⇒ 那么 **dedup PBA 究竟由谁回收？** 三种可能：
1. **需要一条独立的"扫 dedup hash 找可释放"任务**（你记忆里的那个），它**不** gate 在
   rc==0 上，而是遍历 dedup index（`DedupIndex::iter()` 存在，
   [indexes.rs:218](../metadb/src/db/indexes.rs#L218)）+ 比对 live L2P，释放不再被引用的。
   —— 但我在 master 上**没找到它被 wire 起来的调用**。⇒ **可能还没实现 / 在分支上 /
   正是 heat-map 这条线要建的东西。**
2. `reclaim_retired_extents` 的 Gate 1（rc==0）在 Phase 5 本就该改/去掉，让扫描（Gate 2）
   成为唯一判据 —— 那 heat map 现在优化的扫描其实**还没真正接到 dedup 回收上**。
3. 我对某处仍有误读。

**这直接决定 heat map / yield-gate / B2 当前到底优化了什么**：
- 若 dedup 回收还没接上 ⇒ heat map 现在只在 §3c 上优化**非 dedup 块**的确认扫描，
  而那条 dedup=0 时是高产出、heat-defer 纯亏（= 之前 A/B 的现象，吻合）。
- heat map 要真正发光，得先把它接到**那条不 gate rc==0 的 dedup 回收扫描**上。

## 7. 已同步的关键修正
- RangeDelete / DropSnapshot 已改成 PBA rc-neutral。
- `lineage_gc_emit_freepbas=false` 已在 metadb create/open 被拒绝，堵死退回 Phase 3
  chain-truncation-only 的路径。
- `async_reclaim` 的历史后台 lineage pass 不再能在 Phase 5 下只截链不发 FreePbas。

---
*维护：本文是 2026-06-03 一次代码梳理的快照；改回收路径时同步更新。*
