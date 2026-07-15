# RC checkpoint streaming A/B 与 apply 阶段拆解（2026-07-14）

## 结论

这轮已经把方向收敛清楚：**保留 RC checkpoint streaming 和每批 4096 页的上限，不扩大 commit
queue**。在 `rc_authoritative_reclaim=true` 的受控 600 秒 A/B 中，streaming 让写 IOPS 基本持平
（+0.23%），同时把写 p99 / p99.9 分别降低 17.34% / 15.08%，workload 窗口的 RC apply 总时间降低
14.22%、单 action 成本降低 13.15%，forced checkpoint 最大值降低 10.00%。它是有效优化，但并未解决
p99.99，后者反而上升 37.57%，不能把这轮结果描述成尾延迟已经全面收敛。

加入同周期 checkpoint trace 和 `stage_batch` 细分指标后的 fresh-Meta 600 秒基线进一步证明：

- `stage_batch` 的 fold/slot 锁等待很小，workload 窗口 epoch retry 仅 92 / 81,404（0.113%）；扩大队列或
  增加并发不能针对当前长尾。
- 42.790 秒的最慢 forced checkpoint 主要由 L2P prefold wait 17.043 秒、IO 11.065 秒、sample/RC
  drain wall 5.721 秒和 install 1.788 秒构成；RC fold lock wait 只有 31 微秒（16 shard 求和）。
- checkpoint-free batching variant 证明 `PageStore::free_many + cache invalidation` 在慢周期中只占
  4.86% wall；该样本呈现“更少但更大”的 checkpoint，IOPS、三档尾延迟和 drain 全部回归，因此已回退。
- mixed-u64 hasher 则把 workload `stage_batch` 总成本降低 12.78%，pending scan / delta merge 的采样
  单位成本降低 23.78% / 21.76%；IOPS +0.28%，p99 / p99.9 / p99.99 分别下降 26.16% / 6.30% /
  6.93%，因此保留。
- hasher run 的 forced max 仍从 42.79 秒升到 58.16 秒，同时每轮 L2P/RC checkpoint cohort 约大
  49%，fold/prefold 更长，而 `stage_batch` 本身没有回归。
- generation-aware BFG admission 已把 4M 配置从一次性通知改成真实 work 上界；fresh-Meta 4M A'
  实测最大 cohort 为 4,006,123（理论上界 4,008,191），forced max 从 58.16 秒降到 20.64 秒，write
  IOPS / p99 为 46,878.08 / 113.77 ms。2M 虽把 forced max 继续降到 13.25 秒，但完整生命周期吞吐、
  fio tails、归一化 drain 和 checkpoint 写放大均更差，因此拒绝。

下一阶段应在保留 **4M 严格 cohort 上界**的前提下继续降低 RC sample/prefold/page-IO service max；
不再缩 admission bound，也不扩大 commit queue。

### ZFS 调度对照与并发排空口径

4M A' 的 `46,878.08 IOPS / 806 MB/s` 是 LV2 durable ingress，不是 LV3 后台 service rate。600 秒
workload 内 `lv3_write_batch_bytes` 只增长 105.877 GB，即 176.45 MB/s；fio 停止后 pending drain 又写出
178.256 GB，并在 190.052 秒内完成，即 **937.93 MB/s**。因此此前按 fio + physical drain 摊平得到的
583 MB/s 是完整生命周期有效速率，不能解释为后端单独运行时的刷新上限。

workload 内 `flush_qos_wait_ns` 累计 7,468.44 秒。原控制器直到 LV2 logical/physical fill 达 40% 才从
foreground-protected rate 向 384 MiB/s ramp，且要到 65% 才解除限速；对 512 GiB durable ring 而言，
这等于先允许约 200 GiB backlog，明显晚于设备饱和所需的 dirty window。A' 实际需要的完整 LV3 写量为
284.133 GB，若要在 workload 内不增长 pending，平均只需约 473.53 MB/s，低于已测得的 idle drain 能力。

本地 OpenZFS 对照表明正确顺序是先让 async backend 达到自然 service rate；若 dirty data 仍增长，再对
新前台事务施加双曲线 delay，而不是为了保护前台反向限制后台。Onyx 首轮候选因此禁用 flusher p99 token
bucket，将 write-window pressure 提前到 2%，并只在 5%-10% physical fill 对新 LV2 append 渐进节流。
后台始终由实际 completion 自时钟运行，physical ring 的 condvar hard backpressure 继续作为最终安全线；
5%-10% 窄窗口使用 basis-point fill 计算，避免 512 GiB ring 上整数百分比造成前台从不限速直接跳到
2 ms/append。该变化不会扩大 commit queue，也不改变 4M BFG admission bound。

首轮 `rc-auth-on-zfs-dirty-throttle-4m-600s` 证明“后台不限速、债务压前台”的方向正确，但否定了
`buffer_write_window_physical_pressure_pct=2`。后台在 workload 内完成 253.395 GB，前台写入
254.835 GB，physical fill 全程约 3%-10%，fio 结束时 pending 已为 0，physical ring 15.117 秒归零；
相比 A' workload 内仅 105.877 GB 的 LV3 写出，后台并发 service 已恢复。但 2% pressure 几乎立即
绕过 30 秒 overwrite residence window，使完整 LV3/前台字节比从 A' 的 58.72% 恶化到 99.44%。fio
因此从 46,878 IOPS / 806.38 MB/s 降到 23,741 IOPS / 424.49 MB/s，p99 从 113.77 ms 升至
299.89 ms；按 physical ring 归零计算的完整生命周期有效吞吐也低于 A'，不能保留该阈值。

下一候选保留 `foreground_flush_target_p99_ms=0` 和 5%-10% 前台双曲线 delay，只把 write-window
physical pressure 提到 10%。这样 30 秒内仍可做 overwrite collapse；只有达到前台 throttle ceiling
时才旁路 residence window，让后台立即消费全部债务。验收仍要求后台 QoS wait=0、physical fill 不越过
10%、无 hard backpressure，同时 LV3/前台字节比和完整生命周期吞吐必须恢复。

`rc-auth-on-zfs-dirty-throttle-window10-4m-600s` 同样否决 10% ceiling。它比 2% 候选改善到
27,062 IOPS / 481.35 MB/s，但 workload + drain 的 LV3/前台字节比仍为 96.62%，p99 达 408.94 ms；
85.1% appends 被显式节流，physical ring 归零仍需 51.011 秒。实测 4M BFG 按当前平均 entry 大约占
整个 LV2 ring 的 13%-15%，所以 10% 连一代满 cohort 都容不下，正常 checkpoint 重叠被错误地当成
容量危险。

下一档按实际 cohort footprint 而不是拍脑袋百分比设置：20% 开始、30% 达 cap，并在 30% 才旁路
write window。该窗口允许约一到两代 4M BFG 重叠，仍显著早于旧控制器 40%-65% 的 recovery/emergency；
后台 admission 继续为零。只有它同时恢复 overwrite collapse、前台吞吐与 bounded physical debt，才保留
“压前台、后台全速”的调度方向。

下面的“受控 A/B”和“阶段拆解 fresh 基线”不是同一组可互换样本。后者包含额外正确性修复、指标和新
Meta LD，用于 correctness + hotspot 定位，**不能拿它的 fio 数字与旧 A/B 直接计算优化幅度**。

---

## 实验边界

### 无效的 RC-neutral 预实验

最初的 `stream-off-600s` / `stream-on-600s` 虽然都完整跑了 600 秒且门禁为 0，但两边
`flush_rc_fold_service_us=0`、`flush_rc_stream_pages=0`、`flush_rc_stream_service_us=0`。也就是说 RC
checkpoint 路径没有实际工作，开关未被施压。

| RC-neutral run | write IOPS | write p99 | RC stream pages |
|---|---:|---:|---:|
| OFF | 37,120.14 | 252.707 ms | 0 |
| ON | 36,009.62 | 208.667 ms | 0 |

这组结果只能证明脚本和基础运行路径可用，**不能用于判断 streaming 的收益或回归**，后续结论均不引用
它的差值。

### 有效的 RC-active A/B

有效样本为：

- `.dev/prod-streaming-ab-20260714/rc-auth-off-600s`
- `.dev/prod-streaming-ab-20260714/rc-auth-on-600s`

两次均为 fresh stack、600 秒纯 4K write fio，`rc_authoritative_reclaim=true`，top/metadb commit 和
release binary hash 相同。配置 diff 只有：

```toml
rc_checkpoint_streaming_enabled = false # OFF
rc_checkpoint_streaming_enabled = true  # ON
```

两次脚本的 fio、watchdog、drain、RC mode、RC work、shutdown 等门禁全部为 0。性能对照成立；但旧 ON
样本随后的 strict 审计暴露了 checkpoint/shutdown 正确性缺口，因此这组 A/B 用于判断 streaming
性能方向，不作为最终部署正确性验收。正确性由后面的 fresh-Meta 基线重新闭环。

---

## 受控 A/B 结果

### 前台 fio

| 600s run | write IOPS | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|
| RC streaming OFF | 36,233.67 | 181.404 ms | 417.333 ms | 759.169 ms |
| RC streaming ON | 36,316.99 | 149.946 ms | 354.419 ms | 1,044.382 ms |
| ON 相对 OFF | **+0.23%** | **-17.34%** | **-15.08%** | **+37.57%** |

吞吐没有用明显降速换尾延迟，p99 和 p99.9 有稳定收益；但 p99.99 的回归是真实残余，不能被较好的
p99 掩盖。

### workload 窗口 apply

| 指标 | OFF | ON | 变化 |
|---|---:|---:|---:|
| applied RC actions | 19,400,318 | 19,162,010 | -1.23% |
| RC stage apply total | 12.424 s | 10.658 s | **-14.22%** |
| RC stage apply / action | 0.640 us | 0.556 us | **-13.15%** |
| RC stage apply max | 477.484 ms | 170.306 ms | **-64.33%** |
| full RC apply（含 grouping） | 14.456 s | 12.706 s | -12.11% |
| commit apply total | 72.136 s | 66.054 s | -8.43% |

这说明 streaming 不只改变 checkpoint 的写回方式，也降低了持续施压期间 RC staged apply 的服务成本
和单次最大停顿。

### 完整 drain / checkpoint

| 指标 | OFF | ON | 变化 |
|---|---:|---:|---:|
| forced checkpoint max | 38.953 s | 35.059 s | **-10.00%** |
| checkpoint total | 390.374 s | 370.615 s | **-5.06%** |
| full-drain RC apply max | 1.679 s | 0.333 s | **-80.18%** |
| full-drain pending scan max | 1.673 s | 0.199 s | **-88.09%** |
| full-drain commit apply max | 1.783 s | 0.723 s | **-59.44%** |
| pending drain | 174.450 s | 158.667 s | **-9.05%** |
| physical ring drain | 199.803 s | 199.401 s | -0.20% |

workload high-water mark 下降约 0.317 GiB，完整 drain high-water mark 下降约 1.855 GiB。物理 ring drain
只改善 0.20%，说明 streaming 已明显削减 MetaDB apply/checkpoint 的一部分成本，但持续写入最终仍受后端
物理服务率约束；继续放大 commit queue 只会增加滞留工作量。

---

## 正确性审计与修复

### 旧样本暴露的问题

旧 OFF strict audit 干净；旧 ON audit 发现：

- 39,487 个 L2P orphan pages；
- 12 个同时被标为 live + free 的 reserve pages。

修复链条覆盖 failed streaming page reclaim、shutdown 时 deferred metadata 的 durable drain、stable
metadata 与陈旧 free bitmap 的隔离，以及 staged/standalone manifest publish 后 cache 刷新：

| nested metadb commit | 修复 |
|---|---|
| `3acf133` | failed streaming pages 回收 |
| `a4ee3d5` | shutdown durable drain deferred metadata |
| `5a0ade8` | stale free bitmap 不再污染 stable metadata |
| `4f62a2e` | staged publish 后刷新 manifest cache |
| `ef9048e` | standalone commit 后刷新 manifest cache |

`rc-auth-on-terminal-fix-600s` 随后所有运行门禁为 0，write IOPS 38,432.19，p99 / p99.9 / p99.99 为
133.693 / 312.476 / 624.951 ms，pending / physical drain 为 168.582 / 188.702 秒。但 strict audit 仍有
1,289 个 L2P orphan pages。逐页核对后确认它们来自 10 段 `PageBuf` 256-page lease 的未使用 suffix，
不是 RC allocator 泄漏；旧页头显示的 RC 类型只是未清零的陈旧字节。

`92149ff fix(shutdown): release unused L2P page reservations` 增加显式 lease release 和 Drop fallback，
terminal drain 的顺序改为 flush -> release reserves -> reclaim -> 下一轮持久化 bitmap。强回归测试会重开
设备并断言 255 个 reserve ID 在物理页和 bitmap 中都为 Free。新 Meta 初始 metadata-only close 直接回收
8,160 页（32 x 255），最终 600 秒 run 的 terminal shutdown 又显式回收 2,452 页。

### fresh Meta 严格验收

为避免旧 Meta 污染，保留 LV2/LV3，重建 Meta LD：

- Meta LD：`43b3e523-3b49-44ae-8283-d5a3a81f5c9a`
- volume：`stream-ab`，274,877,906,944 bytes，LZ4

| strict audit | manifest seq | high water | scanned | live | free | orphan | warning | issue |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| workload 前 | 6 | 7,968 | 7,966 | 61 | 7,905 | **0** | **0** | **0** |
| drain + clean shutdown 后 | 199 | 4,055,722 | 4,055,720 | 3,294,959 | 760,824 | **0** | **0** | **0** |

后验 `metadb-verify --strict --json` exit code 为 0。至此 streaming + terminal drain + page lease release 的
fresh-Meta 正确性链条闭环。

---

## 阶段拆解 fresh 基线

样本：`.dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s`。该 binary 包含正确性修复、
`stage_batch` 分段计时和同周期 slow-checkpoint trace，且使用上面的新 Meta LD。所有运行门禁为 0，fio
runtime 600.024 秒，最终 logical/physical ring/payload/used 全部归零。

再次强调：这是新的诊断基线，不是旧 A/B 的第三行；write IOPS 34,991.80，p99 / p99.9 / p99.99 为
248.513 / 566.231 / 1,149.239 ms，只能用来解释这个 fresh run 内部的阶段构成。

### workload 窗口的 `stage_batch`

| 指标 | 数值 |
|---|---:|
| RC actions / batches | 17,762,563 / 81,312 |
| RC stage apply total / max | 11.698 s / 172.040 ms |
| sampled PBAs | 985,609（约 1/16） |
| base lookup attempts / epoch retries | 81,404 / 92（0.113%） |
| sampled fold lock wait | 90.473 ms（0.0918 us / sampled PBA） |
| sampled slot lock wait | 1.126 ms（0.0011 us / sampled PBA） |
| sampled pending slot scan | 425.528 ms（0.4317 us / sampled PBA） |
| sampled delta merge | 281.170 ms（0.2853 us / sampled PBA） |
| sampled base-page lookup | 73.430 ms（0.0745 us / sampled PBA） |

锁等待和 epoch retry 都不是 workload 窗口的主成本。`pending slot scan` 与 `delta merge` 的服务成本比
锁等待更值得优化；这也是后续 mixed-u64 hasher variant 的直接验证目标，而不是增加 worker 或 queue。

完整 drain 后 RC actions 达 62,226,356，RC stage apply total / max 为 284.551 秒 / 342.932 毫秒；base
lookup attempts / epoch retries 为 281,186 / 1,620（0.576%）。采样累计 fold wait 5.532 秒、slot wait
1.478 秒、pending scan 2.935 秒、delta merge 1.377 秒。drain 压力下锁等待会增长，但 retry 仍低于 1%；
它解释 RC apply 的一部分成本，却解释不了 42.8 秒 forced checkpoint 的主墙钟。

### checkpoint 临界路径

完整 drain 的关键最大值：

| 阶段 | max |
|---|---:|
| forced checkpoint total | **42.790 s** |
| L2P fold | **21.076 s** |
| RC drain wall | 5.470 s |
| checkpoint IO | **12.088 s** |
| install | 2.011 s |
| manifest | 13.617 ms |
| apply gate wait / hold | 13 us / 3.171 ms |

最慢同周期 trace（BFG 190）为：

| 字段 | 数值 |
|---|---:|
| total | 42.789869 s |
| L2P fold（本周期） | 0.030 ms |
| sample wall / L2P walk / RC drain wall | 5.721 / 0.251 / 5.470 s |
| IO | 11.065 s |
| manifest / install | 9.689 ms / 1.788 s |
| prefold wait | **17.043 s** |
| RC fold lock wait sum | **31 us** |
| L2P dirty pages / RC deltas | 548,015 / 7,989,033 |
| RC stream pages / nonstream pages | 700,862 / 551,759 |

`rc_fold_service_sum=21.492s`、`rc_stream_write_sum=49.122s` 是 16 shards 的并行/嵌套服务时间求和，
**不能与墙钟阶段相加**。该周期已显式计时的互斥墙钟阶段合计约 35.63 秒，留下约 7.16 秒残差。
代码检查把这段缩到 install 之后的 `PageStore::free_many(checkpoint_frees)` 和逐页 cache invalidation。
后续 variant 的独立计时显示，该路径在 27 个慢周期中合计 19.308 秒 / 7,232,908 页，即
2.670 us/page，只占慢周期 wall 的 4.86%；基线的 7.16 秒残差不能整体归因给 cache invalidation。

这些数据排除了两个方向：RC fold lock 不是 42.8 秒 checkpoint 的主因，apply gate 也不是。当前最大
容量瓶颈是 **L2P fold/prefold + metadata page IO**；`stage_batch` 的 CPU 服务成本应靠数据结构/哈希
优化，不靠加深 queue。

---

## fresh-Meta 单变体结果

两次单变体都使用相同 fio 参数、seed、LV2/LV3 和配置，配置 diff 只有 fresh Meta LD ID，全部运行门禁
为 0。远端 checkout 的 git HEAD 字段是陈旧部署元数据，样本身份使用实际同步文件和 release binary
SHA：基线 `327bb306...`，hasher-only `b5689abd...`。完整 artifact 分别位于
`.dev/prod-streaming-ab-20260714/rc-auth-on-checkpoint-batch-600s` 和
`.dev/prod-streaming-ab-20260714/rc-auth-on-u64-hasher-600s`。

### checkpoint free batching：拒绝并回退

`db701fd perf(checkpoint): batch retired page cache invalidation` 按 cache shard 聚合失效，并增加
`checkpoint_free_us/pages`。它通过测试和 strict correctness，但没有通过端到端性能门槛：

| 指标 | stage-split baseline | checkpoint batching | 变化 |
|---|---:|---:|---:|
| write IOPS | 34,991.80 | 31,820.39 | **-9.06%** |
| p99 / p99.9 / p99.99 | 248.513 / 566.231 / 1,149.239 ms | 263.193 / 583.008 / 1,199.571 ms | +5.91% / +2.96% / +4.38% |
| pending / physical drain | 189.749 / 219.883 s | 215.236 / 255.442 s | +13.43% / +16.17% |
| workload RC stage total | 11.698 s | 21.000 s | +79.52% |
| full RC stage total | 284.551 s | 318.750 s | +12.02% |
| workload forced calls / total / max | 179 / 211.279 s / 10.441 s | 29 / 168.409 s / 18.139 s | 少而更大 |
| full forced calls / total / max | 189 / 419.914 s / 42.790 s | 37 / 398.143 s / 46.805 s | 少而更大 |

27 个慢周期的 checkpoint-free 合计 19.308 秒、最大 4.219 秒，只占慢周期 wall 的 4.86%；最慢周期
仍由 prefold、IO 和 RC sample 主导。后验 strict 扫描 3,837,854 页，orphan / warning / issue 均为 0。
这是正确但端到端回归的优化，已由 `26322e5` 回退，不能因代理指标看似合理而保留。

### `stage_batch` mixed-u64 hasher：保留

`72f05de perf(refcount): use mixed u64 hashing for staged deltas` 为内部 PBA/PageId 使用 SplitMix64 mixer，
覆盖 staged DeltaMap 和 read-view map；这些 key 是可信的内部整数，测试同时覆盖固定向量和对齐 key 扩散。

| workload 指标 | stage-split baseline | hasher-only | 变化 |
|---|---:|---:|---:|
| write IOPS | 34,991.80 | 35,088.23 | +0.28% |
| p99 | 248.513 ms | 183.501 ms | **-26.16%** |
| p99.9 / p99.99 | 566.231 / 1,149.239 ms | 530.579 / 1,069.548 ms | -6.30% / -6.93% |
| RC stage total | 11.698 s | 10.203 s | **-12.78%** |
| RC stage / action | 0.659 us | 0.579 us | **-12.11%** |
| pending scan / sampled PBA | 0.432 us | 0.329 us | **-23.78%** |
| delta merge / sampled PBA | 0.285 us | 0.223 us | **-21.76%** |
| base lookup / sampled PBA | 0.075 us | 0.053 us | -29.32% |
| epoch retry rate | 0.1130% | 0.0946% | -0.0184 pp |

full drain 后 RC stage / action 仍下降 1.42%，pending scan / delta merge 的采样单位成本仍下降 29.24% /
19.97%，full apply max 342.932 ms 降到 323.074 ms。workload 结束时 pending 只多 0.261%，Meta high
water 只多 0.342%，RSS HWM 反而下降 0.071 GiB；不是靠减少工作量或堆内存换来的前台收益。

代价是 pending / physical drain 变为 212.450 / 237.580 秒（+11.96% / +8.05%），full forced max
42.790 -> 58.155 秒。该 run 的 forced calls 189 -> 126，每轮 dirty L2P pages 和 RC deltas 约多 49%；
最慢周期由 L2P fold 24.436 秒、IO 11.430 秒、prefold wait 8.279 秒、RC drain wall 3.902 秒和约
8.492 秒未拆残差构成。目标 `stage_batch` 本身在 workload/full 均改善，前台三档尾延迟也改善，因此
保留 hasher；drain 回归作为下一轮 checkpoint cohort/L2P 优化的独立问题，不用扩大 queue 掩盖。

hasher fresh Meta 的 workload 前 strict 为 0/0/0；后验 strict exit code 为 0，manifest sequence 136，
扫描 4,028,086 页，live / free 为 3,291,741 / 736,348，orphan / warning / issue 均为 0。两个单变体都
不改变 commit queue 深度，也不扩大 RC streaming 的 4096-page cap。

---

## BFG work admission 上界

### 根因与实现

原 `l2p_buffer_soft_entries` 只在一个 Open BFG 首次跨过阈值时发通知；如果 quiesce worker 正阻塞在
前一代 `promote_to_syncing`，当前 Open BFG 仍可继续接收 commit，cohort 会随 checkpoint service time
无界增长。`l2p_buffer_hard_entries` 实际没有消费者，不能提供第二道上界。

`7b5c486 perf(checkpoint): bound BFG work admission` 将计数和 admission 放在 LSN 分配前原子完成：

- crossing batch 被接纳并关闭当前 generation；后续 commit 等下一 BFG 成功打开；
- generation-tagged force notification 丢弃 stale 请求，不会误滚下一代；迟到请求也不再重置 timer；
- shutdown / abort 会唤醒 admission waiter；snapshot live 时保持原来的 lifecycle 边界；
- `commit_ops`、`commit_ops_unlogged`、`stage_ops` 使用同一入口，指标记录 admission wait total / max。

因此单 BFG 的提交工作量上界为 `limit + max_single_batch - 1`。本实现没有改 commit worker、queue、
pipeline depth、75ms coalesce window 或 RC streaming 的 4096-page chunk。

### 2M / 4M fresh-Meta 控制

同一 release binary SHA 为 `8f8da620...`；2M 与 4M A' 使用相同 seed、LV2/LV3、runner 和显式
`RUST_LOG=onyx_storage=info,onyx_metadb=warn,onyx_chunklet=error`，只改变 fresh Meta LD 和 soft bound：

| 600s 指标 | 2M | 4M A' | 4M 变化 |
|---|---:|---:|---:|
| write IOPS | 42,493.51 | **46,878.08** | +10.32% |
| p99 / p99.9 / p99.99 | 116.916 / 258.998 / 434.110 ms | **113.770 / 221.250 / 404.750 ms** | -2.69% / -14.57% / -6.76% |
| pending / physical drain | 208.664 / 233.798 s | **190.052 / 230.313 s** | -8.92% / -1.49% |
| full forced max | **13.249 s** | 20.637 s | +55.76% |
| full prefold wait max | **2.059 s** | 7.725 s | +275.16% |
| observed max BFG work | 2,007,461 | 4,006,123 | both bounded |
| RSS HWM | **62.42 GiB** | 68.72 GiB | +10.08% |

两边 workload slow trace 都没有触达各自阈值（2M max 1,477,259；4M A' max 1,302,236），workload
admission wait 也只有 3.813 / 24.808 ms。因此不能把两次 fio 差异归因于 admission gate；最初 4M A
只有 33,331.99 IOPS，而 A' 达到 46,878.08，也证明 fresh Meta placement / 运行态方差很大。

最终决策仍保留 4M，依据是完整生命周期方向一致，而不是单个 fio 数字：

- 4M pending drain rate 约 67.71K entries/s，2M 为 57.88K/s；physical drain rate 也高约 16%；
- full RC stage / action 为 3.173 / 3.361 us，full commit apply / LBA 为 8.629 / 8.797 us；
- workload checkpoint bytes / fio bytes 为 0.1151 / 0.1255，4M 低 8.31%；full pages written / L2P
  apply 低 12.07%；
- 2M full forced calls 为 417 次，4M 为 312 次；累计 admission waiter time 为 482.4 / 221.8 秒。

2M 的确直接缩短单次内部 checkpoint，但没有转化成更好的用户 tails 或后端 service rate。4M 已把
hasher 的 58.155 秒 forced max 降到 20.637 秒（-64.51%），同时保住更好的持续能力；继续降到 1M/2M
会越过当前写放大与 checkpoint 频率的收益拐点。

### 正确性与 CRC 边界

4M A' 的 fresh-Meta pre/post strict 均为 exit 0：pre 扫描 7,966 页，post 扫描 5,545,445 页；post
live / free 为 3,864,169 / 1,681,364，orphan / warning / issue 全为 0。2M post strict 同样为 exit 0，
扫描 7,147,905 页且三项为 0；其 pre strict 因另一进程持有 chunklet pool flock 返回 1，未被伪装成
有效验收。三轮 admission artifact 的 foreground/dedup CRC、decompress 和 flush error 增量也全部为 0。

这证明已知 LV2/LV3 overlap 和 premature-free CRC 类没有在本轮复现；`metadb-verify` 审计的是 Meta
页可达性，不是全量 LV3 live-payload scrub，因此不能把它描述成一次全盘 payload CRC 证明。

完整 artifact：

- `.dev/prod-streaming-ab-20260714/rc-auth-on-bfg-admission-2m-600s`
- `.dev/prod-streaming-ab-20260714/rc-auth-on-bfg-admission-4m-aprime-600s`

---

## 代码与门禁

本轮已落地的主要提交：

| repo | commit | 内容 |
|---|---|---|
| metadb | `57ae13c` | streaming RC page writeback |
| metadb | `5cd5eee` | 精确 streaming A/B 开关 |
| metadb | `3acf133`..`ef9048e` | checkpoint/shutdown/manifest correctness 修复链 |
| metadb | `92149ff` | 释放未使用 L2P page reservations |
| metadb | `7b521ff` | checkpoint 与 RC apply 阶段拆解 |
| metadb | `72f05de` | staged DeltaMap/read-view mixed-u64 hashing |
| metadb | `db701fd` / `26322e5` | checkpoint cache batching 实验及性能回退 |
| metadb | `7b5c486` | generation-aware BFG work admission 上界 |
| metadb | `7cf48a5` | 澄清未启用的 hard threshold 配置语义 |
| top | `1a390a1` | 暴露 RC streaming phases |
| top | `70cdae0` | 暴露 streaming A/B mode |
| top | `613a24a` | shutdown terminal reclaim 持久化 |
| top | `ea96cdb` | 暴露 RC stage breakdown |
| top | `0058ef4` | 暴露 BFG admission wait |

最终保留树的 nested metadb 全量主库测试为 793 passed、0 failed、3 ignored，所有 integration/doc tests
通过；top `cargo test --lib metrics::` 9/9，analyzer unittest 2/2；本地与 nvme-box release build 均通过。

复现/分析入口：

```bash
python3 scripts/analyze_commit_benchmark.py \
  --before  .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/metrics-before.json \
  --after   .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/metrics-after.json \
  --fio     .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/fio-clean.json \
  --status-before .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/status-before.json \
  --status-after  .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/status-after.json \
  --drain   .dev/prod-streaming-ab-20260714/rc-auth-on-stage-split-final-600s/drain.json

target/release/onyx-storage \
  -c .dev/prod-streaming-ab-20260714/config-rc-auth-ab.toml \
  metadb-verify --strict --json
```

一句话状态：**streaming、mixed-u64 hasher 和 4M strict BFG admission 已保留，2M bound 与
checkpoint-free batching 因完整生命周期回归被拒绝；剩余最大容量瓶颈是在 4M cohort 内的 L2P
fold/prefold、RC sample 和 metadata page IO，不是 commit queue。**
