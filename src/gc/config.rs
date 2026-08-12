use serde::Deserialize;

/// GC configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GcConfig {
    /// Whether GC is enabled (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Scan interval in milliseconds (default 5000).
    #[serde(default = "default_scan_interval_ms")]
    pub scan_interval_ms: u64,
    /// Dead ratio threshold to trigger repack (default 0.25).
    #[serde(default = "default_dead_ratio_threshold")]
    pub dead_ratio_threshold: f64,
    /// Buffer-fill percentage at which the compactor's idle pacing reaches zero
    /// effort (default 80). The resident compactor scales its per-cycle scan +
    /// rewrite budget by `idle_factor = (max_pct - fill_pct)/max_pct`, so at
    /// `fill >= buffer_usage_max_pct` it does no compaction work UNLESS space
    /// pressure (low free%) overrides the idle backoff. Replaces the old hard
    /// on/off pause.
    #[serde(default = "default_buffer_usage_max_pct")]
    pub buffer_usage_max_pct: u8,
    /// Legacy hysteresis floor for the old on/off GC pause. Unused by the
    /// resident idle-paced compactor (pacing is now the continuous `idle_factor`
    /// ramp against `buffer_usage_max_pct`); kept for config/wire stability.
    #[serde(default = "default_buffer_usage_resume_pct")]
    pub buffer_usage_resume_pct: u8,
    /// Max candidates to rewrite per scan cycle (default 64). Scaled by the
    /// compactor's per-cycle `effort` (idle/urgency); `effort>0` always yields
    /// at least one rewrite.
    #[serde(default = "default_max_rewrite_per_cycle")]
    pub max_rewrite_per_cycle: usize,
    /// Reclaim grace: a retired extent is not freed until it has been seen in
    /// the retired set for at least this many seconds (default 300 = 5 min).
    /// This guarantees the settle window the retire→reclaim path otherwise gets
    /// only incidentally from the GC cycle cadence: a reference committed to the
    /// LV2 log but whose metadb L2P apply is still in flight (or mid-BFG fold)
    /// is transiently invisible to the reclaim reverify; waiting the grace lets
    /// it land before the free decision, closing the premature-free race that
    /// corrupted reads. `0` disables the grace (incidental cycle delay only).
    #[serde(default = "default_reclaim_grace_secs")]
    pub reclaim_grace_secs: u64,

    /// Resident-compactor dead-ratio threshold used when free space is plentiful
    /// (free% > 50, default 0.85). The compactor is ALWAYS resident — `free_pct`
    /// only tunes how aggressive it is, it is no longer an on/off switch. At
    /// plentiful space only clearly-dead units (>= this ratio) are compacted:
    /// cheap, high-yield, and enough to keep packing-slack debt bounded instead
    /// of letting it grow forever on a large/thin device. As free% drops the
    /// threshold lowers (0.50 / 0.30 / `dead_ratio_threshold`).
    #[serde(default = "default_compactor_resident_threshold")]
    pub compactor_resident_threshold: f64,
    /// Per-cycle bound on the resident compactor's candidate scan, in LBAs
    /// (default 1_000_000). Each GC cycle scans ONE ~1M-LBA window of one volume
    /// via a lap cursor instead of the full ~80M-entry blockmap, sweeping the
    /// whole L2P over many cycles. Scaled down by the per-cycle `effort`.
    /// **`0` is a kill-switch**: the compactor scan is skipped entirely (no
    /// background compaction; retire→reclaim of fully-dead extents still runs).
    #[serde(default = "default_compactor_scan_max_lbas_per_cycle")]
    pub compactor_scan_max_lbas_per_cycle: u64,

    // --- Slot-aware compaction (evacuate live fragments pinning mostly-dead
    //     packed slots; the per-fragment dead-ratio path can never free these) ---
    /// Master switch for slot-aware compaction selection (default FALSE —
    /// behavior change, A/B before flip). When ON, the compactor scan also
    /// targets *whole packed 4 KiB slots that are mostly dead by bytes*,
    /// promoting their few live fragments for rewrite so the slot reaches rc→0 →
    /// retire → reclaim. The per-fragment dead-ratio path selects the *dead*
    /// fragment (which frees nothing — the slot is pinned by a live sibling);
    /// this selects the *live* fragment that actually pins the slot. Selection
    /// only — the rewriter and the retire/free path are unchanged.
    ///
    /// Relies on rc being authoritative (rc(P) == live-LBA count for slot P) to
    /// cheaply prove a window saw all of a slot's live references, so it is
    /// inert unless `rc_authoritative_reclaim` is also on (checked at runtime).
    #[serde(default = "default_compactor_slot_evac_enabled")]
    pub compactor_slot_evac_enabled: bool,
    /// Max live blocks a packed slot may hold and still be evacuated (default
    /// 16). Bounds the rewrite/IO cost paid to free one slot — only slots pinned
    /// by few live blocks are cheap enough to relocate. The compactor
    /// additionally clamps this to the per-cycle `rewrite_budget` so a single
    /// slot can never exceed the budget (whole-slot evacuation is atomic).
    #[serde(default = "default_compactor_slot_evac_max_live")]
    pub compactor_slot_evac_max_live: u16,

    // --- Defrag: physical-neighborhood compaction (evacuate the few live
    //     blocks pinning confetti clusters so reclaim can rebuild large,
    //     stripe-capable free runs; per-unit dead-ratio can never see these).
    //
    //     Selection is split in two by whether a window has a LIVE pinner.
    //     Retired-pinned windows (whole remainder free-or-retired) need no
    //     rewrite at all — reclaim finishes them — so they are selected by the
    //     RESIDENT defragger thread straight off the retired set, with no
    //     metadata scan (`defrag_interval_ms`,
    //     `defrag_classify_max_windows_per_cycle`). Live-pinned windows need a
    //     PBA -> LBA answer, which only the compactor's forward L2P scan gives,
    //     so they stay SCAN-DRIVEN on the gc-runner thread, downshifted to one
    //     pass every `defrag_scan_interval_cycles` with its own
    //     `defrag_scan_timebox_ms`.
    //
    //     The pre-2026-08-06 design instead walked the free list DESCENDING and
    //     pre-picked <= 32 targets, then hoped a later scan lap would stumble on
    //     their live pinners — a reverse lookup served by a full forward scan,
    //     which measured as literally zero work over 480 s on the box.
    //     `defrag_gap_max_blocks` / `defrag_scan_extents_per_cycle` /
    //     `defrag_max_active_targets` (the 32-slot cap) belonged to that walk and
    //     are gone: the budgets are now block/span/rate bounds, never a slot
    //     count (memory: `coalesce_admission_cap_64_is_the_drain_wall` — an
    //     arbitrary per-cycle count cap on admitted work becomes the next wall).
    //     ---
    /// Master switch for defrag target selection (default true). Trigger-gated:
    /// inert until the free pool's stripe-capable fraction
    /// (`eff_capacity / free_blocks`) drops below
    /// `defrag_stripe_capable_min_pct`, so a healthy pool pays nothing.
    /// Selection only — the rewriter and the retire/reclaim gates are unchanged.
    #[serde(default = "default_defrag_enabled")]
    pub defrag_enabled: bool,
    /// Enter defrag mode when the stripe-capable fraction of the free pool is
    /// below this percent (default 30). Exit at this + 10 (fixed hysteresis).
    #[serde(default = "default_defrag_stripe_capable_min_pct")]
    pub defrag_stripe_capable_min_pct: u8,
    /// Don't enter defrag mode below this free% (default 5) — under real space
    /// exhaustion the urgency ladder owns the problem and extra background
    /// movement only competes for the last blocks.
    #[serde(default = "default_defrag_min_free_pct")]
    pub defrag_min_free_pct: u8,
    /// A stripe window qualifies as a defrag target iff
    /// `(free + retired) blocks / stripe >= this percent` (default 50) — i.e.
    /// the live pinners to evacuate are at most half the window.
    #[serde(default = "default_defrag_min_free_density_pct")]
    pub defrag_min_free_density_pct: u8,
    /// Cap on the total span (blocks) of active defrag target ranges
    /// (default 32_768 = 128 MiB at 4 KiB). Bounds the scanner's per-entry range
    /// check and — the part that actually bites — how much free space a
    /// quarantine can hold OUT of the allocatable pools at once.
    ///
    /// ⚠ Was 262_144 (1 GiB) until 2026-08-10. A quarantine is not free: its
    /// window's already-free fragments leave the general pool until the whole
    /// stripe clears, so this is a direct upper bound on the free space the
    /// flusher cannot see. The 2026-08-07 box run measured `SpaceExhausted`
    /// amplified 2.7× with defrag on; the dominant channel was reclaim
    /// starvation (+23.7 GiB of `retired_depth`) rather than the quarantine's
    /// own 1 GiB, but 1 GiB was still the wrong standing exposure for a
    /// best-effort optimiser. With the resident defragger the exposure is also
    /// short-lived by construction (its targets wait only for reclaim, not for a
    /// rewrite lap), so a smaller budget costs nothing but bounds the worst case
    /// 8× tighter. See memory `defrag_gc_lever`,
    /// `gc_reclaim_defrag_criticality_inversion`.
    #[serde(default = "default_defrag_max_target_blocks")]
    pub defrag_max_target_blocks: u64,
    /// Resident defragger cadence in milliseconds (default 1000, floor 50).
    ///
    /// The allocator-side half of defrag runs on its OWN thread
    /// (`gc::defrag_runner`) so that mandatory reclaim's cadence on the
    /// `gc-runner` thread can never be stretched by an optional optimiser
    /// (memory: `gc_reclaim_defrag_criticality_inversion` — 8.6 → 13.7 s cost
    /// 23.7 GiB of unallocatable `retired_depth`). Together with
    /// `defrag_classify_max_windows_per_cycle` this is the RATE CAP that is the
    /// design's real gate.
    #[serde(default = "default_defrag_interval_ms")]
    pub defrag_interval_ms: u64,
    /// How often the resident defragger re-evaluates its TRIGGER LATCH, in
    /// milliseconds (default 5000 = the old GC cycle). Target
    /// completion/cancellation still runs every `defrag_interval_ms`; only the
    /// enter/exit decision is on this slower clock.
    ///
    /// ⚠ This split is not cosmetic. The latch needs
    /// `SpaceAllocator::contiguity_stats`, which takes **one lock per address
    /// region — 2048 at the default `storage.allocator_regions`** — and it also
    /// publishes the `allocator_*` contiguity gauges. Running it at the
    /// selection cadence made that walk 5× more frequent than it was as a GC
    /// phase (box, 2026-08-10: `free_lock.audit acquisitions=354304` in ~170 s ≈
    /// 2048/s), which is precisely the lock-COUNT increase the design is gated on
    /// (memory: `fragmentation_unaligned_alloc_1889_locks`). Keeping it at 5000
    /// leaves that traffic exactly where mainline had it while the cheap part of
    /// maintenance (O(active targets) single-region lookups) still runs at 1 Hz.
    #[serde(default = "default_defrag_trigger_interval_ms")]
    pub defrag_trigger_interval_ms: u64,
    /// Stripe windows the resident defragger classifies per cycle (default
    /// 4096). `0` disables allocator-side selection (maintenance/publication
    /// still runs, so existing targets still complete).
    ///
    /// ⚠ THIS IS THE GATE, and its verdict is a lock-attribution number, not a
    /// throughput number: `free_lock.defrag_classify` and
    /// `retired_lock.defrag_windows` acquisitions must not appear inside a flush
    /// writer's `writer_refill` / `writer_unaligned` wait. Lock COUNT is what the
    /// writers pay — one unaligned allocation already takes 1,889 acquisitions
    /// (memory: `fragmentation_unaligned_alloc_1889_locks`) — so classification
    /// is batched per region (~128 windows per hold) and capped here. At the
    /// defaults that is tens of acquisitions per second. Only raise it against
    /// two `status` samples showing the writers' wait unchanged.
    #[serde(default = "default_defrag_classify_max_windows_per_cycle")]
    pub defrag_classify_max_windows_per_cycle: usize,
    /// Run the compactor's SCAN-DRIVEN defrag selection only every Nth GC cycle
    /// (default 8; `1` = every cycle, `0` disables it entirely).
    ///
    /// That half needs an extra bounded L2P window pre-scan to find the live
    /// pinners a stripe window is stuck behind, and it runs on the gc-runner
    /// thread that mandatory reclaim shares. It is also the narrower of the two
    /// halves: on the box only 1.3% of its selected candidates were actually
    /// rewritten within the budget, so it over-produced against its consumer
    /// (memory: `defrag_gc_lever`). Downshifting it keeps the capability for the
    /// windows nothing else can reach — permanently live-pinned ones — at an
    /// eighth of the cycle cost.
    #[serde(default = "default_defrag_scan_interval_cycles")]
    pub defrag_scan_interval_cycles: u64,
    /// Wall-clock cap on the compactor's defrag selection loop, milliseconds
    /// (default 200; `0` = uncapped). Independent of, and additive with,
    /// `COMPACTOR_REWRITE_TIMEBOX_MS`: this bounds SELECTION (classify +
    /// quarantine lock trips), that one bounds the rewrites. Both exist so one
    /// oversized batch cannot eat the cycle reclaim needs.
    #[serde(default = "default_defrag_scan_timebox_ms")]
    pub defrag_scan_timebox_ms: u64,
    /// Cancel a target that has made no physical-free progress for this many
    /// seconds (default 120). Cancellation only releases its quarantined free
    /// fragments; live mappings remain authoritative, so it is always safe.
    /// The old 3600 was sized for "quarantine the window and wait for its pin
    /// to die on its own"; under scan-driven selection a target's pinners are
    /// rewritten in the same cycle it is picked, so its whole lifetime is
    /// rewrite + a few reclaim cycles. Re-selection is now cheap, so releasing
    /// a stuck target's free blocks back to the pool beats holding them.
    #[serde(default = "default_defrag_target_stall_secs")]
    pub defrag_target_stall_secs: u64,
    /// Blocks of defrag candidates rewritten per GC cycle at full effort
    /// (default 32_768 = 128 MiB ≈ 25 MB/s idle movement), scaled by the
    /// per-cycle `effort`. Sized so the sequential rewrite loop (one
    /// synchronous LV3 read per unit) stays well under the cycle interval;
    /// independent of (additive with) `max_rewrite_per_cycle`.
    #[serde(default = "default_defrag_max_rewrite_blocks_per_cycle")]
    pub defrag_max_rewrite_blocks_per_cycle: u64,
    /// Effort floor while defrag mode is latched (default 0.15). Without it
    /// the fragmented steady state (buffer full AND free% > 50 → effort < 0.01)
    /// idles the compactor exactly when defrag is needed. Only applies while
    /// the trigger is latched; the urgency/idle formula is untouched.
    #[serde(default = "default_defrag_min_effort")]
    pub defrag_min_effort: f64,
    /// Skip defrag REWRITES while the write buffer is at or above this fill
    /// percent (default 90). Relocation goes through the ordinary write path,
    /// so it competes for ring space; an unbounded rewrite loop on a full ring
    /// starved reclaim on the same GC thread and wedged the engine once
    /// (2026-07-03, fixed by the still-present rewrite timebox).
    ///
    /// ⚠ Load-bearing relationship with `defrag_min_effort`: that floor only
    /// changes the outcome when `compute_effort < 0.15`, i.e. when
    /// `fill_pct > 0.85 * buffer_usage_max_pct` (68 at the defaults). The old
    /// hardcoded 70 therefore blocked every defrag rewrite in all but a
    /// 2-point sliver of the exact regime the floor exists to serve — the two
    /// gates cancelled each other. Keep this ABOVE that crossover.
    #[serde(default = "default_defrag_fill_stop_pct")]
    pub defrag_fill_stop_pct: u8,

    // --- Adaptive reclaim heat map (Stage A: observe-only) ---
    /// Master switch for the background PBA heat-map refresh (default true).
    /// Observe-only in Stage A: builds the map but changes no reclaim
    /// behaviour. Set false to skip the refresh and allocate no bucket array.
    #[serde(default = "default_heat_enabled")]
    pub heat_enabled: bool,
    /// Blocks per heat bucket (default 256 = 1 MiB at 4 KiB blocks). Rounded
    /// up to the next power of two. Sizes the array: `total_blocks / this * 8 B`
    /// (~176 MiB for a 21 TiB device at 256). Read once when the map is built.
    #[serde(default = "default_heat_bucket_size_blocks")]
    pub heat_bucket_size_blocks: u64,
    /// Live blockmap entries the background refresh walks per GC cycle
    /// (default 1_000_000). `0` disables the refresh. Bounds the per-cycle
    /// cost and, with the volume set's total LBA count, the worst-case
    /// region-revisit interval (the staleness floor).
    #[serde(default = "default_heat_refresh_max_lbas_per_cycle")]
    pub heat_refresh_max_lbas_per_cycle: u64,

    // --- Stage-B: reclaim consumes the heat map (behavior change) ---
    /// Master switch for reclaim *consuming* the heat map as a prior to defer
    /// the confirm scan of retired extents whose region still looks hot
    /// (default FALSE — Stage B is a behavior change, A/B-prove before flip).
    /// Requires `heat_enabled` (a real, refreshed map) to have any effect.
    #[serde(default = "default_heat_reclaim_enabled")]
    pub heat_reclaim_enabled: bool,
    /// Max region age (completed sweeps since last counted) still considered
    /// "fresh" enough to defer a retired extent on (default 1 = only this/last
    /// sweep). A region older than this is confirmed-by-scan, so a mass-discard
    /// that stops bumping a region drains it within a sweep or two.
    #[serde(default = "default_heat_fresh_max_age")]
    pub heat_fresh_max_age: u32,
    /// Every Nth GC cycle, reclaim force-confirms ALL retired survivors
    /// regardless of heat — a belt-and-suspenders periodic full check so no
    /// deferred extent is starved (default 64; 0 disables the periodic pass).
    #[serde(default = "default_heat_force_confirm_interval_cycles")]
    pub heat_force_confirm_interval_cycles: u64,
    /// Reserved for Stage-B2 adaptive refresh cadence (refresh scans hot
    /// regions less often): the hard revisit floor that forces a rescan of a
    /// region not counted in this many sweeps. Unused while the refresh is
    /// uniform (every region is counted every sweep, so age never grows for a
    /// live region) — kept so the knob is stable across the B→B2 step.
    #[serde(default = "default_heat_staleness_floor_sweeps")]
    pub heat_staleness_floor_sweeps: u32,

    // --- Stage-B yield gate (self-correct the "hot ⇒ defer" heuristic) ---
    /// If the recent confirm-scan reclaim YIELD (reclaimed extents / scanned
    /// extents, EMA) is ≥ this percent, STOP deferring hot regions this window:
    /// a productive scan means the "hot region ⇒ extent still referenced ⇒ scan
    /// wasted" premise is FALSE for this workload (e.g. unique/discard churn,
    /// where retired extents are genuinely dead), so deferring would only lose
    /// real reclaim. Low yield (e.g. dedup-heavy, where retired rc==0 extents
    /// are often still referenced via an un-drained re-share) keeps deferring —
    /// that is where skipping the scan actually pays. Default 25.
    #[serde(default = "default_heat_defer_yield_suppress_pct")]
    pub heat_defer_yield_suppress_pct: u8,
    /// Every Nth cycle, confirm-all to (re)measure the confirm-scan yield even
    /// while deferring — bounds the cold-start / workload-change blind-defer
    /// window to this many cycles (default 8; 0 disables periodic recalibration,
    /// leaving only the force-confirm pass to sample yield). Distinct from
    /// `heat_force_confirm_interval_cycles` (anti-starvation, coarser).
    #[serde(default = "default_heat_defer_recalibrate_interval_cycles")]
    pub heat_defer_recalibrate_interval_cycles: u64,
    /// If allocator free-space ≤ this percent, STOP deferring (confirm-all) so
    /// reclaim is not delayed under space pressure — the retired-reclaim analog
    /// of the rewrite-GC `dynamic_threshold`. Default 10; 0 disables the gate.
    #[serde(default = "default_heat_defer_min_free_pct")]
    pub heat_defer_min_free_pct: u8,

    // --- Stage-B2: adaptive (per-volume) refresh budget ---
    /// Master switch for biasing the heat-refresh budget toward high-churn
    /// volumes (scan changing volumes more, stable ones less), with
    /// `heat_staleness_floor_sweeps` guaranteeing every volume is fully covered
    /// at least that often regardless of churn. Default FALSE — behavior change,
    /// A/B before flip; no effect with a single volume (degrades to uniform).
    #[serde(default = "default_heat_adaptive_refresh_enabled")]
    pub heat_adaptive_refresh_enabled: bool,

    // --- Stage-4: fold dedup cold-tail warming into the heat-refresh walk ---
    /// Master switch for folding the dedup cold-tail scan into the GC
    /// heat-refresh walk (`docs/adaptive-reclaim-heatmap.md` Stage 4). When ON,
    /// the heat walk (already decoding every non-zero `BlockmapValue`) also
    /// emits cold candidates over a bounded channel; the dedup scanner drains
    /// that channel instead of running its own independent
    /// `scan_blockmap_range` traversal. Eliminates the duplicate live-L2P walk
    /// and lets cold-tail warming ride the heat walk's fast, churn-weighted
    /// coverage at unchanged LV3 read IO. Default FALSE — behavior change,
    /// A/B before flip. Requires `heat_enabled` (the walk must exist) AND a
    /// configured `ReadPool` AND `dedup.enabled`; otherwise the fold is inert
    /// and both subsystems run their legacy paths. Read once at engine open
    /// (toggling needs a restart).
    #[serde(default = "default_heat_fold_cold_tail_enabled")]
    pub heat_fold_cold_tail_enabled: bool,
    /// Bounded capacity of the cold-tail fold channel (default 2048). Producer
    /// `try_send`s and drops on full, so this only bounds how many recent cold
    /// candidates the consumer can choose from; dropping costs dedup ratio,
    /// never correctness.
    #[serde(default = "default_heat_fold_channel_capacity")]
    pub heat_fold_channel_capacity: usize,
    /// Max cold candidates the heat walk pushes per refresh cycle (default
    /// 256). Bounds producer work + channel refill rate. The consumer's own
    /// `dedup.cold_tail_max_per_cycle` still bounds the (unchanged) LV3 read
    /// IO independently. `0` disables pushing (heat walk runs, no fold).
    #[serde(default = "default_heat_fold_push_max_per_cycle")]
    pub heat_fold_push_max_per_cycle: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            scan_interval_ms: default_scan_interval_ms(),
            dead_ratio_threshold: default_dead_ratio_threshold(),
            buffer_usage_max_pct: default_buffer_usage_max_pct(),
            buffer_usage_resume_pct: default_buffer_usage_resume_pct(),
            max_rewrite_per_cycle: default_max_rewrite_per_cycle(),
            reclaim_grace_secs: default_reclaim_grace_secs(),
            compactor_resident_threshold: default_compactor_resident_threshold(),
            compactor_scan_max_lbas_per_cycle: default_compactor_scan_max_lbas_per_cycle(),
            compactor_slot_evac_enabled: default_compactor_slot_evac_enabled(),
            compactor_slot_evac_max_live: default_compactor_slot_evac_max_live(),
            defrag_enabled: default_defrag_enabled(),
            defrag_stripe_capable_min_pct: default_defrag_stripe_capable_min_pct(),
            defrag_min_free_pct: default_defrag_min_free_pct(),
            defrag_min_free_density_pct: default_defrag_min_free_density_pct(),
            defrag_max_target_blocks: default_defrag_max_target_blocks(),
            defrag_interval_ms: default_defrag_interval_ms(),
            defrag_trigger_interval_ms: default_defrag_trigger_interval_ms(),
            defrag_classify_max_windows_per_cycle: default_defrag_classify_max_windows_per_cycle(),
            defrag_scan_interval_cycles: default_defrag_scan_interval_cycles(),
            defrag_scan_timebox_ms: default_defrag_scan_timebox_ms(),
            defrag_target_stall_secs: default_defrag_target_stall_secs(),
            defrag_max_rewrite_blocks_per_cycle: default_defrag_max_rewrite_blocks_per_cycle(),
            defrag_min_effort: default_defrag_min_effort(),
            defrag_fill_stop_pct: default_defrag_fill_stop_pct(),
            heat_enabled: default_heat_enabled(),
            heat_bucket_size_blocks: default_heat_bucket_size_blocks(),
            heat_refresh_max_lbas_per_cycle: default_heat_refresh_max_lbas_per_cycle(),
            heat_reclaim_enabled: default_heat_reclaim_enabled(),
            heat_fresh_max_age: default_heat_fresh_max_age(),
            heat_force_confirm_interval_cycles: default_heat_force_confirm_interval_cycles(),
            heat_staleness_floor_sweeps: default_heat_staleness_floor_sweeps(),
            heat_defer_yield_suppress_pct: default_heat_defer_yield_suppress_pct(),
            heat_defer_recalibrate_interval_cycles: default_heat_defer_recalibrate_interval_cycles(
            ),
            heat_defer_min_free_pct: default_heat_defer_min_free_pct(),
            heat_adaptive_refresh_enabled: default_heat_adaptive_refresh_enabled(),
            heat_fold_cold_tail_enabled: default_heat_fold_cold_tail_enabled(),
            heat_fold_channel_capacity: default_heat_fold_channel_capacity(),
            heat_fold_push_max_per_cycle: default_heat_fold_push_max_per_cycle(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
fn default_scan_interval_ms() -> u64 {
    5000
}
fn default_dead_ratio_threshold() -> f64 {
    0.25
}
fn default_buffer_usage_max_pct() -> u8 {
    80
}
fn default_buffer_usage_resume_pct() -> u8 {
    50
}
fn default_max_rewrite_per_cycle() -> usize {
    64
}
fn default_reclaim_grace_secs() -> u64 {
    // 30s settle window. The hazard barrier + Gate-2 fold-consistent rc recheck
    // are the real premature-free safety; the grace is a belt-and-suspenders
    // window (the actual race is microseconds). 300s was overkill and, combined
    // with the old coalesce re-aging, starved reclaim (hardware: grace 300→10
    // lifted reclaim conversion 0.4%→16.5%, crc=0). The per-original-retire age
    // log makes any grace value safe from re-aging; 30s keeps a comfortable
    // margin without holding the retired backlog hostage.
    30
}
fn default_compactor_resident_threshold() -> f64 {
    0.85
}
fn default_compactor_scan_max_lbas_per_cycle() -> u64 {
    1_000_000
}
fn default_compactor_slot_evac_enabled() -> bool {
    false
}
fn default_compactor_slot_evac_max_live() -> u16 {
    16
}
fn default_defrag_enabled() -> bool {
    // Default OFF since 2026-08-12. Publishing a quarantined window is the only
    // path that hands a whole stripe back to the allocatable pool without a
    // per-block proof, and on the box it published a window that still held live
    // blocks: 20 hours of clean running, first `defrag published stripes` at
    // 04:51:36, first foreground `CRC mismatch` at 05:02:21 — 10.75 minutes
    // later — then 476 double-claimed blocks whose consecutive runs each sat
    // inside ONE stripe-aligned window (memory: `defrag_stripe_publish_crc_p0`).
    // `complete_defrag_quarantine`'s gate is structural now, so the specific
    // publish is blocked, but the upstream accounting drift that made the window
    // look complete is NOT root-caused yet. Selection also measured ZERO benefit
    // on the box (`gc_reclaim_defrag_criticality_inversion`), so there is nothing
    // to trade against the residual risk. Re-enable per-config to test it.
    false
}
fn default_defrag_stripe_capable_min_pct() -> u8 {
    30
}
fn default_defrag_min_free_pct() -> u8 {
    5
}
fn default_defrag_min_free_density_pct() -> u8 {
    50
}
fn default_defrag_max_target_blocks() -> u64 {
    32_768 // 128 MiB at 4 KiB blocks
}
fn default_defrag_interval_ms() -> u64 {
    1000
}
fn default_defrag_trigger_interval_ms() -> u64 {
    5000 // the cadence contiguity_stats ran at as a GC phase
}
fn default_defrag_classify_max_windows_per_cycle() -> usize {
    4096
}
fn default_defrag_scan_interval_cycles() -> u64 {
    8
}
fn default_defrag_scan_timebox_ms() -> u64 {
    200
}
fn default_defrag_target_stall_secs() -> u64 {
    120
}
fn default_defrag_max_rewrite_blocks_per_cycle() -> u64 {
    32_768 // 128 MiB/cycle at full effort ≈ 25 MB/s movement
}
fn default_defrag_min_effort() -> f64 {
    0.15
}
fn default_defrag_fill_stop_pct() -> u8 {
    90
}
fn default_heat_enabled() -> bool {
    true
}
fn default_heat_bucket_size_blocks() -> u64 {
    256 // 1 MiB at 4 KiB blocks
}
fn default_heat_refresh_max_lbas_per_cycle() -> u64 {
    1_000_000
}
fn default_heat_reclaim_enabled() -> bool {
    false
}
fn default_heat_staleness_floor_sweeps() -> u32 {
    4
}
fn default_heat_fresh_max_age() -> u32 {
    1
}
fn default_heat_force_confirm_interval_cycles() -> u64 {
    64
}
fn default_heat_defer_yield_suppress_pct() -> u8 {
    25
}
fn default_heat_defer_recalibrate_interval_cycles() -> u64 {
    8
}
fn default_heat_defer_min_free_pct() -> u8 {
    10
}
fn default_heat_adaptive_refresh_enabled() -> bool {
    false
}
fn default_heat_fold_cold_tail_enabled() -> bool {
    false
}
fn default_heat_fold_channel_capacity() -> usize {
    2048
}
fn default_heat_fold_push_max_per_cycle() -> usize {
    256
}
