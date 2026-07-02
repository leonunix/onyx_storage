//! DIAGNOSTIC ONLY — per-PBA lifecycle trace + rebuild reserved-set classifier.
//!
//! Gated behind `ONYX_PBA_TRACE=1`; zero-cost (one relaxed atomic load) when
//! off. Built for the post-restart replay CRC hunt (2026-07-02): every CRC
//! victim needs three facts to classify the failure —
//!
//! 1. **lifecycle**: has this PBA ever been freed / retired / reclaimed /
//!    re-allocated since engine start? A `free_count > 0` or `alloc_count > 1`
//!    trail on a live-mapped PBA is the onyx-side premature-free signature.
//!    A single-alloc-no-free trail whose data still mismatches means the
//!    *mapping* is wrong (metadb read-side corruption), not the block.
//! 2. **reserved-set membership**: was the PBA reserved at the last
//!    `rebuild_from_metadata`, and from which source (blockmap vs dedup_index)?
//!    `(false, false)` on a victim allocated before restart = a rebuild gap;
//!    `true` = the rebuild covered it and something ran over it afterwards.
//! 3. (paired externally with `ONYX_TRACE_LV3_WRITES` write records: who last
//!    wrote the physical block.)
//!
//! Revert this file (and its call sites) before shipping; it is deliberately
//! self-contained so the diff stays one-directional.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::space::extent::Extent;
use crate::types::Pba;

const SHARDS: usize = 64;
/// Classifier bitmap capacity in blocks (1 << 28 blocks = 1 TiB of 4K LV3).
/// PBAs past this are counted but not classified — fine for the repro configs.
const CLASSIFIER_BITS: usize = 1 << 28;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ENABLED_INIT: AtomicBool = AtomicBool::new(false);
static GSEQ: AtomicU64 = AtomicU64::new(0);

pub fn enabled() -> bool {
    if !ENABLED_INIT.load(Ordering::Acquire) {
        let on = std::env::var("ONYX_PBA_TRACE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        ENABLED.store(on, Ordering::Release);
        ENABLED_INIT.store(true, Ordering::Release);
        if on {
            tracing::warn!("ONYX_PBA_TRACE=1: per-PBA lifecycle trace + reserved classifier ON");
        }
    }
    ENABLED.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PbaTrace {
    pub last_alloc: Option<(u64, &'static str)>,
    pub last_free: Option<(u64, &'static str)>,
    pub last_retire: Option<(u64, &'static str)>,
    pub last_reclaim: Option<(u64, &'static str)>,
    pub alloc_count: u32,
    pub free_count: u32,
    pub retire_count: u32,
    pub reclaim_count: u32,
}

enum Kind {
    Alloc,
    Free,
    Retire,
    Reclaim,
}

struct TraceTable {
    shards: Vec<Mutex<HashMap<u64, PbaTrace>>>,
}

static TABLE: OnceLock<TraceTable> = OnceLock::new();

fn table() -> &'static TraceTable {
    TABLE.get_or_init(|| TraceTable {
        shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
    })
}

fn record(extent: Extent, ctx: &'static str, kind: Kind) {
    if !enabled() {
        return;
    }
    let gseq = GSEQ.fetch_add(1, Ordering::Relaxed) + 1;
    let t = table();
    for off in 0..extent.count {
        let pba = extent.start.0 + off as u64;
        let mut shard = t.shards[(pba as usize) % SHARDS].lock().unwrap();
        let e = shard.entry(pba).or_default();
        match kind {
            Kind::Alloc => {
                e.last_alloc = Some((gseq, ctx));
                e.alloc_count = e.alloc_count.saturating_add(1);
            }
            Kind::Free => {
                e.last_free = Some((gseq, ctx));
                e.free_count = e.free_count.saturating_add(1);
            }
            Kind::Retire => {
                e.last_retire = Some((gseq, ctx));
                e.retire_count = e.retire_count.saturating_add(1);
            }
            Kind::Reclaim => {
                e.last_reclaim = Some((gseq, ctx));
                e.reclaim_count = e.reclaim_count.saturating_add(1);
            }
        }
    }
}

pub fn trace_alloc(extent: Extent, ctx: &'static str) {
    record(extent, ctx, Kind::Alloc);
}

pub fn trace_free(extent: Extent, ctx: &'static str) {
    record(extent, ctx, Kind::Free);
}

pub fn trace_retire(extent: Extent, ctx: &'static str) {
    record(extent, ctx, Kind::Retire);
}

pub fn trace_reclaim(extent: Extent, ctx: &'static str) {
    record(extent, ctx, Kind::Reclaim);
}

pub fn lookup(pba: Pba) -> Option<PbaTrace> {
    if !enabled() {
        return None;
    }
    let t = table();
    let shard = t.shards[(pba.0 as usize) % SHARDS].lock().unwrap();
    shard.get(&pba.0).copied()
}

/// One-line human-readable trail for CRC-site logging.
/// `"alloc#2@g1234(stripe_cache) free#1@g1100(free_extent) retire#0 reclaim#0 | reserved=bm:N,dd:Y"`.
/// Returns `None` when the trace is disabled (log sites stay quiet).
pub fn describe_pba(pba: Pba) -> Option<String> {
    if !enabled() {
        return None;
    }
    let t = lookup(pba).unwrap_or_default();
    let fmt_slot = |name: &str, count: u32, slot: Option<(u64, &'static str)>| match slot {
        Some((g, ctx)) => format!("{name}#{count}@g{g}({ctx})"),
        None => format!("{name}#{count}"),
    };
    let (bm, dd) = reserved_bits(pba);
    Some(format!(
        "{} {} {} {} | reserved=bm:{},dd:{}",
        fmt_slot("alloc", t.alloc_count, t.last_alloc),
        fmt_slot("free", t.free_count, t.last_free),
        fmt_slot("retire", t.retire_count, t.last_retire),
        fmt_slot("reclaim", t.reclaim_count, t.last_reclaim),
        if bm { "Y" } else { "N" },
        if dd { "Y" } else { "N" },
    ))
}

// ---------------------------------------------------------------------------
// Rebuild reserved-set classifier: which source(s) reserved each PBA during
// the most recent `iter_allocated_blocks` scan (blockmap∪l2p_buffer vs
// dedup_index). Reset at the start of every scan so the bitmaps always
// reflect the latest rebuild snapshot.
// ---------------------------------------------------------------------------

struct ClassifierBitmaps {
    blockmap: Vec<AtomicU64>,
    dedup: Vec<AtomicU64>,
    overflow: AtomicU64,
}

static CLASSIFIER: OnceLock<ClassifierBitmaps> = OnceLock::new();

fn classifier() -> &'static ClassifierBitmaps {
    CLASSIFIER.get_or_init(|| {
        let words = CLASSIFIER_BITS / 64;
        let mut blockmap = Vec::with_capacity(words);
        let mut dedup = Vec::with_capacity(words);
        blockmap.resize_with(words, || AtomicU64::new(0));
        dedup.resize_with(words, || AtomicU64::new(0));
        ClassifierBitmaps {
            blockmap,
            dedup,
            overflow: AtomicU64::new(0),
        }
    })
}

pub fn classifier_reset() {
    if !enabled() {
        return;
    }
    let c = classifier();
    for w in &c.blockmap {
        w.store(0, Ordering::Relaxed);
    }
    for w in &c.dedup {
        w.store(0, Ordering::Relaxed);
    }
    c.overflow.store(0, Ordering::Relaxed);
}

fn mark(bits: &[AtomicU64], overflow: &AtomicU64, pba: Pba) {
    let idx = pba.0 as usize;
    if idx >= CLASSIFIER_BITS {
        overflow.fetch_add(1, Ordering::Relaxed);
        return;
    }
    bits[idx / 64].fetch_or(1u64 << (idx % 64), Ordering::Relaxed);
}

pub fn mark_reserved_blockmap(pba: Pba) {
    if !enabled() {
        return;
    }
    let c = classifier();
    mark(&c.blockmap, &c.overflow, pba);
}

pub fn mark_reserved_dedup(pba: Pba) {
    if !enabled() {
        return;
    }
    let c = classifier();
    mark(&c.dedup, &c.overflow, pba);
}

/// `(reserved_blockmap, reserved_dedup)` membership at the most recent scan.
pub fn reserved_bits(pba: Pba) -> (bool, bool) {
    if !enabled() {
        return (false, false);
    }
    let idx = pba.0 as usize;
    if idx >= CLASSIFIER_BITS {
        return (false, false);
    }
    let c = classifier();
    let word = idx / 64;
    let bit = 1u64 << (idx % 64);
    (
        c.blockmap[word].load(Ordering::Relaxed) & bit != 0,
        c.dedup[word].load(Ordering::Relaxed) & bit != 0,
    )
}
