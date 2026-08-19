//! Memory scheduling for the hot data path.
//!
//! Onyx's steady state hands the same few buffer shapes back and forth between a
//! fixed set of threads, thousands of times per second. Routing that through the
//! general-purpose allocator costs measurably more than the work it protects:
//! per-buffer `mmap` + `alloc_zeroed`, a redundant `fill(0)`, and — when the
//! allocation is released — `madvise(MADV_DONTNEED)` and its cross-CPU TLB
//! shootdown. On the box, the LV3 flush writer spent **21.1 ms of every 59.66 ms
//! `io` leg** (35.4 %) on exactly that, all of it provably redundant.
//!
//! This module owns the replacement:
//!
//! - [`SlabArena`] — per-lane, size-classed, pre-faulted, grow-only slot pool.
//!   Take is a pop, release is a push, and the handle carries an `Arc` so a
//!   release is correct on any thread.
//! - [`SlabFill`] — cover-or-zero buffer assembly, so dropping the blanket
//!   `fill(0)` cannot leak a recycled slot's previous content to disk.
//! - [`MemRegistry`] — one owner per engine, keyed by `(role, lane)`.
//!
//! Consumers wired up so far: the LV3 flush writer's run and unit buffers
//! (`buffer::flush::writer::passthrough`). Everything else still uses the heap;
//! new consumers are expected to arrive with their own measurement, not on
//! principle.

mod arena;
#[cfg(test)]
mod bench;
mod classes;
mod fill;
mod registry;

pub use arena::SlabArena;
pub use classes::ClassTable;
pub use fill::SlabFill;
pub use registry::{MemRegistry, MemRole, MemTotals};

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Resident ceiling per lane. Arenas grow on demand and never shrink, so this is
/// a ceiling and not a reservation: a warm LV3 writer lane measured a 7.2 MiB
/// working set, and 16 lanes at this cap is 1 GiB on a 250 GiB box. Past the cap
/// a take falls back to the heap and bumps `mem_arena.overflow` — a cap is never
/// a write failure.
const DEFAULT_ARENA_BYTES_PER_LANE: usize = 64 * 1024 * 1024;

/// Largest request served from an arena, in 4 KiB blocks. 64 = 256 KiB, which
/// covers a full stripe (6 blocks here) and the largest possible compressed unit
/// (`flush.coalesce_max_raw_bytes` 128 KiB = 32 blocks) with room to spare.
const DEFAULT_ARENA_MAX_CLASS_BLOCKS: u32 = 64;

// Runtime-overridable so an arm can be run without a rebuild, matching
// `io::engine::set_lv3_batch_tuning`. `0` keeps the compiled default.
static ARENA_ENABLED: AtomicBool = AtomicBool::new(true);
static ARENA_MAX_BYTES_PER_LANE: AtomicUsize = AtomicUsize::new(0);
static ARENA_MAX_CLASS_BLOCKS: AtomicUsize = AtomicUsize::new(0);
static ARENA_HUGEPAGE: AtomicBool = AtomicBool::new(false);

/// Apply the `[mem]` config section. Applies to arenas created afterwards, i.e.
/// to subsequently started engines.
pub fn set_mem_tuning(
    arena_enabled: bool,
    max_bytes_per_lane: usize,
    max_class_blocks: usize,
    hugepage: bool,
) {
    ARENA_ENABLED.store(arena_enabled, Ordering::Relaxed);
    ARENA_MAX_BYTES_PER_LANE.store(max_bytes_per_lane, Ordering::Relaxed);
    ARENA_MAX_CLASS_BLOCKS.store(max_class_blocks, Ordering::Relaxed);
    ARENA_HUGEPAGE.store(hugepage, Ordering::Relaxed);
}

/// Whether arena-backed buffers are in force **right now**.
///
/// Read at every allocation rather than captured at startup, so the A/B can
/// alternate inside ONE process. That is not a preference: on the perf box an
/// arm-per-restart comparison measures run-order drift, not the knob — two
/// identical baseline arms once came out 2.13x apart (119.2 vs 253.3 MB/s), see
/// memory `allocator_global_lock_is_the_writer_wall`. The load is a relaxed
/// atomic against a ~90 ns arena take, and it is on the same path that used to
/// cost 35-50 µs.
pub fn arena_enabled() -> bool {
    ARENA_ENABLED.load(Ordering::Relaxed)
}

/// Flip arena-backed buffers on/off in a running engine (IPC `mem-arena on|off`).
///
/// Safe at any moment: the two paths differ only in where a buffer's memory came
/// from, and each buffer's provenance travels with it, so buffers allocated under
/// one setting are released correctly after a flip. Turning it off leaves the
/// arenas mapped and idle — flipping back needs no re-fault.
pub fn set_arena_enabled(enabled: bool) {
    ARENA_ENABLED.store(enabled, Ordering::Relaxed);
}

fn arena_max_bytes_per_lane() -> usize {
    match ARENA_MAX_BYTES_PER_LANE.load(Ordering::Relaxed) {
        0 => DEFAULT_ARENA_BYTES_PER_LANE,
        bytes => bytes,
    }
}

/// Raise the largest arena-served request to at least `blocks`, never lowering it.
///
/// Used by `flush.stripe_run_max_stripes` (design D1): a bundle buffer is wider than
/// any single stripe, and a request past the top class falls back to the heap. The
/// cap is a class-table bound, not a reservation — `mem.arena_max_bytes_per_lane`
/// still bounds resident bytes, so widening the table costs nothing until a wide
/// buffer is actually asked for.
pub fn raise_arena_max_class_blocks(blocks: u32) {
    let blocks = usize::from(blocks != 0) * blocks as usize;
    ARENA_MAX_CLASS_BLOCKS
        .fetch_max(blocks.max(DEFAULT_ARENA_MAX_CLASS_BLOCKS as usize), Ordering::Relaxed);
}

fn arena_max_class_blocks() -> u32 {
    match ARENA_MAX_CLASS_BLOCKS.load(Ordering::Relaxed) {
        0 => DEFAULT_ARENA_MAX_CLASS_BLOCKS,
        blocks => blocks.min(u32::MAX as usize) as u32,
    }
}

/// `MADV_HUGEPAGE` on arena regions. Default off: it is a separate arm, because
/// THP can introduce allocation stalls that would be charged to whatever else is
/// being measured.
fn arena_hugepage() -> bool {
    ARENA_HUGEPAGE.load(Ordering::Relaxed)
}
