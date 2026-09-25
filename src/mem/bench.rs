//! Four-arm microbenchmark for the LV3 write-buffer change.
//!
//! Reproduces one writer cycle's buffer shape in-process so the two independent
//! halves of the change can be attributed separately:
//!
//! - **allocation**: `AlignedBuf::new` (heap + the 8-slot thread-local parking
//!   lot) vs [`SlabArena::take`];
//! - **zeroing**: blanket `fill(0)` vs [`SlabFill`] cover-or-zero.
//!
//! The cycle shape is the measured one (box, 2026-08-17): 301 buffers of 24 KiB
//! (one RAID6 6+2 full stripe), 6 members each, **all live at once** and dropped
//! together — that last part is what pins the heap arm to its 8/301 = 2.7 % pool
//! hit-rate ceiling, and it is what the real writer does because it blocks in one
//! `submit_many` with every buffer outstanding.
//!
//! Run:
//! `cargo test --release --lib mem::bench -- --ignored --nocapture`
//!
//! ⚠ This is a CPU-side microbenchmark on one thread. It says nothing about
//! end-to-end throughput — the box arm decides that (see
//! `tools/lv3_run.sh`). It exists so the box arm can be read against a known
//! per-buffer cost instead of a guess.

use std::sync::Arc;
use std::time::Instant;

use super::{SlabArena, SlabFill};
use crate::io::aligned::AlignedBuf;
use crate::types::BLOCK_SIZE;

const BUFFERS_PER_CYCLE: usize = 301;
const MEMBERS_PER_BUFFER: usize = 6;
const CYCLES: usize = 40;

fn payload(len: usize) -> Vec<u8> {
    vec![0x5A; len]
}

/// One cycle: allocate every buffer, fill it, hold them all, then release.
/// Returns (alloc_ns, zero_ns, copy_ns).
fn run_cycle(
    arena: Option<&Arc<SlabArena>>,
    cover_or_zero: bool,
    payload_len: usize,
    src: &[u8],
) -> (u64, u64, u64) {
    let bs = BLOCK_SIZE as usize;
    let total = MEMBERS_PER_BUFFER * bs;
    let mut held: Vec<AlignedBuf> = Vec::with_capacity(BUFFERS_PER_CYCLE);
    let (mut alloc_ns, mut zero_ns, mut copy_ns) = (0u64, 0u64, 0u64);

    for _ in 0..BUFFERS_PER_CYCLE {
        let t = Instant::now();
        let mut buf = match arena {
            Some(arena) => arena.take(total).unwrap(),
            None => AlignedBuf::new(total, false).unwrap(),
        };
        alloc_ns += t.elapsed().as_nanos() as u64;

        if cover_or_zero {
            let t = Instant::now();
            let mut fill = SlabFill::new(buf.as_mut_slice());
            for m in 0..MEMBERS_PER_BUFFER {
                fill.region(m * bs, payload_len).copy_from_slice(src);
            }
            let gap = fill.finish();
            let elapsed = t.elapsed().as_nanos() as u64;
            // Copy and gap-zero are interleaved by construction; split them by
            // the byte volume each moved so the arms stay comparable.
            let copied = MEMBERS_PER_BUFFER * payload_len;
            let share = gap as f64 / (copied + gap) as f64;
            zero_ns += (elapsed as f64 * share) as u64;
            copy_ns += (elapsed as f64 * (1.0 - share)) as u64;
        } else {
            let t = Instant::now();
            buf.as_mut_slice().fill(0);
            zero_ns += t.elapsed().as_nanos() as u64;
            let t = Instant::now();
            for m in 0..MEMBERS_PER_BUFFER {
                buf.as_mut_slice()[m * bs..m * bs + payload_len].copy_from_slice(src);
            }
            copy_ns += t.elapsed().as_nanos() as u64;
        }
        held.push(buf);
    }
    drop(held);
    (alloc_ns, zero_ns, copy_ns)
}

fn arm(name: &str, arena: Option<&Arc<SlabArena>>, cover_or_zero: bool, payload_len: usize) {
    let src = payload(payload_len);
    // Warm up: the arena's growth and the heap's first faults are startup costs,
    // not steady state.
    run_cycle(arena, cover_or_zero, payload_len, &src);

    let (mut a, mut z, mut c) = (0u64, 0u64, 0u64);
    for _ in 0..CYCLES {
        let (da, dz, dc) = run_cycle(arena, cover_or_zero, payload_len, &src);
        a += da;
        z += dz;
        c += dc;
    }
    let takes = (CYCLES * BUFFERS_PER_CYCLE) as f64;
    println!(
        "  {:<28} alloc {:8.0} ns  zero {:8.0} ns  copy {:8.0} ns  total {:8.0} ns/buffer",
        name,
        a as f64 / takes,
        z as f64 / takes,
        c as f64 / takes,
        (a + z + c) as f64 / takes
    );
}

#[test]
#[ignore = "perf microbench"]
fn bench_lv3_write_buffer_arms() {
    let bs = BLOCK_SIZE as usize;
    // Big enough that the arena never falls back: 301 live 24 KiB buffers = 7.2
    // MiB, matching the measured per-lane working set.
    let arena = SlabArena::new(
        crate::mem::MemRole::Lv3Writer,
        32 * 1024 * 1024,
        64,
        false,
        None,
    );

    println!(
        "\n== LV3 write buffer, {} x {} KiB per cycle, all live at once ==",
        BUFFERS_PER_CYCLE,
        MEMBERS_PER_BUFFER * bs / 1024
    );
    println!("-- block-aligned payloads (incompressible input: zero gap) --");
    arm("heap + blanket fill(0)", None, false, bs);
    arm("heap + cover-or-zero", None, true, bs);
    arm("arena + blanket fill(0)", Some(&arena), false, bs);
    arm("arena + cover-or-zero", Some(&arena), true, bs);

    println!("-- 3000-byte payloads (compressible: 1096-byte gap per member) --");
    arm("heap + blanket fill(0)", None, false, 3000);
    arm("arena + cover-or-zero", Some(&arena), true, 3000);

    println!(
        "  arena resident {:.1} MiB, live slots {}",
        arena.resident_bytes() as f64 / 1048576.0,
        arena.live_slots()
    );
}
