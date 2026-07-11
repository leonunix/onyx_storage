//! LV2 write-buffer on a chunklet RAID10 LD — the Phase-2 durability contract.
//!
//! These tests stand up a `WriteBufferPool` directly over a chunklet
//! `LogicalDisk` (no full engine), exercising the sync thread's
//! `write_many_at` + `flush` ack-after-durable path and crash recovery from the
//! ring through the chunklet read path. The LD owns its cross-PD io_uring
//! internally, so onyx passes `uring_sq_entries=None` and the sync loop takes
//! the synchronous batched path.
//!
//! A single chunklet is 1 GiB, so the smallest mirror LD is ~1 GiB; we build
//! only an LV2 LD (a 2-way mirror) rather than the full LV3+LV2+meta trio so the
//! test stays cheap on sparse files.

use std::sync::Arc;

use onyx_chunklet::io::{IoBackendKind, RawDevice as CkRaw};
use onyx_chunklet::pool::LdSpec;
use onyx_chunklet::{Pool, PoolConfig};

use onyx_storage::buffer::pool::{BufferRuntimeLimits, WriteBufferPool};
use onyx_storage::io::block_backend::{BlockBackend, ChunkletBackend};
use onyx_storage::types::{Lba, BLOCK_SIZE};

/// Spin up a pool over `n` sparse PDs and create one RAID10 (2-way mirror) LV2
/// LD. Returns the pool and the LD id so callers can re-resolve it after a
/// simulated crash (drop + reopen).
fn lv2_pool(dir: &std::path::Path, n: usize) -> (Arc<Pool>, onyx_chunklet::types::LdId) {
    let mut raws = Vec::with_capacity(n);
    for i in 0..n {
        let p = dir.join(format!("pd{i}"));
        raws.push(CkRaw::open_or_create(&p, 4 << 30).unwrap());
    }
    let pool = Pool::create(
        raws,
        PoolConfig {
            spare_pct: 0,
            io_backend: IoBackendKind::Sync,
        },
    )
    .unwrap();
    // mirror(copies=2, row_size=1, num_rows=1, strip_log2=0): a single 1 GiB
    // mirrored chunklet — minimal LV2 footprint.
    let lv2 = pool.create_ld(LdSpec::mirror(2, 1, 1, 0)).unwrap();
    (pool, lv2)
}

fn backend_for(pool: &Arc<Pool>, lv2: onyx_chunklet::types::LdId) -> Arc<dyn BlockBackend> {
    let ld = pool.open_ld(lv2).unwrap();
    Arc::new(ChunkletBackend::with_pool(ld, pool.clone()))
}

fn open_buffer(device: Arc<dyn BlockBackend>) -> WriteBufferPool {
    WriteBufferPool::open_with_options_full_and_limits(
        device,
        std::time::Duration::from_micros(50),
        1, // shards
        256,
        std::time::Duration::ZERO,
        0,    // max_payload_memory=0 → force lazy hydration from the LD
        None, // uring_sq_entries=None → chunklet write_many_at + flush path
        BufferRuntimeLimits::default(),
    )
    .unwrap()
}

fn block(fill: u8) -> Vec<u8> {
    vec![fill; BLOCK_SIZE as usize]
}

/// append() must not return until the entry is crash-durable on the chunklet LD
/// (the sync thread did write_many_at + flush + advanced the watermark), and the
/// payload must read back through the chunklet read path.
#[test]
fn chunklet_lv2_append_is_durable_and_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, lv2) = lv2_pool(dir.path(), 4);
    let buffer = open_buffer(backend_for(&pool, lv2));

    let n = 8u64;
    let mut last_seq = None;
    for i in 0..n {
        let payload = block(0x40 + i as u8);
        let seq = buffer.append("ckbuf", Lba(i), 1, &payload, 1).unwrap();
        // ack-after-durable: append only returns once the sync thread flushed
        // this seq to the LD. Seqs are strictly increasing.
        if let Some(prev) = last_seq {
            assert!(seq > prev, "seqs strictly increasing");
        }
        last_seq = Some(seq);
    }
    assert_eq!(
        buffer.pending_count(),
        n,
        "nothing flushed yet → all pending"
    );

    // Read each back (payload cache is disabled → hydrates from the LD).
    for i in 0..n {
        let hit = buffer
            .lookup("ckbuf", Lba(i))
            .unwrap()
            .expect("entry present");
        let payload = hit.payload.expect("payload hydrated from chunklet LD");
        assert_eq!(payload.as_ref(), block(0x40 + i as u8).as_slice());
    }

    drop(buffer);
    drop(pool);
}

/// Drop the pool (simulated crash: no graceful flush) and reopen over the SAME
/// LD. The ring is the only durable record of those appends, so recovery must
/// rebuild every pending entry by scanning the chunklet LD, and lazy hydration
/// must pull each payload back through the chunklet read path.
#[test]
fn chunklet_lv2_recovers_pending_ring_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let (pool, lv2) = lv2_pool(dir.path(), 4);

    let n = 6u64;
    {
        let buffer = open_buffer(backend_for(&pool, lv2));
        for i in 0..n {
            let payload = block(0xA0 + i as u8);
            buffer.append("recov", Lba(i), 1, &payload, 7).unwrap();
        }
        assert_eq!(buffer.pending_count(), n);
        // Simulate a crash: drop the buffer pool WITHOUT draining/flushing. The
        // sync threads stop, but every acked append is already durable on the LD.
        drop(buffer);
    }

    // Reopen over a freshly resolved handle to the same LD.
    let reopened = open_buffer(backend_for(&pool, lv2));
    assert_eq!(
        reopened.pending_count(),
        n,
        "all pending entries recovered from the chunklet ring"
    );
    for i in 0..n {
        let hit = reopened
            .lookup("recov", Lba(i))
            .unwrap()
            .expect("recovered entry present after crash");
        let payload = hit
            .payload
            .expect("payload hydrated from chunklet LD post-crash");
        assert_eq!(payload.as_ref(), block(0xA0 + i as u8).as_slice());
    }

    drop(reopened);
    drop(pool);
}
