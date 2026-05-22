use super::*;

#[test]
fn compress_loop_bypasses_low_savings_units() {
    let metrics = EngineMetrics::default();
    let running = AtomicBool::new(true);
    let (tx, rx) = bounded::<CoalesceUnit>(1);
    let (out_tx, out_rx) = bounded::<CompressedUnit>(1);

    let mut raw = Vec::with_capacity(8 * BLOCK_SIZE as usize);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..8 * BLOCK_SIZE as usize {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        raw.push((state & 0xff) as u8);
    }
    let raw_arc: Arc<[u8]> = Arc::from(raw.clone());
    let raw_blocks = (0..8)
        .map(|i| crate::buffer::pipeline::RawBlockRef {
            payload: raw_arc.clone(),
            offset: i * BLOCK_SIZE as usize,
        })
        .collect();

    tx.send(CoalesceUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(10_000),
        lba_count: 8,
        raw_blocks,
        compression: CompressionAlgo::Lz4,
        vol_created_at: 1,
        seq_lba_ranges: vec![(1, Lba(10_000), 8)],
        dedup_skipped: false,
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_completion: None,
    })
    .unwrap();
    drop(tx);

    BufferFlusher::compress_loop(&rx, &out_tx, &running, &metrics, 12);
    drop(out_tx);

    let unit = out_rx
        .try_recv()
        .expect("compressed unit should be emitted");
    assert_eq!(unit.compression, CompressionAlgo::None.to_u8());
    assert_eq!(unit.compressed_data, raw);
    assert_eq!(unit.original_size, (8 * BLOCK_SIZE) as u32);
    assert_eq!(unit.lba_count, 8);
    assert_eq!(unit.crc32, crc32fast::hash(&unit.compressed_data));
    assert_eq!(
        metrics.compress_bypass_units.load(Ordering::Relaxed),
        1,
        "low-savings compression attempts should be accounted"
    );
    assert_eq!(
        metrics.compress_bypass_bytes.load(Ordering::Relaxed),
        (8 * BLOCK_SIZE) as u64
    );
}

#[test]
fn coalesce_enqueue_caps_ready_window_bytes() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 96 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();

    let payload = vec![0x5A; 256 * BLOCK_SIZE as usize];
    let mut seqs = Vec::new();
    for i in 0..20u64 {
        seqs.push(
            pool.append("window-vol", Lba(i * 256), 256, &payload, 0)
                .unwrap(),
        );
    }

    let tracker = FlusherInFlightTracker::default();
    let in_flight = HashMap::new();
    let mut seen = HashSet::new();
    let mut queued_bytes = 0usize;
    let mut new_entries = Vec::new();
    let mut window_full = false;
    let test_metrics = EngineMetrics::default();

    for seq in seqs {
        if matches!(
            BufferFlusher::try_enqueue_pending_seq(
                seq,
                &pool,
                &in_flight,
                &tracker,
                &mut seen,
                &mut queued_bytes,
                &mut new_entries,
                &test_metrics,
                true,
            ),
            EnqueuePendingSeq::WindowFull
        ) {
            window_full = true;
            break;
        }
    }

    assert!(
        window_full,
        "coalescer should stop once the ready window is full"
    );
    assert_eq!(queued_bytes, BufferFlusher::COALESCE_READY_WINDOW_BYTES);
    assert_eq!(new_entries.len(), 16);
}

/// When a pending entry's LBAs have all been superseded by a later seq in
/// the ring, `try_enqueue_pending_seq` should drop it up-front and bump
/// the `coalesce_superseded_*` counters instead of pushing it into the
/// pipeline. This is the optimization that skips dedup hashing, compression,
/// and dedup_index churn for soon-dead data.
#[test]
fn fully_superseded_entry_skipped_at_coalesce() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 32 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();

    let payload = vec![0xCC; 4 * BLOCK_SIZE as usize];
    // seq 100, seq 200 → same 4-LBA range. In real flow the append of seq
    // 200 would update `latest_lba_seq` to (200, 0) for every LBA covered
    // by the entry. Simulate that with the test hook so we can exercise
    // the supersession check in isolation.
    let seq_old = pool
        .append("vol-sup", Lba(0), 4, &payload, 0)
        .expect("append old");
    let seq_new = pool
        .append("vol-sup", Lba(0), 4, &payload, 0)
        .expect("append new");
    for off in 0..4 {
        pool.note_latest_lba_seq_for_test("vol-sup", Lba(off), seq_new, 0);
    }

    let tracker = FlusherInFlightTracker::default();
    let in_flight = HashMap::new();
    let mut seen = HashSet::new();
    let mut queued_bytes = 0usize;
    let mut new_entries = Vec::new();
    let test_metrics = EngineMetrics::default();

    // Old seq must be dropped and counters bumped.
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_old,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        true,
    );
    assert_eq!(out, EnqueuePendingSeq::Skipped(SkipReason::Superseded));
    assert!(new_entries.is_empty(), "superseded entry must not enqueue");
    let snap = test_metrics.snapshot();
    assert_eq!(snap.coalesce_superseded_entries, 1);
    assert_eq!(snap.coalesce_superseded_lbas, 4);

    // New seq is the head of the line — must still enqueue normally.
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_new,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        true,
    );
    assert_eq!(out, EnqueuePendingSeq::Queued);
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].seq, seq_new);

    // With the flag off, the old seq would have gone into the pipeline —
    // verify by feeding a fresh pair.
    let seq_old2 = pool
        .append("vol-sup", Lba(8), 4, &payload, 0)
        .expect("append old2");
    let seq_new2 = pool
        .append("vol-sup", Lba(8), 4, &payload, 0)
        .expect("append new2");
    for off in 8..12 {
        pool.note_latest_lba_seq_for_test("vol-sup", Lba(off), seq_new2, 0);
    }
    let out = BufferFlusher::try_enqueue_pending_seq(
        seq_old2,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut new_entries,
        &test_metrics,
        false, // disabled
    );
    assert_eq!(out, EnqueuePendingSeq::Queued);
    // Counter didn't advance beyond the first drop.
    let snap = test_metrics.snapshot();
    assert_eq!(snap.coalesce_superseded_entries, 1);
}
