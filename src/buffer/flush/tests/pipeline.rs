use super::*;

#[test]
fn stalled_lease_requires_both_old_seq_and_idle_writer() {
    assert!(!BufferFlusher::stalled_lease_ready(
        Duration::from_secs(29),
        Duration::from_secs(60)
    ));
    assert!(!BufferFlusher::stalled_lease_ready(
        Duration::from_secs(60),
        Duration::from_secs(4)
    ));
    assert!(BufferFlusher::stalled_lease_ready(
        Duration::from_secs(30),
        Duration::from_secs(5)
    ));
}

#[test]
fn compress_loop_keeps_none_payload_scattered_until_writer() {
    let metrics = EngineMetrics::default();
    let running = AtomicBool::new(true);
    let (tx, rx) = bounded::<CoalesceUnit>(1);
    let (out_tx, out_rx) = bounded::<CompressedUnit>(1);

    let raw: Vec<u8> = (0..4 * BLOCK_SIZE as usize)
        .map(|idx| (idx as u8).wrapping_mul(31))
        .collect();
    let raw_arc: Arc<[u8]> = Arc::from(raw.clone());
    let raw_blocks = (0..4)
        .map(|i| crate::buffer::pipeline::RawBlockRef {
            payload: raw_arc.clone(),
            offset: i * BLOCK_SIZE as usize,
            relocation_source: None,
        })
        .collect();
    tx.send(CoalesceUnit {
        vol_id: "flush-race".into(),
        start_lba: Lba(20_000),
        lba_count: 4,
        raw_blocks,
        compression: CompressionAlgo::None,
        vol_created_at: 1,
        seq_lba_ranges: vec![(1, Lba(20_000), 4)],
        dedup_skipped: false,
        block_hashes: None,
        dedup_stale_repairs: None,
        dedup_completion: None,
    })
    .unwrap();
    drop(tx);

    BufferFlusher::compress_loop(&rx, &out_tx, &running, &metrics, 12);
    drop(out_tx);

    let unit = out_rx.try_recv().expect("raw unit should be emitted");
    assert!(unit.payload_contiguous().is_none());
    assert_eq!(unit.payload_len(), raw.len());
    assert_eq!(unit.crc32, crc32fast::hash(&raw));
    assert_eq!(unit.materialize_payload(), raw);
}

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
            relocation_source: None,
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
    let payload = unit.materialize_payload();
    assert_eq!(payload, raw);
    assert_eq!(unit.original_size, (8 * BLOCK_SIZE) as u32);
    assert_eq!(unit.lba_count, 8);
    assert_eq!(unit.crc32, crc32fast::hash(&payload));
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
                BufferFlusher::COALESCE_READY_WINDOW_BYTES,
                true,
                Duration::ZERO,
                None,
                false,
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
///
/// NOTE: Under the ack-after-LV2-fdatasync design, the sync thread's
/// `retire_superseded_by_durable_entries` already retires the old seq
/// before the new append returns, so this coalesce-level fast path mostly
/// fires for entries superseded between sync batches in different shards.
/// This test bypasses the sync thread by pre-publishing only the OLD seq
/// before any later writes can drive it through fdatasync-time retirement.
#[test]
#[ignore = "superseded path is now driven by sync-time retire; coalesce-level fast path needs a different harness"]
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
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        true,
        Duration::ZERO,
        None,
        false,
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
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        true,
        Duration::ZERO,
        None,
        false,
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
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        false, // disabled
        Duration::ZERO,
        None,
        false,
    );
    assert_eq!(out, EnqueuePendingSeq::Queued);
    // Counter didn't advance beyond the first drop.
    let snap = test_metrics.snapshot();
    assert_eq!(snap.coalesce_superseded_entries, 1);
}

#[test]
fn coalesce_write_window_releases_mature_entries_or_pressure_bypass() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 32 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();
    let payload = vec![0xA5; BLOCK_SIZE as usize];
    let seq = pool.append("window-vol", Lba(7), 1, &payload, 0).unwrap();
    let tracker = FlusherInFlightTracker::default();
    let in_flight = HashMap::new();
    let metrics = EngineMetrics::default();

    let mut seen = HashSet::new();
    let mut queued_bytes = 0;
    let mut entries = Vec::new();
    let deferred = BufferFlusher::try_enqueue_pending_seq(
        seq,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut entries,
        &metrics,
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        true,
        Duration::from_secs(60),
        Instant::now().checked_sub(Duration::from_secs(60)),
        false,
    );
    assert_eq!(
        deferred,
        EnqueuePendingSeq::Skipped(SkipReason::WriteWindow)
    );
    assert!(entries.is_empty());

    seen.clear();
    std::thread::sleep(Duration::from_millis(2));
    let mature_cutoff = Instant::now().checked_sub(Duration::from_millis(1));
    let admitted_when_mature = BufferFlusher::try_enqueue_pending_seq(
        seq,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut entries,
        &metrics,
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        true,
        Duration::from_millis(1),
        mature_cutoff,
        false,
    );
    assert_eq!(admitted_when_mature, EnqueuePendingSeq::Queued);
    assert_eq!(entries.len(), 1);

    entries.clear();
    queued_bytes = 0;
    seen.clear();
    let admitted_under_pressure = BufferFlusher::try_enqueue_pending_seq(
        seq,
        &pool,
        &in_flight,
        &tracker,
        &mut seen,
        &mut queued_bytes,
        &mut entries,
        &metrics,
        BufferFlusher::COALESCE_READY_WINDOW_BYTES,
        true,
        Duration::from_secs(60),
        None,
        true,
    );
    assert_eq!(admitted_under_pressure, EnqueuePendingSeq::Queued);
    assert_eq!(entries.len(), 1);
}

#[test]
fn coalesce_write_window_bypasses_after_foreground_idle_grace() {
    assert!(!BufferFlusher::write_window_bypass_ready(
        10,
        50,
        20,
        80,
        Duration::from_secs(4),
    ));
    assert!(BufferFlusher::write_window_bypass_ready(
        50,
        50,
        20,
        80,
        Duration::ZERO,
    ));
    assert!(BufferFlusher::write_window_bypass_ready(
        10,
        50,
        20,
        80,
        Duration::from_secs(5),
    ));
}

#[test]
fn coalesce_write_window_bypasses_at_payload_pressure_threshold() {
    // Payload pressure admits old work at its own high watermark; it no longer
    // inherits a deliberately low physical-ring emergency threshold.
    assert!(BufferFlusher::write_window_bypass_ready(
        1,
        20,
        80,
        80,
        Duration::ZERO,
    ));
    assert!(!BufferFlusher::write_window_bypass_ready(
        1,
        20,
        20,
        80,
        Duration::ZERO,
    ));
    assert!(BufferFlusher::write_window_bypass_ready(
        20,
        20,
        20,
        80,
        Duration::ZERO,
    ));
}
