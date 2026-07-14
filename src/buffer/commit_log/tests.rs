use super::*;
use std::sync::Arc;
use tempfile::NamedTempFile;

struct NoUringCountingBackend {
    inner: RawDevice,
    flushes: AtomicU64,
    fail_remaining: AtomicU64,
    packed_write_attempts: AtomicU64,
    fail_packed_writes: AtomicBool,
    checkpoint_writes: Mutex<Vec<(u64, usize)>>,
}

impl NoUringCountingBackend {
    fn open(path: &std::path::Path, size: u64, fail_remaining: u64) -> Self {
        Self {
            inner: RawDevice::open_or_create(path, size).unwrap(),
            flushes: AtomicU64::new(0),
            fail_remaining: AtomicU64::new(fail_remaining),
            packed_write_attempts: AtomicU64::new(0),
            fail_packed_writes: AtomicBool::new(false),
            checkpoint_writes: Mutex::new(Vec::new()),
        }
    }

    fn reset_io_counts(&self) {
        self.flushes.store(0, Ordering::Relaxed);
        self.packed_write_attempts.store(0, Ordering::Relaxed);
        self.checkpoint_writes.lock().unwrap().clear();
    }
}

impl BlockBackend for NoUringCountingBackend {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        self.inner.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        // Tests below open four-shard pools, whose complete v3 checkpoint area
        // is [4 KiB, 20 KiB). Payload starts after it.
        if (COMMIT_LOG_SUPERBLOCK_SIZE..COMMIT_LOG_SUPERBLOCK_SIZE + 4 * SHARD_CHECKPOINT_SIZE)
            .contains(&offset)
        {
            self.checkpoint_writes
                .lock()
                .unwrap()
                .push((offset, buf.len()));
        }
        if let Ok(page) = <&[u8; SHARD_CHECKPOINT_SIZE as usize]>::try_from(buf) {
            if PackedCheckpointTable::has_magic(page) {
                self.packed_write_attempts.fetch_add(1, Ordering::Relaxed);
                if self.fail_packed_writes.load(Ordering::Acquire) {
                    return Err(OnyxError::Io(std::io::Error::other(
                        "injected packed checkpoint write failure",
                    )));
                }
            }
        }
        self.inner.write_at(buf, offset)
    }

    fn flush(&self) -> OnyxResult<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        if self
            .fail_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n > 0 {
                    Some(n - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(OnyxError::Io(std::io::Error::other(
                "injected root flush failure",
            )));
        }
        self.inner.sync()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn direct_io(&self) -> bool {
        self.inner.is_direct_io()
    }
}

fn packed_test_checkpoints(shards: usize, seq_base: u64) -> Vec<ShardCheckpoint> {
    (0..shards)
        .map(|idx| ShardCheckpoint {
            head_offset: idx as u64 * 4096,
            tail_offset: idx as u64 * 2048,
            max_seq: seq_base + idx as u64,
            used_bytes: idx as u64 * 8192,
        })
        .collect()
}

#[test]
fn packed_checkpoint_roundtrip_crc_and_generation_slots() {
    let table = PackedCheckpointTable::new(41, packed_test_checkpoints(64, 1_000)).unwrap();
    let encoded = table.encode();
    assert_eq!(PackedCheckpointTable::decode(&encoded), Some(table.clone()));
    assert_eq!(PackedCheckpointTable::slot_for_generation(41), 0);
    assert_eq!(PackedCheckpointTable::slot_for_generation(42), 1);

    let mut corrupt = encoded;
    corrupt[PACKED_CHECKPOINT_HEADER_SIZE + 17] ^= 0x80;
    assert!(PackedCheckpointTable::has_magic(&corrupt));
    assert!(PackedCheckpointTable::decode(&corrupt).is_none());
}

#[test]
fn packed_checkpoint_selects_latest_valid_and_detects_double_corruption() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 64 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let older = PackedCheckpointTable::new(7, packed_test_checkpoints(4, 100)).unwrap();
    let newer = PackedCheckpointTable::new(8, packed_test_checkpoints(4, 200)).unwrap();
    WriteBufferPool::write_packed_checkpoint(&dev, &older).unwrap();
    WriteBufferPool::write_packed_checkpoint(&dev, &newer).unwrap();

    match WriteBufferPool::read_packed_checkpoint(&dev, 4).unwrap() {
        PackedCheckpointLoad::Packed(table) => assert_eq!(table, newer),
        _ => panic!("newest packed checkpoint was not selected"),
    }

    let mut newer_corrupt = newer.encode();
    newer_corrupt[128] ^= 0x55;
    dev.write_at(
        &newer_corrupt,
        COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE,
    )
    .unwrap();
    match WriteBufferPool::read_packed_checkpoint(&dev, 4).unwrap() {
        PackedCheckpointLoad::Packed(table) => assert_eq!(table, older),
        _ => panic!("older valid packed checkpoint was not used"),
    }

    let mut older_corrupt = older.encode();
    older_corrupt[256] ^= 0xAA;
    dev.write_at(&older_corrupt, COMMIT_LOG_SUPERBLOCK_SIZE)
        .unwrap();
    assert!(matches!(
        WriteBufferPool::read_packed_checkpoint(&dev, 4).unwrap(),
        PackedCheckpointLoad::Corrupt
    ));

    // Format detection must not depend solely on mutable packed magic bytes.
    // If both A/B pages lose their magic after migration, treating the device
    // as legacy would expose stale per-shard pages that were never updated
    // again. Neither page decodes as a valid SHCK, so recovery must full-scan.
    for slot in 0..PACKED_CHECKPOINT_SLOT_COUNT {
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + slot as u64 * SHARD_CHECKPOINT_SIZE;
        let mut page = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        dev.read_at(&mut page, offset).unwrap();
        page[..4].fill(0);
        dev.write_at(&page, offset).unwrap();
    }
    assert!(matches!(
        WriteBufferPool::read_packed_checkpoint(&dev, 4).unwrap(),
        PackedCheckpointLoad::Corrupt
    ));
}

#[test]
fn global_sync_loop_coalesces_shards_and_recovers_acked_entries() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 128 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let backend = Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0));
    let pool = Arc::new(
        WriteBufferPool::open_with_options_full_and_limits(
            backend.clone(),
            Duration::from_millis(10),
            4,
            1,
            Duration::from_secs(1),
            64 * 1024 * 1024,
            None,
            BufferRuntimeLimits::default(),
        )
        .unwrap(),
    );
    let metrics = Arc::new(EngineMetrics::default());
    pool.attach_metrics(metrics.clone());
    backend.reset_io_counts();
    backend.fail_remaining.store(1, Ordering::Release);
    let start = Arc::new(std::sync::Barrier::new(65));
    let mut writers = Vec::new();
    for i in 0..64u64 {
        let pool = pool.clone();
        let start = start.clone();
        writers.push(std::thread::spawn(move || {
            start.wait();
            pool.append("vol", Lba(i), 1, &[i as u8; BLOCK_SIZE as usize], 1)
                .unwrap();
        }));
    }
    start.wait();
    for writer in writers {
        writer.join().unwrap();
    }

    let batches = metrics.buffer_sync_batches.load(Ordering::Relaxed);
    let flushes = metrics.buffer_sync_flushes.load(Ordering::Relaxed);
    assert!(
        batches >= 4,
        "expected work from all four shards, got {batches}"
    );
    assert!(
        flushes < batches,
        "global epochs must cover multiple shard batches: flushes={flushes} batches={batches}"
    );
    let checkpoint_writes = backend.checkpoint_writes.lock().unwrap().clone();
    assert!(!checkpoint_writes.is_empty());
    assert!(checkpoint_writes.iter().all(|(offset, len)| {
        (*offset == COMMIT_LOG_SUPERBLOCK_SIZE
            || *offset == COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE)
            && *len == SHARD_CHECKPOINT_SIZE as usize
    }));
    assert_eq!(
        checkpoint_writes.len() as u64,
        backend.flushes.load(Ordering::Relaxed),
        "every global barrier attempt must have exactly one packed 4 KiB write"
    );
    let stage_metrics = metrics.snapshot();
    assert_eq!(
        stage_metrics
            .buffer_append_wait_durable_fine_latency_buckets
            .iter()
            .sum::<u64>(),
        64
    );
    assert_eq!(
        stage_metrics
            .buffer_lv2_staging_queue_latency_buckets
            .iter()
            .sum::<u64>(),
        64
    );
    for (name, samples) in [
        (
            "prepared_queue",
            stage_metrics
                .buffer_lv2_prepared_queue_latency_buckets
                .iter()
                .sum::<u64>(),
        ),
        (
            "group_collect",
            stage_metrics
                .buffer_lv2_group_collect_latency_buckets
                .iter()
                .sum::<u64>(),
        ),
        (
            "payload_write",
            stage_metrics
                .buffer_lv2_payload_write_latency_buckets
                .iter()
                .sum::<u64>(),
        ),
        (
            "checkpoint_write",
            stage_metrics
                .buffer_lv2_checkpoint_write_latency_buckets
                .iter()
                .sum::<u64>(),
        ),
        (
            "root_flush",
            stage_metrics
                .buffer_lv2_root_flush_latency_buckets
                .iter()
                .sum::<u64>(),
        ),
    ] {
        assert!(samples > 0, "missing global LV2 {name} samples");
    }
    drop(pool);

    let reopened = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0)),
        Duration::from_millis(10),
        4,
        1,
        Duration::from_secs(1),
        64 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    assert_eq!(reopened.pending_count(), 64);
    for i in 0..64u64 {
        assert_eq!(
            reopened
                .lookup("vol", Lba(i))
                .unwrap()
                .unwrap()
                .payload
                .unwrap()[0],
            i as u8
        );
    }
}

#[test]
fn global_packed_checkpoint_failure_does_not_advance_durability() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 64 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let backend = Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0));
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        backend.clone(),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    backend.reset_io_counts();
    backend.fail_packed_writes.store(true, Ordering::Release);

    let ticket = pool
        .append_deferred("vol", Lba(0), 1, &[0x5A; BLOCK_SIZE as usize], 1)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while backend.packed_write_attempts.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < deadline,
            "packed write was never attempted"
        );
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(5));
    assert!(
        !ticket.is_durable(),
        "checkpoint write failure must not advance the LV2 watermark"
    );
    assert_eq!(
        backend.flushes.load(Ordering::Relaxed),
        0,
        "root barrier must not run until the packed table write succeeds"
    );

    backend.fail_packed_writes.store(false, Ordering::Release);
    assert_eq!(ticket.wait(), 1);
    assert!(backend.flushes.load(Ordering::Relaxed) >= 1);
}

fn corrupt_packed_slot(path: &std::path::Path, size: u64, slot: usize) {
    let dev = RawDevice::open_or_create(path, size).unwrap();
    let offset = COMMIT_LOG_SUPERBLOCK_SIZE + slot as u64 * SHARD_CHECKPOINT_SIZE;
    let mut page = [0u8; SHARD_CHECKPOINT_SIZE as usize];
    dev.read_at(&mut page, offset).unwrap();
    assert!(PackedCheckpointTable::has_magic(&page));
    page[PACKED_CHECKPOINT_HEADER_SIZE + 3] ^= 0x5A;
    dev.write_at(&page, offset).unwrap();
    dev.sync().unwrap();
}

#[test]
fn packed_reopen_falls_back_to_older_generation_then_full_scans_if_both_corrupt() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 64 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let backend = Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0));
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        backend.clone(),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    for i in 0..8u64 {
        pool.append("vol", Lba(i), 1, &[i as u8 + 1; BLOCK_SIZE as usize], 1)
            .unwrap();
    }
    pool.persist_checkpoints().unwrap();
    drop(pool);
    drop(backend);

    let (newest_generation, newest_slot, older_generation) = {
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let mut tables = Vec::new();
        for slot in 0..PACKED_CHECKPOINT_SLOT_COUNT {
            let mut page = [0u8; SHARD_CHECKPOINT_SIZE as usize];
            dev.read_at(
                &mut page,
                COMMIT_LOG_SUPERBLOCK_SIZE + slot as u64 * SHARD_CHECKPOINT_SIZE,
            )
            .unwrap();
            tables.push((
                slot,
                PackedCheckpointTable::decode(&page).expect("both A/B slots must be valid"),
            ));
        }
        tables.sort_unstable_by_key(|(_, table)| table.generation);
        (tables[1].1.generation, tables[1].0, tables[0].1.generation)
    };
    assert!(newest_generation > older_generation);
    corrupt_packed_slot(tmp.path(), size, newest_slot);

    let reopened = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0)),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    assert_eq!(reopened.pending_count(), 8);
    assert_eq!(
        reopened
            .packed_checkpoint
            .as_ref()
            .unwrap()
            .lock()
            .generation,
        older_generation
    );
    drop(reopened); // Repairs the damaged slot with the next clean generation.

    for slot in 0..PACKED_CHECKPOINT_SLOT_COUNT {
        corrupt_packed_slot(tmp.path(), size, slot);
    }
    let full_scan = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0)),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    assert_eq!(full_scan.pending_count(), 8);
    assert_eq!(
        full_scan
            .packed_checkpoint
            .as_ref()
            .unwrap()
            .lock()
            .generation,
        0,
        "double corruption must rebuild from full scans, not stale legacy pages"
    );
}

#[test]
fn packed_clean_checkpoint_reopens_empty_and_preserves_seq_floor() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 32 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0)),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    let seq = pool
        .append("vol", Lba(11), 1, &[0xC3; BLOCK_SIZE as usize], 1)
        .unwrap();
    pool.mark_applied(seq, Lba(11), 1).unwrap();
    pool.durable_seq_handle().store(seq, Ordering::Release);
    assert_eq!(pool.release_below(seq).unwrap(), 1);
    pool.persist_checkpoints().unwrap();
    drop(pool);

    let reopened = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0)),
        Duration::ZERO,
        4,
        1,
        Duration::from_secs(1),
        8 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    assert_eq!(reopened.pending_count(), 0);
    let next = reopened
        .append("vol", Lba(12), 1, &[0xD4; BLOCK_SIZE as usize], 1)
        .unwrap();
    assert!(
        next > seq,
        "packed max_seq must preserve the restart seq floor"
    );
}

#[test]
fn global_sync_records_watermark_to_dispatcher_latency() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 32 * 1024 * 1024;
    tmp.as_file().set_len(size).unwrap();
    let backend = Arc::new(NoUringCountingBackend::open(tmp.path(), size, 0));
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        backend,
        Duration::from_millis(10),
        2,
        1,
        Duration::from_millis(1),
        16 * 1024 * 1024,
        None,
        BufferRuntimeLimits::default(),
    )
    .unwrap();
    let metrics = Arc::new(EngineMetrics::default());
    pool.attach_metrics(metrics.clone());

    let ticket = pool
        .append_deferred("vol", Lba(0), 1, &[0x5a; BLOCK_SIZE as usize], 1)
        .unwrap();
    let (wake_tx, wake_rx) = bounded(1);
    if !ticket.arm_wakeup(&wake_tx) {
        wake_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    assert!(ticket
        .completion_dispatch_delay_ns(Instant::now())
        .is_some());
    ticket.finish_dispatched();

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot
            .buffer_lv2_watermark_dispatch_latency_buckets
            .iter()
            .sum::<u64>(),
        1
    );
}

fn create_pool(size: u64, group_commit_wait: Duration) -> (WriteBufferPool, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    (
        WriteBufferPool::open_with_group_commit_wait(dev, group_commit_wait).unwrap(),
        tmp,
    )
}

#[test]
fn dropped_deferred_ticket_is_published_by_sync() {
    let (pool, _tmp) = create_pool(16 * 1024 * 1024, Duration::from_millis(5));
    let ticket = pool
        .append_deferred("test-vol", Lba(9), 1, &[0x5a; BLOCK_SIZE as usize], 0)
        .unwrap();
    let seq = ticket.seq();
    drop(ticket);

    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    assert_eq!(
        pool.lookup("test-vol", Lba(9))
            .unwrap()
            .unwrap()
            .payload
            .unwrap()[0],
        0x5a
    );
}

#[test]
fn uring_sync_batch_chunks_over_sq_depth() {
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let data_start = COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE;
    let size = data_start + 128 * slot;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open_with_options_full(
        dev,
        Duration::from_millis(1),
        1,
        256,
        Duration::ZERO,
        0,
        None,
    )
    .unwrap();
    let shard = &pool.shards[0].shard;
    let shard_device =
        slice_backend(pool.root_device.clone(), data_start, size - data_start).unwrap();
    let ring = Arc::new(IoUringSession::new(4).unwrap());
    let metrics = Arc::new(OnceLock::new());

    let mut entries = Vec::new();
    for i in 0..10u64 {
        let payload: Arc<[u8]> = vec![0x80u8 + i as u8; BLOCK_SIZE as usize].into();
        let payload_crc = crc32fast::hash(payload.as_ref());
        let encoded = BufferEntry::encode(
            i + 1,
            "test-vol",
            Lba(i),
            1,
            payload_crc,
            false,
            7,
            payload.as_ref(),
        )
        .unwrap();
        let disk_len = encoded.len() as u32;
        let pending = Arc::new(PendingEntry {
            seq: i + 1,
            vol_id: "test-vol".to_string(),
            start_lba: Lba(i),
            lba_count: 1,
            payload_crc32: payload_crc,
            vol_created_at: 7,
            payload: Some(payload.clone()),
            disk_offset: i * (disk_len as u64 + slot),
            disk_len,
            enqueued_at: Instant::now(),
            durability_advanced_at_ns: AtomicU64::new(0),
            superseded_ranges: Vec::new(),
        });
        entries.push(StagedEntry {
            pending,
            payload,
            staged_at: Instant::now(),
        });
    }

    WriteBufferPool::write_batch_and_sync_uring(
        shard_device.as_ref(),
        shard,
        &ring,
        &shard.io_lock,
        &entries,
        10,
        &metrics,
    )
    .unwrap();

    for entry in &entries {
        let mut buf = vec![0u8; entry.pending.disk_len as usize];
        shard_device
            .read_at(&mut buf, entry.pending.disk_offset)
            .unwrap();
        let decoded = BufferEntry::from_bytes(&buf).unwrap();
        assert_eq!(decoded.seq, entry.pending.seq);
        assert_eq!(decoded.payload.as_ref(), entry.payload.as_ref());
    }
}

#[test]
fn uring_pipeline_concurrent_appends_durable_and_visible() {
    // Open a uring-backed pool (default lv2_sync_pipeline_depth = 2 via
    // BufferRuntimeLimits::default), drive concurrent appends so multiple fsync
    // chains are in flight, and assert: every ack'd append is read-after-write
    // visible, the watermark advanced for all of them (append blocks until
    // durable), and the pipelined on-disk format replays on reopen.
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let data_start = COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE;
    let size = data_start + 4096 * slot;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = Arc::new(
        WriteBufferPool::open_with_options_full(
            dev,
            Duration::from_millis(1),
            1,   // shards
            256, // zone size blocks
            Duration::ZERO,
            0,
            Some(64), // uring_sq_entries → pipelined sync path active
        )
        .unwrap(),
    );

    let threads = 4usize;
    let per = 16u64;
    let mut handles = Vec::new();
    for t in 0..threads {
        let pool = pool.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..per {
                let lba = Lba(t as u64 * 1000 + i);
                let byte = (t as u8).wrapping_add(i as u8).wrapping_add(1);
                let data = vec![byte; BLOCK_SIZE as usize];
                let seq = pool.append("vol", lba, 1, &data, 0).unwrap();
                assert!(seq >= 1);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let total = threads as u64 * per;
    assert_eq!(
        pool.pending_count(),
        total,
        "all appends still pending (no flusher)"
    );
    for t in 0..threads {
        for i in 0..per {
            let lba = Lba(t as u64 * 1000 + i);
            let byte = (t as u8).wrapping_add(i as u8).wrapping_add(1);
            let found = pool.lookup("vol", lba).unwrap().unwrap();
            assert_eq!(
                &**found.payload.as_ref().unwrap(),
                &vec![byte; BLOCK_SIZE as usize][..],
                "read-after-write mismatch at t={t} i={i}"
            );
        }
    }
    drop(pool);

    // Reopen: pipelined writes use the identical on-disk entry format, so every
    // durable entry replays.
    let dev2 = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool2 = WriteBufferPool::open_with_options_full(
        dev2,
        Duration::from_millis(1),
        1,
        256,
        Duration::ZERO,
        0,
        Some(64),
    )
    .unwrap();
    assert_eq!(
        pool2.recover().unwrap().len() as u64,
        total,
        "all durable entries replay on reopen"
    );
}

#[test]
fn uring_sync_fast_path_single_linked_chain() {
    // Contiguous disk offsets coalesce into ONE span, and the ring is large
    // enough that data-write + fsync (+ checkpoint) fit in a single submit —
    // exercising the IO_LINK fast path in `write_batch_and_sync_uring`.
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let data_start = COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE;
    let size = data_start + 128 * slot;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open_with_options_full(
        dev,
        Duration::from_millis(1),
        1,
        256,
        Duration::ZERO,
        0,
        None,
    )
    .unwrap();
    let shard = &pool.shards[0].shard;
    let shard_device =
        slice_backend(pool.root_device.clone(), data_start, size - data_start).unwrap();
    // Big ring → fast path (single linked submit), unlike the sq=4 chunk test.
    let ring = Arc::new(IoUringSession::new(64).unwrap());
    let metrics = Arc::new(OnceLock::new());

    // Build entries at CONTIGUOUS disk offsets so they coalesce to one span.
    let mut entries = Vec::new();
    let mut next_off = 0u64;
    for i in 0..8u64 {
        let payload: Arc<[u8]> = vec![0x40u8 + i as u8; BLOCK_SIZE as usize].into();
        let payload_crc = crc32fast::hash(payload.as_ref());
        let encoded = BufferEntry::encode(
            i + 1,
            "fast-vol",
            Lba(i),
            1,
            payload_crc,
            false,
            9,
            payload.as_ref(),
        )
        .unwrap();
        let disk_len = encoded.len() as u32;
        let pending = Arc::new(PendingEntry {
            seq: i + 1,
            vol_id: "fast-vol".to_string(),
            start_lba: Lba(i),
            lba_count: 1,
            payload_crc32: payload_crc,
            vol_created_at: 9,
            payload: Some(payload.clone()),
            disk_offset: next_off,
            disk_len,
            enqueued_at: Instant::now(),
            durability_advanced_at_ns: AtomicU64::new(0),
            superseded_ranges: Vec::new(),
        });
        next_off += disk_len as u64;
        entries.push(StagedEntry {
            pending,
            payload,
            staged_at: Instant::now(),
        });
    }

    WriteBufferPool::write_batch_and_sync_uring(
        shard_device.as_ref(),
        shard,
        &ring,
        &shard.io_lock,
        &entries,
        8,
        &metrics,
    )
    .unwrap();

    for entry in &entries {
        let mut buf = vec![0u8; entry.pending.disk_len as usize];
        shard_device
            .read_at(&mut buf, entry.pending.disk_offset)
            .unwrap();
        let decoded = BufferEntry::from_bytes(&buf).unwrap();
        assert_eq!(decoded.seq, entry.pending.seq);
        assert_eq!(decoded.payload.as_ref(), entry.payload.as_ref());
    }
}

#[test]
fn meta_fence_rejects_new_appends_but_not_reads() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));

    // A write before the fence trips succeeds and is visible.
    let seq = pool
        .append("test-vol", Lba(3), 1, &vec![0x11; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    assert!(pool.lookup("test-vol", Lba(3)).unwrap().is_some());
    assert!(!pool.is_meta_fenced());

    // Trip the fence (as the durability thread would on fatal checkpoint loss).
    pool.fence_meta("meta device capacity exhausted: need 10 pages, capacity 8");
    assert!(pool.is_meta_fenced());
    assert_eq!(
        pool.meta_fence_reason(),
        Some("meta device capacity exhausted: need 10 pages, capacity 8")
    );

    // New appends fail fast with MetaFenced instead of silently acking.
    let err = pool
        .append("test-vol", Lba(4), 1, &vec![0x22; BLOCK_SIZE as usize], 0)
        .unwrap_err();
    assert!(
        matches!(err, OnyxError::MetaFenced(_)),
        "expected MetaFenced, got {err:?}"
    );

    // Reads are never fenced — the earlier entry is still visible.
    assert!(pool.lookup("test-vol", Lba(3)).unwrap().is_some());

    // Fencing is idempotent: a second reason does not overwrite the first.
    pool.fence_meta("some later reason");
    assert_eq!(
        pool.meta_fence_reason(),
        Some("meta device capacity exhausted: need 10 pages, capacity 8")
    );
}

#[test]
fn flushed_entry_cannot_be_reinstalled_by_stale_eviction_state() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq = pool
        .append("test-vol", Lba(7), 1, &vec![0xA5; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    let pending = shard.pending_entry_arc_hydrated(seq).unwrap();
    assert!(pending.payload.is_some());

    shard.mark_flushed(seq, Lba(7), 1).unwrap();
    assert_eq!(pool.payload_memory_bytes(), 0);

    let evicted = BufferShard::evicted_pending_entry(pending.as_ref());
    assert!(
        !shard.replace_pending_entry_if_current(&pending, evicted),
        "stale payload eviction state must not resurrect a flushed seq"
    );
    assert!(shard.pending_entry_arc(seq).is_none());
    assert!(pool.lookup("test-vol", Lba(7)).unwrap().is_none());
    assert_eq!(pool.payload_memory_bytes(), 0);
}

#[test]
fn hydrated_payload_is_not_reinstalled_after_flush_race() {
    // Under the ack-after-LV2-fdatasync design, append stores payload eagerly
    // into PendingEntry.payload, so the "hydrate then flush" race window the
    // pre-volatile design had no longer exists. This test pins the invariant
    // anyway: once mark_flushed retires a seq, no later
    // replace_pending_entry_if_current call can resurrect it (Arc::ptr_eq
    // guard ensures only the original Arc would match, and the original is
    // gone from pending_entries).
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq = pool
        .append("test-vol", Lba(11), 1, &vec![0x5C; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let pending = shard.pending_entry_arc(seq).unwrap();
    assert!(
        pending.payload.is_some(),
        "appended entry should carry payload immediately"
    );
    shard.mark_flushed(seq, Lba(11), 1).unwrap();
    assert_eq!(pool.payload_memory_bytes(), 0);

    let replacement = {
        let mut clone = pending.as_ref().clone();
        clone.payload = Some(Arc::<[u8]>::from(vec![0xFF; BLOCK_SIZE as usize]));
        Arc::new(clone)
    };
    assert!(
        !shard.replace_pending_entry_if_current(&pending, replacement),
        "post-flush replacement must not resurrect a flushed seq"
    );
    assert!(shard.pending_entry_arc(seq).is_none());
    assert_eq!(pool.payload_memory_bytes(), 0);
}

// Two legacy tests removed: `inflight_missing_volatile_payload_is_not_hydrated_from_disk`
// and `lookup_hydrates_from_disk_without_volatile_payload`. Both asserted
// behavior of the pre-ack-after-LV2 design (volatile cache + lifecycle.inflight
// gate). Under the current design, `pool.append()` blocks until the seq's
// payload is fdatasync'd on LV2, so neither the volatile cache nor the inflight
// set exists.

#[test]
fn lookup_primary_range_batches_contiguous_hydration() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 32 * 8192, Duration::from_millis(1));
    let metrics = Arc::new(EngineMetrics::default());

    let mut expected = Vec::new();
    for i in 0..8u64 {
        let payload = vec![0x30 + i as u8; BLOCK_SIZE as usize];
        expected.push(payload.clone());
        let seq = pool.append("test-vol", Lba(i), 1, &payload, 0).unwrap();
        assert_eq!(
            pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
            seq
        );
    }

    pool.attach_metrics(metrics.clone());
    let hits = pool.lookup_primary_range("test-vol", Lba(0), 8).unwrap();
    assert_eq!(hits.len(), 8);
    for (idx, hit) in hits.into_iter().enumerate() {
        let pending = hit.unwrap();
        assert_eq!(pending.start_lba, Lba(idx as u64));
        assert_eq!(pending.payload.as_deref(), Some(expected[idx].as_slice()));
    }

    assert_eq!(
        metrics
            .buffer_lookup_hydrate_ops
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "freshly-appended entries already have payload cached; no LV2 hydration needed"
    );
}

#[test]
fn flusher_hydration_uses_cached_payload_after_append() {
    // In the ack-after-LV2-fdatasync design, append populates
    // PendingEntry.payload eagerly, so flusher hydration never re-reads from
    // disk for a freshly-appended seq. (Crash-recovered entries hydrate
    // lazily; that path is covered elsewhere.)
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;
    let payload = vec![0x71; BLOCK_SIZE as usize];

    let seq = pool.append("test-vol", Lba(31), 1, &payload, 0).unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let before = shard.pending_entry_arc(seq).unwrap();
    assert_eq!(
        before.payload.as_deref(),
        Some(payload.as_slice()),
        "appended entries carry payload from append time onward"
    );

    let hydrated = shard.pending_entry_arc_hydrated(seq).unwrap();
    assert_eq!(hydrated.payload.as_deref(), Some(payload.as_slice()));
}

#[test]
fn guided_recovery_treats_zero_used_checkpoint_as_empty_even_if_offsets_differ() {
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let capacity = 8 * slot;
    tmp.as_file().set_len(capacity).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), capacity).unwrap();
    let payload = vec![0x42; BLOCK_SIZE as usize];
    let encoded = BufferEntry::encode(
        42,
        "test-vol",
        Lba(7),
        1,
        crc32fast::hash(&payload),
        false,
        1,
        &payload,
    )
    .unwrap();
    dev.write_at(&encoded, 0).unwrap();
    dev.sync().unwrap();

    let lba_index = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let latest_lba_seq = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_lba_buckets = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_entries = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_count = AtomicU64::new(0);
    let checkpoint = ShardCheckpoint {
        head_offset: 2 * slot,
        tail_offset: 0,
        max_seq: 42,
        used_bytes: 0,
    };

    let scan = BufferShard::rebuild_indices_guided(
        &dev,
        capacity,
        &checkpoint,
        &lba_index,
        &latest_lba_seq,
        &pending_lba_buckets,
        &pending_entries,
        &pending_count,
    )
    .unwrap();

    assert_eq!(scan.used_bytes, 0);
    assert_eq!(scan.head_offset, checkpoint.head_offset);
    assert_eq!(scan.tail_offset, checkpoint.head_offset);
    assert!(scan.log_order.is_empty());
    assert!(pending_entries.is_empty());
    assert!(lba_index.is_empty());
}

#[test]
fn guided_recovery_crosses_one_wrap_gap_from_previous_packed_generation() {
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let capacity = 7 * slot;
    tmp.as_file().set_len(capacity).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), capacity).unwrap();
    let payload = vec![0x7B; BLOCK_SIZE as usize];
    let encoded = BufferEntry::encode(
        4,
        "wrap-vol",
        Lba(9),
        1,
        crc32fast::hash(&payload),
        false,
        1,
        &payload,
    )
    .unwrap();
    dev.write_at(&encoded, 0).unwrap();
    dev.sync().unwrap();

    let lba_index = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let latest_lba_seq = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_lba_buckets = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_entries = DashMap::with_shard_amount(DASHMAP_SHARDS);
    let pending_count = AtomicU64::new(0);
    let checkpoint = ShardCheckpoint {
        // The next 8 KiB entry did not fit in the final 4 KiB, so allocation
        // left [6*slot, capacity) as a gap and resumed at offset zero.
        head_offset: 6 * slot,
        tail_offset: 6 * slot,
        max_seq: 3,
        used_bytes: 0,
    };

    let scan = BufferShard::rebuild_indices_guided(
        &dev,
        capacity,
        &checkpoint,
        &lba_index,
        &latest_lba_seq,
        &pending_lba_buckets,
        &pending_entries,
        &pending_count,
    )
    .unwrap();
    assert!(pending_entries.contains_key(&4));
    assert_eq!(pending_count.load(Ordering::Relaxed), 1);
    assert_eq!(scan.tail_offset, 0);
    assert_eq!(scan.head_offset, encoded.len() as u64);
}

#[test]
fn empty_ring_checkpoint_normalizes_tail_to_head() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;
    let slot = BufferShard::slot_size();
    {
        let mut ring = shard.ring.lock();
        ring.used_bytes = 0;
        ring.head_offset = 3 * slot;
        ring.tail_offset = slot;
    }

    let checkpoint = shard.snapshot_checkpoint();

    assert_eq!(checkpoint.used_bytes, 0);
    assert_eq!(checkpoint.head_offset, 3 * slot);
    assert_eq!(checkpoint.tail_offset, checkpoint.head_offset);
}

#[test]
fn shard_snapshot_reports_head_remaining_lbas_and_age() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));

    let seq = pool
        .append(
            "test-vol",
            Lba(32),
            3,
            &vec![0xAB; 3 * BLOCK_SIZE as usize],
            0,
        )
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    thread::sleep(Duration::from_millis(2));

    let snap = &pool.shard_snapshots()[0];
    assert_eq!(snap.head_seq, Some(seq));
    assert_eq!(snap.head_remaining_lbas, Some(3));
    assert!(snap.head_age_ms.is_some());
    assert!(snap.head_residency_ms.is_some());

    pool.mark_flushed(seq, Lba(32), 1).unwrap();
    let snap = &pool.shard_snapshots()[0];
    assert_eq!(snap.head_seq, Some(seq));
    assert_eq!(snap.head_remaining_lbas, Some(2));
}

#[test]
fn flushed_offsets_tracks_partial_progress() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));

    let seq = pool
        .append(
            "test-vol",
            Lba(100),
            4,
            &vec![0xCD; 4 * BLOCK_SIZE as usize],
            0,
        )
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    // No flush progress yet.
    assert!(pool.flushed_offsets_for_shard(0, seq).is_none());

    // Partial flush: LBA 100 and 102 (offsets 0 and 2).
    pool.mark_flushed(seq, Lba(100), 1).unwrap();
    pool.mark_flushed(seq, Lba(102), 1).unwrap();

    let offsets = pool.flushed_offsets_for_shard(0, seq).unwrap();
    assert!(offsets.contains(&0));
    assert!(!offsets.contains(&1));
    assert!(offsets.contains(&2));
    assert!(!offsets.contains(&3));
    assert_eq!(offsets.len(), 2);

    // Entry still exists (not fully flushed).
    assert!(pool.pending_entry_arc(seq).is_some());

    // Flush remaining LBAs 101 and 103.
    pool.mark_flushed(seq, Lba(101), 1).unwrap();
    pool.mark_flushed(seq, Lba(103), 1).unwrap();

    // Entry removed; flush_progress cleaned up.
    assert!(pool.flushed_offsets_for_shard(0, seq).is_none());
    assert!(pool.pending_entry_arc(seq).is_none());
}

// ── Unit tests for ring allocator gap accounting ─────────────────

fn make_ring(capacity_slots: u32) -> RingState {
    RingState {
        used_bytes: 0,
        capacity_bytes: BufferShard::slot_bytes(capacity_slots),
        reclaim_ready: 0,
        head_offset: 0,
        tail_offset: 0,
        log_order: VecDeque::new(),
        flushed_seqs: HashSet::new(),
        pending_seqs: BTreeSet::new(),
        head_became_at: None,
    }
}

#[test]
fn ring_wrap_gap_prevents_overlap() {
    // Reproduce the scenario that caused overlapping offsets:
    // 1. Fill [tail, head) with entries
    // 2. Wrap an entry to offset 0, creating a gap at [head, capacity)
    // 3. Fill remaining free space [entry_end, tail)
    // 4. At this point head == tail; the ring MUST reject further allocations.
    let slot = BufferShard::slot_size();
    let mut ring = make_ring(10); // 10 slots = 40KB

    // Fill slots [0, 7) — 7 slots used.
    let o = BufferShard::reserve_log_space(&mut ring, 1, 7);
    assert_eq!(o, Some(0));
    assert_eq!(ring.head_offset, 7 * slot);
    assert_eq!(ring.used_bytes, 7 * slot);

    // Reclaim entry 1 so that tail advances.
    ring.flushed_seqs.insert(1);
    BufferShard::reclaim_log_prefix(&mut ring, u64::MAX);
    assert_eq!(ring.used_bytes, 0);
    // head=7*slot, tail=7*slot (ring empty).

    // Fill slots [7, 9) — 2 slots.
    let o = BufferShard::reserve_log_space(&mut ring, 2, 2);
    assert_eq!(o, Some(7 * slot));
    assert_eq!(ring.head_offset, 9 * slot);
    assert_eq!(ring.used_bytes, 2 * slot);

    // Wrap: allocate 3 slots starting at offset 0 (doesn't fit at end).
    // Creates a 1-slot gap at [9*slot, 10*slot).
    let o = BufferShard::reserve_log_space(&mut ring, 3, 3);
    assert_eq!(o, Some(0));
    assert_eq!(ring.head_offset, 3 * slot);
    // used_bytes includes the gap: 2 (entry 2) + 1 (gap) + 3 (entry 3) = 6 slots.
    assert_eq!(ring.used_bytes, 6 * slot);

    // Fill remaining free space [3*slot, 7*slot) — 4 slots.
    let o = BufferShard::reserve_log_space(&mut ring, 4, 4);
    assert_eq!(o, Some(3 * slot));
    assert_eq!(ring.head_offset, 7 * slot); // head == tail

    // Ring is now full: entries [7,9) + gap [9,10) + entries [0,3) + entries [3,7).
    // used_bytes = 2 + 1 + 3 + 4 = 10 slots = capacity.
    assert_eq!(ring.used_bytes, 10 * slot);

    // Any further allocation MUST fail — the ring is full.
    assert!(BufferShard::reserve_log_space(&mut ring, 5, 1).is_none());
}

#[test]
fn ring_gap_freed_on_reclaim_past_wrap() {
    let slot = BufferShard::slot_size();
    let mut ring = make_ring(10);

    // Entry 1 at [0, 8*slot).
    BufferShard::reserve_log_space(&mut ring, 1, 8);
    // Entry 2 wraps to offset 0, gap at [8*slot, 10*slot).
    // (need to move head first)
    // Actually entry 1 is at [0, 8*slot), head=8*slot, tail=0.
    // Entry 2 at [8*slot, 10*slot) — fits at end.
    BufferShard::reserve_log_space(&mut ring, 2, 2);
    // head=0, tail=0, used=10*slot. Ring full, no gap.

    // Reset — use a scenario where gap is created.
    let mut ring = make_ring(10);

    // Entry 1: 7 slots at [0, 7*slot).
    BufferShard::reserve_log_space(&mut ring, 1, 7);
    // Reclaim entry 1.
    ring.flushed_seqs.insert(1);
    BufferShard::reclaim_log_prefix(&mut ring, u64::MAX);
    // head=7*slot, tail=7*slot, used=0.

    // Entry 2: 2 slots at [7*slot, 9*slot).
    BufferShard::reserve_log_space(&mut ring, 2, 2);
    // Entry 3: 3 slots — wraps to [0, 3*slot). Gap at [9*slot, 10*slot).
    BufferShard::reserve_log_space(&mut ring, 3, 3);
    assert_eq!(ring.used_bytes, 6 * slot); // 2 + 1(gap) + 3 = 6

    // Reclaim entry 2 — should also free the gap.
    ring.flushed_seqs.insert(2);
    BufferShard::reclaim_log_prefix(&mut ring, u64::MAX);
    // Entry 2 was 2 slots, gap was 1 slot. Total freed = 3 slots.
    assert_eq!(ring.used_bytes, 3 * slot); // only entry 3 remains
    assert_eq!(ring.tail_offset, 0); // tail advanced past gap to entry 3

    // Reclaim entry 3.
    ring.flushed_seqs.insert(3);
    BufferShard::reclaim_log_prefix(&mut ring, u64::MAX);
    assert_eq!(ring.used_bytes, 0);
}

#[test]
fn ring_empty_wrap_tracks_gap() {
    let slot = BufferShard::slot_size();
    let mut ring = make_ring(10);

    // Simulate head at 9*slot (from prior usage).
    ring.head_offset = 9 * slot;
    ring.tail_offset = 9 * slot;

    // Allocate 3 slots: doesn't fit at end (only 1 slot left), wraps to 0.
    let o = BufferShard::reserve_log_space(&mut ring, 1, 3);
    assert_eq!(o, Some(0));
    // Gap = 1 slot at [9*slot, 10*slot).
    assert_eq!(ring.used_bytes, 4 * slot); // 3 (entry) + 1 (gap)

    // Reclaim — gap should be freed along with the entry.
    ring.flushed_seqs.insert(1);
    BufferShard::reclaim_log_prefix(&mut ring, u64::MAX);
    assert_eq!(ring.used_bytes, 0);
}

#[test]
fn ring_head_eq_tail_used_zero_still_allocates() {
    let slot = BufferShard::slot_size();
    let mut ring = make_ring(10);
    ring.head_offset = 5 * slot;
    ring.tail_offset = 5 * slot;
    // Ring is empty. Should allocate normally.
    let o = BufferShard::reserve_log_space(&mut ring, 1, 3);
    assert_eq!(o, Some(5 * slot));
    assert_eq!(ring.used_bytes, 3 * slot);
}

#[test]
fn throttle_resolved_disabled_when_either_knob_zero() {
    let off = ThrottleSettings::default();
    assert!(off.resolved().is_none());
    let only_min = ThrottleSettings {
        min_pct: 60,
        ..Default::default()
    };
    assert!(only_min.resolved().is_none());
    let only_scale = ThrottleSettings {
        scale_us: 1_000,
        ..Default::default()
    };
    assert!(only_scale.resolved().is_none());
}

#[test]
fn throttle_resolved_fills_defaults() {
    let raw = ThrottleSettings {
        min_pct: 60,
        max_pct: 0,
        scale_us: 1_000,
        cap_us: 0,
    };
    let r = raw.resolved().unwrap();
    assert_eq!(r.min_pct, 60);
    assert_eq!(r.max_pct, 100);
    assert_eq!(r.scale_us, 1_000);
    assert_eq!(r.cap_us, 100_000);
}

#[test]
fn throttle_resolved_rejects_inverted_window() {
    let bad = ThrottleSettings {
        min_pct: 90,
        max_pct: 80,
        scale_us: 1_000,
        cap_us: 0,
    };
    assert!(bad.resolved().is_none());
    let equal = ThrottleSettings {
        min_pct: 80,
        max_pct: 80,
        scale_us: 1_000,
        cap_us: 0,
    };
    assert!(equal.resolved().is_none());
}

#[test]
fn throttle_curve_zero_below_floor() {
    let r = ThrottleSettings {
        min_pct: 60,
        max_pct: 100,
        scale_us: 1_000,
        cap_us: 100_000,
    };
    assert_eq!(r.delay_us_for_fill(0), 0);
    assert_eq!(r.delay_us_for_fill(59), 0);
    assert_eq!(r.delay_us_for_fill(60), 0);
}

#[test]
fn throttle_curve_caps_at_max_pct_and_above() {
    let r = ThrottleSettings {
        min_pct: 60,
        max_pct: 100,
        scale_us: 1_000,
        cap_us: 100_000,
    };
    assert_eq!(r.delay_us_for_fill(100), 100_000);
    assert_eq!(r.delay_us_for_fill(105), 100_000);
}

#[test]
fn throttle_curve_is_monotonic_and_below_cap() {
    let r = ThrottleSettings {
        min_pct: 60,
        max_pct: 100,
        scale_us: 333,
        cap_us: 100_000,
    };
    // delay(90) = 333 * 30 / 10 = 999 us ≈ 1 ms
    assert_eq!(r.delay_us_for_fill(90), 999);
    // Strict monotonic non-decrease across the interior.
    let mut prev = 0u64;
    for fill in 61..=99 {
        let d = r.delay_us_for_fill(fill);
        assert!(d >= prev, "non-monotonic at fill={}", fill);
        assert!(d <= r.cap_us, "delay {} exceeded cap at fill={}", d, fill);
        prev = d;
    }
}

#[test]
fn physical_fill_tracks_checkpoint_retained_entries_after_pending_drains() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;
    assert!(pool.physical_is_empty());

    let seq = pool
        .append("test-vol", Lba(7), 1, &vec![0xA5; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let expected_physical_fill = {
        let ring = shard.ring.lock();
        ((ring.used_bytes * 100) / ring.capacity_bytes) as u8
    };
    assert!(expected_physical_fill > 0);
    assert!(pool.fill_percentage() > 0);

    pool.mark_applied(seq, Lba(7), 1).unwrap();

    assert_eq!(
        pool.fill_percentage(),
        0,
        "pending pressure must be drained"
    );
    assert_eq!(
        pool.physical_fill_percentage(),
        expected_physical_fill,
        "checkpoint-retained bytes must remain visible to the write throttle"
    );
    assert!(!pool.physical_is_empty());
}

#[test]
fn write_throttle_paces_only_the_target_ring_shard() {
    let tmp = NamedTempFile::new().unwrap();
    let slot = BufferShard::slot_size();
    let data_start = COMMIT_LOG_SUPERBLOCK_SIZE + 2 * SHARD_CHECKPOINT_SIZE;
    let size = data_start + 20 * slot;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let runtime_limits = BufferRuntimeLimits::default().with_throttle(ThrottleSettings {
        min_pct: 60,
        max_pct: 100,
        scale_us: 100,
        cap_us: 1_000,
    });
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        Arc::new(dev),
        Duration::from_millis(1),
        2,
        256,
        Duration::ZERO,
        0,
        None,
        runtime_limits,
    )
    .unwrap();

    {
        let mut ring = pool.shards[0].shard.ring.lock();
        assert_eq!(BufferShard::reserve_log_space(&mut ring, 1, 8), Some(0));
    }
    assert_eq!(pool.physical_fill_percentage_for_shard(0), 80);
    assert_eq!(pool.physical_fill_percentage_for_shard(1), 0);

    pool.throttle_states[1]
        .cached_fill_pct
        .store(80, Ordering::Relaxed);
    pool.throttle_states[1]
        .sample_counter
        .store(1, Ordering::Relaxed);
    pool.throttle_states[1]
        .last_wakeup_ns
        .store(1_000_000, Ordering::Relaxed);
    pool.apply_write_throttle(1);
    assert_eq!(
        pool.throttle_states[1]
            .last_wakeup_ns
            .load(Ordering::Relaxed),
        0
    );

    pool.apply_write_throttle(0);
    assert!(
        pool.throttle_states[0]
            .last_wakeup_ns
            .load(Ordering::Relaxed)
            > 0
    );
    assert_eq!(
        pool.throttle_states[1]
            .last_wakeup_ns
            .load(Ordering::Relaxed),
        0
    );
}

// ── Phase A.2: mark_applied + release_below split ────────────────────
//
// The buffer-as-sole-journal plan needs to "apply" a buffer entry into
// in-memory metadb state before the metadb checkpoint covers it, then
// release the ring space later. Today's `mark_flushed` does both in one
// shot. These tests pin the new split behaviour: `mark_applied` clears
// the read path and accounts the entry as ring-reclaim-eligible without
// advancing the head, and `release_below(seq_cap)` runs the reclaim pass
// bounded by `min(durable_seq, seq_cap)`.

#[test]
fn mark_applied_clears_read_path_but_does_not_release_ring() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq = pool
        .append("test-vol", Lba(7), 1, &vec![0xA5; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    // Pre-state: entry is in pending + lba_index, ring slot held.
    assert!(shard.pending_entry_arc(seq).is_some());
    assert!(pool.lookup("test-vol", Lba(7)).unwrap().is_some());
    let used_before = shard.ring.lock().used_bytes;
    assert!(used_before > 0);

    pool.mark_applied(seq, Lba(7), 1).unwrap();

    // Read path no longer sees the entry — metadb is now authoritative
    // for the mapping.
    assert!(shard.pending_entry_arc(seq).is_none());
    assert!(pool.lookup("test-vol", Lba(7)).unwrap().is_none());
    assert_eq!(pool.payload_memory_bytes(), 0);

    // Ring head HAS NOT advanced — seq is in flushed_seqs awaiting the
    // checkpoint release.
    let ring = shard.ring.lock();
    assert_eq!(
        ring.used_bytes, used_before,
        "mark_applied must not advance the ring head"
    );
    assert!(ring.flushed_seqs.contains(&seq));
}

#[test]
fn applied_frontier_does_not_jump_over_older_pending_seq() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 16 * 8192, Duration::from_millis(1));
    let seq1 = pool
        .append("test-vol", Lba(1), 1, &vec![0x11; BLOCK_SIZE as usize], 0)
        .unwrap();
    let seq2 = pool
        .append("test-vol", Lba(2), 1, &vec![0x22; BLOCK_SIZE as usize], 0)
        .unwrap();

    pool.mark_applied(seq2, Lba(2), 1).unwrap();
    assert_eq!(
        pool.applied_frontier(),
        seq1.saturating_sub(1),
        "a completed high seq must not hide an older pending entry"
    );

    pool.mark_applied(seq1, Lba(1), 1).unwrap();
    assert_eq!(pool.applied_frontier(), seq2);
}

#[test]
fn release_below_advances_ring_only_when_checkpoint_covers_seq() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq = pool
        .append("test-vol", Lba(11), 1, &vec![0x5C; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    pool.mark_applied(seq, Lba(11), 1).unwrap();

    // Production wires the engine's watermark thread to advance
    // durable_seq once the entry's payload is fsync-durable. The unit
    // test stands in for that thread.
    pool.durable_seq_handle()
        .store(seq, std::sync::atomic::Ordering::Release);

    let used_before = shard.ring.lock().used_bytes;
    assert!(used_before > 0);

    // Checkpoint hasn't covered this seq yet — ring stays put.
    let advanced = pool.release_below(seq - 1).unwrap();
    assert_eq!(advanced, 0);
    assert_eq!(shard.ring.lock().used_bytes, used_before);

    // Checkpoint covers the seq — ring advances on the next pass.
    let advanced = pool.release_below(seq).unwrap();
    assert_eq!(advanced, 1);
    assert_eq!(shard.ring.lock().used_bytes, 0);
}

#[test]
fn explicit_checkpoint_persists_released_ring_state() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let seq = pool
        .append("test-vol", Lba(11), 1, &vec![0x5C; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );
    pool.mark_applied(seq, Lba(11), 1).unwrap();
    pool.durable_seq_handle()
        .store(seq, std::sync::atomic::Ordering::Release);
    assert_eq!(pool.release_below(seq).unwrap(), 1);

    pool.persist_checkpoints().unwrap();

    let mut encoded = [0u8; SHARD_CHECKPOINT_SIZE as usize];
    pool.root_device
        .read_at(&mut encoded, COMMIT_LOG_SUPERBLOCK_SIZE)
        .unwrap();
    let checkpoint = ShardCheckpoint::decode(&encoded).unwrap();
    assert_eq!(checkpoint.used_bytes, 0);
    assert_eq!(checkpoint.head_offset, checkpoint.tail_offset);
    assert!(checkpoint.max_seq >= seq);
}

// ── Regression: pending_seqs index must drop on every removal path ────
//
// cfec549 added a per-shard BTreeSet<u64> `pending_seqs` to short-circuit
// the coalescer's bounded "oldest pending" lookup, but only wired the
// drop into `note_applied` (mark_applied path). The non-mark_applied
// removal paths — `free_seq_allocation` (mark_flushed / supersede
// retire), `free_seq_allocation_durable` (purge / discard), and
// `evict_pending_entry` (staging-send shutdown) — kept removing
// pending_entries without touching pending_seqs, so stale-front seqs
// accumulated and wedged the lookup the way 643548f originally fixed.

#[test]
fn mark_flushed_drops_seq_from_pending_seqs_index() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq1 = pool
        .append("test-vol", Lba(0), 1, &vec![0x11; BLOCK_SIZE as usize], 0)
        .unwrap();
    let seq2 = pool
        .append("test-vol", Lba(1), 1, &vec![0x22; BLOCK_SIZE as usize], 0)
        .unwrap();
    pool.recv_ready_timeout(Duration::from_secs(2)).unwrap();
    pool.recv_ready_timeout(Duration::from_secs(2)).unwrap();

    assert_eq!(
        shard
            .ring
            .lock()
            .pending_seqs
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![seq1, seq2],
        "both appends must land in pending_seqs"
    );

    // mark_flushed routes through free_seq_allocation — the path that
    // the bug left without a pending_seqs.remove.
    pool.mark_flushed(seq1, Lba(0), 1).unwrap();

    let leftover: Vec<u64> = shard.ring.lock().pending_seqs.iter().copied().collect();
    assert_eq!(
        leftover,
        vec![seq2],
        "free_seq_allocation must drop seq from pending_seqs to mirror pending_entries"
    );
    assert!(shard.pending_entry_arc(seq1).is_none());
}

#[test]
fn head_pending_stuck_skips_stale_pending_seqs_entries() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq_live = pool
        .append("test-vol", Lba(0), 1, &vec![0xAB; BLOCK_SIZE as usize], 0)
        .unwrap();
    pool.recv_ready_timeout(Duration::from_secs(2)).unwrap();

    // Simulate the pre-fix bug: a stale seq at the front of pending_seqs
    // whose pending_entries slot has already been removed. Pre-fix this
    // permanently blocked head_pending_seq_if_stuck for *every* call.
    {
        let mut ring = shard.ring.lock();
        ring.pending_seqs.insert(seq_live.saturating_sub(1).max(0));
    }

    pool.durable_seq_handle()
        .store(seq_live, std::sync::atomic::Ordering::Release);
    shard
        .lv2_durability
        .synced_seq
        .store(seq_live, std::sync::atomic::Ordering::Release);

    // min_age=0 so the age gate doesn't mask the test; the stale-front
    // walk must still surface the live seq.
    let head = shard.head_pending_seq_if_stuck(Duration::ZERO);
    assert_eq!(
        head,
        Some(seq_live),
        "stale-front seq must not hide the genuine pending head"
    );

    // oldest_pending_arcs must also see past the stale entry.
    let oldest = shard.oldest_pending_arcs(4);
    assert_eq!(oldest.len(), 1);
    assert_eq!(oldest[0].seq, seq_live);
}

#[test]
fn oldest_pending_arcs_walks_and_prunes_stale_prefix_beyond_scan_window() {
    let slot = BufferShard::slot_size();
    let (pool, _tmp) = create_pool(
        COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE + 256 * slot,
        Duration::from_millis(1),
    );
    let shard = &pool.shards[0].shard;
    let mut seqs = Vec::new();

    for lba in 0..97 {
        seqs.push(
            pool.append("test-vol", Lba(lba), 1, &[0xCD; BLOCK_SIZE as usize], 0)
                .unwrap(),
        );
    }

    // Model a stale index prefix longer than the cursor's minimum 64-entry
    // scan window. The final seq remains live and must still be returned.
    for seq in &seqs[..96] {
        shard.pending_entries.remove(seq);
    }
    let live_seq = *seqs.last().unwrap();
    shard
        .lv2_durability
        .synced_seq
        .store(live_seq, std::sync::atomic::Ordering::Release);

    let oldest = shard.oldest_pending_arcs(1);
    assert_eq!(oldest.len(), 1);
    assert_eq!(oldest[0].seq, live_seq);

    let indexed: Vec<u64> = shard.ring.lock().pending_seqs.iter().copied().collect();
    assert_eq!(indexed, vec![live_seq], "stale prefix must be pruned");
}

#[test]
fn oldest_pending_arcs_byte_budget_stops_after_covering_window() {
    let slot = BufferShard::slot_size();
    let (pool, _tmp) = create_pool(
        COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE + 64 * slot,
        Duration::from_millis(1),
    );
    let shard = &pool.shards[0].shard;
    let payload = vec![0x5A; 4 * BLOCK_SIZE as usize];
    let mut seqs = Vec::new();
    for i in 0..3 {
        seqs.push(pool.append("test-vol", Lba(i * 4), 4, &payload, 0).unwrap());
    }

    let snapshot = shard.oldest_pending_arcs_with_budget(100, 5 * BLOCK_SIZE as usize);
    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot[0].seq, seqs[0]);
    assert_eq!(snapshot[1].seq, seqs[1]);
}

#[test]
fn shard_ready_channel_coalesces_wakes_without_hiding_pending_entries() {
    let slot = BufferShard::slot_size();
    let (pool, _tmp) = create_pool(
        COMMIT_LOG_SUPERBLOCK_SIZE + SHARD_CHECKPOINT_SIZE + 64 * slot,
        Duration::from_millis(1),
    );
    for i in 0..8 {
        pool.append("test-vol", Lba(i), 1, &[0xA5; BLOCK_SIZE as usize], 0)
            .unwrap();
    }

    assert_eq!(pool.shard_ready_rxs[0].len(), 1);
    assert_eq!(pool.oldest_ready_pending_arcs_for_shard(0, 16).len(), 8);
}

#[test]
fn ring_reservation_does_not_publish_pending_seq_before_entry_exists() {
    let mut ring = make_ring(128);
    assert_eq!(BufferShard::reserve_log_space(&mut ring, 42, 1), Some(0));
    assert!(
        !ring.pending_seqs.contains(&42),
        "reservation must not expose a seq before pending_entries insertion"
    );
}

// ── Lv2DurabilityWaiter targeted-wakeup tests ────────────────────────────
// The waiter wakes only the appenders whose seq is now durable; later-seq
// waiters stay parked. These guard the durability/lost-wakeup invariants the
// targeted unpark relies on (replacing the old shared-condvar notify_all).

#[test]
fn durability_waiter_fast_path_when_already_synced() {
    let w = Lv2DurabilityWaiter::new(0);
    w.advance(10);
    // seq <= synced returns immediately with zero registration.
    assert_eq!(w.wait_for(5), Duration::ZERO);
    assert_eq!(w.wait_for(10), Duration::ZERO);
}

#[test]
fn durability_waiter_channel_fast_path_when_already_synced() {
    let w = Lv2DurabilityWaiter::new(0);
    let (tx, rx) = bounded(1);
    w.advance(10);

    assert!(w.arm_channel(10, &tx));
    assert!(
        rx.try_recv().is_err(),
        "fast path must not queue a stale wake"
    );
}

#[test]
fn durability_waiter_channel_wakes_dispatcher() {
    let w = Lv2DurabilityWaiter::new(0);
    let (tx, rx) = bounded(1);

    assert!(!w.arm_channel(7, &tx));
    w.advance(7);

    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(w.synced_seq.load(Ordering::Acquire) >= 7);
}

#[test]
fn durability_waiter_wakes_parked_appender() {
    let w = Arc::new(Lv2DurabilityWaiter::new(0));
    let w2 = w.clone();
    let h = std::thread::spawn(move || {
        w2.wait_for(5);
    });
    // Give the waiter time to park, then advance past it.
    std::thread::sleep(Duration::from_millis(20));
    w.advance(5);
    h.join().unwrap();
    assert!(w.synced_seq.load(Ordering::Acquire) >= 5);
}

#[test]
fn durability_waiter_selective_wake_leaves_later_seq_parked() {
    let w = Arc::new(Lv2DurabilityWaiter::new(0));
    let early_done = Arc::new(AtomicBool::new(false));
    let late_done = Arc::new(AtomicBool::new(false));

    let (we, ee) = (w.clone(), early_done.clone());
    let early = std::thread::spawn(move || {
        we.wait_for(5);
        ee.store(true, Ordering::Release);
    });
    let (wl, el) = (w.clone(), late_done.clone());
    let late = std::thread::spawn(move || {
        wl.wait_for(10);
        el.store(true, Ordering::Release);
    });

    std::thread::sleep(Duration::from_millis(20));
    // Advance to 7: only the seq-5 waiter is durable.
    w.advance(7);
    early.join().unwrap();
    assert!(
        early_done.load(Ordering::Acquire),
        "seq-5 appender must wake"
    );
    // The seq-10 waiter must still be parked (advance(7) < 10).
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        !late_done.load(Ordering::Acquire),
        "seq-10 appender must stay parked until advance covers it"
    );
    // Now cover it.
    w.advance(12);
    late.join().unwrap();
    assert!(late_done.load(Ordering::Acquire));
}

#[test]
fn durability_waiter_advance_over_gap_wakes_lower_seqs() {
    // A batch's max_seq need not equal any waiter's seq (cancelled/coalesced
    // gaps). advance(max) must still wake every waiter with seq <= max.
    let w = Arc::new(Lv2DurabilityWaiter::new(0));
    let mut handles = Vec::new();
    for seq in [3u64, 6, 9] {
        let wc = w.clone();
        handles.push(std::thread::spawn(move || wc.wait_for(seq)));
    }
    std::thread::sleep(Duration::from_millis(20));
    // max_seq=11 matches no waiter exactly but covers all three.
    w.advance(11);
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn durability_waiter_no_lost_wakeup_under_race() {
    // Hammer the register-vs-advance race: each appender's seq is advanced
    // concurrently from another thread. If the lock re-check / unpark ordering
    // were wrong, some join would hang (the harness would time out).
    for _ in 0..200 {
        let w = Arc::new(Lv2DurabilityWaiter::new(0));
        let seq = 1u64;
        let wc = w.clone();
        let waiter = std::thread::spawn(move || wc.wait_for(seq));
        let wa = w.clone();
        let advancer = std::thread::spawn(move || wa.advance(seq));
        waiter.join().unwrap();
        advancer.join().unwrap();
        assert!(w.synced_seq.load(Ordering::Acquire) >= seq);
    }
}

#[test]
fn durability_waiter_channel_has_no_lost_wakeup_under_race() {
    for _ in 0..200 {
        let w = Arc::new(Lv2DurabilityWaiter::new(0));
        let (tx, rx) = bounded(1);
        let wc = w.clone();
        let register = std::thread::spawn(move || wc.arm_channel(1, &tx));
        let wa = w.clone();
        let advance = std::thread::spawn(move || wa.advance(1));

        let already_durable = register.join().unwrap();
        advance.join().unwrap();
        if !already_durable {
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        assert!(w.synced_seq.load(Ordering::Acquire) >= 1);
    }
}

#[test]
fn durability_waiter_many_appenders_all_wake() {
    // Convoy analog: many appenders park, one advance covers them all.
    let w = Arc::new(Lv2DurabilityWaiter::new(0));
    let woke = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for seq in 1..=64u64 {
        let wc = w.clone();
        let woke = woke.clone();
        handles.push(std::thread::spawn(move || {
            wc.wait_for(seq);
            woke.fetch_add(1, Ordering::Relaxed);
        }));
    }
    std::thread::sleep(Duration::from_millis(30));
    w.advance(64);
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(woke.load(Ordering::Relaxed), 64);
}

#[test]
fn global_prepared_queue_depth_preserves_auto_default_and_explicit_override() {
    assert_eq!(
        WriteBufferPool::resolve_global_prepared_queue_depth(0, 16),
        16
    );
    assert_eq!(
        WriteBufferPool::resolve_global_prepared_queue_depth(4, 16),
        4
    );
    assert_eq!(
        WriteBufferPool::resolve_global_prepared_queue_depth(0, 0),
        1
    );
}
