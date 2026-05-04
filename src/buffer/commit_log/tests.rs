use super::*;
use std::sync::Arc;
use tempfile::NamedTempFile;

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
    let shard_device = pool
        .root_device
        .slice(data_start, size - data_start)
        .unwrap();
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
            superseded_ranges: Vec::new(),
        });
        entries.push(StagedEntry { pending, payload });
    }

    WriteBufferPool::write_batch_and_sync_uring(
        &shard_device,
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
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;

    let seq = pool
        .append("test-vol", Lba(11), 1, &vec![0x5C; BLOCK_SIZE as usize], 0)
        .unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    let pending = loop {
        let pending = shard
            .pending_entries
            .get(&seq)
            .map(|entry| entry.value().clone())
            .unwrap();
        if pending.payload.is_none() {
            break pending;
        }
        assert!(
            Instant::now() < deadline,
            "sync loop did not publish an uncached ready entry in time"
        );
        thread::sleep(Duration::from_millis(10));
    };

    let payload = shard.read_payload_from_disk(pending.as_ref()).unwrap();
    shard.mark_flushed(seq, Lba(11), 1).unwrap();
    assert_eq!(pool.payload_memory_bytes(), 0);

    let mut hydrated = pending.as_ref().clone();
    hydrated.payload = Some(payload);
    let hydrated = Arc::new(hydrated);
    assert!(
        !shard.replace_pending_entry_if_current(&pending, hydrated),
        "hydration must not reinstall a seq that was already flushed"
    );
    assert!(shard.pending_entry_arc(seq).is_none());
    assert_eq!(pool.payload_memory_bytes(), 0);
}

#[test]
fn inflight_missing_volatile_payload_is_not_hydrated_from_disk() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 4096 + 8 * 8192;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let runtime_limits = BufferRuntimeLimits {
        staging_channel_capacity: 1,
        sync_batch_max_entries: 1,
        sync_batch_max_bytes: usize::MAX,
        volatile_payload_memory: 0,
    };
    let pool = WriteBufferPool::open_with_options_full_and_limits(
        dev,
        Duration::from_millis(1),
        1,
        256,
        Duration::ZERO,
        0,
        None,
        runtime_limits,
    )
    .unwrap();
    let shard = &pool.shards[0].shard;
    let payload = vec![0xA7; BLOCK_SIZE as usize];

    let seq = pool.append("test-vol", Lba(17), 1, &payload, 0).unwrap();
    let pending = shard.pending_entries.get(&seq).unwrap().value().clone();
    {
        let mut lc = shard.lifecycle.lock();
        lc.inflight.insert(seq);
    }
    let _ = shard.remove_volatile_payload(seq);

    assert!(
        shard.pending_entry_arc_hydrated(seq).is_none(),
        "flusher hydration must not parse a pre-sync ring slot"
    );
    assert!(
        shard.pending_entries.contains_key(&seq),
        "pre-sync entry must not be evicted as corrupt"
    );
    assert!(
        shard.lookup_hydrated("test-vol", Lba(17)).is_err(),
        "foreground read must fail loudly instead of falling through to stale LV3 data"
    );
    assert!(Arc::ptr_eq(
        &pending,
        &shard.pending_entries.get(&seq).unwrap().value().clone()
    ));
}

#[test]
fn lookup_hydrates_from_disk_without_volatile_payload() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;
    let payload = vec![0x6D; BLOCK_SIZE as usize];

    let seq = pool.append("test-vol", Lba(23), 1, &payload, 0).unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let before = shard.pending_entry_arc(seq).unwrap();
    assert!(before.payload.is_none());
    assert!(
        shard.volatile_payload(seq).is_none(),
        "ready entries should hydrate from disk, not volatile payload"
    );

    let found = pool.lookup("test-vol", Lba(23)).unwrap().unwrap();
    assert_eq!(found.payload.as_deref(), Some(payload.as_slice()));

    let after = shard.pending_entry_arc(seq).unwrap();
    assert!(after.payload.is_none());
    assert!(Arc::ptr_eq(&before, &after));
}

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
        assert!(
            pool.shards[0].shard.volatile_payload(seq).is_none(),
            "ready entries should hydrate from disk"
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
        1,
        "contiguous one-LBA entries should hydrate with one batched LV2 read"
    );
}

#[test]
fn flusher_hydration_does_not_install_payload_into_indices() {
    let (pool, _tmp) = create_pool(4096 + 4096 + 8 * 8192, Duration::from_millis(1));
    let shard = &pool.shards[0].shard;
    let payload = vec![0x71; BLOCK_SIZE as usize];

    let seq = pool.append("test-vol", Lba(31), 1, &payload, 0).unwrap();
    assert_eq!(
        pool.recv_ready_timeout(Duration::from_secs(2)).unwrap(),
        seq
    );

    let before = shard.pending_entry_arc(seq).unwrap();
    assert!(before.payload.is_none());
    assert!(shard.volatile_payload(seq).is_none());

    let hydrated = shard.pending_entry_arc_hydrated(seq).unwrap();
    assert_eq!(hydrated.payload.as_deref(), Some(payload.as_slice()));

    let after = shard.pending_entry_arc(seq).unwrap();
    assert!(after.payload.is_none());
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(pool.payload_memory_bytes(), 0);
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
