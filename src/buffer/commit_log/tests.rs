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
