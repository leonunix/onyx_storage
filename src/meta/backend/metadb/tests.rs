use super::*;
use crate::meta::backend::metadb::values::from_dedup_value;
use crate::space::extent::Extent;
use crate::types::{CompressionAlgo, VolumeId};

#[test]
fn pba_newtypes_cross_backend_losslessly() {
    let pba = Pba(1234);
    assert_eq!(from_metadb_pba(to_metadb_pba(pba)), pba);
}

#[test]
fn dedup_value_uses_zero_padded_metadb_slot() {
    let entry = DedupEntry {
        pba: Pba(7),
        slot_offset: 5,
        compression: 1,
        unit_compressed_size: 1024,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xDEAD_BEEF,
    };

    let value = to_dedup_value(&entry);
    assert_eq!(value.as_bytes()[27], 0);
    assert_eq!(from_dedup_value(value), Some(entry));
}

#[test]
fn volume_catalog_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(CATALOG_FILE);
    let mut catalog = VolumeCatalog::default();
    catalog.by_id.insert(
        "vol-a".to_string(),
        VolumeCatalogEntry {
            ordinal: 3,
            config: VolumeConfig {
                id: VolumeId("vol-a".to_string()),
                size_bytes: 4096,
                block_size: 4096,
                compression: CompressionAlgo::Lz4,
                created_at: 10,
                zone_count: 4,
            },
        },
    );

    catalog.persist(&path).unwrap();
    let loaded = VolumeCatalog::load(&path).unwrap();

    let entry = loaded.by_id.get("vol-a").unwrap();
    assert_eq!(entry.ordinal, 3);
    assert_eq!(entry.config.size_bytes, 4096);
    assert_eq!(entry.config.compression, CompressionAlgo::Lz4);
}

#[test]
fn backend_volume_catalog_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };

    {
        let backend = MetadbBackend::open(&meta).unwrap();
        backend.put_volume(&vol).unwrap();
    }

    let backend = MetadbBackend::open(&meta).unwrap();
    let loaded = backend.get_volume(&vol.id).unwrap().unwrap();
    assert_eq!(loaded.id, vol.id);
    assert_eq!(loaded.size_bytes, vol.size_bytes);
    assert_eq!(loaded.compression, vol.compression);
    let listed = backend.list_volumes().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, vol.id);
}

#[test]
fn backend_reads_l2p_values_by_volume_id() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let value = BlockmapValue {
        pba: Pba(77),
        compression: 1,
        unit_compressed_size: 1234,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xCAFE_BABE,
        slot_offset: 0,
        flags: 0,
    };

    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();
    let ord = backend.volume_ordinal(&vol.id).unwrap();
    backend
        .db
        .insert(ord, 3, to_l2p_value(&value))
        .expect("insert test mapping");

    assert_eq!(backend.get_mapping(&vol.id, Lba(3)).unwrap(), Some(value));
    assert_eq!(
        backend
            .multi_get_mappings(&vol.id, &[Lba(2), Lba(3)])
            .unwrap(),
        vec![None, Some(value)]
    );
    assert_eq!(
        backend
            .multi_get_mappings_ord(ord, &[Lba(2), Lba(3)])
            .unwrap(),
        vec![None, Some(value)]
    );
    assert_eq!(
        backend.get_mappings_range(&vol.id, Lba(0), Lba(8)).unwrap(),
        vec![(Lba(3), value)]
    );
    assert_eq!(
        backend
            .get_mappings_range_unordered_ord(ord, Lba(0), Lba(8))
            .unwrap(),
        vec![(Lba(3), value)]
    );
}

#[test]
fn atomic_batch_write_updates_refcounts_and_reports_freed_pba() {
    // Rc-neutral path: hot-path `atomic_batch_write` no longer mutates
    // global rc, and the L2pRemap outcome's `freed_pba` is always
    // None. `newly_zeroed_from_remaps` filters its decrements map
    // to only `pba_freed=true` entries — so the returned cleanups
    // map is empty even when an old PBA was logically overwritten.
    // The retire flow now runs via the dead-list → Lineage GC →
    // FreePbas channel (not exercised here).
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let old = BlockmapValue {
        pba: Pba(10),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 1,
        slot_offset: 0,
        flags: 0,
    };
    let new = BlockmapValue {
        pba: Pba(20),
        crc32: 2,
        ..old
    };

    backend
        .atomic_batch_write(&vol.id, &[(Lba(0), old), (Lba(1), old)], 2)
        .unwrap();
    assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 0);

    let freed = backend
        .atomic_batch_write(&vol.id, &[(Lba(0), new), (Lba(1), new)], 2)
        .unwrap();

    assert_eq!(backend.get_refcount(Pba(10)).unwrap(), 0);
    assert_eq!(backend.get_refcount(Pba(20)).unwrap(), 0);
    assert!(
        freed.is_empty(),
        "Rc-neutral path: L2pRemap surfaces no freed_pba on the hot path; cleanups map empty"
    );
    // L2P state moved to the new mapping.
    let m = backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap();
    assert_eq!(m.pba, Pba(20));
}

/// Sanity check that passthrough's range-emission helper turns a
/// contiguous batch into one range op and persists every LBA
/// correctly. The metadb-side apply test
/// (`l2p_remap_range_writes_each_lba_and_increfs_distinct_pbas`)
/// already validates the apply lane; this test guards the onyx
/// glue: helper construction, ordinal lookup, refcount aggregation,
/// freed-pba decoder.
#[test]
fn atomic_batch_write_range_contiguous_lbas() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 32,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    // 8 LBAs starting at LBA 0, each pointing at a distinct PBA so we
    // can verify per-LBA writes survive the range emission.
    let batch: Vec<(Lba, BlockmapValue)> = (0..8u64)
        .map(|i| {
            let v = BlockmapValue {
                pba: Pba(100 + i),
                compression: 1,
                unit_compressed_size: 4096,
                unit_original_size: 4096,
                unit_lba_count: 1,
                offset_in_unit: 0,
                crc32: i as u32,
                slot_offset: 0,
                flags: 0,
            };
            (Lba(i), v)
        })
        .collect();
    backend.atomic_batch_write(&vol.id, &batch, 1).unwrap();
    for i in 0..8u64 {
        let m = backend.get_mapping(&vol.id, Lba(i)).unwrap().unwrap();
        assert_eq!(m.pba, Pba(100 + i));
        // Rc-neutral path: hot-path atomic_batch_write doesn't touch rc.
        assert_eq!(backend.get_refcount(Pba(100 + i)).unwrap(), 0);
    }
}

/// LBAs with a gap split into two range ops. Both must commit and
/// the outcomes decoder must walk new_values in order across both
/// outcomes — a regression here would either crash on the
/// "outcomes consumed more/fewer values than the batch produced"
/// error or lose the second range's mappings.
#[test]
fn atomic_batch_write_range_with_gap_splits_into_two_ops() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 32,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let bv = |pba: u64, crc: u32| BlockmapValue {
        pba: Pba(pba),
        compression: 1,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: crc,
        slot_offset: 0,
        flags: 0,
    };

    // Two contiguous runs separated by a gap: [0..3) and [10..12).
    let batch = vec![
        (Lba(0), bv(200, 0)),
        (Lba(1), bv(201, 1)),
        (Lba(2), bv(202, 2)),
        (Lba(10), bv(210, 10)),
        (Lba(11), bv(211, 11)),
    ];
    backend.atomic_batch_write(&vol.id, &batch, 1).unwrap();

    for i in 0..3u64 {
        assert_eq!(
            backend.get_mapping(&vol.id, Lba(i)).unwrap().unwrap().pba,
            Pba(200 + i),
        );
    }
    for (lba, pba) in [(10u64, 210u64), (11, 211)] {
        assert_eq!(
            backend.get_mapping(&vol.id, Lba(lba)).unwrap().unwrap().pba,
            Pba(pba),
        );
    }
    assert!(backend.get_mapping(&vol.id, Lba(5)).unwrap().is_none());
}

#[test]
fn atomic_batch_write_range_outcomes_exclude_following_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        l2p_buffer_enabled: true,
        commit_direct_apply_enabled: true,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-range-outcomes".to_string()),
        size_bytes: 4096 * 32,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let value = |pba: u64| BlockmapValue {
        pba: Pba(pba),
        compression: 1,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: pba as u32,
        slot_offset: 0,
        flags: 0,
    };
    let singleton = vec![(Lba(0), value(300))];
    let range = vec![(Lba(10), value(310)), (Lba(11), value(311))];
    let units = vec![
        (&vol.id, singleton.as_slice(), 1u32),
        (&vol.id, range.as_slice(), 1u32),
    ];
    let dedup_entries = [(
        [7u8; 8],
        DedupEntry {
            pba: Pba(300),
            slot_offset: 0,
            compression: 1,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 300,
        },
    )];
    let (_cleanups, accepted) = backend
        .atomic_batch_write_multi_with_dedup(&units, &dedup_entries, &[1, 2, 3])
        .unwrap();

    assert_eq!(accepted, vec![true, true, true]);
    for (lba, pba) in [(0, 300), (10, 310), (11, 311)] {
        assert_eq!(
            backend.get_mapping(&vol.id, Lba(lba)).unwrap().unwrap().pba,
            Pba(pba),
        );
    }

    // Cover the single-unit entry point's independent remap-outcome prefix.
    let second_range = vec![(Lba(20), value(320)), (Lba(21), value(321))];
    let second_dedup = [(
        [8u8; 8],
        DedupEntry {
            pba: Pba(320),
            slot_offset: 0,
            compression: 1,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 320,
        },
    )];
    let (_cleanups, second_accepted) = backend
        .atomic_batch_write_with_dedup(&vol.id, &second_range, 1, &second_dedup, &[4, 5])
        .unwrap();
    assert_eq!(second_accepted, vec![true, true]);
    assert_eq!(
        backend.get_dedup(&second_dedup[0].0).unwrap(),
        Some(second_dedup[0].1)
    );
}

#[test]
fn dedup_entries_and_flag_scan_round_trip() {
    // Rc-neutral path: `dedup_entry_is_live` checks rc(entry.pba)>0 to
    // catch entries pointing at PBAs whose final decref already
    // landed. Hot-path L2pRemap no longer maintains rc, so we
    // seed rc explicitly via the test helper to mirror the
    // production state shape (a dedup index entry is only put
    // for a verified-shared PBA whose rc was bumped by a
    // promotion or prior put_dedup).
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let value = BlockmapValue {
        pba: Pba(30),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 1,
        slot_offset: 0,
        flags: FLAG_DEDUP_SKIPPED,
    };
    backend
        .atomic_batch_write(&vol.id, &[(Lba(0), value)], 1)
        .unwrap();
    backend.set_refcount(value.pba, 1).unwrap(); // Rc-neutral mode rc seed

    let hash = [9u8; 8];
    let dedup = DedupEntry {
        pba: value.pba,
        slot_offset: value.slot_offset,
        compression: value.compression,
        unit_compressed_size: value.unit_compressed_size,
        unit_original_size: value.unit_original_size,
        unit_lba_count: value.unit_lba_count,
        offset_in_unit: value.offset_in_unit,
        crc32: value.crc32,
    };
    backend.put_dedup_entries(&[(hash, dedup)]).unwrap();

    assert_eq!(backend.get_dedup(&hash).unwrap(), Some(dedup));
    assert!(backend.dedup_entry_is_live(&hash, &dedup).unwrap());
    assert_eq!(backend.scan_dedup_skipped(8).unwrap().len(), 1);

    backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();
    assert!(backend.scan_dedup_skipped(8).unwrap().is_empty());

    let replacement = BlockmapValue {
        pba: Pba(40),
        flags: 0,
        ..value
    };
    backend
        .atomic_batch_write(&vol.id, &[(Lba(0), replacement)], 1)
        .unwrap();
    // Rc-neutral path: hot-path L2pRemap doesn't touch rc, so the
    // replacement leaves rc(value.pba) at its prior value. The
    // earlier `put_dedup_entries` issued a `WalOp::DedupPut`
    // whose apply increfs the new head_pba by 1 (rc 1 → 2). Old
    // mapping cleanup of the displaced PBA flows through the
    // dead-list / Lineage GC retire path, not the hot path.
    assert_eq!(backend.get_refcount(value.pba).unwrap(), 2);
}

#[test]
fn delete_range_and_volume_report_freed_extents() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let a = BlockmapValue {
        pba: Pba(100),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 1,
        slot_offset: 0,
        flags: 0,
    };
    let b = BlockmapValue {
        pba: Pba(200),
        crc32: 2,
        ..a
    };
    backend
        .atomic_batch_write(&vol.id, &[(Lba(0), a), (Lba(1), b), (Lba(2), b)], 3)
        .unwrap();
    // Rc-neutral path: hot-path atomic_batch_write, delete_blockmap_range, and
    // delete_volume are all rc-neutral for ordinary PBA refs. Seed rc to
    // mirror a shared/dedup PBA and verify logical deletes do not drive it
    // to zero.
    backend.set_refcount(Pba(100), 1).unwrap();
    backend.set_refcount(Pba(200), 2).unwrap();

    let freed = backend
        .delete_blockmap_range(&vol.id, Lba(1), Lba(3))
        .unwrap();
    assert!(
        freed.is_empty(),
        "range delete must not free shared rc-positive PBA: {freed:?}"
    );
    assert_eq!(backend.count_blockmap_refs_for_pba(Pba(100)).unwrap(), 1);
    assert_eq!(backend.get_refcount(Pba(200)).unwrap(), 2);

    let freed = backend.delete_volume(&vol.id).unwrap();
    assert!(
        freed.is_empty(),
        "delete volume must not free rc-positive PBA by per-LBA decref: {freed:?}"
    );
    assert_eq!(backend.get_refcount(Pba(100)).unwrap(), 1);
    assert!(backend.get_volume(&vol.id).unwrap().is_none());
}

#[test]
fn referenced_extents_batches_and_matches_per_pba_probe() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let mk = |pba: u64| BlockmapValue {
        pba: Pba(pba),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 1,
        slot_offset: 0,
        flags: 0,
    };
    // Live mappings reference PBAs 100, 101 and 300.
    backend
        .atomic_write_mapping(&vol.id, Lba(0), &mk(100))
        .unwrap();
    backend
        .atomic_write_mapping(&vol.id, Lba(1), &mk(101))
        .unwrap();
    backend
        .atomic_write_mapping(&vol.id, Lba(2), &mk(300))
        .unwrap();

    // Mix of referenced and unreferenced candidate extents, in arbitrary order.
    let extents = [
        (Pba(100), 1u32), // covers live 100  -> referenced
        (Pba(200), 4u32), // 200..204 unused  -> not referenced
        (Pba(101), 1u32), // covers live 101  -> referenced
        (Pba(298), 4u32), // 298..302 covers live 300 -> referenced
        (Pba(400), 2u32), // 400..402 unused  -> not referenced
    ];
    let got = backend.referenced_extents(&extents).unwrap();
    assert_eq!(got, vec![true, false, true, true, false]);

    // Each batched answer must equal a brute-force per-PBA probe.
    for (i, &(start, blocks)) in extents.iter().enumerate() {
        let brute = (0..u64::from(blocks))
            .any(|off| backend.has_any_blockmap_ref(Pba(start.0 + off)).unwrap());
        assert_eq!(got[i], brute, "extent {i} ({start:?}, {blocks}) mismatch");
    }

    // Empty input is a no-op.
    assert!(backend.referenced_extents(&[]).unwrap().is_empty());

    // All-referenced input exercises the early-stop path (remaining hits 0
    // before the scan finishes); correctness is what we assert.
    let all_referenced = backend
        .referenced_extents(&[(Pba(100), 1), (Pba(101), 1), (Pba(300), 1)])
        .unwrap();
    assert_eq!(all_referenced, vec![true, true, true]);

    // Zero-length candidates are never referenced and don't perturb others.
    let with_zero = backend
        .referenced_extents(&[(Pba(100), 0), (Pba(100), 1)])
        .unwrap();
    assert_eq!(with_zero, vec![false, true]);
}

#[test]
fn diagnostic_helpers_track_refs_and_allocated_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();
    let value = BlockmapValue {
        pba: Pba(55),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 1,
        slot_offset: 0,
        flags: 0,
    };

    backend
        .atomic_write_mapping(&vol.id, Lba(0), &value)
        .unwrap();
    // Rc-neutral path: exclusive PBAs (no dedup_index entry) never get
    // their global rc bumped on write — only lineage events
    // (clone promotion + drop_volume) touch rc.
    assert_eq!(backend.iter_refcounts().unwrap(), Vec::<(Pba, u32)>::new());
    assert_eq!(backend.iter_allocated_blocks().unwrap(), vec![(Pba(55), 1)]);
    assert!(backend.has_any_blockmap_ref(Pba(55)).unwrap());

    backend.delete_mapping(&vol.id, Lba(0)).unwrap();
    assert_eq!(backend.get_refcount(Pba(55)).unwrap(), 0);
}

/// Documents the underlying metadb behaviour the fix relies on
/// **avoiding**: any `l2p_remap` whose encoded `L2pValue` carries
/// seq=0 silently passes `seq_guard_rejects` regardless of the
/// stored seq, because the guard short-circuits on `new_seq == 0`
/// (`metadb/src/db/apply.rs::seq_guard_rejects`).
///
/// This was the P0 mapping-loss vector. Before the fix,
/// `update_blockmap_flags` emitted seq=0 (via `to_l2p_value` ->
/// `blockmap_to_l2p_bytes_with_seq(v, 0)`), so a `DedupScanner`
/// flag-clear that raced a buffer-flusher commit would clobber the
/// newer mapping back to a stale PBA. Production fio crc32c verify
/// (`tier2b-stage1-verify-20260516T151612Z`) saw 84 silent verify
/// errors and read_path.unmapped=45M.
///
/// The fix is at the *caller* layer: `update_blockmap_flags` now
/// carries the observed seq forward so apply's `seq_guard_rejects`
/// can actually reject losing-the-race cases. This test pins the
/// underlying metadb behaviour so a future contributor doesn't
/// re-introduce a seq=0 emitter assuming the guard catches it.
#[test]
fn metadb_seq0_in_l2p_remap_bypasses_guard_and_clobbers_newer_write() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    // Newer write commits at seq=200, pba=P2.
    let bv_new = BlockmapValue {
        pba: Pba(40),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xBBBB_BBBB,
        slot_offset: 0,
        flags: 0,
    };
    backend
        .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv_new)], 1, &[], &[200])
        .unwrap();
    assert_eq!(
        backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap().pba,
        Pba(40)
    );

    // A stale seq=0 update (simulating the pre-fix `update_blockmap_flags`
    // path, or any other caller that omits the seq) directly via the
    // dedup batch API — empty `seqs` slice -> seq_for returns 0.
    let stale_bv = BlockmapValue {
        pba: Pba(30),
        crc32: 0xAAAA_AAAA,
        ..bv_new
    };
    backend
        .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), stale_bv)], 1, &[], &[])
        .unwrap();

    // `seq_guard_rejects(0, _)` returns false, so apply accepts the
    // stale update unconditionally. The newer write is gone. Refcount
    // on the freed P2 drops to 0 -> allocator-reclaimable -> data loss.
    let after = backend.get_mapping(&vol.id, Lba(0)).unwrap().unwrap();
    assert_eq!(
        after.pba,
        Pba(30),
        "seq=0 sentinel must bypass seq_guard at the metadb layer; \
         callers must not emit seq=0 with this race shape"
    );
    assert_eq!(after.crc32, 0xAAAA_AAAA);
    // Rc-neutral path: hot-path atomic_batch_write_with_dedup no longer
    // moves global rc, so both PBAs are observed at 0. The L2P
    // clobber (the actual data-loss vector under test) is still
    // there — that's the load-bearing assertion above.
    assert_eq!(backend.get_refcount(Pba(40)).unwrap(), 0);
    assert_eq!(backend.get_refcount(Pba(30)).unwrap(), 0);
}

/// Regression test for the fix: `update_blockmap_flags` must carry
/// the observed L2pValue seq through to its `l2p_remap` so apply's
/// `seq_guard_rejects` can guard against the scanner-versus-flusher
/// race documented above. The pre-fix path emitted seq=0
/// unconditionally and silently won races it should have lost.
#[test]
fn update_blockmap_flags_preserves_observed_seq() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-a".to_string()),
        size_bytes: 4096 * 8,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    // Initial write at seq=100 with DEDUP_SKIPPED.
    let bv = BlockmapValue {
        pba: Pba(30),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0xAAAA_AAAA,
        slot_offset: 0,
        flags: FLAG_DEDUP_SKIPPED,
    };
    backend
        .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv)], 1, &[], &[100])
        .unwrap();
    let ord = backend.volume_ordinal(&vol.id).unwrap();
    assert_eq!(backend.db.get(ord, 0).unwrap().unwrap().seq(), 100);

    // The flag-clear path.
    backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();

    // Flags cleared, PBA preserved, and — critically — the seq is
    // still 100. Pre-fix the seq would be 0, which is the sentinel
    // value that bypasses seq_guard.
    let raw_after = backend.db.get(ord, 0).unwrap().unwrap();
    assert_eq!(
        raw_after.seq(),
        100,
        "update_blockmap_flags must preserve the observed seq so apply's \
         seq_guard can reject losing-the-race callers"
    );
    let bv_after = blockmap_from_l2p_bytes(&raw_after.0).unwrap();
    assert_eq!(bv_after.flags, 0);
    assert_eq!(bv_after.pba, Pba(30));
    assert_eq!(bv_after.crc32, 0xAAAA_AAAA);

    // A concurrent newer write at seq=200 must win against a
    // subsequent flag-clear that observed the seq=100 state. Here
    // we encode that interleaving explicitly: commit the newer
    // write first (so update_blockmap_flags will see it and
    // early-return on matching flags), then a fresh DEDUP_SKIPPED
    // write at seq=300, then update_blockmap_flags. The new
    // observed seq (300) is preserved and the apply accepts.
    let bv_newer = BlockmapValue {
        pba: Pba(40),
        crc32: 0xBBBB_BBBB,
        flags: FLAG_DEDUP_SKIPPED,
        ..bv
    };
    backend
        .atomic_batch_write_with_dedup(&vol.id, &[(Lba(0), bv_newer)], 1, &[], &[300])
        .unwrap();
    backend.update_blockmap_flags(&vol.id, Lba(0), 0).unwrap();
    let raw_after = backend.db.get(ord, 0).unwrap().unwrap();
    assert_eq!(raw_after.seq(), 300);
    let bv_after = blockmap_from_l2p_bytes(&raw_after.0).unwrap();
    assert_eq!(bv_after.flags, 0);
    assert_eq!(bv_after.pba, Pba(40));
}

#[test]
fn coalesce_free_pbas_empty_is_empty() {
    assert!(coalesce_free_pbas_to_extents(&[]).is_empty());
}

#[test]
fn coalesce_free_pbas_merges_contiguous_runs() {
    let pbas = vec![Pba(10), Pba(11), Pba(12), Pba(20), Pba(21), Pba(50)];
    let extents = coalesce_free_pbas_to_extents(&pbas);
    assert_eq!(
        extents,
        vec![
            Extent::new(Pba(10), 3),
            Extent::new(Pba(20), 2),
            Extent::new(Pba(50), 1),
        ]
    );
}

#[test]
fn coalesce_free_pbas_sorts_and_dedups_unsorted_input() {
    // Reordered + duplicates: walker may emit the same PBA more than
    // once across overlapping segments. Coalesce must collapse them.
    let pbas = vec![
        Pba(21),
        Pba(10),
        Pba(11),
        Pba(12),
        Pba(20),
        Pba(10),
        Pba(21),
        Pba(11),
    ];
    let extents = coalesce_free_pbas_to_extents(&pbas);
    assert_eq!(
        extents,
        vec![Extent::new(Pba(10), 3), Extent::new(Pba(20), 2)]
    );
}

#[test]
fn coalesce_free_pbas_singleton() {
    let extents = coalesce_free_pbas_to_extents(&[Pba(7)]);
    assert_eq!(extents, vec![Extent::new(Pba(7), 1)]);
}

#[test]
fn drain_lineage_freed_pbas_returns_empty_when_idle() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    assert!(backend.drain_lineage_freed_pbas().is_empty());
}

/// End-to-end: simulate metadb's GC dispatching a freed-PBA outcome
/// through the sink. The sink ingests via the cloned sender (mirrors
/// the path metadb's GC driver thread takes), and `drain_lineage_freed_pbas`
/// returns the queued PBAs in arrival order.
#[test]
fn drain_lineage_freed_pbas_returns_dispatched_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        ..Default::default()
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    // Simulate the sink firing. In production this is invoked by the
    // closure registered with `Db::set_freed_pbas_sink` inside
    // `MetadbBackend::open`; here we drive the channel directly so the
    // test does not depend on a fully wired GC cycle and the
    // lineage_gc_emit_freepbas flag.
    let tx = backend.lineage_freed_pbas_sender();
    tx.send(Pba(101)).unwrap();
    tx.send(Pba(102)).unwrap();
    tx.send(Pba(200)).unwrap();

    let drained = backend.drain_lineage_freed_pbas();
    assert_eq!(drained, vec![Pba(101), Pba(102), Pba(200)]);
    // Second drain is empty — channel is fully consumed.
    assert!(backend.drain_lineage_freed_pbas().is_empty());

    let extents = coalesce_free_pbas_to_extents(&drained);
    assert_eq!(
        extents,
        vec![Extent::new(Pba(101), 2), Extent::new(Pba(200), 1)]
    );
}

/// Onyx-adapter sanity check: with `commit_deferred_outcomes_enabled =
/// true` the `*_deferred` entry point returns a handle whose `recv()`
/// yields the same cleanup tuple shape as the synchronous wrapper.
/// We don't assert on the
/// HashMap contents (Rc-neutral mode default keeps `pba_freed` empty for
/// fresh writes — Lineage GC owns that flow now); only that the
/// adapter wires the round trip end-to-end and produces
/// `accepted = [true; N]` for fresh writes.
#[test]
fn atomic_batch_write_multi_with_dedup_deferred_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        block_cache_mb: 8,
        memtable_budget_mb: 64,
        index_pin_mb: 64,
        lsm_bloom_bits_per_entry: 10,
        checkpoint_interval_ms: 5000,
        group_commit_timeout_us: 1,
        wal_dir: None,
        dedup_shards: 1,
        dedup_cuckoo_buckets: 1_000_000,
        dedup_l1_cache_entries: 256_000,
        // Deferred-outcome mode on; pair with l2p_buffer_enabled so
        // the L2P-buffer code path that the deferred drain piggybacks
        // on is also active.
        l2p_buffer_enabled: true,
        commit_direct_apply_enabled: true,
        commit_deferred_outcomes_enabled: true,
        l2p_buffer_max_interval_ms: 25,
        ..Default::default()
    };
    let vol = VolumeConfig {
        id: VolumeId("vol-deferred".to_string()),
        size_bytes: 4096 * 64,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 10,
        zone_count: 4,
    };
    let backend = MetadbBackend::open(&meta).unwrap();
    backend.put_volume(&vol).unwrap();

    let batch_values: Vec<(Lba, BlockmapValue)> = (0..8u64)
        .map(|i| {
            (
                Lba(i),
                BlockmapValue {
                    pba: Pba(1000 + i),
                    compression: 1,
                    unit_compressed_size: 1234,
                    unit_original_size: 4096,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0xDEAD_BEEF,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();

    let units: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
        vec![(&vol.id, batch_values.as_slice(), 1u32)];
    let seqs: Vec<u64> = (1..=batch_values.len() as u64).collect();

    let handle = backend
        .atomic_batch_write_multi_with_dedup_deferred(&units, &[], &seqs)
        .expect("deferred submit accepted");
    // LSN is available before `recv` resolves.
    assert!(handle.lsn().is_some());
    let (cleanup, accepted) = handle.recv().expect("deferred recv resolves");
    assert_eq!(accepted.len(), batch_values.len());
    assert!(
        accepted.iter().all(|&ok| ok),
        "every fresh write must accept under seq_guard"
    );
    // Rc-neutral path: freed PBAs flow via Lineage GC, not outcomes — so the
    // cleanup map is empty for these fresh writes (no prior mapping
    // to displace, no rc-decref → no freed_pba in outcomes).
    assert!(
        cleanup.is_empty(),
        "fresh writes produce no displaced PBAs under Rc-neutral path: {cleanup:?}"
    );

    // The synchronous wrapper still works and is byte-equivalent.
    let batch_values2: Vec<(Lba, BlockmapValue)> = (8..16u64)
        .map(|i| {
            (
                Lba(i),
                BlockmapValue {
                    pba: Pba(2000 + i),
                    compression: 1,
                    unit_compressed_size: 1234,
                    unit_original_size: 4096,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0xCAFE_F00D,
                    slot_offset: 0,
                    flags: 0,
                },
            )
        })
        .collect();
    let units2: Vec<(&VolumeId, &[(Lba, BlockmapValue)], u32)> =
        vec![(&vol.id, batch_values2.as_slice(), 1u32)];
    let seqs2: Vec<u64> = (9..9 + batch_values2.len() as u64).collect();
    let (sync_cleanup, sync_accepted) = backend
        .atomic_batch_write_multi_with_dedup(&units2, &[], &seqs2)
        .expect("sync wrapper still works");
    assert_eq!(sync_accepted.len(), batch_values2.len());
    assert!(sync_accepted.iter().all(|&ok| ok));
    assert!(sync_cleanup.is_empty());
}

#[test]
fn l2p_buffer_interval_controls_metadb_bfg_roll_timer() {
    let cfg = MetaConfig {
        l2p_buffer_max_interval_ms: 31_337,
        ..Default::default()
    };
    let mapped = super::metadb_config_from_onyx(std::path::Path::new("/tmp/metadb"), &cfg);
    assert_eq!(mapped.bfg_timeout_ms, 31_337);
}

#[test]
fn rc_checkpoint_streaming_toggle_maps_to_metadb() {
    for enabled in [false, true] {
        let cfg = MetaConfig {
            rc_checkpoint_streaming_enabled: enabled,
            ..Default::default()
        };
        let mapped = super::metadb_config_from_onyx(std::path::Path::new("/tmp/metadb"), &cfg);
        assert_eq!(mapped.rc_checkpoint_streaming_enabled, enabled);
    }
}

#[test]
fn rc_delta_run_shadow_toggle_maps_to_metadb() {
    for enabled in [false, true] {
        let cfg = MetaConfig {
            rc_delta_run_shadow_enabled: enabled,
            ..Default::default()
        };
        let mapped = super::metadb_config_from_onyx(std::path::Path::new("/tmp/metadb"), &cfg);
        assert_eq!(mapped.rc_delta_run_shadow_enabled, enabled);
    }
}

#[test]
fn meta_store_exposes_terminal_durable_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = MetaConfig {
        path: Some(dir.path().to_path_buf()),
        dedup_shards: 1,
        dedup_cuckoo_buckets: 256,
        dedup_l1_cache_entries: 256,
        async_reclaim_max_pages_per_cycle: 1,
        ..Default::default()
    };
    let store = crate::meta::store::MetaStore::open(&cfg).unwrap();
    store.sync_durable().unwrap();
    store.drain_deferred_reclaim_durable().unwrap();
}

#[test]
fn offline_audit_config_disables_continuous_mutators() {
    let cfg = MetaConfig {
        dedup_shards: 8,
        dedup_drainer_enabled: true,
        refcount_drainer_enabled: true,
        l2p_writeback_enabled: true,
        l2p_buffer_enabled: true,
        bfg_threads_enabled: true,
        parallel_l2p_drain_enabled: true,
        parallel_l2p_drain_workers: 4,
        l2p_checkpoint_pipeline_enabled: true,
        async_reclaim_enabled: true,
        lineage_gc_enabled: true,
        ..Default::default()
    };

    let audit = super::metadb_config_for_offline_audit(std::path::Path::new("/tmp/metadb"), &cfg);

    assert!(!audit.bfg_threads_enabled);
    assert!(!audit.parallel_l2p_drain_enabled);
    assert_eq!(audit.parallel_l2p_drain_workers, 4);
    assert!(!audit.l2p_checkpoint_pipeline_enabled);
    assert!(!audit.l2p_writeback_enabled);
    assert!(!audit.dedup_drainer_enabled);
    assert!(!audit.refcount_drainer_enabled);
    assert!(!audit.async_reclaim_enabled);
    assert_eq!(audit.livelist_condense_min_segments, 0);
    assert!(!audit.lineage_gc_enabled);
    assert!(!audit.reclaim_orphans_on_open);

    // Layout and recovery semantics still match production. In particular,
    // lifecycle replay sees the same shard topology and L2P representation.
    assert_eq!(audit.dedup_shards, 8);
    assert!(audit.l2p_buffer_enabled);
}

/// REGRESSION: the offline-audit config assembly must produce a config metadb
/// will actually OPEN. `sanitize_offline_audit_config` disarms
/// `bfg_threads_enabled` (no background mutators during an audit) while
/// `rc_delta_run_persist_enabled` stays as configured, and metadb's production
/// cross-check refuses exactly that pair — so with
/// `config/nvme-chunklet.toml`'s `rc_delta_run_persist_enabled = true`,
/// `metadb-verify` and `metadb-probe` failed at open with
/// "rc_delta_run_persist_enabled requires bfg_threads_enabled +
/// rc_checkpoint_streaming_enabled". That is the one path that can count orphan
/// pages on a live pool, so it silently had no coverage.
#[test]
fn offline_audit_config_opens_with_delta_run_persist_on() {
    for persist in [false, true] {
        let cfg = MetaConfig {
            rc_delta_run_persist_enabled: persist,
            ..Default::default()
        };
        let mapped =
            super::metadb_config_for_offline_audit(std::path::Path::new("/tmp/metadb"), &cfg);
        assert_eq!(mapped.rc_delta_run_persist_enabled, persist);
        assert!(!mapped.bfg_threads_enabled, "no background mutators");
        assert!(mapped.offline_audit, "metadb must know this is an audit");

        // Create the store the way production would, then re-open it the way
        // the audit does. Only the second open exercises the cross-check that
        // used to make every audit entry point unopenable.
        let dir = tempfile::tempdir().unwrap();
        let mut prod_cfg = super::metadb_config_from_onyx(dir.path(), &cfg);
        prod_cfg.path = dir.path().to_path_buf();
        drop(onyx_metadb::Db::create_with_config(prod_cfg).expect("production create"));

        let mut audit_cfg = mapped.clone();
        audit_cfg.path = dir.path().to_path_buf();
        let db = onyx_metadb::Db::open_with_config(audit_cfg)
            .unwrap_or_else(|err| panic!("audit open rejected (persist={persist}): {err}"));
        drop(db);
    }
}
