use onyx_storage::config::MetaConfig;
use onyx_storage::meta::schema::*;
use onyx_storage::meta::store::{DedupHitResult, MetaStore};
use onyx_storage::types::*;
use tempfile::tempdir;

// --- schema tests ---

#[test]
fn blockmap_key_roundtrip() {
    let vol_id = VolumeId("test-vol".into());
    let lba = Lba(999);
    let key = encode_blockmap_key(&vol_id, lba).unwrap();
    let (id_str, l) = decode_blockmap_key(&key).unwrap();
    assert_eq!(id_str, "test-vol");
    assert_eq!(l, lba);
}

#[test]
fn blockmap_key_ordering() {
    let vol_id = VolumeId("test-vol".into());
    let k1 = encode_blockmap_key(&vol_id, Lba(0)).unwrap();
    let k2 = encode_blockmap_key(&vol_id, Lba(1)).unwrap();
    let k3 = encode_blockmap_key(&vol_id, Lba(2)).unwrap();
    assert!(k1 < k2);
    assert!(k2 < k3);
}

#[test]
fn blockmap_value_roundtrip() {
    let v = BlockmapValue {
        pba: Pba(42),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,

        unit_lba_count: 1,

        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    let encoded = encode_blockmap_value(&v);
    let decoded = decode_blockmap_value(&encoded).unwrap();
    assert_eq!(decoded, v);
}

#[test]
fn refcount_roundtrip() {
    let pba = Pba(12345);
    let key = encode_refcount_key(pba);
    assert_eq!(decode_refcount_key(&key), Some(pba));

    let val = encode_refcount_value(7);
    assert_eq!(decode_refcount_value(&val), Some(7));
}

// --- store tests ---

fn test_config(dir: &std::path::Path) -> MetaConfig {
    MetaConfig {
        rocksdb_path: Some(dir.to_path_buf()),
        block_cache_mb: 8,
        wal_dir: None,
    }
}

fn test_volume() -> VolumeConfig {
    VolumeConfig {
        id: VolumeId("test-vol".into()),
        size_bytes: 1024 * 1024 * 1024,
        block_size: 4096,
        compression: CompressionAlgo::Lz4,
        created_at: 1700000000,
        zone_count: 4,
    }
}

#[test]
fn volume_crud() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let vol = test_volume();
    store.put_volume(&vol).unwrap();

    let loaded = store.get_volume(&vol.id).unwrap().unwrap();
    assert_eq!(loaded.id, vol.id);
    assert_eq!(loaded.size_bytes, vol.size_bytes);

    let volumes = store.list_volumes().unwrap();
    assert_eq!(volumes.len(), 1);

    store.delete_volume(&vol.id).unwrap();
    assert!(store.get_volume(&vol.id).unwrap().is_none());
}

#[test]
fn blockmap_crud() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let val = BlockmapValue {
        pba: Pba(100),
        compression: 1,
        unit_compressed_size: 2048,
        unit_original_size: 4096,

        unit_lba_count: 1,

        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };

    store.put_mapping(&vol_id, Lba(0), &val).unwrap();
    let loaded = store.get_mapping(&vol_id, Lba(0)).unwrap().unwrap();
    assert_eq!(loaded, val);

    assert!(store.get_mapping(&vol_id, Lba(1)).unwrap().is_none());

    store.delete_mapping(&vol_id, Lba(0)).unwrap();
    assert!(store.get_mapping(&vol_id, Lba(0)).unwrap().is_none());
}

#[test]
fn blockmap_range_query() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    for i in 0..10 {
        let val = BlockmapValue {
            pba: Pba(i * 10),
            compression: 1,
            unit_compressed_size: 4000,
            unit_original_size: 4096,

            unit_lba_count: 1,

            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.put_mapping(&vol_id, Lba(i), &val).unwrap();
    }

    let range = store.get_mappings_range(&vol_id, Lba(3), Lba(7)).unwrap();
    assert_eq!(range.len(), 4);
    assert_eq!(range[0].0, Lba(3));
    assert_eq!(range[3].0, Lba(6));
}

#[test]
fn refcount_operations() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    assert_eq!(store.get_refcount(Pba(42)).unwrap(), 0);

    store.set_refcount(Pba(42), 1).unwrap();
    assert_eq!(store.get_refcount(Pba(42)).unwrap(), 1);

    let new = store.increment_refcount(Pba(42)).unwrap();
    assert_eq!(new, 2);

    let new = store.decrement_refcount(Pba(42)).unwrap();
    assert_eq!(new, 1);

    let new = store.decrement_refcount(Pba(42)).unwrap();
    assert_eq!(new, 0);
    assert_eq!(store.get_refcount(Pba(42)).unwrap(), 0);
}

#[test]
fn atomic_write_mapping() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let val = BlockmapValue {
        pba: Pba(50),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,

        unit_lba_count: 1,

        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };

    store.atomic_write_mapping(&vol_id, Lba(10), &val).unwrap();

    let loaded = store.get_mapping(&vol_id, Lba(10)).unwrap().unwrap();
    assert_eq!(loaded.pba, Pba(50));
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 1);
}

#[test]
fn reconcile_refcount_repairs_blockmap_undercount() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());
    let pba = Pba(77);

    let batch_values = vec![
        (
            Lba(0),
            BlockmapValue {
                pba,
                compression: 1,
                unit_compressed_size: 2000,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: 0,
                crc32: 0x1111_2222,
                slot_offset: 0,
                flags: 0,
            },
        ),
        (
            Lba(1),
            BlockmapValue {
                pba,
                compression: 1,
                unit_compressed_size: 2000,
                unit_original_size: 8192,
                unit_lba_count: 2,
                offset_in_unit: 1,
                crc32: 0x1111_2222,
                slot_offset: 0,
                flags: 0,
            },
        ),
    ];

    store.atomic_batch_write(&vol_id, &batch_values, 2).unwrap();
    assert_eq!(store.get_refcount(pba).unwrap(), 2);

    store.set_refcount(pba, 1).unwrap();
    assert_eq!(store.count_blockmap_refs_for_pba(pba).unwrap(), 2);

    let repaired = store.reconcile_refcount_for_pba(pba).unwrap();
    assert_eq!(repaired, 2);
    assert_eq!(store.get_refcount(pba).unwrap(), 2);
}

#[test]
fn atomic_remap() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let old_val = BlockmapValue {
        pba: Pba(100),
        compression: 1,
        unit_compressed_size: 2000,
        unit_original_size: 4096,

        unit_lba_count: 1,

        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    store
        .atomic_write_mapping(&vol_id, Lba(5), &old_val)
        .unwrap();

    let new_val = BlockmapValue {
        pba: Pba(200),
        compression: 2,
        unit_compressed_size: 1500,
        unit_original_size: 4096,

        unit_lba_count: 1,

        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    store
        .atomic_remap(&vol_id, Lba(5), Some(Pba(100)), &new_val)
        .unwrap();

    let loaded = store.get_mapping(&vol_id, Lba(5)).unwrap().unwrap();
    assert_eq!(loaded.pba, Pba(200));
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 1);
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 0);
}

#[test]
fn wal_recovery() {
    let dir = tempdir().unwrap();
    let vol_id = VolumeId("test-vol".into());

    {
        let store = MetaStore::open(&test_config(dir.path())).unwrap();
        let val = BlockmapValue {
            pba: Pba(77),
            compression: 1,
            unit_compressed_size: 3000,
            unit_original_size: 4096,

            unit_lba_count: 1,

            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.atomic_write_mapping(&vol_id, Lba(42), &val).unwrap();
    }

    {
        let store = MetaStore::open(&test_config(dir.path())).unwrap();
        let loaded = store.get_mapping(&vol_id, Lba(42)).unwrap().unwrap();
        assert_eq!(loaded.pba, Pba(77));
        assert_eq!(store.get_refcount(Pba(77)).unwrap(), 1);
    }
}

#[test]
fn iter_refcounts() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    store.set_refcount(Pba(10), 1).unwrap();
    store.set_refcount(Pba(20), 2).unwrap();
    store.set_refcount(Pba(30), 1).unwrap();

    let refs = store.iter_refcounts().unwrap();
    assert_eq!(refs.len(), 3);
    assert_eq!(refs[0].0, Pba(10));
    assert_eq!(refs[1].0, Pba(20));
    assert_eq!(refs[2].0, Pba(30));
}

// --- regression tests ---

/// Fix #6: deleting a volume must clean up its blockmap and refcount entries.
#[test]
fn delete_volume_frees_blockmap_and_refcount() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let vol = test_volume();
    store.put_volume(&vol).unwrap();

    let vol_id = &vol.id;

    // Write several block mappings for this volume
    for i in 0..5 {
        let val = BlockmapValue {
            pba: Pba(100 + i),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,

            unit_lba_count: 1,

            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.atomic_write_mapping(vol_id, Lba(i), &val).unwrap();
    }

    // Verify they exist
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 1);
    assert_eq!(store.get_refcount(Pba(104)).unwrap(), 1);

    // Delete volume
    let freed = store.delete_volume(&vol.id).unwrap();
    assert_eq!(freed.len(), 5);

    // Volume record gone
    assert!(store.get_volume(&vol.id).unwrap().is_none());

    // Blockmap entries gone
    assert!(store.get_mapping(vol_id, Lba(0)).unwrap().is_none());
    assert!(store.get_mapping(vol_id, Lba(4)).unwrap().is_none());

    // Refcount entries gone
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 0);
    assert_eq!(store.get_refcount(Pba(104)).unwrap(), 0);

    // iter_refcounts should return nothing for these PBAs
    let refs = store.iter_refcounts().unwrap();
    assert!(refs.is_empty());
}

/// Delete a nonexistent volume returns empty freed list, no error.
#[test]
fn delete_nonexistent_volume() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let freed = store.delete_volume(&VolumeId("ghost".into())).unwrap();
    assert!(freed.is_empty());
}

/// get_volume for nonexistent -> None.
#[test]
fn get_nonexistent_volume() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    assert!(store
        .get_volume(&VolumeId("nope".into()))
        .unwrap()
        .is_none());
}

/// list_volumes on empty DB -> empty vec.
#[test]
fn list_volumes_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    assert!(store.list_volumes().unwrap().is_empty());
}

/// Multiple volumes can coexist.
#[test]
fn multiple_volumes() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    for i in 0..5 {
        let vol = VolumeConfig {
            id: VolumeId(format!("vol-{}", i)),
            size_bytes: (i + 1) * 1024 * 1024,
            block_size: 4096,
            compression: CompressionAlgo::Lz4,
            created_at: 1700000000 + i,
            zone_count: 4,
        };
        store.put_volume(&vol).unwrap();
    }

    let volumes = store.list_volumes().unwrap();
    assert_eq!(volumes.len(), 5);
}

/// Schema decode with wrong-length inputs -> None.
#[test]
fn schema_decode_bad_lengths() {
    assert!(decode_blockmap_key(&[0u8; 5]).is_none());
    assert!(decode_blockmap_key(&[0u8; 20]).is_none());
    assert!(decode_blockmap_value(&[0u8; 3]).is_none());
    assert!(decode_refcount_key(&[0u8; 3]).is_none());
    assert!(decode_refcount_value(&[0u8; 2]).is_none());
}

/// get_mapping for nonexistent key -> None.
#[test]
fn get_nonexistent_mapping() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());
    assert!(store.get_mapping(&vol_id, Lba(999)).unwrap().is_none());
}

/// Empty range query returns empty vec.
#[test]
fn range_query_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());
    let range = store.get_mappings_range(&vol_id, Lba(0), Lba(100)).unwrap();
    assert!(range.is_empty());
}

/// Fix: delete_volume with multiple LBAs pointing to same PBA (dedup scenario).
/// Refcount must be decremented by total count, not just 1.
#[test]
fn delete_volume_shared_pba_refcount() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let vol = test_volume();
    store.put_volume(&vol).unwrap();
    let vol_id = &vol.id;

    // Simulate dedup: 3 LBAs all point to PBA 500, refcount = 3
    let shared_pba = Pba(500);
    store.set_refcount(shared_pba, 3).unwrap();
    for lba in 0..3u64 {
        let val = BlockmapValue {
            pba: shared_pba,
            compression: 1,
            unit_compressed_size: 2000,
            unit_original_size: 4096,

            unit_lba_count: 1,

            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.put_mapping(vol_id, Lba(lba), &val).unwrap();
    }

    let freed = store.delete_volume(&vol.id).unwrap();

    // All 3 decrements aggregated: 3 - 3 = 0, PBA freed
    assert_eq!(freed.len(), 1);
    assert_eq!(freed[0].0, shared_pba);
    assert_eq!(store.get_refcount(shared_pba).unwrap(), 0);
}

/// Fix: delete_volume with shared PBA but refcount > volume's mapping count.
/// PBA should NOT be freed if other volumes still reference it.
#[test]
fn delete_volume_shared_pba_partial_decrement() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let vol = test_volume();
    store.put_volume(&vol).unwrap();
    let vol_id = &vol.id;

    // PBA 600 has refcount 5 (2 from this volume + 3 from another)
    let shared_pba = Pba(600);
    store.set_refcount(shared_pba, 5).unwrap();
    for lba in 0..2u64 {
        let val = BlockmapValue {
            pba: shared_pba,
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,

            unit_lba_count: 1,

            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.put_mapping(vol_id, Lba(lba), &val).unwrap();
    }

    let freed = store.delete_volume(&vol.id).unwrap();

    // 5 - 2 = 3, not freed
    assert!(freed.is_empty());
    assert_eq!(store.get_refcount(shared_pba).unwrap(), 3);
}

/// Fix: volume ID too long is rejected at creation.
#[test]
fn reject_volume_id_too_long() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let long_name = "x".repeat(256); // 256 > MAX 255
    let vol = VolumeConfig {
        id: VolumeId(long_name),
        size_bytes: 1024 * 1024,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 0,
        zone_count: 1,
    };
    assert!(store.put_volume(&vol).is_err());
}

/// Fix: empty volume ID is rejected.
#[test]
fn reject_volume_id_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let vol = VolumeConfig {
        id: VolumeId(String::new()),
        size_bytes: 1024 * 1024,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 0,
        zone_count: 1,
    };
    assert!(store.put_volume(&vol).is_err());
}

/// Volume ID at exactly 255 bytes is accepted.
#[test]
fn accept_volume_id_max_length() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();

    let name = "v".repeat(255);
    let vol = VolumeConfig {
        id: VolumeId(name),
        size_bytes: 1024 * 1024,
        block_size: 4096,
        compression: CompressionAlgo::None,
        created_at: 0,
        zone_count: 1,
    };
    assert!(store.put_volume(&vol).is_ok());
}

// --- multi_get_mappings tests ---

#[test]
fn multi_get_mappings_basic() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    // Write 5 mappings
    for i in 0..5u64 {
        let val = BlockmapValue {
            pba: Pba(100 + i),
            compression: 1,
            unit_compressed_size: 4000,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.put_mapping(&vol_id, Lba(i), &val).unwrap();
    }

    // Multi-get all 5
    let lbas: Vec<Lba> = (0..5).map(Lba).collect();
    let results = store.multi_get_mappings(&vol_id, &lbas).unwrap();
    assert_eq!(results.len(), 5);
    for (i, result) in results.iter().enumerate() {
        let val = result.as_ref().unwrap();
        assert_eq!(val.pba, Pba(100 + i as u64));
    }
}

#[test]
fn multi_get_mappings_with_gaps() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    // Only write LBA 0 and 2, skip 1 and 3
    for i in [0u64, 2] {
        let val = BlockmapValue {
            pba: Pba(100 + i),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        };
        store.put_mapping(&vol_id, Lba(i), &val).unwrap();
    }

    let lbas: Vec<Lba> = (0..4).map(Lba).collect();
    let results = store.multi_get_mappings(&vol_id, &lbas).unwrap();
    assert_eq!(results.len(), 4);
    assert!(results[0].is_some());
    assert!(results[1].is_none());
    assert!(results[2].is_some());
    assert!(results[3].is_none());
    assert_eq!(results[0].as_ref().unwrap().pba, Pba(100));
    assert_eq!(results[2].as_ref().unwrap().pba, Pba(102));
}

#[test]
fn multi_get_mappings_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let results = store.multi_get_mappings(&vol_id, &[]).unwrap();
    assert!(results.is_empty());
}

// --- atomic_batch_write_multi tests ---

#[test]
fn atomic_batch_write_multi_single_unit() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    let batch_values = vec![
        (
            Lba(0),
            BlockmapValue {
                pba: Pba(200),
                compression: 1,
                unit_compressed_size: 2000,
                unit_original_size: 4096,
                unit_lba_count: 2,
                offset_in_unit: 0,
                crc32: 0,
                slot_offset: 0,
                flags: 0,
            },
        ),
        (
            Lba(1),
            BlockmapValue {
                pba: Pba(200),
                compression: 1,
                unit_compressed_size: 2000,
                unit_original_size: 4096,
                unit_lba_count: 2,
                offset_in_unit: 1,
                crc32: 0,
                slot_offset: 0,
                flags: 0,
            },
        ),
    ];
    let units = vec![(&vol_id, batch_values.as_slice(), 2u32)];

    store.atomic_batch_write_multi(&units).unwrap();

    let m0 = store.get_mapping(&vol_id, Lba(0)).unwrap().unwrap();
    assert_eq!(m0.pba, Pba(200));
    assert_eq!(m0.offset_in_unit, 0);
    let m1 = store.get_mapping(&vol_id, Lba(1)).unwrap().unwrap();
    assert_eq!(m1.offset_in_unit, 1);
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 2);
}

#[test]
fn atomic_batch_write_multi_two_units_different_pbas() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_a = VolumeId("vol-a".into());
    let vol_b = VolumeId("vol-b".into());

    let batch_a = vec![(
        Lba(0),
        BlockmapValue {
            pba: Pba(300),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];
    let batch_b = vec![(
        Lba(0),
        BlockmapValue {
            pba: Pba(400),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];
    let units = vec![
        (&vol_a, batch_a.as_slice(), 1u32),
        (&vol_b, batch_b.as_slice(), 1u32),
    ];
    store.atomic_batch_write_multi(&units).unwrap();

    assert_eq!(
        store.get_mapping(&vol_a, Lba(0)).unwrap().unwrap().pba,
        Pba(300)
    );
    assert_eq!(
        store.get_mapping(&vol_b, Lba(0)).unwrap().unwrap().pba,
        Pba(400)
    );
    assert_eq!(store.get_refcount(Pba(300)).unwrap(), 1);
    assert_eq!(store.get_refcount(Pba(400)).unwrap(), 1);
}

#[test]
fn atomic_batch_write_multi_with_decrements() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    // Write initial mapping at PBA 50 with refcount 3
    let initial = vec![(
        Lba(0),
        BlockmapValue {
            pba: Pba(50),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];
    store.atomic_batch_write(&vol_id, &initial, 3).unwrap();
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 3);

    // Overwrite Lba(0) with new PBA 60; function internally reads old mapping (PBA 50)
    // and decrements it by 1
    let new_batch = vec![(
        Lba(0),
        BlockmapValue {
            pba: Pba(60),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];

    let units = vec![(&vol_id, new_batch.as_slice(), 1u32)];
    store.atomic_batch_write_multi(&units).unwrap();

    assert_eq!(
        store.get_mapping(&vol_id, Lba(0)).unwrap().unwrap().pba,
        Pba(60)
    );
    assert_eq!(store.get_refcount(Pba(60)).unwrap(), 1);
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 2); // 3 - 1 = 2
}

#[test]
fn atomic_batch_write_multi_aggregates_decrements_across_units() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol_id = VolumeId("test-vol".into());

    // Set up: Lba(0), Lba(10), Lba(20) all map to PBA 50 with refcount 3
    let bv50 = BlockmapValue {
        pba: Pba(50),
        compression: 0,
        unit_compressed_size: 4096,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    };
    let initial = vec![(Lba(0), bv50), (Lba(10), bv50), (Lba(20), bv50)];
    store.atomic_batch_write(&vol_id, &initial, 3).unwrap();
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 3);

    // Two units overwrite Lba(10) and Lba(20) to new PBAs.
    // The function internally reads old mappings (PBA 50) for each LBA
    // and decrements PBA 50 by 1 per LBA → total -2
    let batch_a = vec![(
        Lba(10),
        BlockmapValue {
            pba: Pba(70),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];
    let batch_b = vec![(
        Lba(20),
        BlockmapValue {
            pba: Pba(80),
            compression: 0,
            unit_compressed_size: 4096,
            unit_original_size: 4096,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: 0,
            slot_offset: 0,
            flags: 0,
        },
    )];

    let units = vec![
        (&vol_id, batch_a.as_slice(), 1u32),
        (&vol_id, batch_b.as_slice(), 1u32),
    ];
    store.atomic_batch_write_multi(&units).unwrap();

    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 1); // 3 - 1 - 1 = 1
    assert_eq!(store.get_refcount(Pba(70)).unwrap(), 1);
    assert_eq!(store.get_refcount(Pba(80)).unwrap(), 1);
}

#[test]
fn atomic_batch_write_multi_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    // Empty batch should be a no-op
    store.atomic_batch_write_multi(&[]).unwrap();
}

// --- atomic_batch_dedup_hits tests ---

fn make_bv(pba: u64, compressed: u32, lba_count: u16, offset: u16) -> BlockmapValue {
    BlockmapValue {
        pba: Pba(pba),
        compression: 0,
        unit_compressed_size: compressed,
        unit_original_size: 4096 * lba_count as u32,
        unit_lba_count: lba_count,
        offset_in_unit: offset,
        crc32: 0,
        slot_offset: 0,
        flags: 0,
    }
}

/// Create a unique ContentHash from a u8 seed.
fn test_hash(seed: u8) -> ContentHash {
    [seed; 32]
}

/// Register dedup_reverse entries so the dedup_reverse guard in
/// atomic_batch_dedup_hits accepts the hits.
fn register_dedup_reverse(store: &MetaStore, pba: u64, hashes: &[ContentHash]) {
    let entries: Vec<(ContentHash, DedupEntry)> = hashes
        .iter()
        .map(|h| {
            (
                *h,
                DedupEntry {
                    pba: Pba(pba),
                    slot_offset: 0,
                    compression: 0,
                    unit_compressed_size: 4096,
                    unit_original_size: 4096,
                    unit_lba_count: 1,
                    offset_in_unit: 0,
                    crc32: 0,
                },
            )
        })
        .collect();
    store.put_dedup_entries(&entries).unwrap();
}

#[test]
fn batch_dedup_hits_basic() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // Set up target PBA 100 with refcount 5
    store.set_refcount(Pba(100), 5).unwrap();
    let h = [test_hash(1), test_hash(2), test_hash(3)];
    register_dedup_reverse(&store, 100, &h);

    // Three hits, no old mapping, all targeting PBA 100
    let hits = vec![
        (Lba(0), make_bv(100, 4096, 1, 0), h[0]),
        (Lba(1), make_bv(100, 4096, 1, 0), h[1]),
        (Lba(2), make_bv(100, 4096, 1, 0), h[2]),
    ];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    assert_eq!(results.len(), 3);
    for r in &results {
        match r {
            DedupHitResult::Accepted(None) => {}
            _ => panic!("expected Ok(None) for hit with no old mapping"),
        }
    }
    // Refcount should be 5 + 3 = 8
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 8);
    // Blockmap entries should be written
    assert!(store.get_mapping(&vol, Lba(0)).unwrap().is_some());
    assert!(store.get_mapping(&vol, Lba(1)).unwrap().is_some());
    assert!(store.get_mapping(&vol, Lba(2)).unwrap().is_some());
}

#[test]
fn batch_dedup_hits_rejected_target_freed() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // PBA 100 alive, PBA 200 freed (refcount 0)
    store.set_refcount(Pba(100), 2).unwrap();
    // PBA 200 has no refcount entry → 0
    let h0 = test_hash(1);
    let h1 = test_hash(2);
    register_dedup_reverse(&store, 100, &[h0]);
    // No dedup_reverse for PBA 200 → rejected by dedup_reverse guard

    let hits = vec![
        (Lba(0), make_bv(100, 4096, 1, 0), h0),
        (Lba(1), make_bv(200, 4096, 1, 0), h1),
    ];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    assert_eq!(results.len(), 2);
    match results[0] {
        DedupHitResult::Accepted(None) => {}
        _ => panic!("expected Ok(None) for PBA 100 hit"),
    }
    match results[1] {
        DedupHitResult::Rejected => {}
        _ => panic!("expected Rejected for freed PBA 200"),
    }
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 3);
    // PBA 200 should remain 0
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 0);
    // Only LBA 0 should have blockmap entry
    assert!(store.get_mapping(&vol, Lba(0)).unwrap().is_some());
    assert!(store.get_mapping(&vol, Lba(1)).unwrap().is_none());
}

#[test]
fn batch_dedup_hits_same_pba_refresh() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // LBA 5 already maps to PBA 100
    let existing = make_bv(100, 4096, 1, 0);
    store.atomic_write_mapping(&vol, Lba(5), &existing).unwrap();
    let rc_before = store.get_refcount(Pba(100)).unwrap();

    // Dedup hit targets the same PBA 100 → blockmap refresh, no refcount change
    let h = test_hash(1);
    register_dedup_reverse(&store, 100, &[h]);
    let hits = vec![(Lba(5), make_bv(100, 4096, 1, 0), h)];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    match results[0] {
        DedupHitResult::Accepted(None) => {}
        _ => panic!("expected Accepted(None) for same-PBA hit"),
    }
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), rc_before);
}

#[test]
fn batch_dedup_hits_old_pba_decrement() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // LBA 5 maps to PBA 100 (refcount 3)
    let existing = make_bv(100, 4096, 1, 0);
    store.atomic_write_mapping(&vol, Lba(5), &existing).unwrap();
    store.set_refcount(Pba(100), 3).unwrap();
    // Target PBA 200 with refcount 5
    store.set_refcount(Pba(200), 5).unwrap();
    let h = test_hash(1);
    register_dedup_reverse(&store, 200, &[h]);

    let hits = vec![(Lba(5), make_bv(200, 4096, 1, 0), h)];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    match results[0] {
        DedupHitResult::Accepted(Some((old_pba, _))) => {
            assert_eq!(old_pba, Pba(100));
        }
        _ => panic!("expected Ok(Some) with old PBA 100"),
    }
    // PBA 200 should be 5 + 1 = 6
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 6);
    // PBA 100 should be decremented (merge-based, read back to check)
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 2);
    // Blockmap should point to PBA 200 now
    let m = store.get_mapping(&vol, Lba(5)).unwrap().unwrap();
    assert_eq!(m.pba, Pba(200));
}

#[test]
fn batch_dedup_hits_multiple_same_target() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // All target PBA 100 with refcount 5, no old mappings
    store.set_refcount(Pba(100), 5).unwrap();
    let h = [test_hash(1), test_hash(2), test_hash(3)];
    register_dedup_reverse(&store, 100, &h);
    let hits = vec![
        (Lba(10), make_bv(100, 4096, 1, 0), h[0]),
        (Lba(11), make_bv(100, 4096, 1, 0), h[1]),
        (Lba(12), make_bv(100, 4096, 1, 0), h[2]),
    ];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 8);
}

#[test]
fn batch_dedup_hits_multiple_decrement_same_old() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // 3 LBAs all map to PBA 100 (refcount 5)
    store.set_refcount(Pba(100), 5).unwrap();
    for i in 0..3 {
        let bv = make_bv(100, 4096, 1, 0);
        store.atomic_write_mapping(&vol, Lba(i), &bv).unwrap();
    }
    // Re-set refcount to 5 after atomic_write_mapping set it to 1 each time
    store.set_refcount(Pba(100), 5).unwrap();
    // Target PBA 200 with refcount 10
    store.set_refcount(Pba(200), 10).unwrap();
    let h = [test_hash(1), test_hash(2), test_hash(3)];
    register_dedup_reverse(&store, 200, &h);

    let hits = vec![
        (Lba(0), make_bv(200, 4096, 1, 0), h[0]),
        (Lba(1), make_bv(200, 4096, 1, 0), h[1]),
        (Lba(2), make_bv(200, 4096, 1, 0), h[2]),
    ];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    for r in &results {
        match r {
            DedupHitResult::Accepted(Some((old_pba, _))) => assert_eq!(*old_pba, Pba(100)),
            _ => panic!("expected Ok(Some) for overwrite hit"),
        }
    }
    // PBA 200: 10 + 3 = 13
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 13);
    // PBA 100: 5 - 3 = 2 (merge-based)
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 2);
}

#[test]
fn batch_dedup_hits_pba_is_both_target_and_old() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // LBA 5 maps to PBA 100 (refcount 3)
    store.set_refcount(Pba(100), 3).unwrap();
    store
        .atomic_write_mapping(&vol, Lba(5), &make_bv(100, 4096, 1, 0))
        .unwrap();
    store.set_refcount(Pba(100), 3).unwrap();

    // PBA 200 with refcount 5
    store.set_refcount(Pba(200), 5).unwrap();

    // Hit 1: LBA 5 maps 100→200 (decrement 100, increment 200)
    // Hit 2: LBA 6 (no old mapping) targets 100 (increment 100)
    let h0 = test_hash(1);
    let h1 = test_hash(2);
    register_dedup_reverse(&store, 200, &[h0]);
    register_dedup_reverse(&store, 100, &[h1]);
    let hits = vec![
        (Lba(5), make_bv(200, 4096, 1, 0), h0),
        (Lba(6), make_bv(100, 4096, 1, 0), h1),
    ];
    let (results, _newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &hits).unwrap();
    assert_eq!(results.len(), 2);
    // PBA 100: base=3, +1 (hit 2 targets it), -1 (hit 1 old) = 3
    assert_eq!(store.get_refcount(Pba(100)).unwrap(), 3);
    // PBA 200: base=5, +1 (hit 1 targets it) = 6
    assert_eq!(store.get_refcount(Pba(200)).unwrap(), 6);
}

#[test]
fn batch_dedup_hits_empty() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());
    let (results, newly_zeroed) = store.atomic_batch_dedup_hits(&vol, &[]).unwrap();
    assert!(results.is_empty());
    assert!(newly_zeroed.is_empty());
}

// --- merge-based decrement tests for atomic_batch_write_packed ---

#[test]
fn batch_write_packed_merge_decrement() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // Pre-existing: LBA 0,1 map to PBA 50 with refcount 5
    store.set_refcount(Pba(50), 5).unwrap();
    for i in 0..2 {
        store
            .atomic_write_mapping(&vol, Lba(i), &make_bv(50, 4096, 1, 0))
            .unwrap();
    }
    store.set_refcount(Pba(50), 5).unwrap();

    // Write packed slot with PBA 300 containing LBA 0,1
    let batch = vec![
        (vol.clone(), Lba(0), make_bv(300, 2000, 2, 0)),
        (vol.clone(), Lba(1), make_bv(300, 2000, 2, 1)),
    ];
    let old_meta = store
        .atomic_batch_write_packed(&batch, Pba(300), 2)
        .unwrap();

    // PBA 50 should be decremented by 2 → 3 (merge-based)
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 3);
    // PBA 300 refcount = 2
    assert_eq!(store.get_refcount(Pba(300)).unwrap(), 2);
    // old_meta should report PBA 50 was decremented
    assert!(old_meta.contains_key(&Pba(50)));
    assert_eq!(old_meta[&Pba(50)].0, 2); // decrement count
}

#[test]
fn batch_write_packed_merge_decrement_to_zero() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // PBA 50 with refcount 1, LBA 0 maps to it
    store.set_refcount(Pba(50), 1).unwrap();
    store
        .atomic_write_mapping(&vol, Lba(0), &make_bv(50, 4096, 1, 0))
        .unwrap();
    store.set_refcount(Pba(50), 1).unwrap();

    // Overwrite with PBA 300
    let batch = vec![(vol.clone(), Lba(0), make_bv(300, 2000, 1, 0))];
    store
        .atomic_batch_write_packed(&batch, Pba(300), 1)
        .unwrap();

    // PBA 50: 1 - 1 = 0 (merge clamps to 0, key still exists with value 0)
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 0);
    assert_eq!(store.get_refcount(Pba(300)).unwrap(), 1);
}

// --- merge-based decrement tests for atomic_batch_write_multi ---

#[test]
fn batch_write_multi_merge_decrement_only() {
    let dir = tempdir().unwrap();
    let store = MetaStore::open(&test_config(dir.path())).unwrap();
    let vol = VolumeId("test-vol".into());

    // Pre-existing: LBA 0 maps to PBA 50 (refcount 3)
    store.set_refcount(Pba(50), 3).unwrap();
    store
        .atomic_write_mapping(&vol, Lba(0), &make_bv(50, 4096, 1, 0))
        .unwrap();
    store.set_refcount(Pba(50), 3).unwrap();

    // Overwrite LBA 0 with PBA 400. PBA 50 is decrement-only (not a target).
    let batch_values = vec![(Lba(0), make_bv(400, 4096, 1, 0))];
    let units = vec![(&vol, batch_values.as_slice(), 1u32)];
    let old_meta = store.atomic_batch_write_multi(&units).unwrap();

    assert!(old_meta.contains_key(&Pba(50)));
    // PBA 50: 3 - 1 = 2 (merge path)
    assert_eq!(store.get_refcount(Pba(50)).unwrap(), 2);
    // PBA 400: 0 + 1 = 1 (new path)
    assert_eq!(store.get_refcount(Pba(400)).unwrap(), 1);
}
