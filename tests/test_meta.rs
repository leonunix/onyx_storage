use onyx_storage::config::MetaConfig;
use onyx_storage::meta::schema::*;
use onyx_storage::meta::store::MetaStore;
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
