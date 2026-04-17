//! MetaStore integration tests exercising the redb-backed blockmap paths
//! + the dirty-recovery rebuild logic added in phase 4.

use std::path::Path;

use tempfile::TempDir;

use crate::config::MetaConfig;
use crate::meta::schema::{
    encode_dedup_reverse_key, encode_refcount_key, encode_refcount_value, BlockmapValue,
    ContentHash, DedupEntry, CF_DEDUP_INDEX, CF_DEDUP_REVERSE, CF_REFCOUNT,
};
use crate::meta::store::MetaStore;
use crate::types::{Lba, Pba, VolumeConfig, VolumeId};

fn sample_bv(pba: u64) -> BlockmapValue {
    BlockmapValue {
        pba: Pba(pba),
        compression: 1,
        unit_compressed_size: 1024,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: (pba as u32).wrapping_mul(0x9e37_79b9),
        slot_offset: 0,
        flags: 0,
    }
}

fn sample_dedup_entry(pba: u64) -> DedupEntry {
    DedupEntry {
        pba: Pba(pba),
        slot_offset: 0,
        compression: 1,
        unit_compressed_size: 1024,
        unit_original_size: 4096,
        unit_lba_count: 1,
        offset_in_unit: 0,
        crc32: (pba as u32).wrapping_mul(0x9e37_79b9),
    }
}

fn fresh_meta(path: &Path) -> MetaStore {
    MetaStore::open(&MetaConfig {
        rocksdb_path: Some(path.to_path_buf()),
        redb_path: None,
        block_cache_mb: 8,
        wal_dir: None,
    })
    .unwrap()
}

fn vol_config(id: &str) -> VolumeConfig {
    VolumeConfig {
        id: VolumeId(id.to_string()),
        size_bytes: 1024 * 1024 * 1024,
        block_size: 4096,
        compression: crate::types::CompressionAlgo::Lz4,
        created_at: 0,
        zone_count: 1,
    }
}

#[test]
fn put_and_get_mapping_via_redb() {
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    let bv = sample_bv(10);
    meta.put_mapping(&vol, Lba(5), &bv).unwrap();
    assert_eq!(meta.get_mapping(&vol, Lba(5)).unwrap(), Some(bv));
    assert!(meta.get_mapping(&vol, Lba(6)).unwrap().is_none());
}

#[test]
fn atomic_batch_write_round_trip() {
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    let entries: Vec<(Lba, BlockmapValue)> = (0..8)
        .map(|i| (Lba(i), sample_bv(100)))
        .collect();
    let newly_zeroed = meta.atomic_batch_write(&vol, &entries, 1).unwrap();
    assert!(newly_zeroed.is_empty());

    for (lba, bv) in &entries {
        assert_eq!(meta.get_mapping(&vol, *lba).unwrap(), Some(*bv));
    }
    assert_eq!(meta.get_refcount(Pba(100)).unwrap(), 1);
}

#[test]
fn delete_volume_cleans_up_blockmap_and_decrements_refcount() {
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    let entries: Vec<(Lba, BlockmapValue)> =
        (0..4).map(|i| (Lba(i), sample_bv(100))).collect();
    meta.atomic_batch_write(&vol, &entries, 1).unwrap();
    assert_eq!(meta.get_refcount(Pba(100)).unwrap(), 1);

    let freed = meta.delete_volume(&vol).unwrap();
    assert_eq!(freed.len(), 1);
    assert_eq!(freed[0].0, Pba(100));
    assert!(meta.get_mapping(&vol, Lba(0)).unwrap().is_none());
    assert_eq!(meta.get_refcount(Pba(100)).unwrap(), 0);
}

#[test]
fn rebuild_refcount_fills_missing_entries() {
    // Redb has mappings; refcount CF is empty. Rebuild should populate it.
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    // Insert mappings directly via redb to bypass RocksDB refcount update.
    for i in 0..5 {
        meta.redb
            .put_mapping("v", Lba(i), sample_bv(200 + i))
            .unwrap();
    }

    // Pre-rebuild: refcount CF has nothing for PBAs 200..205.
    for i in 0..5 {
        assert_eq!(meta.get_refcount(Pba(200 + i)).unwrap(), 0);
    }

    let summary = meta.rebuild_refcount_from_blockmap().unwrap();
    assert_eq!(summary.referenced_pbas, 5);
    assert!(summary.fixed_entries >= 5);

    for i in 0..5 {
        assert_eq!(meta.get_refcount(Pba(200 + i)).unwrap(), 1);
    }
}

#[test]
fn rebuild_refcount_removes_orphans() {
    // Refcount CF has rows that nobody references in redb.
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    meta.put_volume(&vol_config("v")).unwrap();

    // Write a bogus refcount row for PBA 999 (no blockmap ref).
    let cf = meta.db.cf_handle(CF_REFCOUNT).unwrap();
    let key = encode_refcount_key(Pba(999));
    meta.db
        .put_cf(&cf, &key, &encode_refcount_value(7))
        .unwrap();
    assert_eq!(meta.get_refcount(Pba(999)).unwrap(), 7);

    let summary = meta.rebuild_refcount_from_blockmap().unwrap();
    assert!(summary.orphan_entries_removed >= 1);
    assert_eq!(meta.get_refcount(Pba(999)).unwrap(), 0);
}

#[test]
fn rebuild_refcount_counts_dedup_index_claims() {
    // A PBA is referenced only by a dedup_index entry (no LBA maps to it).
    // Rebuild must still give it refcount >= 1 so the dedup claim stays live.
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    meta.put_volume(&vol_config("v")).unwrap();

    let hash: ContentHash = [7u8; 32];
    let entry = sample_dedup_entry(300);
    meta.put_dedup_entries(&[(hash, entry)]).unwrap();
    let rev_cf = meta.db.cf_handle(CF_DEDUP_REVERSE).unwrap();
    let rev_key = encode_dedup_reverse_key(Pba(300), &hash);
    assert!(meta.db.get_cf(&rev_cf, &rev_key).unwrap().is_some());
    let idx_cf = meta.db.cf_handle(CF_DEDUP_INDEX).unwrap();
    assert!(meta.db.get_cf(&idx_cf, &hash).unwrap().is_some());

    let summary = meta.rebuild_refcount_from_blockmap().unwrap();
    assert!(summary.referenced_pbas >= 1);
    assert_eq!(meta.get_refcount(Pba(300)).unwrap(), 1);
}

#[test]
fn rebuild_refcount_handles_multiple_lbas_per_pba() {
    // Dedup scenario: multiple LBAs share one PBA. Rebuild must sum the
    // references correctly instead of capping at 1.
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    for i in 0..3 {
        meta.redb
            .put_mapping("v", Lba(i), sample_bv(400))
            .unwrap();
    }

    meta.rebuild_refcount_from_blockmap().unwrap();
    assert_eq!(meta.get_refcount(Pba(400)).unwrap(), 3);
}

#[test]
fn rebuild_refcount_fixes_drifted_value() {
    // Refcount CF has a value that does not match the true count.
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    // Two LBAs → PBA 500 (true count = 2).
    for i in 0..2 {
        meta.redb
            .put_mapping("v", Lba(i), sample_bv(500))
            .unwrap();
    }

    // Corrupt refcount to 99.
    let cf = meta.db.cf_handle(CF_REFCOUNT).unwrap();
    let key = encode_refcount_key(Pba(500));
    meta.db
        .put_cf(&cf, &key, &encode_refcount_value(99))
        .unwrap();

    let summary = meta.rebuild_refcount_from_blockmap().unwrap();
    assert!(summary.fixed_entries >= 1);
    assert_eq!(meta.get_refcount(Pba(500)).unwrap(), 2);
}

#[test]
fn scan_all_blockmap_entries_via_redb_matches_put_set() {
    let dir = TempDir::new().unwrap();
    let meta = fresh_meta(dir.path());
    let vol = VolumeId("v".into());
    meta.put_volume(&vol_config("v")).unwrap();

    let entries: Vec<(Lba, BlockmapValue)> = (0..16)
        .map(|i| (Lba(i * 3), sample_bv(600 + i)))
        .collect();
    for (lba, bv) in &entries {
        meta.put_mapping(&vol, *lba, bv).unwrap();
    }

    let mut seen: Vec<Pba> = Vec::new();
    meta.scan_all_blockmap_entries(&mut |_vol, _key, val| {
        let bv = crate::meta::schema::decode_blockmap_value(val).unwrap();
        seen.push(bv.pba);
    })
    .unwrap();
    seen.sort();
    let mut expected: Vec<Pba> = entries.iter().map(|(_, bv)| bv.pba).collect();
    expected.sort();
    assert_eq!(seen, expected);
}
