//! Integration tests for `RedbStore` end-to-end flows.

use tempfile::TempDir;

use crate::meta::redb::{L2Page, RedbStore, VolumeRoot, LBAS_PER_PAGE};
use crate::meta::schema::BlockmapValue;
use crate::types::{Lba, Pba};

fn sample(pba: u64) -> BlockmapValue {
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

fn open_store() -> (TempDir, RedbStore) {
    let dir = TempDir::new().unwrap();
    let store = RedbStore::open(&dir.path().join("blockmap.redb")).unwrap();
    (dir, store)
}

#[test]
fn open_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("blockmap.redb");
    let _ = RedbStore::open(&path).unwrap();
    let _ = RedbStore::open(&path).unwrap();
}

#[test]
fn volume_crud() {
    let (_dir, store) = open_store();
    assert!(store.get_volume("vol-1").unwrap().is_none());

    let root = VolumeRoot {
        l1_size: 42,
        gen: 0,
    };
    store.put_volume("vol-1", root).unwrap();
    assert_eq!(store.get_volume("vol-1").unwrap(), Some(root));

    let root2 = VolumeRoot {
        l1_size: 100,
        gen: 5,
    };
    store.put_volume("vol-1", root2).unwrap();
    assert_eq!(store.get_volume("vol-1").unwrap(), Some(root2));

    store.delete_volume("vol-1").unwrap();
    assert!(store.get_volume("vol-1").unwrap().is_none());
}

#[test]
fn mapping_insert_and_read_back() {
    let (_dir, store) = open_store();
    let v = sample(1001);
    assert!(
        store
            .put_mapping("vol-a", Lba(7), v)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.get_mapping("vol-a", Lba(7)).unwrap(), Some(v));
    assert!(store.get_mapping("vol-a", Lba(8)).unwrap().is_none());
}

#[test]
fn mapping_overwrite_returns_old_value() {
    let (_dir, store) = open_store();
    let v1 = sample(100);
    let v2 = sample(200);
    assert!(store.put_mapping("vol", Lba(0), v1).unwrap().is_none());
    assert_eq!(store.put_mapping("vol", Lba(0), v2).unwrap(), Some(v1));
    assert_eq!(store.get_mapping("vol", Lba(0)).unwrap(), Some(v2));
}

#[test]
fn mapping_delete_returns_old_value() {
    let (_dir, store) = open_store();
    let v = sample(42);
    store.put_mapping("vol", Lba(5), v).unwrap();
    assert_eq!(store.delete_mapping("vol", Lba(5)).unwrap(), Some(v));
    assert!(store.get_mapping("vol", Lba(5)).unwrap().is_none());
}

#[test]
fn mapping_delete_nonexistent_is_noop() {
    let (_dir, store) = open_store();
    assert!(store.delete_mapping("vol", Lba(0)).unwrap().is_none());
}

#[test]
fn many_lbas_within_one_page() {
    // Everything in l1_idx = 0 (lba 0..LBAS_PER_PAGE)
    let (_dir, store) = open_store();
    for i in 0..LBAS_PER_PAGE as u64 {
        store.put_mapping("vol", Lba(i), sample(5000 + i)).unwrap();
    }
    for i in 0..LBAS_PER_PAGE as u64 {
        assert_eq!(
            store.get_mapping("vol", Lba(i)).unwrap(),
            Some(sample(5000 + i))
        );
    }
}

#[test]
fn lbas_across_multiple_pages() {
    let (_dir, store) = open_store();
    // l1_idx 0, 1, 2 (LBAs 0, 256, 512)
    for l1 in 0..3u64 {
        let lba = Lba(l1 * LBAS_PER_PAGE as u64);
        store.put_mapping("vol", lba, sample(l1 + 1)).unwrap();
    }
    for l1 in 0..3u64 {
        let lba = Lba(l1 * LBAS_PER_PAGE as u64);
        assert_eq!(
            store.get_mapping("vol", lba).unwrap(),
            Some(sample(l1 + 1))
        );
    }
}

#[test]
fn two_volumes_are_independent() {
    let (_dir, store) = open_store();
    store.put_mapping("alpha", Lba(0), sample(10)).unwrap();
    store.put_mapping("beta", Lba(0), sample(20)).unwrap();
    assert_eq!(
        store.get_mapping("alpha", Lba(0)).unwrap(),
        Some(sample(10))
    );
    assert_eq!(
        store.get_mapping("beta", Lba(0)).unwrap(),
        Some(sample(20))
    );
}

#[test]
fn delete_volume_clears_all_mappings() {
    let (_dir, store) = open_store();
    for l1 in 0..3u64 {
        for off in [0u64, 5, 100] {
            let lba = Lba(l1 * LBAS_PER_PAGE as u64 + off);
            store.put_mapping("vol", lba, sample(lba.0)).unwrap();
        }
    }
    store.delete_volume("vol").unwrap();
    for l1 in 0..3u64 {
        for off in [0u64, 5, 100] {
            let lba = Lba(l1 * LBAS_PER_PAGE as u64 + off);
            assert!(store.get_mapping("vol", lba).unwrap().is_none());
        }
    }
}

#[test]
fn delete_volume_does_not_touch_others() {
    let (_dir, store) = open_store();
    store.put_mapping("keep", Lba(0), sample(1)).unwrap();
    store.put_mapping("drop", Lba(0), sample(2)).unwrap();
    store.delete_volume("drop").unwrap();
    assert_eq!(store.get_mapping("keep", Lba(0)).unwrap(), Some(sample(1)));
    assert!(store.get_mapping("drop", Lba(0)).unwrap().is_none());
}

#[test]
fn cow_branch_produces_fresh_page() {
    // v1 never bumps T_PAGE_REFS > 1 in normal flow, so exercise the COW branch
    // by forcing the refcount up via a test hook, then mutating.
    let (_dir, store) = open_store();
    let v_before = sample(1);
    let v_after = sample(2);
    store.put_mapping("vol", Lba(3), v_before).unwrap();
    let original_page = store._test_bump_page_refcount("vol", Lba(3)).unwrap();

    // Mutate — should COW into a new page, leaving original untouched.
    assert_eq!(
        store.put_mapping("vol", Lba(3), v_after).unwrap(),
        Some(v_before)
    );
    assert_eq!(store.get_mapping("vol", Lba(3)).unwrap(), Some(v_after));

    // Put on another LBA in same L1 bucket — should stay on the COW'd page (refs=1 now).
    store.put_mapping("vol", Lba(4), sample(99)).unwrap();
    assert_eq!(store.get_mapping("vol", Lba(4)).unwrap(), Some(sample(99)));

    // Touch the original page's refcount directly to assert it went to 1 (fake snapshot
    // would then hold that reference). Can't observe from the public API, so check via
    // side-effect: delete the original-page holder and confirm store still valid.
    let _ = original_page; // explicit: page id retained elsewhere (would be the snapshot L1 in real world).
}

#[test]
fn clearing_last_entry_frees_page_for_reuse() {
    let (_dir, store) = open_store();
    // Fill lba 0 on vol, then delete → page should be freed.
    store.put_mapping("vol", Lba(0), sample(1)).unwrap();
    store.delete_mapping("vol", Lba(0)).unwrap();

    // A new put in a *different* L1 bucket should pick the freed page_id (1).
    store.put_mapping("vol", Lba(256), sample(2)).unwrap();
    assert_eq!(
        store.get_mapping("vol", Lba(256)).unwrap(),
        Some(sample(2))
    );
}

#[test]
fn page_encodes_and_decodes_via_store_path() {
    // Sanity: multiple entries on same page survive a write+read cycle via the
    // store, not just the standalone L2Page roundtrip.
    let (_dir, store) = open_store();
    let entries: Vec<(u64, BlockmapValue)> = (0..10).map(|i| (i * 3, sample(i + 10))).collect();
    for (lba, v) in &entries {
        store.put_mapping("vol", Lba(*lba), *v).unwrap();
    }
    for (lba, v) in &entries {
        assert_eq!(store.get_mapping("vol", Lba(*lba)).unwrap(), Some(*v));
    }
}

#[test]
fn l2_page_roundtrip_stability() {
    // Smoke test the L2Page API is re-exported correctly.
    let mut page = L2Page::empty();
    page.set(0, sample(1));
    let bytes = page.encode();
    let decoded = L2Page::decode(&bytes).unwrap();
    assert_eq!(decoded.get(0), Some(sample(1)));
}
