use onyx_storage::buffer::entry::*;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::error::OnyxError;
use onyx_storage::io::device::RawDevice;
use onyx_storage::types::{Lba, BLOCK_SIZE};
use tempfile::NamedTempFile;

// --- entry tests ---

#[test]
fn superblock_roundtrip() {
    let sb = BufferSuperblock::new(1000);
    let bytes = sb.to_bytes();
    let restored = BufferSuperblock::from_bytes(&bytes).unwrap();
    assert_eq!(restored.capacity_entries, 1000);
    assert_eq!(restored.head_seq, 0);
    assert_eq!(restored.tail_seq, 0);
}

#[test]
fn superblock_corrupt_detected() {
    let sb = BufferSuperblock::new(100);
    let mut bytes = sb.to_bytes();
    bytes[10] ^= 0xFF;
    assert!(BufferSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn entry_roundtrip() {
    let payload = vec![0xAB; 2048];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 42,
        vol_id: "some-vol".to_string(),
        lba: Lba(100),
        compression: 1,
        original_size: 4096,
        compressed_size: 2048,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };

    let bytes = entry.to_bytes().unwrap();
    let restored = BufferEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.seq, 42);
    assert_eq!(restored.vol_id, "some-vol");
    assert_eq!(restored.lba, Lba(100));
    assert_eq!(restored.compression, 1);
    assert_eq!(restored.compressed_size, 2048);
    assert_eq!(restored.payload.len(), 2048);
    assert!(!restored.flushed);
}

#[test]
fn entry_corrupt_detected() {
    let entry = BufferEntry {
        seq: 1,
        vol_id: String::new(),
        lba: Lba(0),
        compression: 0,
        original_size: 10,
        compressed_size: 10,
        payload_crc32: 0,
        flushed: false,
        payload: vec![0; 10],
    };
    let mut bytes = entry.to_bytes().unwrap();
    bytes[6] ^= 0xFF;
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

#[test]
fn superblock_offset_calculation() {
    let sb = BufferSuperblock::new(100);
    assert_eq!(sb.offset_for_seq(0), BUFFER_SUPERBLOCK_SIZE);
    assert_eq!(
        sb.offset_for_seq(1),
        BUFFER_SUPERBLOCK_SIZE + BUFFER_ENTRY_SIZE
    );
    assert_eq!(sb.offset_for_seq(100), BUFFER_SUPERBLOCK_SIZE);
}

// --- pool tests ---

fn create_test_pool(num_entries: u64) -> (WriteBufferPool, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let size = BUFFER_SUPERBLOCK_SIZE + num_entries * BUFFER_ENTRY_SIZE;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();
    (pool, tmp)
}

#[test]
fn basic_append_and_recover() {
    let (pool, _tmp) = create_test_pool(10);

    let data = b"hello world test data";
    let seq = pool.append("test-vol", Lba(5), data).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(pool.pending_count(), 1);

    let unflushed = pool.recover().unwrap();
    assert_eq!(unflushed.len(), 1);
    assert_eq!(unflushed[0].lba, Lba(5));
    assert_eq!(unflushed[0].payload, data);
}

#[test]
fn flush_and_advance() {
    let (pool, _tmp) = create_test_pool(10);

    for i in 0..5 {
        pool.append("test-vol", Lba(i), &[0xAB; 10]).unwrap();
    }
    assert_eq!(pool.pending_count(), 5);

    for i in 0..3 {
        pool.mark_flushed(i).unwrap();
    }

    let advanced = pool.advance_tail().unwrap();
    assert_eq!(advanced, 3);
    assert_eq!(pool.pending_count(), 2);

    let unflushed = pool.recover().unwrap();
    assert_eq!(unflushed.len(), 2);
}

#[test]
fn full_buffer_rejected() {
    let (pool, _tmp) = create_test_pool(3);

    for _ in 0..3 {
        pool.append("test-vol", Lba(0), &[0; 4]).unwrap();
    }

    let result = pool.append("test-vol", Lba(0), &[0; 4]);
    assert!(matches!(result, Err(OnyxError::BufferPoolFull(_))));
}

#[test]
fn persistence_across_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let size = BUFFER_SUPERBLOCK_SIZE + 10 * BUFFER_ENTRY_SIZE;
    tmp.as_file().set_len(size).unwrap();

    {
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open(dev).unwrap();
        pool.append("test-vol", Lba(42), &[0xCD; 100]).unwrap();
        pool.append("test-vol", Lba(43), &[0xEF; 200]).unwrap();
    }

    {
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open(dev).unwrap();
        let unflushed = pool.recover().unwrap();
        assert_eq!(unflushed.len(), 2);
        assert_eq!(unflushed[0].lba, Lba(42));
        assert_eq!(unflushed[1].lba, Lba(43));
    }
}

#[test]
fn fill_percentage() {
    let (pool, _tmp) = create_test_pool(10);
    assert_eq!(pool.fill_percentage(), 0);

    for _ in 0..5 {
        pool.append("test-vol", Lba(0), &[0; 4]).unwrap();
    }
    assert_eq!(pool.fill_percentage(), 50);
}

// --- regression tests ---

/// Fix #5: entry with corrupt compressed_size exceeding slot boundary must return None.
#[test]
fn entry_corrupt_compressed_size_overflow() {
    let entry = BufferEntry {
        seq: 1,
        vol_id: String::new(),
        lba: Lba(0),
        compression: 0,
        original_size: 4096,
        compressed_size: 4096, // valid
        payload_crc32: 0,
        flushed: false,
        payload: vec![0; 4096],
    };
    let mut bytes = entry.to_bytes().unwrap();
    // Corrupt compressed_size to exceed 8KB slot: set to 9000
    let bad_size: u32 = 9000;
    bytes[28..32].copy_from_slice(&bad_size.to_le_bytes());
    // Fix header CRC so the size field is the only corruption
    let header_crc = crc32fast::hash(&bytes[0..36]);
    bytes[36..40].copy_from_slice(&header_crc.to_le_bytes());

    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

/// Buffer pool lookup finds the most recent unflushed entry for a (vol_id, lba).
#[test]
fn buffer_pool_lookup() {
    let (pool, _tmp) = create_test_pool(10);

    pool.append("test-vol", Lba(0), &[0xAA; 50]).unwrap();
    pool.append("test-vol", Lba(1), &[0xBB; 60]).unwrap();

    let found = pool.lookup("test-vol", Lba(0)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().payload, vec![0xAA; 50]);

    let found = pool.lookup("test-vol", Lba(1)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().payload, vec![0xBB; 60]);

    let found = pool.lookup("test-vol", Lba(999)).unwrap();
    assert!(found.is_none());
}

/// Lookup returns the latest overwrite.
#[test]
fn buffer_pool_lookup_latest() {
    let (pool, _tmp) = create_test_pool(10);

    pool.append("test-vol", Lba(5), &[0x11; 30]).unwrap();
    pool.append("test-vol", Lba(5), &[0x22; 40]).unwrap();

    let found = pool.lookup("test-vol", Lba(5)).unwrap().unwrap();
    assert_eq!(found.payload, vec![0x22; 40]);
}

/// Lookup on an empty pool returns None.
#[test]
fn buffer_pool_lookup_empty() {
    let (pool, _tmp) = create_test_pool(10);
    let found = pool.lookup("test-vol", Lba(0)).unwrap();
    assert!(found.is_none());
}

/// Capacity returns correct value.
#[test]
fn buffer_pool_capacity() {
    let (pool, _tmp) = create_test_pool(25);
    assert_eq!(pool.capacity(), 25);
}

/// Entry with flushed=true roundtrips correctly.
#[test]
fn entry_flushed_flag_roundtrip() {
    let entry = BufferEntry {
        seq: 99,
        vol_id: "dead-vol".to_string(),
        lba: Lba(42),
        compression: 2,
        original_size: 4096,
        compressed_size: 100,
        payload_crc32: crc32fast::hash(&[0; 100]),
        flushed: true,
        payload: vec![0; 100],
    };
    let bytes = entry.to_bytes().unwrap();
    let restored = BufferEntry::from_bytes(&bytes).unwrap();
    assert!(restored.flushed);
    assert_eq!(restored.seq, 99);
    assert_eq!(restored.vol_id, "dead-vol");
}

/// Bad magic in entry bytes -> None.
#[test]
fn entry_bad_magic() {
    let mut bytes = [0u8; BUFFER_ENTRY_SIZE as usize];
    bytes[0..4].copy_from_slice(&0xDEADu32.to_le_bytes());
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

/// Bad magic in superblock bytes -> None.
#[test]
fn superblock_bad_magic() {
    let mut bytes = [0u8; 4096];
    bytes[0..4].copy_from_slice(&0xBAADu32.to_le_bytes());
    assert!(BufferSuperblock::from_bytes(&bytes).is_none());
}

/// Opening a buffer device that's too small -> error.
#[test]
fn buffer_pool_device_too_small() {
    let tmp = NamedTempFile::new().unwrap();
    // Only superblock, no room for entries
    tmp.as_file().set_len(BUFFER_SUPERBLOCK_SIZE).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), BUFFER_SUPERBLOCK_SIZE).unwrap();
    let result = WriteBufferPool::open(dev);
    assert!(result.is_err());
}

/// Superblock pending_count and is_full.
#[test]
fn superblock_pending_and_full() {
    let mut sb = BufferSuperblock::new(5);
    assert_eq!(sb.pending_count(), 0);
    assert!(!sb.is_full());

    sb.head_seq = 5;
    assert_eq!(sb.pending_count(), 5);
    assert!(sb.is_full());

    sb.tail_seq = 3;
    assert_eq!(sb.pending_count(), 2);
    assert!(!sb.is_full());
}

// --- vol_id length and CRC coverage regression tests ---

/// Vol_id up to 255 bytes roundtrips correctly through buffer entry.
#[test]
fn entry_long_vol_id_roundtrip() {
    let long_id = "v".repeat(255);
    let payload = vec![0xDD; 200];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 7,
        vol_id: long_id.clone(),
        lba: Lba(42),
        compression: 1,
        original_size: 4096,
        compressed_size: 200,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let bytes = entry.to_bytes().unwrap();
    let restored = BufferEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.vol_id, long_id);
    assert_eq!(restored.payload.len(), 200);
}

/// Pool append + lookup with 255-byte vol_id works end-to-end.
#[test]
fn pool_long_vol_id_lookup() {
    let (pool, _tmp) = create_test_pool(10);
    let long_id = "x".repeat(255);
    pool.append(&long_id, Lba(0), &[0xEE; 100]).unwrap();

    let found = pool.lookup(&long_id, Lba(0)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().vol_id, long_id);
}

/// Corrupting the vol_id region in a serialized entry → from_bytes returns None.
#[test]
fn entry_corrupt_vol_id_detected() {
    let payload = vec![0xAA; 50];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "test-vol".to_string(),
        lba: Lba(0),
        compression: 0,
        original_size: 50,
        compressed_size: 50,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let mut bytes = entry.to_bytes().unwrap();
    // Corrupt a byte in the vol_id region (offset 40+)
    bytes[42] ^= 0xFF;
    // CRC covers vol_id, so this should be detected
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

/// Corrupting the payload region → from_bytes returns None.
#[test]
fn entry_corrupt_payload_detected() {
    let payload = vec![0xBB; 100];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "test-vol".to_string(),
        lba: Lba(0),
        compression: 0,
        original_size: 100,
        compressed_size: 100,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let mut bytes = entry.to_bytes().unwrap();
    // Corrupt a byte in the payload region
    // vol_id is 8 bytes, so payload starts at 40 + 8 = 48
    bytes[60] ^= 0xFF;
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

/// Vol_id with invalid UTF-8 in the buffer → from_bytes returns None, not empty string.
#[test]
fn entry_invalid_utf8_vol_id_rejected() {
    let payload = vec![0; 10];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "ok".to_string(),
        lba: Lba(0),
        compression: 0,
        original_size: 10,
        compressed_size: 10,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let mut bytes = entry.to_bytes().unwrap();
    // Write invalid UTF-8 into the vol_id region
    bytes[40] = 0xFF; // not valid UTF-8 start byte
    bytes[41] = 0xFE;
    // Fix the entry CRC so the only problem is UTF-8
    let id_len = u16::from_le_bytes(bytes[22..24].try_into().unwrap()) as usize;
    let compressed_size = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;
    let payload_end = 40 + id_len + compressed_size;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[0..36]);
    hasher.update(&bytes[40..payload_end]);
    let new_crc = hasher.finalize();
    bytes[36..40].copy_from_slice(&new_crc.to_le_bytes());
    // Also fix payload CRC since payload didn't change
    // but we need to recalculate because payload_start moved? No, id_len didn't change.
    // The payload itself is fine, only vol_id bytes are corrupt.
    // But the entry_crc check will pass (we fixed it), then UTF-8 check should fail.
    // However, payload_crc check happens after UTF-8 check, and we broke the entry CRC
    // by changing vol_id bytes... Wait, we re-computed entry_crc to cover the corrupted bytes.
    // So entry_crc passes. Then UTF-8 decode of [0xFF, 0xFE] fails → returns None. Good.
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

#[test]
fn append_rejects_payload_larger_than_block() {
    let (pool, _tmp) = create_test_pool(4);
    let err = pool
        .append("test-vol", Lba(0), &vec![0xAA; BLOCK_SIZE as usize + 1])
        .unwrap_err();
    assert!(matches!(err, OnyxError::Config(_)));
}
