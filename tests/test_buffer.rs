use onyx_storage::buffer::entry::*;
use onyx_storage::buffer::pool::WriteBufferPool;
use onyx_storage::error::OnyxError;
use onyx_storage::io::device::RawDevice;
use onyx_storage::types::{Lba, BLOCK_SIZE};
use tempfile::NamedTempFile;

// --- entry tests ---

#[test]
fn superblock_roundtrip() {
    let sb = BufferSuperblock::new(100 * 8192);
    let bytes = sb.to_bytes();
    let restored = BufferSuperblock::from_bytes(&bytes).unwrap();
    assert_eq!(restored.capacity_bytes, 100 * 8192);
    assert_eq!(restored.head_offset, BUFFER_SUPERBLOCK_SIZE);
    assert_eq!(restored.tail_offset, BUFFER_SUPERBLOCK_SIZE);
}

#[test]
fn superblock_corrupt_detected() {
    let sb = BufferSuperblock::new(100 * 8192);
    let mut bytes = sb.to_bytes();
    bytes[10] ^= 0xFF;
    assert!(BufferSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn entry_roundtrip() {
    let payload = vec![0xAB; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 42,
        vol_id: "some-vol".to_string(),
        start_lba: Lba(100),
        lba_count: 1,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };

    let bytes = entry.to_bytes().unwrap();
    let restored = BufferEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.seq, 42);
    assert_eq!(restored.vol_id, "some-vol");
    assert_eq!(restored.start_lba, Lba(100));
    assert_eq!(restored.lba_count, 1);
    assert_eq!(restored.payload.len(), 4096);
    assert!(!restored.flushed);
}

#[test]
fn entry_corrupt_detected() {
    let payload = vec![0; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: String::new(),
        start_lba: Lba(0),
        lba_count: 1,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let mut bytes = entry.to_bytes().unwrap();
    bytes[6] ^= 0xFF;
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

// --- pool tests ---

fn create_test_pool(num_entries: u64) -> (WriteBufferPool, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + num_entries * 8192;
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();
    (pool, tmp)
}

#[test]
fn basic_append_and_recover() {
    let (pool, _tmp) = create_test_pool(10);

    let data = vec![0xAB; 4096];
    let seq = pool.append("test-vol", Lba(5), 1, &data).unwrap();
    assert!(seq >= 1); // seq counter starts at 1 for a fresh pool
    assert_eq!(pool.pending_count(), 1);

    let unflushed = pool.recover().unwrap();
    assert_eq!(unflushed.len(), 1);
    assert_eq!(unflushed[0].start_lba, Lba(5));
    assert_eq!(unflushed[0].payload, data);
}

#[test]
fn flush_and_advance() {
    let (pool, _tmp) = create_test_pool(10);

    let mut seqs = Vec::new();
    for i in 0..5 {
        let seq = pool.append("test-vol", Lba(i), 1, &vec![0xAB; 4096]).unwrap();
        seqs.push((seq, Lba(i)));
    }
    assert_eq!(pool.pending_count(), 5);

    for &(seq, lba) in &seqs[..3] {
        pool.mark_flushed(seq, lba, 1).unwrap();
    }

    let advanced = pool.advance_tail().unwrap();
    assert_eq!(advanced, 3);
    assert_eq!(pool.pending_count(), 2);

    let unflushed = pool.recover().unwrap();
    assert_eq!(unflushed.len(), 2);
}

#[test]
fn full_buffer_rejected() {
    // Create a pool with capacity for ~2.5 entries (each entry is 8192 bytes for single-LBA)
    // so after 2 appends there's not enough room for a 3rd
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 2 * 8192 + 4096; // capacity = 20480, can hold 2 entries (16384) with 4096 leftover (< 8192)
    tmp.as_file().set_len(size).unwrap();
    let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
    let pool = WriteBufferPool::open(dev).unwrap();

    for _ in 0..2 {
        pool.append("test-vol", Lba(0), 1, &vec![0; 4096]).unwrap();
    }

    // 3rd append should fail -- 4096 free < 8192 needed
    let result = pool.append("test-vol", Lba(0), 1, &vec![0; 4096]);
    assert!(matches!(result, Err(OnyxError::BufferPoolFull(_))));
}

#[test]
fn persistence_across_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let size = 4096 + 10 * 8192;
    tmp.as_file().set_len(size).unwrap();

    {
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open(dev).unwrap();
        pool.append("test-vol", Lba(42), 1, &vec![0xCD; 4096]).unwrap();
        pool.append("test-vol", Lba(43), 1, &vec![0xEF; 4096]).unwrap();
    }

    {
        let dev = RawDevice::open_or_create(tmp.path(), size).unwrap();
        let pool = WriteBufferPool::open(dev).unwrap();
        let unflushed = pool.recover().unwrap();
        assert_eq!(unflushed.len(), 2);
        assert_eq!(unflushed[0].start_lba, Lba(42));
        assert_eq!(unflushed[1].start_lba, Lba(43));
    }
}

#[test]
fn fill_percentage() {
    let (pool, _tmp) = create_test_pool(10);
    assert_eq!(pool.fill_percentage(), 0);

    for _ in 0..5 {
        pool.append("test-vol", Lba(0), 1, &vec![0; 4096]).unwrap();
    }
    assert!(pool.fill_percentage() > 0);
}

// --- regression tests ---

/// Buffer pool lookup finds the most recent unflushed entry for a (vol_id, lba).
#[test]
fn buffer_pool_lookup() {
    let (pool, _tmp) = create_test_pool(10);

    pool.append("test-vol", Lba(0), 1, &vec![0xAA; 4096]).unwrap();
    pool.append("test-vol", Lba(1), 1, &vec![0xBB; 4096]).unwrap();

    let found = pool.lookup("test-vol", Lba(0)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().payload, vec![0xAA; 4096]);

    let found = pool.lookup("test-vol", Lba(1)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().payload, vec![0xBB; 4096]);

    let found = pool.lookup("test-vol", Lba(999)).unwrap();
    assert!(found.is_none());
}

/// Lookup returns the latest overwrite.
#[test]
fn buffer_pool_lookup_latest() {
    let (pool, _tmp) = create_test_pool(10);

    pool.append("test-vol", Lba(5), 1, &vec![0x11; 4096]).unwrap();
    pool.append("test-vol", Lba(5), 1, &vec![0x22; 4096]).unwrap();

    let found = pool.lookup("test-vol", Lba(5)).unwrap().unwrap();
    assert_eq!(found.payload, vec![0x22; 4096]);
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
    // capacity_bytes = device_size - superblock = (4096 + 25*8192) - 4096 = 25*8192
    assert_eq!(pool.capacity(), 25 * 8192);
}

/// Entry with flushed=true roundtrips correctly.
#[test]
fn entry_flushed_flag_roundtrip() {
    let payload = vec![0; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 99,
        vol_id: "dead-vol".to_string(),
        start_lba: Lba(42),
        lba_count: 1,
        payload_crc32: payload_crc,
        flushed: true,
        payload,
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
    let mut bytes = vec![0u8; 4096];
    // Set total_len to 4096
    bytes[0..4].copy_from_slice(&4096u32.to_le_bytes());
    // Set bad magic
    bytes[4..8].copy_from_slice(&0xDEADu32.to_le_bytes());
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

/// Superblock has_room check.
#[test]
fn superblock_has_room() {
    let sb = BufferSuperblock::new(5 * 8192);
    assert!(sb.has_room(4096));
    assert!(sb.has_room(5 * 8192));
    assert!(!sb.has_room(5 * 8192 + 1));
}

// --- vol_id length and CRC coverage regression tests ---

/// Vol_id up to 255 bytes roundtrips correctly through buffer entry.
#[test]
fn entry_long_vol_id_roundtrip() {
    let long_id = "v".repeat(255);
    let payload = vec![0xDD; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 7,
        vol_id: long_id.clone(),
        start_lba: Lba(42),
        lba_count: 1,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let bytes = entry.to_bytes().unwrap();
    let restored = BufferEntry::from_bytes(&bytes).unwrap();
    assert_eq!(restored.vol_id, long_id);
    assert_eq!(restored.payload.len(), 4096);
}

/// Pool append + lookup with 255-byte vol_id works end-to-end.
#[test]
fn pool_long_vol_id_lookup() {
    let (pool, _tmp) = create_test_pool(10);
    let long_id = "x".repeat(255);
    pool.append(&long_id, Lba(0), 1, &vec![0xEE; 4096]).unwrap();

    let found = pool.lookup(&long_id, Lba(0)).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().vol_id, long_id);
}

/// Corrupting the vol_id region in a serialized entry -> from_bytes returns None.
#[test]
fn entry_corrupt_vol_id_detected() {
    let payload = vec![0xAA; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "test-vol".to_string(),
        start_lba: Lba(0),
        lba_count: 1,
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

/// Corrupting the payload region -> from_bytes returns None.
#[test]
fn entry_corrupt_payload_detected() {
    let payload = vec![0xBB; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "test-vol".to_string(),
        start_lba: Lba(0),
        lba_count: 1,
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

/// Vol_id with invalid UTF-8 in the buffer -> from_bytes returns None, not empty string.
#[test]
fn entry_invalid_utf8_vol_id_rejected() {
    let payload = vec![0; 4096];
    let payload_crc = crc32fast::hash(&payload);
    let entry = BufferEntry {
        seq: 1,
        vol_id: "ok".to_string(),
        start_lba: Lba(0),
        lba_count: 1,
        payload_crc32: payload_crc,
        flushed: false,
        payload,
    };
    let mut bytes = entry.to_bytes().unwrap();
    // Write invalid UTF-8 into the vol_id region
    bytes[40] = 0xFF; // not valid UTF-8 start byte
    bytes[41] = 0xFE;
    // Fix the entry CRC so the only problem is UTF-8
    let id_len = u16::from_le_bytes(bytes[28..30].try_into().unwrap()) as usize;
    let payload_len = 1 * BLOCK_SIZE as usize; // lba_count=1
    let data_end = 40 + id_len + payload_len;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[0..36]);
    hasher.update(&bytes[40..data_end]);
    let new_crc = hasher.finalize();
    bytes[36..40].copy_from_slice(&new_crc.to_le_bytes());
    assert!(BufferEntry::from_bytes(&bytes).is_none());
}

#[test]
fn append_rejects_wrong_payload_size() {
    let (pool, _tmp) = create_test_pool(4);
    // payload must be exactly lba_count * 4096 = 4096 bytes for lba_count=1
    let err = pool
        .append("test-vol", Lba(0), 1, &vec![0xAA; BLOCK_SIZE as usize + 1])
        .unwrap_err();
    assert!(matches!(err, OnyxError::Config(_)));
}

#[test]
fn partial_mark_flushed_is_idempotent_for_multi_lba_entry() {
    let (pool, _tmp) = create_test_pool(8);
    let payload = vec![0x5A; 2 * BLOCK_SIZE as usize];

    let seq = pool.append("test-vol", Lba(100), 2, &payload).unwrap();
    assert_eq!(pool.pending_count(), 1);

    pool.mark_flushed(seq, Lba(100), 1).unwrap();
    assert_eq!(pool.pending_count(), 1);
    assert_eq!(pool.recover().unwrap().len(), 1);
    assert!(pool.lookup("test-vol", Lba(100)).unwrap().is_some());
    assert!(pool.lookup("test-vol", Lba(101)).unwrap().is_some());

    // Retrying the same partial range must not double-count progress.
    pool.mark_flushed(seq, Lba(100), 1).unwrap();
    assert_eq!(pool.pending_count(), 1);
    assert_eq!(pool.recover().unwrap().len(), 1);

    pool.mark_flushed(seq, Lba(101), 1).unwrap();
    assert_eq!(pool.pending_count(), 0);
    assert!(pool.recover().unwrap().is_empty());
    assert!(pool.lookup("test-vol", Lba(100)).unwrap().is_none());
    assert!(pool.lookup("test-vol", Lba(101)).unwrap().is_none());

    let advanced = pool.advance_tail().unwrap();
    assert_eq!(advanced, 1);
}
