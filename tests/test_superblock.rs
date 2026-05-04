use std::sync::Arc;
use std::time::Duration;

use onyx_storage::io::device::RawDevice;
use onyx_storage::io::superblock::*;
use onyx_storage::io::uring::IoUringSession;
use onyx_storage::types::RESERVED_BLOCKS;
use tempfile::NamedTempFile;

fn create_test_device(blocks: u64) -> (RawDevice, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096 * blocks).unwrap();
    let dev = RawDevice::open(tmp.path()).unwrap();
    (dev, tmp)
}

// ──────────────────────────── DataSuperblock ────────────────────────────

#[test]
fn superblock_roundtrip() {
    let sb = DataSuperblock::new(4096 * 1000);
    let bytes = sb.to_bytes();
    let restored = DataSuperblock::from_bytes(&bytes).unwrap();

    assert_eq!(restored.magic, DATA_SUPERBLOCK_MAGIC);
    assert_eq!(restored.version, DATA_SUPERBLOCK_VERSION);
    assert_eq!(restored.device_size_bytes, 4096 * 1000);
    assert_eq!(restored.reserved_blocks, RESERVED_BLOCKS as u32);
    assert_eq!(restored.device_uuid, sb.device_uuid);
    assert_eq!(restored.creation_timestamp, sb.creation_timestamp);
    assert_eq!(restored.format_timestamp, sb.format_timestamp);
    assert_eq!(restored.flags, 0);
}

#[test]
fn superblock_crc_detects_corruption() {
    let sb = DataSuperblock::new(4096 * 100);
    let mut bytes = sb.to_bytes();
    bytes[10] ^= 0xFF; // corrupt a UUID byte
    assert!(DataSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn superblock_bad_magic() {
    let mut bytes = [0u8; 4096];
    bytes[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
    assert!(DataSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn superblock_bad_version() {
    let mut sb = DataSuperblock::new(4096 * 100);
    sb.version = 99;
    sb.update_crc();
    let bytes = sb.to_bytes();
    assert!(DataSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn superblock_all_zeros() {
    let bytes = [0u8; 4096];
    assert!(DataSuperblock::from_bytes(&bytes).is_none());
}

#[test]
fn superblock_uuid_is_unique() {
    let sb1 = DataSuperblock::new(4096 * 100);
    let sb2 = DataSuperblock::new(4096 * 100);
    assert_ne!(sb1.device_uuid, sb2.device_uuid);
}

#[test]
fn superblock_uuid_string_format() {
    let sb = DataSuperblock::new(4096 * 100);
    let uuid = sb.uuid_string();
    // 8-4-4-4-12 hex format = 36 chars
    assert_eq!(uuid.len(), 36);
    assert_eq!(uuid.chars().nth(8), Some('-'));
    assert_eq!(uuid.chars().nth(13), Some('-'));
    assert_eq!(uuid.chars().nth(18), Some('-'));
    assert_eq!(uuid.chars().nth(23), Some('-'));
}

// ──────────────────────────── HeartbeatBlock ────────────────────────────

#[test]
fn heartbeat_roundtrip() {
    let hb = HeartbeatBlock::new(42, 100);
    let bytes = hb.to_bytes();
    let restored = HeartbeatBlock::from_bytes(&bytes).unwrap();

    assert_eq!(restored.magic, HEARTBEAT_MAGIC);
    assert_eq!(restored.version, HEARTBEAT_VERSION);
    assert_eq!(restored.node_id, 42);
    assert_eq!(restored.sequence, 100);
    assert!(restored.timestamp_nanos > 0);
}

#[test]
fn heartbeat_crc_corruption() {
    let hb = HeartbeatBlock::new(1, 1);
    let mut bytes = hb.to_bytes();
    bytes[20] ^= 0xFF; // corrupt node_id area
    assert!(HeartbeatBlock::from_bytes(&bytes).is_none());
}

#[test]
fn heartbeat_bad_magic() {
    let mut bytes = [0u8; 4096];
    bytes[0..4].copy_from_slice(&0xBAADu32.to_le_bytes());
    assert!(HeartbeatBlock::from_bytes(&bytes).is_none());
}

#[test]
fn heartbeat_age_is_small() {
    let hb = HeartbeatBlock::new(1, 1);
    // Just created, age should be < 1 second
    assert!(hb.age_secs() < 1.0);
}

// ──────────────────────────── HaLockBlock ────────────────────────────

#[test]
fn ha_lock_roundtrip() {
    let lock = HaLockBlock::new(7, 42, Duration::from_secs(30));
    let bytes = lock.to_bytes();
    let restored = HaLockBlock::from_bytes(&bytes).unwrap();

    assert_eq!(restored.magic, HA_LOCK_MAGIC);
    assert_eq!(restored.version, HA_LOCK_VERSION);
    assert_eq!(restored.owner_node_id, 7);
    assert_eq!(restored.fence_token, 42);
    assert_eq!(restored.lease_duration_nanos, 30_000_000_000);
    assert!(!restored.is_expired());
}

#[test]
fn ha_lock_crc_corruption() {
    let lock = HaLockBlock::new(1, 1, Duration::from_secs(10));
    let mut bytes = lock.to_bytes();
    bytes[12] ^= 0xFF;
    assert!(HaLockBlock::from_bytes(&bytes).is_none());
}

#[test]
fn ha_lock_is_expired() {
    let mut lock = HaLockBlock::new(1, 1, Duration::from_secs(0));
    // Set lease start to the past to guarantee expiry
    lock.lease_start_nanos = 1; // epoch + 1ns, definitely expired
    lock.lease_duration_nanos = 1;
    lock.update_crc();
    assert!(lock.is_expired());
}

#[test]
fn ha_lock_remaining() {
    let lock = HaLockBlock::new(1, 1, Duration::from_secs(300));
    let remaining = lock.remaining();
    // Should be close to 300 seconds (allow 5s tolerance)
    assert!(remaining > Duration::from_secs(290));
}

// ──────────────────────────── Device I/O ────────────────────────────

#[test]
fn format_and_read_superblock() {
    let (dev, _tmp) = create_test_device(100);

    // Fresh device: read_superblock returns None
    assert!(read_superblock(&dev).unwrap().is_none());

    // Format
    let sb = format_device(&dev).unwrap();
    assert_eq!(sb.device_size_bytes, 4096 * 100);

    // Read back
    let read_sb = read_superblock(&dev).unwrap().unwrap();
    assert_eq!(read_sb.device_uuid, sb.device_uuid);
    assert_eq!(read_sb.device_size_bytes, sb.device_size_bytes);
}

#[test]
fn format_zeros_reserved_blocks() {
    let (dev, _tmp) = create_test_device(100);

    // Write garbage to blocks 1-7 first
    let garbage = vec![0xFFu8; 4096];
    for i in 1..RESERVED_BLOCKS {
        dev.write_at(&garbage, i * 4096).unwrap();
    }

    // Format should zero blocks 1-7
    format_device(&dev).unwrap();

    for i in 1..RESERVED_BLOCKS {
        let mut buf = [0u8; 4096];
        dev.read_at(&mut buf, i * 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0), "block {} should be zeroed", i);
    }
}

#[test]
fn device_too_small() {
    let (dev, _tmp) = create_test_device(4); // Only 4 blocks, need 8
    let result = read_superblock(&dev);
    assert!(result.is_err());
}

#[test]
fn write_and_read_ha_lock() {
    let (dev, _tmp) = create_test_device(100);
    format_device(&dev).unwrap();

    let lock = HaLockBlock::new(5, 99, Duration::from_secs(60));
    write_ha_lock(&dev, &lock).unwrap();

    let read_lock = read_ha_lock(&dev).unwrap().unwrap();
    assert_eq!(read_lock.owner_node_id, 5);
    assert_eq!(read_lock.fence_token, 99);
}

#[test]
fn read_heartbeat_from_fresh_device() {
    let (dev, _tmp) = create_test_device(100);
    format_device(&dev).unwrap();

    // Block 1 is zeroed — no valid heartbeat
    assert!(read_heartbeat(&dev).unwrap().is_none());
}

// ──────────────────────────── HeartbeatWriter ────────────────────────────

#[test]
fn heartbeat_writer_writes_periodically() {
    let (dev, _tmp) = create_test_device(100);
    format_device(&dev).unwrap();

    // Start heartbeat writer with short interval
    let hb_dev = RawDevice::open(_tmp.path()).unwrap();
    let mut writer = HeartbeatWriter::start(hb_dev, 42, Duration::from_millis(100));

    // Wait for at least 2 heartbeats
    std::thread::sleep(Duration::from_millis(350));

    writer.stop();

    // Read heartbeat block
    let hb = read_heartbeat(&dev).unwrap().unwrap();
    assert_eq!(hb.node_id, 42);
    assert!(
        hb.sequence >= 2,
        "expected >= 2 heartbeats, got {}",
        hb.sequence
    );
    assert!(hb.timestamp_nanos > 0);
}

#[test]
fn heartbeat_writer_stop_is_idempotent() {
    let (_dev, tmp) = create_test_device(100);
    let hb_dev = RawDevice::open(tmp.path()).unwrap();
    let mut writer = HeartbeatWriter::start(hb_dev, 1, Duration::from_secs(1));
    writer.stop();
    writer.stop(); // Should not panic
}

#[test]
fn heartbeat_writer_uring_writes_periodically() {
    let (dev, _tmp) = create_test_device(100);
    format_device(&dev).unwrap();

    let session = Arc::new(IoUringSession::new(8).unwrap());
    let hb_dev = RawDevice::open(_tmp.path()).unwrap();
    let mut writer = HeartbeatWriter::start_uring(hb_dev, 99, Duration::from_millis(80), session);

    std::thread::sleep(Duration::from_millis(300));
    writer.stop();

    let hb = read_heartbeat(&dev).unwrap().unwrap();
    assert_eq!(hb.node_id, 99);
    assert!(
        hb.sequence >= 2,
        "expected >= 2 heartbeats via io_uring, got {}",
        hb.sequence
    );
    assert!(hb.timestamp_nanos > 0);
}
