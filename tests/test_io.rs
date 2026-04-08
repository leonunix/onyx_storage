use onyx_storage::io::aligned::{round_up, AlignedBuf};
use onyx_storage::io::device::RawDevice;
use onyx_storage::io::engine::IoEngine;
use onyx_storage::types::*;
use tempfile::NamedTempFile;

// --- aligned buf tests ---

#[test]
fn aligned_buf_basic() {
    let mut buf = AlignedBuf::new(4096, false).unwrap();
    assert_eq!(buf.len(), 4096);
    assert_eq!(buf.as_ptr() as usize % 4096, 0);

    buf.as_mut_slice().fill(0xAB);
    assert!(buf.as_slice().iter().all(|&b| b == 0xAB));
}

#[test]
fn aligned_buf_rounds_up() {
    let buf = AlignedBuf::new(100, false).unwrap();
    assert_eq!(buf.len(), 4096);
}

#[test]
fn aligned_buf_large() {
    let buf = AlignedBuf::new(1024 * 1024, false).unwrap();
    assert_eq!(buf.len(), 1024 * 1024);
    assert_eq!(buf.as_ptr() as usize % 4096, 0);
}

#[test]
fn round_up_test() {
    assert_eq!(round_up(0, 4096), 0);
    assert_eq!(round_up(1, 4096), 4096);
    assert_eq!(round_up(4096, 4096), 4096);
    assert_eq!(round_up(4097, 4096), 8192);
}

// --- raw device tests ---

#[test]
fn device_read_write_at() {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096 * 10).unwrap();

    let dev = RawDevice::open(tmp.path()).unwrap();
    assert_eq!(dev.size(), 4096 * 10);

    let write_data = vec![0xABu8; 4096];
    dev.write_at(&write_data, 0).unwrap();

    let mut read_data = vec![0u8; 4096];
    dev.read_at(&mut read_data, 0).unwrap();
    assert_eq!(read_data, write_data);
}

#[test]
fn device_read_write_offset() {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096 * 10).unwrap();

    let dev = RawDevice::open(tmp.path()).unwrap();

    let data = vec![0xCDu8; 4096];
    dev.write_at(&data, 4096 * 5).unwrap();

    let mut buf = vec![0u8; 4096];
    dev.read_at(&mut buf, 4096 * 5).unwrap();
    assert_eq!(buf, data);

    dev.read_at(&mut buf, 0).unwrap();
    assert!(buf.iter().all(|&b| b == 0));
}

// --- io engine tests ---

fn test_engine() -> (IoEngine, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096 * 100).unwrap();
    let dev = RawDevice::open(tmp.path()).unwrap();
    let engine = IoEngine::new(dev, false);
    (engine, tmp)
}

#[test]
fn engine_write_read_block() {
    let (engine, _tmp) = test_engine();

    let data = b"hello world, this is test data!!";
    engine.write_block(Pba(0), data).unwrap();
    let read_payload = engine.read_block(Pba(0), data.len()).unwrap();
    assert_eq!(read_payload, data);
}

#[test]
fn engine_write_read_different_pbas() {
    let (engine, _tmp) = test_engine();

    for i in 0..5u64 {
        let data = format!("block-{}", i);
        let payload = data.as_bytes();
        engine.write_block(Pba(i), payload).unwrap();
    }

    for i in 0..5u64 {
        let expected = format!("block-{}", i);
        let payload = engine.read_block(Pba(i), expected.len()).unwrap();
        assert_eq!(payload, expected.as_bytes());
    }
}

/// Read from invalid (unwritten) PBA -- returns zeroed data, not an error,
/// since there is no on-disk header/magic to validate.
#[test]
fn engine_read_unwritten_pba() {
    let (engine, _tmp) = test_engine();
    // Reading raw payload from an unwritten PBA just returns zeros
    let result = engine.read_block(Pba(50), 100);
    assert!(result.is_ok());
    let payload = result.unwrap();
    assert!(payload.iter().all(|&b| b == 0));
}

/// Write payload that exactly fills BLOCK_SIZE (no header overhead now).
#[test]
fn engine_max_payload() {
    let (engine, _tmp) = test_engine();
    let data = vec![0xCC; 4096]; // full block, no header overhead
    engine.write_block(Pba(0), &data).unwrap();
    let payload = engine.read_block(Pba(0), 4096).unwrap();
    assert_eq!(payload, data);
}

/// Payload too large for block -> error.
#[test]
fn engine_payload_too_large() {
    let (engine, _tmp) = test_engine();
    let too_big = vec![0xFF; 4097]; // exceeds BLOCK_SIZE
    let result = engine.write_block(Pba(0), &too_big);
    assert!(result.is_err());
}

/// Device open_or_create creates a new file.
#[test]
fn device_open_or_create_new() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("newdev");
    let dev = RawDevice::open_or_create(&path, 4096 * 10).unwrap();
    assert_eq!(dev.size(), 4096 * 10);
    assert!(path.exists());
}

/// Device read beyond EOF -> error.
#[test]
fn device_read_beyond_eof() {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096).unwrap();
    let dev = RawDevice::open(tmp.path()).unwrap();

    let mut buf = vec![0u8; 4096];
    let result = dev.read_at(&mut buf, 4096 * 10); // way beyond file size
    assert!(result.is_err());
}

/// Device sync works without error.
#[test]
fn device_sync() {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096).unwrap();
    let dev = RawDevice::open(tmp.path()).unwrap();
    dev.write_at(&[0xAA; 4096], 0).unwrap();
    dev.sync().unwrap();
}

/// Device properties.
#[test]
fn device_properties() {
    let tmp = NamedTempFile::new().unwrap();
    tmp.as_file().set_len(4096 * 5).unwrap();
    let dev = RawDevice::open(tmp.path()).unwrap();
    assert_eq!(dev.size(), 4096 * 5);
    assert_eq!(dev.path(), tmp.path());
    // On macOS, O_DIRECT is not supported, so is_direct_io() should be false
    #[cfg(not(target_os = "linux"))]
    assert!(!dev.is_direct_io());
}

/// IoEngine total_blocks and device_size.
#[test]
fn engine_metadata() {
    let (engine, _tmp) = test_engine();
    assert_eq!(engine.device_size(), 4096 * 100);
    assert_eq!(engine.total_blocks(), 100);
}

/// IoEngine sync.
#[test]
fn engine_sync() {
    let (engine, _tmp) = test_engine();
    engine.sync().unwrap();
}
