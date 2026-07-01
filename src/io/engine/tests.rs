use super::*;
use std::path::PathBuf;
use tempfile::TempDir;

fn fresh_device(dir: &TempDir, name: &str, size: u64) -> RawDevice {
    let path: PathBuf = dir.path().join(name);
    RawDevice::open_or_create(&path, size).unwrap()
}

#[test]
fn syscall_round_trip() {
    let dir = TempDir::new().unwrap();
    let dev = fresh_device(&dir, "lv3", 1024 * 1024);
    let engine = IoEngine::new_raw(dev, false);

    let payload = vec![0xABu8; 4096];
    engine.write_blocks(Pba(0), &payload).unwrap();
    let read = engine.read_blocks(Pba(0), 4096).unwrap();
    assert_eq!(read, payload);
}

#[test]
fn uring_round_trip() {
    let dir = TempDir::new().unwrap();
    let dev = fresh_device(&dir, "lv3", 1024 * 1024);
    let session = Arc::new(IoUringSession::new(16).unwrap());
    let engine = IoEngine::with_options(Arc::new(dev), false, 0, None, IoBackend::Uring(session));

    let payload = vec![0xCDu8; 4096];
    engine.write_blocks(Pba(2), &payload).unwrap();
    let read = engine.read_blocks(Pba(2), 4096).unwrap();
    assert_eq!(read, payload);
}

#[test]
fn uring_batch_writes_then_reads() {
    let dir = TempDir::new().unwrap();
    let dev = fresh_device(&dir, "lv3", 1024 * 1024);
    let session = Arc::new(IoUringSession::new(64).unwrap());
    let engine = IoEngine::with_options(Arc::new(dev), false, 0, None, IoBackend::Uring(session));

    let payloads: Vec<Vec<u8>> = (0..8).map(|i| vec![i as u8; 4096]).collect();
    let writes: Vec<LvOp> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| LvOp::Write {
            pba: Pba(i as u64),
            payload: p.as_slice(),
        })
        .collect();
    let results = engine.submit_batch(writes, true).unwrap();
    assert_eq!(results.len(), 8);
    for r in &results {
        match r {
            LvOpResult::Write(Ok(())) => {}
            _ => panic!("write should have succeeded"),
        }
    }

    let reads: Vec<LvOp> = (0..8)
        .map(|i| LvOp::Read {
            pba: Pba(i as u64),
            size: 4096,
        })
        .collect();
    let results = engine.submit_batch(reads, false).unwrap();
    for (i, r) in results.into_iter().enumerate() {
        match r {
            LvOpResult::Read(Ok(bytes)) => {
                assert_eq!(bytes, payloads[i], "read {} mismatch", i);
            }
            _ => panic!("read {} failed", i),
        }
    }
}

#[test]
fn uring_batch_chunks_when_ops_exceed_sq_entries() {
    let dir = TempDir::new().unwrap();
    let dev = fresh_device(&dir, "lv3", 1024 * 1024);
    let session = Arc::new(IoUringSession::new(4).unwrap());
    let engine = IoEngine::with_options(Arc::new(dev), false, 0, None, IoBackend::Uring(session));

    let payloads: Vec<Vec<u8>> = (0..10).map(|i| vec![(i + 1) as u8; 4096]).collect();
    let writes: Vec<LvOp> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| LvOp::Write {
            pba: Pba(i as u64),
            payload: p.as_slice(),
        })
        .collect();
    let results = engine.submit_batch(writes, true).unwrap();
    assert_eq!(results.len(), payloads.len());
    for result in results {
        assert!(matches!(result, LvOpResult::Write(Ok(()))));
    }

    let reads: Vec<LvOp> = (0..payloads.len())
        .map(|i| LvOp::Read {
            pba: Pba(i as u64),
            size: 4096,
        })
        .collect();
    let results = engine.submit_batch(reads, false).unwrap();
    for (i, result) in results.into_iter().enumerate() {
        match result {
            LvOpResult::Read(Ok(bytes)) => assert_eq!(bytes, payloads[i]),
            _ => panic!("read {i} failed"),
        }
    }
}

#[test]
fn syscall_batch_writes_then_reads() {
    let dir = TempDir::new().unwrap();
    let dev = fresh_device(&dir, "lv3", 1024 * 1024);
    let engine = IoEngine::new_raw(dev, false);

    let payloads: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 0x10) as u8; 4096]).collect();
    let writes: Vec<LvOp> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| LvOp::Write {
            pba: Pba(i as u64),
            payload: p.as_slice(),
        })
        .collect();
    let _ = engine.submit_batch(writes, true).unwrap();

    let reads: Vec<LvOp> = (0..4)
        .map(|i| LvOp::Read {
            pba: Pba(i as u64),
            size: 4096,
        })
        .collect();
    let results = engine.submit_batch(reads, false).unwrap();
    for (i, r) in results.into_iter().enumerate() {
        match r {
            LvOpResult::Read(Ok(bytes)) => assert_eq!(bytes, payloads[i]),
            _ => panic!("read {} failed", i),
        }
    }
}

/// Minimal in-memory backend to exercise the stripe accessors independent of a
/// real chunklet LD: reports a chosen stripe width, no-ops IO.
struct StripeMock {
    stripe: u32,
}
impl crate::io::block_backend::BlockBackend for StripeMock {
    fn read_at(&self, _buf: &mut [u8], _off: u64) -> OnyxResult<()> {
        Ok(())
    }
    fn write_at(&self, _buf: &[u8], _off: u64) -> OnyxResult<()> {
        Ok(())
    }
    fn flush(&self) -> OnyxResult<()> {
        Ok(())
    }
    fn size(&self) -> u64 {
        1 << 30
    }
    fn stripe_blocks(&self) -> u32 {
        self.stripe
    }
}

#[test]
fn stripe_accessors_gate_on_flag() {
    let dev: Arc<dyn crate::io::block_backend::BlockBackend> = Arc::new(StripeMock { stripe: 6 });
    // Flag off: the whole feature no-ops — stripe reported as 1, phase 0.
    let off = IoEngine::with_options(dev.clone(), false, RESERVED_BLOCKS, None, IoBackend::Syscall);
    assert_eq!(off.stripe_blocks(), 1);
    assert_eq!(off.stripe_phase(), 0);
    // Flag on: reports the backend stripe; phase = pba_offset % stripe. With
    // RESERVED_BLOCKS=8 and a 6-block stripe that is 2.
    let on = IoEngine::with_options(dev, false, RESERVED_BLOCKS, None, IoBackend::Syscall)
        .with_full_stripe_writes(true);
    assert_eq!(on.stripe_blocks(), 6);
    assert_eq!(on.stripe_phase(), (RESERVED_BLOCKS % 6) as u32);
    assert_eq!(on.stripe_phase(), 2);
}

#[test]
fn allocator_phase_composes_to_aligned_device_offset() {
    // The allocator alignment and the engine phase compose: a PBA the allocator
    // marks stripe-aligned maps to a stripe-aligned *device* offset through
    // pba_to_offset (the RESERVED_BLOCKS=8 phase, which % 6 != 0, is the trap).
    let dev: Arc<dyn crate::io::block_backend::BlockBackend> = Arc::new(StripeMock { stripe: 6 });
    let engine = IoEngine::with_options(dev, false, RESERVED_BLOCKS, None, IoBackend::Syscall)
        .with_full_stripe_writes(true);
    let alloc = crate::space::allocator::SpaceAllocator::new(1 << 30, 4);
    let stripe_bytes = engine.stripe_blocks() as u64 * BLOCK_SIZE as u64;
    for _ in 0..64 {
        let e = alloc
            .allocate_stripe_extent_for_lane(0, 6, engine.stripe_blocks(), engine.stripe_phase())
            .unwrap();
        let dev_off = (e.start.0 + RESERVED_BLOCKS) * BLOCK_SIZE as u64;
        assert_eq!(dev_off % stripe_bytes, 0, "pba {} unaligned", e.start.0);
    }
}
