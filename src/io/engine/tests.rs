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

struct BatchMock {
    write_many_calls: std::sync::atomic::AtomicUsize,
    write_many_ops: std::sync::atomic::AtomicUsize,
    write_many_max_ops: std::sync::atomic::AtomicUsize,
}

impl crate::io::block_backend::BlockBackend for BatchMock {
    fn read_at(&self, _buf: &mut [u8], _off: u64) -> OnyxResult<()> {
        Ok(())
    }

    fn write_at(&self, _buf: &[u8], _off: u64) -> OnyxResult<()> {
        panic!("chunklet-style batch must not fall back to write_at")
    }

    fn write_many_at(&self, ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        self.write_many_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.write_many_ops
            .fetch_add(ops.len(), std::sync::atomic::Ordering::Relaxed);
        self.write_many_max_ops
            .fetch_max(ops.len(), std::sync::atomic::Ordering::Relaxed);
        assert!(ops.iter().all(|(_, payload)| payload.len() == 4096));
        Ok(())
    }

    fn flush(&self) -> OnyxResult<()> {
        Ok(())
    }

    fn size(&self) -> u64 {
        1 << 30
    }
}

#[test]
fn chunklet_batch_uses_one_write_many_slab_and_records_depth() {
    let backend = Arc::new(BatchMock {
        write_many_calls: std::sync::atomic::AtomicUsize::new(0),
        write_many_ops: std::sync::atomic::AtomicUsize::new(0),
        write_many_max_ops: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = IoEngine::new_chunklet(backend.clone(), false, metrics.clone());
    let payloads = [vec![0x11; 4096], vec![0x22; 3000], vec![0x33; 4096]];
    let ops = payloads
        .iter()
        .enumerate()
        .map(|(idx, payload)| LvOp::Write {
            pba: Pba(idx as u64),
            payload,
        })
        .collect();

    let results = engine.submit_batch(ops, false).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(backend.write_many_calls.load(Ordering::Relaxed), 1);
    assert_eq!(backend.write_many_ops.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.lv3_write_batch_calls.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.lv3_write_batch_ops.load(Ordering::Relaxed), 3);
    assert_eq!(
        metrics.lv3_write_batch_bytes.load(Ordering::Relaxed),
        3 * 4096
    );
    assert!(metrics.lv3_write_batch_ns.load(Ordering::Relaxed) > 0);
    assert_eq!(metrics.lv3_write_batch_inflight.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics.lv3_write_batch_inflight_max.load(Ordering::Relaxed),
        1
    );
    assert_eq!(metrics.lv3_write_slab_allocs.load(Ordering::Relaxed), 1);
}

#[test]
fn chunklet_owned_batch_writes_aligned_buffers_without_repacking() {
    let backend = Arc::new(BatchMock {
        write_many_calls: std::sync::atomic::AtomicUsize::new(0),
        write_many_ops: std::sync::atomic::AtomicUsize::new(0),
        write_many_max_ops: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = IoEngine::new_chunklet(backend.clone(), false, metrics.clone());
    let mut writes = Vec::new();
    for idx in 0..3 {
        let mut buffer = engine.allocate_owned_write_buffer(4096).unwrap();
        buffer.as_mut_slice().fill((idx + 1) as u8);
        writes.push(OwnedLvWrite {
            pba: Pba(idx),
            payload_len: if idx == 1 { 3000 } else { 4096 },
            buffer,
        });
    }

    let results = engine
        .submit_owned_write_batch_on(None, writes, false)
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(backend.write_many_calls.load(Ordering::Relaxed), 1);
    assert_eq!(backend.write_many_ops.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.lv3_write_slab_allocs.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.lv3_write_slab_bytes.load(Ordering::Relaxed), 12288);
}

/// A small batch cannot reach `target_bytes`, but with every executor idle it
/// must NOT pay the coalesce window for that: waiting for company is only free
/// when the device is saturated. The producer's blocked wait must still be split
/// across pickup / window / exec_queue rather than all landing on device time.
#[test]
#[serial_test::serial]
fn lv3_batch_skips_the_window_when_an_executor_is_idle() {
    set_lv3_batch_tuning(0, 0, 0, true);
    let _restore = ResetLv3Tuning;
    let backend = Arc::new(BatchMock {
        write_many_calls: std::sync::atomic::AtomicUsize::new(0),
        write_many_ops: std::sync::atomic::AtomicUsize::new(0),
        write_many_max_ops: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = IoEngine::new_chunklet(backend.clone(), false, metrics.clone());
    let mut writes = Vec::new();
    for idx in 0..3 {
        let mut buffer = engine.allocate_owned_write_buffer(4096).unwrap();
        buffer.as_mut_slice().fill((idx + 1) as u8);
        writes.push(OwnedLvWrite {
            pba: Pba(idx),
            payload_len: 4096,
            buffer,
        });
    }
    engine
        .submit_owned_write_batch_on(None, writes, false)
        .unwrap();

    assert_eq!(metrics.lv3_batch_wait_calls.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.lv3_batch_requests.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics.lv3_batch_bytes_at_dispatch.load(Ordering::Relaxed),
        3 * 4096
    );
    // 12 KiB is far below the 4 MiB target, and no other request is in flight,
    // so the aggregator must have skipped the window rather than timed out on it.
    assert_eq!(
        metrics.lv3_batch_idle_dispatches.load(Ordering::Relaxed),
        1,
        "an idle executor must take the batch immediately"
    );
    assert_eq!(metrics.lv3_batch_window_timeouts.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.lv3_batch_target_hits.load(Ordering::Relaxed), 0);
    // `window` is still attributed, but it is now only the dispatch bookkeeping
    // rather than a wait — it must be nowhere near the coalesce window itself.
    let window = metrics.lv3_batch_window_ns.load(Ordering::Relaxed);
    let wait = metrics.lv3_batch_wait_ns.load(Ordering::Relaxed);
    assert!(
        window < lv3_batch_coalesce().as_nanos() as u64 / 4,
        "skipped window {window} ns must be far below the {:?} coalesce window",
        lv3_batch_coalesce()
    );
    assert!(
        wait >= window,
        "producer wait {wait} must cover the coalesce window {window}"
    );
    // The return trip (executor reply -> producer wake) and the executor's
    // pre-call slice assembly are the two legs that used to fall out of the
    // ledger as an unattributed residual. Both must now be inside `wait`.
    let reply = metrics.lv3_batch_reply_ns.load(Ordering::Relaxed);
    let prep = metrics.lv3_batch_exec_prep_ns.load(Ordering::Relaxed);
    assert!(reply > 0, "reply leg must be attributed");
    assert!(
        wait >= window + reply,
        "producer wait {wait} must cover window {window} + reply {reply}"
    );
    let device = metrics.lv3_write_batch_ns.load(Ordering::Relaxed);
    assert!(
        wait >= prep + device,
        "producer wait {wait} must cover exec_prep {prep} + device {device}"
    );
}

/// A backend whose `write_many_at` parks until the test releases it, so a test
/// can hold every executor busy and observe the aggregator's other branch.
struct GatedBatchMock {
    released: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicUsize,
}

impl crate::io::block_backend::BlockBackend for GatedBatchMock {
    fn read_at(&self, _buf: &mut [u8], _off: u64) -> OnyxResult<()> {
        Ok(())
    }

    fn write_at(&self, _buf: &[u8], _off: u64) -> OnyxResult<()> {
        panic!("chunklet-style batch must not fall back to write_at")
    }

    fn write_many_at(&self, _ops: &[(u64, &[u8])]) -> OnyxResult<()> {
        self.entered.fetch_add(1, Ordering::Relaxed);
        while !self.released.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        Ok(())
    }

    fn flush(&self) -> OnyxResult<()> {
        Ok(())
    }

    fn size(&self) -> u64 {
        1 << 30
    }
}

/// The other half of [`lv3_batch_skips_the_window_when_an_executor_is_idle`]:
/// once every executor is occupied the batch is going to queue regardless, so
/// the aggregator must go back to spending the coalesce window on growing it.
/// Pinned to one executor because that makes saturation deterministic.
#[test]
#[serial_test::serial]
fn lv3_batch_coalesces_again_once_every_executor_is_busy() {
    set_lv3_batch_tuning(2_000, 0, 1, true);
    let restore = ResetLv3Tuning;
    let backend = Arc::new(GatedBatchMock {
        released: std::sync::atomic::AtomicBool::new(false),
        entered: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = Arc::new(IoEngine::new_chunklet(
        backend.clone(),
        false,
        metrics.clone(),
    ));

    let submit = |engine: Arc<IoEngine>| {
        std::thread::spawn(move || {
            let mut buffer = engine.allocate_owned_write_buffer(4096).unwrap();
            buffer.as_mut_slice().fill(0x5a);
            engine
                .submit_owned_write_batch_on(
                    None,
                    vec![OwnedLvWrite {
                        pba: Pba(0),
                        payload_len: 4096,
                        buffer,
                    }],
                    false,
                )
                .unwrap();
        })
    };

    // First request: nothing outstanding, so it dispatches immediately and then
    // parks inside the mock, occupying the only executor.
    let first = submit(engine.clone());
    wait_for(|| backend.entered.load(Ordering::Relaxed) == 1, "device entry");
    assert_eq!(metrics.lv3_batch_idle_dispatches.load(Ordering::Relaxed), 1);

    // Second request: the executor is busy, so this one must sit out the window
    // and leave on the timeout.
    let second = submit(engine.clone());
    wait_for(
        || metrics.lv3_batch_window_timeouts.load(Ordering::Relaxed) >= 1,
        "timeout dispatch under saturation",
    );

    backend.released.store(true, Ordering::Release);
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(metrics.lv3_batch_idle_dispatches.load(Ordering::Relaxed), 1);
    drop(restore);
}

/// Puts the global LV3 tuning back on its compiled defaults even if the test
/// panics, so a `#[serial]` failure cannot leak into the next test.
struct ResetLv3Tuning;

impl Drop for ResetLv3Tuning {
    fn drop(&mut self) {
        set_lv3_batch_tuning(0, 0, 0, false);
    }
}

fn wait_for(mut predicate: impl FnMut() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if predicate() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn chunklet_owned_batch_splits_oversized_request_across_executors() {
    const WRITE_COUNT: u64 = CHUNKLET_BATCH_TARGET_BYTES as u64 / BLOCK_SIZE as u64 + 1;
    let backend = Arc::new(BatchMock {
        write_many_calls: std::sync::atomic::AtomicUsize::new(0),
        write_many_ops: std::sync::atomic::AtomicUsize::new(0),
        write_many_max_ops: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = IoEngine::new_chunklet(backend.clone(), false, metrics.clone());
    let mut writes = Vec::new();
    for idx in 0..WRITE_COUNT {
        let mut buffer = engine
            .allocate_owned_write_buffer(BLOCK_SIZE as usize)
            .unwrap();
        buffer.as_mut_slice().fill(idx as u8);
        writes.push(OwnedLvWrite {
            pba: Pba(idx),
            payload_len: BLOCK_SIZE as usize,
            buffer,
        });
    }

    let results = engine
        .submit_owned_write_batch_on(None, writes, false)
        .unwrap();

    assert_eq!(results.len(), WRITE_COUNT as usize);
    assert_eq!(
        backend.write_many_ops.load(Ordering::Relaxed),
        WRITE_COUNT as usize
    );
    assert_eq!(backend.write_many_calls.load(Ordering::Relaxed), 2);
    assert_eq!(
        backend.write_many_max_ops.load(Ordering::Relaxed),
        (WRITE_COUNT - 1) as usize
    );
    assert_eq!(metrics.lv3_write_batch_calls.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.lv3_write_batch_inflight.load(Ordering::Relaxed), 0);
}

#[test]
fn chunklet_batcher_combines_concurrent_callers() {
    let backend = Arc::new(BatchMock {
        write_many_calls: std::sync::atomic::AtomicUsize::new(0),
        write_many_ops: std::sync::atomic::AtomicUsize::new(0),
        write_many_max_ops: std::sync::atomic::AtomicUsize::new(0),
    });
    let metrics = Arc::new(EngineMetrics::default());
    let engine = Arc::new(IoEngine::new_chunklet(
        backend.clone(),
        false,
        metrics.clone(),
    ));
    let start = Arc::new(std::sync::Barrier::new(17));
    let mut handles = Vec::new();
    for caller in 0..16u64 {
        let engine = engine.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            let payloads = (0..8).map(|_| vec![caller as u8; 4096]).collect::<Vec<_>>();
            let ops = payloads
                .iter()
                .enumerate()
                .map(|(idx, payload)| LvOp::Write {
                    pba: Pba(caller * 8 + idx as u64),
                    payload,
                })
                .collect();
            start.wait();
            engine.submit_batch(ops, false).unwrap();
        }));
    }
    start.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(backend.write_many_ops.load(Ordering::Relaxed), 128);
    assert!(
        backend.write_many_calls.load(Ordering::Relaxed) < 16,
        "concurrent producer calls should be combined"
    );
    assert!(
        backend.write_many_max_ops.load(Ordering::Relaxed) > 8,
        "at least one device batch should contain multiple callers"
    );
    assert_eq!(
        metrics.lv3_write_batch_calls.load(Ordering::Relaxed) as usize,
        backend.write_many_calls.load(Ordering::Relaxed)
    );
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
    let off = IoEngine::with_options(
        dev.clone(),
        false,
        RESERVED_BLOCKS,
        None,
        IoBackend::Syscall,
    );
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
