use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::buffer::pipeline::{coalesce, CoalesceUnit, CompressedUnit};
use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::config::FlushConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::meta::schema::BlockmapValue;
use crate::meta::store::MetaStore;
use crate::space::allocator::SpaceAllocator;
use crate::types::{CompressionAlgo, Lba, Pba, VolumeId, BLOCK_SIZE};

/// 3-stage flusher pipeline:
///   Stage 1 (coalescer): drain buffer → sort → coalesce contiguous LBAs
///   Stage 2 (N compress workers): parallel compression
///   Stage 3 (writer): allocate PBAs → write LV3 → update metadata
pub struct BufferFlusher {
    running: Arc<AtomicBool>,
    coalesce_handle: Option<JoinHandle<()>>,
    compress_handles: Vec<JoinHandle<()>>,
    writer_handle: Option<JoinHandle<()>>,
}

impl BufferFlusher {
    pub fn start(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        compression: CompressionAlgo,
        config: &FlushConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let workers = config.compress_workers.max(1);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;

        // Channels between stages
        let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(workers * 4);
        let (write_tx, write_rx) = bounded::<CompressedUnit>(workers * 4);

        // Stage 1: Coalescer
        let running_c = running.clone();
        let pool_c = pool.clone();
        let coalesce_handle = thread::Builder::new()
            .name("flusher-coalesce".into())
            .spawn(move || {
                Self::coalesce_loop(&pool_c, &compress_tx, &running_c, max_raw, max_lbas);
            })
            .expect("failed to spawn coalescer thread");

        // Stage 2: Compress workers
        let mut compress_handles = Vec::with_capacity(workers);
        for i in 0..workers {
            let rx = compress_rx.clone();
            let tx = write_tx.clone();
            let running_w = running.clone();
            let h = thread::Builder::new()
                .name(format!("flusher-compress-{}", i))
                .spawn(move || {
                    Self::compress_loop(&rx, &tx, &running_w, compression);
                })
                .expect("failed to spawn compress worker");
            compress_handles.push(h);
        }
        // Drop the extra sender/receiver clones so channels close properly
        drop(compress_rx);
        drop(write_tx);

        // Stage 3: Writer
        let running_w = running.clone();
        let pool_w = pool.clone();
        let writer_handle = thread::Builder::new()
            .name("flusher-writer".into())
            .spawn(move || {
                Self::writer_loop(
                    &write_rx, &pool_w, &meta, &allocator, &io_engine, &running_w,
                );
            })
            .expect("failed to spawn writer thread");

        Self {
            running,
            coalesce_handle: Some(coalesce_handle),
            compress_handles,
            writer_handle: Some(writer_handle),
        }
    }

    fn coalesce_loop(
        pool: &WriteBufferPool,
        tx: &Sender<CoalesceUnit>,
        running: &AtomicBool,
        max_raw: usize,
        max_lbas: u32,
    ) {
        while running.load(Ordering::Relaxed) {
            match pool.recover() {
                Ok(entries) if !entries.is_empty() => {
                    let units = coalesce(&entries, max_raw, max_lbas);
                    for unit in units {
                        if tx.send(unit).is_err() {
                            return; // channel closed
                        }
                    }
                }
                Ok(_) => thread::sleep(Duration::from_millis(10)),
                Err(e) => {
                    tracing::error!(error = %e, "coalescer: recover failed");
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        algo: CompressionAlgo,
    ) {
        let compressor = create_compressor(algo);

        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let original_size = unit.raw_data.len();
                    let max_out = compressor.max_compressed_size(original_size);
                    let mut compressed_buf = vec![0u8; max_out];

                    let (compression_byte, compressed_data) =
                        match compressor.compress(&unit.raw_data, &mut compressed_buf) {
                            Some(size) => (algo.to_u8(), compressed_buf[..size].to_vec()),
                            None => (0u8, unit.raw_data.clone()), // incompressible
                        };

                    let crc32 = crc32fast::hash(&compressed_data);

                    let cu = CompressedUnit {
                        vol_id: unit.vol_id,
                        start_lba: unit.start_lba,
                        lba_count: unit.lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        entry_seqs: unit.entry_seqs,
                    };

                    if tx.send(cu).is_err() {
                        return;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn writer_loop(
        rx: &Receiver<CompressedUnit>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
        running: &AtomicBool,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    if let Err(e) = Self::write_unit(&unit, pool, meta, allocator, io_engine) {
                        tracing::error!(
                            vol = unit.vol_id,
                            start_lba = unit.start_lba.0,
                            lba_count = unit.lba_count,
                            error = %e,
                            "writer: failed to flush unit"
                        );
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn write_unit(
        unit: &CompressedUnit,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        allocator: &SpaceAllocator,
        io_engine: &IoEngine,
    ) -> OnyxResult<()> {
        let bs = BLOCK_SIZE as usize;
        let blocks_needed = (unit.compressed_data.len() + bs - 1) / bs;

        // Allocate contiguous PBAs
        let pba = if blocks_needed == 1 {
            allocator.allocate_one()?
        } else {
            let extent = allocator.allocate_extent(blocks_needed as u32)?;
            if (extent.count as usize) < blocks_needed {
                // Couldn't get enough contiguous blocks — free what we got and error
                allocator.free_extent(extent)?;
                return Err(crate::error::OnyxError::SpaceExhausted);
            }
            extent.start
        };

        // Write compressed data to LV3
        io_engine.write_blocks(pba, &unit.compressed_data)?;

        // Collect old mappings for all LBAs in this unit
        let vol_id = VolumeId(unit.vol_id.clone());
        let mut old_pba_meta: HashMap<Pba, (u32, u32)> = HashMap::new();

        let mut batch_values = Vec::with_capacity(unit.lba_count as usize);
        for i in 0..unit.lba_count {
            let lba = Lba(unit.start_lba.0 + i as u64);
            if let Some(old) = meta.get_mapping(&vol_id, lba)? {
                let old_blocks = old.unit_compressed_size.div_ceil(BLOCK_SIZE) as u32;
                let entry = old_pba_meta.entry(old.pba).or_insert((0, old_blocks));
                entry.0 += 1;
                entry.1 = entry.1.max(old_blocks);
            }
            batch_values.push((
                lba,
                BlockmapValue {
                    pba,
                    compression: unit.compression,
                    unit_compressed_size: unit.compressed_data.len() as u32,
                    unit_original_size: unit.original_size,
                    unit_lba_count: unit.lba_count as u16,
                    offset_in_unit: i as u16,
                    crc32: unit.crc32,
                },
            ));
        }

        let old_pba_decrements: HashMap<Pba, u32> = old_pba_meta
            .iter()
            .map(|(old_pba, (decrement, _))| (*old_pba, *decrement))
            .collect();

        // Atomic batch: write all blockmap entries + set new refcount + decrement old refcounts
        meta.atomic_batch_write(
            &vol_id,
            &batch_values,
            unit.lba_count as u32,
            &old_pba_decrements,
        )?;

        // Free old PBAs that reached refcount 0
        // (atomic_batch_write handles the refcount math; we need to reclaim space)
        for (old_pba, (_, old_blocks)) in &old_pba_meta {
            let remaining = meta.get_refcount(*old_pba)?;
            if remaining == 0 {
                if *old_blocks == 1 {
                    allocator.free_one(*old_pba)?;
                } else {
                    allocator
                        .free_extent(crate::space::extent::Extent::new(*old_pba, *old_blocks))?;
                }
            }
        }

        // Mark buffer entries as flushed
        for seq in &unit.entry_seqs {
            if let Err(e) = pool.mark_flushed(*seq) {
                tracing::warn!(seq, error = %e, "failed to mark entry flushed");
            }
        }
        pool.advance_tail()?;

        tracing::debug!(
            vol = unit.vol_id,
            start_lba = unit.start_lba.0,
            lba_count = unit.lba_count,
            pba = pba.0,
            compressed = unit.compressed_data.len(),
            original = unit.original_size,
            "flushed compression unit"
        );

        Ok(())
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.coalesce_handle.take() {
            let _ = h.join();
        }
        for h in self.compress_handles.drain(..) {
            let _ = h.join();
        }
        if let Some(h) = self.writer_handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}
