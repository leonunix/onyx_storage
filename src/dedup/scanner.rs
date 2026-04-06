use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::dedup::config::DedupConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::*;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{VolumeId, BLOCK_SIZE};

/// Background dedup scanner: re-processes blocks that skipped dedup under pressure.
pub struct DedupScanner {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl DedupScanner {
    pub fn start(
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        allocator: Arc<SpaceAllocator>,
        lifecycle: Arc<VolumeLifecycleManager>,
        buffer_pool: Arc<WriteBufferPool>,
        config: DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            Arc::new(EngineMetrics::default()),
            meta,
            io_engine,
            allocator,
            lifecycle,
            buffer_pool,
            config,
        )
    }

    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        allocator: Arc<SpaceAllocator>,
        lifecycle: Arc<VolumeLifecycleManager>,
        buffer_pool: Arc<WriteBufferPool>,
        config: DedupConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = thread::Builder::new()
            .name("dedup-scanner".into())
            .spawn(move || {
                Self::scan_loop(
                    &metrics,
                    &meta,
                    &io_engine,
                    &allocator,
                    &lifecycle,
                    &buffer_pool,
                    &config,
                    &running_clone,
                );
            })
            .expect("failed to spawn dedup scanner thread");

        Self {
            running,
            handle: Some(handle),
        }
    }

    fn scan_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        buffer_pool: &WriteBufferPool,
        config: &DedupConfig,
        running: &AtomicBool,
    ) {
        while running.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(config.rescan_interval_ms));
            if !running.load(Ordering::Relaxed) {
                break;
            }

            metrics.dedup_rescan_cycles.fetch_add(1, Ordering::Relaxed);

            // Skip if buffer is under pressure
            if buffer_pool.fill_percentage() > config.buffer_skip_threshold_pct as u8 {
                metrics
                    .dedup_rescan_skipped_cycles
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            match Self::rescan_skipped_blocks(
                meta,
                io_engine,
                allocator,
                lifecycle,
                config.max_rescan_per_cycle,
            ) {
                Ok(stats) => {
                    metrics
                        .dedup_rescan_blocks
                        .fetch_add(stats.rescanned as u64, Ordering::Relaxed);
                    metrics
                        .dedup_rescan_hits
                        .fetch_add(stats.hits as u64, Ordering::Relaxed);
                    metrics
                        .dedup_rescan_misses
                        .fetch_add(stats.misses as u64, Ordering::Relaxed);
                    if stats.rescanned > 0 {
                        tracing::info!(
                            rescanned = stats.rescanned,
                            hits = stats.hits,
                            misses = stats.misses,
                            "dedup scanner: re-processed skipped blocks"
                        );
                    }
                }
                Err(e) => {
                    metrics.dedup_rescan_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "dedup scanner: rescan failed");
                }
            }
        }
    }

    fn rescan_skipped_blocks(
        meta: &MetaStore,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        max_per_cycle: usize,
    ) -> OnyxResult<RescanStats> {
        let skipped = meta.scan_dedup_skipped(max_per_cycle)?;
        let mut stats = RescanStats::default();

        for (vol_id_str, lba, bv) in &skipped {
            let vol_id = VolumeId(vol_id_str.clone());

            // Hold lifecycle read lock
            let result = lifecycle.with_read_lock(vol_id_str, || -> OnyxResult<bool> {
                // Re-read the mapping to ensure it's still the same
                let current = meta.get_mapping(&vol_id, *lba)?;
                let current = match current {
                    Some(c) if c.pba == bv.pba && c.flags & FLAG_DEDUP_SKIPPED != 0 => c,
                    _ => return Ok(false), // Changed or flag already cleared
                };

                // Read the physical data
                let bs = BLOCK_SIZE as usize;
                let read_size = if current.unit_compressed_size < BLOCK_SIZE {
                    bs // packed slot: always read full 4KB
                } else {
                    ((current.unit_compressed_size as usize + bs - 1) / bs) * bs
                };
                let raw = io_engine.read_blocks(current.pba, read_size)?;

                // Extract the compressed fragment
                let start = current.slot_offset as usize;
                let end = start + current.unit_compressed_size as usize;
                if end > raw.len() {
                    return Ok(false);
                }
                let compressed = &raw[start..end];

                // CRC verification — detect silent corruption before trusting data
                let actual_crc = crc32fast::hash(compressed);
                if actual_crc != current.crc32 {
                    tracing::warn!(
                        pba = current.pba.0,
                        slot_offset = current.slot_offset,
                        expected_crc = current.crc32,
                        actual_crc,
                        "dedup scanner: CRC mismatch, skipping corrupt block"
                    );
                    return Ok(false);
                }

                // Decompress
                let algo = crate::types::CompressionAlgo::from_u8(current.compression)
                    .unwrap_or(crate::types::CompressionAlgo::None);
                let compressor = create_compressor(algo);
                let mut decompressed = vec![0u8; current.unit_original_size as usize];
                if current.compression != 0 {
                    compressor.decompress(
                        compressed,
                        &mut decompressed,
                        current.unit_original_size as usize,
                    )?;
                } else {
                    decompressed[..compressed.len()].copy_from_slice(compressed);
                }

                // Extract this LBA's 4KB block
                let offset = current.offset_in_unit as usize * bs;
                if offset + bs > decompressed.len() {
                    return Ok(false);
                }
                let block = &decompressed[offset..offset + bs];

                // Hash the block
                let hash: ContentHash = Sha256::digest(block).into();

                // Look up dedup index
                match meta.get_dedup_entry(&hash)? {
                    Some(existing) if meta.get_refcount(existing.pba)? > 0 => {
                        // Dedup hit! Remap this LBA to existing PBA
                        let new_bv = BlockmapValue {
                            flags: 0, // Clear DEDUP_SKIPPED
                            ..existing.to_blockmap_value()
                        };
                        meta.atomic_dedup_hit(&vol_id, *lba, &new_bv, Some(current.pba))?;

                        // Check if old PBA refcount dropped to 0
                        let remaining = meta.get_refcount(current.pba)?;
                        if remaining == 0 {
                            // Clean up dedup entries for old PBA
                            meta.cleanup_dedup_for_pba_standalone(current.pba)?;
                            let blocks = current.unit_compressed_size.div_ceil(BLOCK_SIZE);
                            if blocks <= 1 {
                                allocator.free_one(current.pba)?;
                            } else {
                                allocator.free_extent(Extent::new(current.pba, blocks))?;
                            }
                        }
                        stats.hits += 1;
                    }
                    _ => {
                        // Dedup miss: register in index and clear flag
                        let entry = DedupEntry {
                            pba: current.pba,
                            slot_offset: current.slot_offset,
                            compression: current.compression,
                            unit_compressed_size: current.unit_compressed_size,
                            unit_original_size: current.unit_original_size,
                            unit_lba_count: current.unit_lba_count,
                            offset_in_unit: current.offset_in_unit,
                            crc32: current.crc32,
                        };
                        meta.put_dedup_entries(&[(hash, entry)])?;
                        meta.update_blockmap_flags(&vol_id, *lba, 0)?;
                        stats.misses += 1;
                    }
                }

                Ok(true)
            })?;

            if result {
                stats.rescanned += 1;
            }
        }

        Ok(stats)
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for DedupScanner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RescanStats {
    rescanned: usize,
    hits: usize,
    misses: usize,
}
