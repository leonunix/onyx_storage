use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::buffer::pool::WriteBufferPool;
use crate::compress::codec::create_compressor;
use crate::dedup::config::DedupConfig;
use crate::error::OnyxResult;
use crate::io::engine::IoEngine;
use crate::lifecycle::VolumeLifecycleManager;
use crate::meta::schema::*;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::packer::packer::HoleMap;
use crate::space::allocator::SpaceAllocator;
use crate::space::extent::Extent;
use crate::types::{VolumeId, BLOCK_SIZE};

/// Background dedup scanner: re-processes blocks that skipped dedup under pressure.
pub struct DedupScanner {
    running: Arc<AtomicBool>,
    config: Arc<ArcSwap<DedupConfig>>,
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
            crate::packer::packer::new_hole_map(),
            config,
        )
    }

    // TEMP(soak-debug/root-cause-fix): scanner now shares the writer's hole map
    // so freeing a packed-slot PBA also purges stale holes. Keep this behavior,
    // but once the bug is fully understood we can re-evaluate whether any extra
    // logging around this path should be removed.
    pub fn start_with_metrics(
        metrics: Arc<EngineMetrics>,
        meta: Arc<MetaStore>,
        io_engine: Arc<IoEngine>,
        allocator: Arc<SpaceAllocator>,
        lifecycle: Arc<VolumeLifecycleManager>,
        buffer_pool: Arc<WriteBufferPool>,
        hole_map: HoleMap,
        config: DedupConfig,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let config = Arc::new(ArcSwap::from_pointee(config));
        let config_clone = config.clone();

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
                    &hole_map,
                    &config_clone,
                    &running_clone,
                );
            })
            .expect("failed to spawn dedup scanner thread");

        Self {
            running,
            config,
            handle: Some(handle),
        }
    }

    /// Hot-reload dedup scanner configuration.
    pub fn update_config(&self, new_config: DedupConfig) {
        tracing::info!("dedup scanner: config updated");
        self.config.store(Arc::new(new_config));
    }

    fn scan_loop(
        metrics: &EngineMetrics,
        meta: &MetaStore,
        io_engine: &IoEngine,
        allocator: &SpaceAllocator,
        lifecycle: &VolumeLifecycleManager,
        buffer_pool: &WriteBufferPool,
        hole_map: &HoleMap,
        config: &ArcSwap<DedupConfig>,
        running: &AtomicBool,
    ) {
        while running.load(Ordering::Relaxed) {
            let cfg = config.load();
            thread::sleep(Duration::from_millis(cfg.rescan_interval_ms));
            if !running.load(Ordering::Relaxed) {
                break;
            }

            metrics.dedup_rescan_cycles.fetch_add(1, Ordering::Relaxed);

            // Skip if buffer is under pressure
            if buffer_pool.fill_percentage() > cfg.buffer_skip_threshold_pct as u8 {
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
                hole_map,
                cfg.max_rescan_per_cycle,
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
        hole_map: &HoleMap,
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
                    Some(c)
                        if c.pba == bv.pba
                            && c.slot_offset == bv.slot_offset
                            && c.unit_compressed_size == bv.unit_compressed_size
                            && c.unit_original_size == bv.unit_original_size
                            && c.unit_lba_count == bv.unit_lba_count
                            && c.offset_in_unit == bv.offset_in_unit
                            && c.compression == bv.compression
                            && c.crc32 == bv.crc32
                            && c.flags & FLAG_DEDUP_SKIPPED != 0 =>
                    {
                        c
                    }
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
                let hash: ContentHash = *blake3::hash(block).as_bytes();

                // Look up dedup index
                match meta.get_dedup_entry(&hash)? {
                    Some(existing) if meta.dedup_entry_is_live(&existing)? => {
                        // Dedup hit! Remap this LBA to existing PBA
                        let new_bv = BlockmapValue {
                            flags: 0, // Clear DEDUP_SKIPPED
                            ..existing.to_blockmap_value()
                        };
                        // atomic_dedup_hit re-reads old mapping inside the lock
                        let decremented = meta.atomic_dedup_hit(&vol_id, *lba, &new_bv)?;

                        // Free old PBA if refcount dropped to 0
                        if let Some((old_pba, old_blocks)) = decremented {
                            let current_refcount = meta.get_refcount(old_pba)?;
                            let remaining = if current_refcount == 0 {
                                let reconciled = meta.reconcile_refcount_for_pba(old_pba)?;
                                if reconciled != 0 {
                                    tracing::error!(
                                        pba = old_pba.0,
                                        reconciled_refcount = reconciled,
                                        context = "dedup_scanner_cleanup",
                                        "detected refcount drift while dedup scanner prepared to free a PBA"
                                    );
                                }
                                reconciled
                            } else {
                                current_refcount
                            };
                            if remaining == 0 {
                                meta.cleanup_dedup_for_pba_standalone(old_pba)?;
                                if old_blocks <= 1 {
                                    crate::packer::packer::remove_holes_for_pba(hole_map, old_pba);
                                    allocator.free_one(old_pba)?;
                                } else {
                                    crate::packer::packer::remove_holes_for_extent(
                                        hole_map,
                                        old_pba,
                                        old_blocks,
                                    );
                                    allocator.free_extent(Extent::new(old_pba, old_blocks))?;
                                }
                            }
                        }
                        stats.hits += 1;
                    }
                    Some(_) => {
                        meta.delete_dedup_index(&hash)?;
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
