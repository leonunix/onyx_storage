use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use onyx_storage::config::{
    BufferConfig, EngineConfig, FlushConfig, MetaConfig, OnyxConfig, StorageConfig, UblkConfig,
};
use onyx_storage::dedup::config::DedupConfig;
use onyx_storage::engine::OnyxEngine;
use onyx_storage::gc::config::GcConfig;
use onyx_storage::meta::schema::{
    decode_blockmap_key, decode_blockmap_value, BlockmapValue, ContentHash, DedupEntry,
};
use onyx_storage::types::{CompressionAlgo, Lba, Pba, BLOCK_SIZE};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::{tempdir, NamedTempFile, TempDir};

#[derive(Debug, Clone)]
pub struct HarnessOptions {
    pub data_bytes: u64,
    pub buffer_bytes: u64,
    pub flush: FlushConfig,
    pub gc: GcConfig,
    pub dedup: DedupConfig,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            data_bytes: 4096 * 32768,
            buffer_bytes: 4096 + 1024 * 4096,
            flush: FlushConfig {
                compress_workers: 2,
                coalesce_max_raw_bytes: 131072,
                coalesce_max_lbas: 32,
            },
            gc: GcConfig {
                enabled: false,
                ..Default::default()
            },
            dedup: DedupConfig::default(),
        }
    }
}

pub struct EngineHarness {
    pub config: OnyxConfig,
    engine: Option<OnyxEngine>,
    _meta_dir: TempDir,
    _buffer_file: NamedTempFile,
    _data_file: NamedTempFile,
}

impl EngineHarness {
    pub fn new(options: HarnessOptions) -> Self {
        let meta_dir = tempdir().unwrap();
        let buffer_file = NamedTempFile::new().unwrap();
        let data_file = NamedTempFile::new().unwrap();
        buffer_file.as_file().set_len(options.buffer_bytes).unwrap();
        data_file.as_file().set_len(options.data_bytes).unwrap();

        let config = OnyxConfig {
            meta: MetaConfig {
                rocksdb_path: Some(meta_dir.path().to_path_buf()),
                block_cache_mb: 32,
                wal_dir: None,
            },
            storage: StorageConfig {
                data_device: Some(data_file.path().to_path_buf()),
                block_size: BLOCK_SIZE,
                use_hugepages: false,
                default_compression: CompressionAlgo::Lz4,
            },
            buffer: BufferConfig {
                device: Some(buffer_file.path().to_path_buf()),
                capacity_mb: ((options.buffer_bytes / 1024 / 1024).max(1)) as usize,
                flush_watermark_pct: 80,
                group_commit_wait_us: 250,
                shards: 1,
                max_memory_mb: 0,
            },
            ublk: UblkConfig::default(),
            flush: options.flush,
            engine: EngineConfig {
                zone_count: 4,
                zone_size_blocks: 128,
            },
            gc: options.gc,
            dedup: options.dedup,
            service: Default::default(),
            ha: Default::default(),
        };

        let engine = OnyxEngine::open(&config).unwrap();
        Self {
            config,
            engine: Some(engine),
            _meta_dir: meta_dir,
            _buffer_file: buffer_file,
            _data_file: data_file,
        }
    }

    pub fn engine(&self) -> &OnyxEngine {
        self.engine.as_ref().unwrap()
    }

    pub fn shutdown(&mut self) {
        if let Some(engine) = self.engine.take() {
            engine.shutdown().unwrap();
            drop(engine);
        }
    }

    pub fn reopen(&mut self) {
        self.shutdown();
        self.engine = Some(OnyxEngine::open(&self.config).unwrap());
    }

    pub fn meta_path(&self) -> PathBuf {
        self.config.meta.rocksdb_path.clone().unwrap()
    }

    pub fn buffer_path(&self) -> PathBuf {
        self.config.buffer.device.clone().unwrap()
    }

    pub fn data_path(&self) -> PathBuf {
        self.config.storage.data_device.clone().unwrap()
    }

    pub fn wait_for_drain(&self, timeout: Duration) {
        let Some(pool) = self.engine().buffer_pool() else {
            return;
        };
        let start = Instant::now();
        while pool.pending_count() > 0 {
            assert!(
                start.elapsed() <= timeout,
                "buffer drain timeout after {:?} (pending={})",
                timeout,
                pool.pending_count()
            );
            thread::sleep(Duration::from_millis(20));
        }
        thread::sleep(Duration::from_millis(80));
    }

    pub fn assert_no_internal_errors(&self) {
        let m = self.engine().metrics_snapshot();
        assert_eq!(m.flush_errors, 0, "flush_errors must stay at 0");
        assert_eq!(m.gc_errors, 0, "gc_errors must stay at 0");
        assert_eq!(m.read_crc_errors, 0, "read_crc_errors must stay at 0");
        assert_eq!(
            m.read_decompress_errors, 0,
            "read_decompress_errors must stay at 0"
        );
        assert_eq!(
            m.dedup_rescan_errors, 0,
            "dedup_rescan_errors must stay at 0"
        );
    }

    pub fn assert_matches_model(&self, model: &ShadowModel) {
        let listed: BTreeMap<_, _> = self
            .engine()
            .list_volumes()
            .unwrap()
            .into_iter()
            .map(|v| (v.id.0.clone(), v))
            .collect();
        let expected_names: HashSet<_> = model.volumes.keys().cloned().collect();
        let actual_names: HashSet<_> = listed.keys().cloned().collect();
        assert_eq!(actual_names, expected_names, "volume set mismatch");

        for (name, volume) in &model.volumes {
            let meta = listed.get(name).expect("volume missing from engine");
            assert_eq!(meta.size_bytes, volume.size_bytes);
            assert_eq!(meta.compression, volume.compression);
            let handle = self.engine().open_volume(name).unwrap();
            let actual = handle.read(0, volume.data.len()).unwrap();
            assert_eq!(
                actual, volume.data,
                "logical image mismatch for volume {name}"
            );
        }
    }

    pub fn audit_consistency(&self) {
        let volumes: BTreeMap<_, _> = self
            .engine()
            .list_volumes()
            .unwrap()
            .into_iter()
            .map(|v| (v.id.0.clone(), v))
            .collect();

        if let Some(pool) = self.engine().buffer_pool() {
            let pending = pool.recover().unwrap();
            let unique_pending = pending
                .iter()
                .map(|entry| entry.seq)
                .collect::<HashSet<_>>();
            assert_eq!(
                unique_pending.len() as u64,
                pool.pending_count(),
                "pending_count must match recover() seq cardinality"
            );
            for entry in pending {
                let volume = volumes.get(&entry.vol_id).unwrap_or_else(|| {
                    panic!("pending entry references deleted volume {}", entry.vol_id)
                });
                let end_lba = entry.start_lba.0 + entry.lba_count as u64;
                assert!(
                    end_lba * BLOCK_SIZE as u64 <= volume.size_bytes,
                    "pending entry out of bounds for volume {}",
                    entry.vol_id
                );
                assert_eq!(
                    entry.payload.len(),
                    entry.lba_count as usize * BLOCK_SIZE as usize,
                    "pending payload length must match lba_count"
                );
            }
        }

        let mut refcount_expected: HashMap<Pba, u32> = HashMap::new();
        let mut live_phys_refs: HashMap<PhysicalKey, Vec<(String, Lba)>> = HashMap::new();
        let mut unit_offsets: HashMap<UnitKey, HashSet<u16>> = HashMap::new();

        self.engine()
            .meta()
            .scan_all_blockmap_entries(&mut |key, value| {
                let (vol_id, lba) = decode_blockmap_key(key).expect("valid blockmap key");
                let mapping = decode_blockmap_value(value).expect("valid blockmap value");
                let volume = volumes
                    .get(&vol_id)
                    .unwrap_or_else(|| panic!("blockmap entry references deleted volume {vol_id}"));

                assert!(
                    (lba.0 + 1) * BLOCK_SIZE as u64 <= volume.size_bytes,
                    "blockmap LBA {lba:?} out of bounds for volume {vol_id}"
                );
                assert!(mapping.unit_lba_count > 0, "unit_lba_count must be > 0");
                assert!(
                    mapping.offset_in_unit < mapping.unit_lba_count,
                    "offset_in_unit must be within unit_lba_count"
                );
                assert!(
                    mapping.unit_compressed_size > 0,
                    "unit_compressed_size must be > 0"
                );
                assert_eq!(
                    mapping.unit_original_size % BLOCK_SIZE,
                    0,
                    "unit_original_size must be block aligned"
                );
                assert_eq!(
                    mapping.unit_original_size / BLOCK_SIZE,
                    mapping.unit_lba_count as u32,
                    "unit_original_size must match unit_lba_count"
                );
                if mapping.unit_compressed_size < BLOCK_SIZE {
                    assert!(
                        mapping.slot_offset as u32 + mapping.unit_compressed_size <= BLOCK_SIZE,
                        "packed fragment must fit inside one 4KB slot"
                    );
                } else {
                    assert_eq!(
                        mapping.slot_offset, 0,
                        "multi-slot units must start at slot offset 0"
                    );
                }

                *refcount_expected.entry(mapping.pba).or_insert(0) += 1;
                live_phys_refs
                    .entry(PhysicalKey::from_blockmap(mapping))
                    .or_default()
                    .push((vol_id.clone(), lba));
                unit_offsets
                    .entry(UnitKey::from_blockmap(mapping))
                    .or_default()
                    .insert(mapping.offset_in_unit);
            })
            .unwrap();

        for (unit, offsets) in &unit_offsets {
            assert!(
                offsets.len() <= unit.unit_lba_count as usize,
                "live offsets cannot exceed unit_lba_count for {:?}",
                unit
            );
            assert!(
                offsets.iter().all(|offset| *offset < unit.unit_lba_count),
                "every offset must stay within unit_lba_count for {:?}",
                unit
            );
        }

        if let Some(allocator) = self.engine().allocator() {
            let allocated = self.engine().meta().iter_allocated_blocks().unwrap();
            let allocated_set = allocated.iter().copied().collect::<HashSet<_>>();
            assert_eq!(
                allocator.allocated_block_count(),
                allocated_set.len() as u64,
                "allocator allocated count must match metadata"
            );
            assert_eq!(
                allocator.free_block_count() + allocator.allocated_block_count(),
                allocator.total_block_count() - onyx_storage::types::RESERVED_BLOCKS,
                "allocator free + allocated must equal usable blocks (total - reserved)"
            );
        }

        let dedup_entries = self.engine().meta().iter_dedup_entries().unwrap();
        let reverse_entries = self.engine().meta().iter_dedup_reverse_entries().unwrap();
        let reverse_set = reverse_entries.iter().copied().collect::<HashSet<_>>();
        let dedup_map: HashMap<ContentHash, DedupEntry> = dedup_entries.iter().copied().collect();

        for (hash, entry) in &dedup_entries {
            assert!(
                reverse_set.contains(&(entry.pba, *hash)),
                "dedup reverse entry missing for hash {:?}",
                hash
            );
            let refs = live_phys_refs
                .get(&PhysicalKey::from_dedup(*entry))
                .unwrap_or_else(|| {
                    panic!(
                        "dedup entry points to non-live physical location: {:?}",
                        entry
                    )
                });
            let (vol_id, lba) = &refs[0];
            let handle = self.engine().open_volume(vol_id).unwrap();
            let block = handle
                .read(lba.0 * BLOCK_SIZE as u64, BLOCK_SIZE as usize)
                .unwrap();
            let actual_hash = blake3::hash(&block);
            assert_eq!(
                actual_hash.as_bytes().as_slice(),
                hash.as_slice(),
                "dedup index hash does not match logical block content"
            );
        }

        for (pba, hash) in reverse_entries {
            let entry = dedup_map
                .get(&hash)
                .unwrap_or_else(|| panic!("reverse dedup entry missing forward hash {:?}", hash));
            assert_eq!(entry.pba, pba, "reverse entry points to stale PBA");
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowVolume {
    pub size_bytes: u64,
    pub compression: CompressionAlgo,
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ShadowModel {
    pub volumes: BTreeMap<String, ShadowVolume>,
}

impl ShadowModel {
    pub fn create_volume(&mut self, name: &str, size_bytes: u64, compression: CompressionAlgo) {
        self.volumes.insert(
            name.to_string(),
            ShadowVolume {
                size_bytes,
                compression,
                data: vec![0u8; size_bytes as usize],
            },
        );
    }

    pub fn delete_volume(&mut self, name: &str) {
        self.volumes.remove(name);
    }

    pub fn write(&mut self, name: &str, offset: u64, payload: &[u8]) {
        let volume = self.volumes.get_mut(name).unwrap();
        let start = offset as usize;
        let end = start + payload.len();
        volume.data[start..end].copy_from_slice(payload);
    }

    pub fn read(&self, name: &str, offset: u64, len: usize) -> Vec<u8> {
        let volume = self.volumes.get(name).unwrap();
        volume.data[offset as usize..offset as usize + len].to_vec()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UnitKey {
    pba: Pba,
    slot_offset: u16,
    compression: u8,
    unit_compressed_size: u32,
    unit_original_size: u32,
    unit_lba_count: u16,
    crc32: u32,
    flags: u8,
}

impl UnitKey {
    fn from_blockmap(v: BlockmapValue) -> Self {
        Self {
            pba: v.pba,
            slot_offset: v.slot_offset,
            compression: v.compression,
            unit_compressed_size: v.unit_compressed_size,
            unit_original_size: v.unit_original_size,
            unit_lba_count: v.unit_lba_count,
            crc32: v.crc32,
            flags: v.flags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PhysicalKey {
    pba: Pba,
    slot_offset: u16,
    compression: u8,
    unit_compressed_size: u32,
    unit_original_size: u32,
    unit_lba_count: u16,
    offset_in_unit: u16,
    crc32: u32,
}

impl PhysicalKey {
    fn from_blockmap(v: BlockmapValue) -> Self {
        Self {
            pba: v.pba,
            slot_offset: v.slot_offset,
            compression: v.compression,
            unit_compressed_size: v.unit_compressed_size,
            unit_original_size: v.unit_original_size,
            unit_lba_count: v.unit_lba_count,
            offset_in_unit: v.offset_in_unit,
            crc32: v.crc32,
        }
    }

    fn from_dedup(v: DedupEntry) -> Self {
        Self {
            pba: v.pba,
            slot_offset: v.slot_offset,
            compression: v.compression,
            unit_compressed_size: v.unit_compressed_size,
            unit_original_size: v.unit_original_size,
            unit_lba_count: v.unit_lba_count,
            offset_in_unit: v.offset_in_unit,
            crc32: v.crc32,
        }
    }
}

pub fn patterned_payload(rng: &mut StdRng, tag: u64, len: usize) -> Vec<u8> {
    match rng.gen_range(0..4) {
        0 => vec![0u8; len],
        1 => vec![(tag as u8).wrapping_mul(31).wrapping_add(7); len],
        2 => {
            let base = ((tag * 17) & 0xFF) as u8;
            let mut out = vec![0u8; len];
            for (idx, byte) in out.iter_mut().enumerate() {
                *byte = base.wrapping_add((idx % 64) as u8);
            }
            out
        }
        _ => {
            let mut out = vec![0u8; len];
            rng.fill(out.as_mut_slice());
            out
        }
    }
}

pub fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}
