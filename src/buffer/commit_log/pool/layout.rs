use super::*;

impl WriteBufferPool {
    /// V2 data bytes: device - superblock.
    pub(super) fn total_data_bytes(device_size: u64) -> OnyxResult<u64> {
        device_size
            .checked_sub(COMMIT_LOG_SUPERBLOCK_SIZE)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// V3 data bytes: device - superblock - per-shard checkpoint blocks.

    pub(super) fn total_data_bytes_v3(device_size: u64, shard_count: usize) -> OnyxResult<u64> {
        let overhead = COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE;
        device_size
            .checked_sub(overhead)
            .ok_or_else(|| OnyxError::Config("persistent slot device too small".into()))
    }

    /// Offset where shard data areas begin in v3 layout.

    pub(super) fn v3_data_area_start(shard_count: usize) -> u64 {
        COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE
    }

    /// Read the on-disk shard count from the superblock. Returns None if the
    /// device has no valid superblock (first use).

    pub fn read_disk_shard_count(device: &dyn BlockBackend) -> OnyxResult<Option<usize>> {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut buf, 0)?;
        Ok(GlobalSuperblock::decode(&buf).map(|sb| sb.shard_count as usize))
    }

    pub(super) fn validate_shard_count(shard_count: usize) -> OnyxResult<()> {
        if shard_count == 0 || shard_count > MAX_SHARDS_ON_DISK {
            return Err(OnyxError::Config(format!(
                "persistent slot buffer supports 1..{} shards, got {}",
                MAX_SHARDS_ON_DISK, shard_count
            )));
        }
        Ok(())
    }

    /// Read shard checkpoint from disk. Returns None if invalid.

    pub(super) fn read_shard_checkpoint(
        device: &dyn BlockBackend,
        shard_idx: usize,
    ) -> OnyxResult<Option<ShardCheckpoint>> {
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        device.read_at(&mut buf, offset)?;
        Ok(ShardCheckpoint::decode(&buf))
    }

    /// Initialize checkpoint blocks to zero (used during v2→v3 migration).

    pub(super) fn init_checkpoint_blocks(
        device: &dyn BlockBackend,
        shard_count: usize,
    ) -> OnyxResult<()> {
        let empty = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq: 0,
            used_bytes: 0,
        };
        let encoded = empty.encode();
        for i in 0..shard_count {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + i as u64 * SHARD_CHECKPOINT_SIZE;
            device.write_at(&encoded, offset)?;
        }
        Ok(())
    }

    /// Check whether the buffer device with an old shard layout has zero
    /// unflushed entries, meaning it is safe to reinitialize with a different
    /// shard count (or migrate to v3).

    pub(super) fn check_old_layout_empty(
        device: &Arc<dyn BlockBackend>,
        sb: &GlobalSuperblock,
    ) -> OnyxResult<bool> {
        let old_shards = sb.shard_count as usize;
        let device_size = device.size();
        let total_data = if sb.is_v3() {
            Self::total_data_bytes_v3(device_size, old_shards)?
        } else {
            Self::total_data_bytes(device_size)?
        };
        let bytes_per_shard = (total_data / old_shards as u64) & !(BLOCK_SIZE as u64 - 1);
        let data_area_start = if sb.is_v3() {
            Self::v3_data_area_start(old_shards)
        } else {
            COMMIT_LOG_SUPERBLOCK_SIZE
        };

        let mut consumed = 0u64;
        for i in 0..old_shards {
            let shard_bytes = if i == old_shards - 1 {
                total_data - consumed
            } else {
                bytes_per_shard
            };
            let shard_offset = data_area_start + consumed;
            consumed += shard_bytes;

            let shard_dev = slice_backend(device.clone(), shard_offset, shard_bytes)?;
            let lba_index = DashMap::with_shard_amount(4);
            let latest_lba_seq = DashMap::with_shard_amount(4);
            let pending_lba_buckets = DashMap::with_shard_amount(4);
            let pending = DashMap::with_shard_amount(4);
            let pending_count = AtomicU64::new(0);
            BufferShard::rebuild_indices(
                shard_dev.as_ref(),
                shard_bytes,
                &lba_index,
                &latest_lba_seq,
                &pending_lba_buckets,
                &pending,
                &pending_count,
            )?;

            if !pending.is_empty() {
                tracing::warn!(
                    shard = i,
                    pending = pending.len(),
                    "buffer shard has unflushed entries — cannot reinit"
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn persist_superblock(&self, sync: bool) -> OnyxResult<()> {
        let sb = GlobalSuperblock {
            shard_count: self.shards.len() as u32,
            version: self.disk_version,
        };
        let bytes = sb.encode();
        self.root_device.write_at(&bytes, 0)?;
        if sync {
            Self::sync_device_impl(self.root_device.as_ref())?;
        }
        Ok(())
    }
}
