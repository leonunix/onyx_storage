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

    pub(in crate::buffer::commit_log) fn v3_data_area_start(shard_count: usize) -> u64 {
        COMMIT_LOG_SUPERBLOCK_SIZE + shard_count as u64 * SHARD_CHECKPOINT_SIZE
    }

    /// Read the on-disk shard count from the superblock. Returns None if the
    /// device has no valid superblock (first use).

    pub fn read_disk_shard_count(device: &dyn BlockBackend) -> OnyxResult<Option<usize>> {
        let mut buf = [0u8; COMMIT_LOG_SUPERBLOCK_SIZE as usize];
        device.read_at(&mut buf, 0)?;
        let superblock = GlobalSuperblock::decode(&buf);
        let primary_marker = LayoutMigrationMarker::decode(&buf);
        let backup_marker = Self::read_layout_migration_backup(device)?;

        // A backup whose target differs from the still-valid old superblock was
        // flushed before the primary handoff began. Resume that target instead
        // of reopening the old layout and forgetting its sequence floor. A
        // matching v3 superblock is already the final published state; there the
        // backup page is simply a valid empty checkpoint-format record awaiting
        // cleanup.
        let marker_in_progress = backup_marker.filter(|marker| {
            superblock.is_none_or(|sb| {
                !sb.is_v3() || sb.shard_count as usize != marker.shard_count as usize
            })
        });
        Ok(marker_in_progress
            .or(primary_marker)
            .map(|marker| marker.shard_count as usize)
            .or_else(|| superblock.map(|sb| sb.shard_count as usize)))
    }

    pub(in crate::buffer::commit_log) fn read_layout_migration_backup(
        device: &dyn BlockBackend,
    ) -> OnyxResult<Option<LayoutMigrationMarker>> {
        // This is the only location that is metadata in every v3 layout. Never
        // discover control records by scanning data pages: LV2 payload is fully
        // user-controlled and can reproduce any public magic/CRC format.
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        device.read_at(&mut buf, COMMIT_LOG_SUPERBLOCK_SIZE)?;
        Ok(LayoutMigrationMarker::decode_checkpoint(&buf))
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

    pub(in crate::buffer::commit_log) fn read_shard_checkpoint(
        device: &dyn BlockBackend,
        shard_idx: usize,
    ) -> OnyxResult<Option<ShardCheckpoint>> {
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + shard_idx as u64 * SHARD_CHECKPOINT_SIZE;
        let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
        device.read_at(&mut buf, offset)?;
        Ok(ShardCheckpoint::decode(&buf))
    }

    pub(in crate::buffer::commit_log) fn read_packed_checkpoint(
        device: &dyn BlockBackend,
        shard_count: usize,
    ) -> OnyxResult<PackedCheckpointLoad> {
        debug_assert!(shard_count >= PACKED_CHECKPOINT_SLOT_COUNT);
        let mut all_legacy = true;
        let mut newest: Option<PackedCheckpointTable> = None;
        for slot in 0..PACKED_CHECKPOINT_SLOT_COUNT {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + slot as u64 * SHARD_CHECKPOINT_SIZE;
            let mut buf = [0u8; SHARD_CHECKPOINT_SIZE as usize];
            device.read_at(&mut buf, offset)?;
            all_legacy &= ShardCheckpoint::decode(&buf).is_some();
            let Some(table) = PackedCheckpointTable::decode(&buf) else {
                continue;
            };
            if table.checkpoints.len() != shard_count {
                continue;
            }
            // A valid older slot is the last completed acknowledgement epoch
            // when the alternate slot tears: global sync writes the new page,
            // barriers it, and only then advances waiters. This recovery model
            // covers crash-during-newer-epoch, not latent media corruption after
            // a successfully acknowledged durability barrier.
            if newest
                .as_ref()
                .is_none_or(|current| table.generation > current.generation)
            {
                newest = Some(table);
            }
        }
        Ok(match newest {
            Some(table) => PackedCheckpointLoad::Packed(table),
            None if all_legacy => PackedCheckpointLoad::Legacy,
            None => PackedCheckpointLoad::Corrupt,
        })
    }

    #[cfg(test)]
    pub(in crate::buffer::commit_log) fn write_packed_checkpoint(
        device: &dyn BlockBackend,
        table: &PackedCheckpointTable,
    ) -> OnyxResult<()> {
        let encoded = table.encode();
        let mut aligned = AlignedBuf::new(SHARD_CHECKPOINT_SIZE as usize, false)?;
        aligned.as_mut_slice().copy_from_slice(&encoded);
        Self::write_packed_checkpoint_page(device, table.generation, aligned.as_slice())
    }

    pub(super) fn write_packed_checkpoint_page(
        device: &dyn BlockBackend,
        generation: u64,
        page: &[u8],
    ) -> OnyxResult<()> {
        debug_assert_eq!(page.len(), SHARD_CHECKPOINT_SIZE as usize);
        let slot = PackedCheckpointTable::slot_for_generation(generation);
        let offset = COMMIT_LOG_SUPERBLOCK_SIZE + slot as u64 * SHARD_CHECKPOINT_SIZE;
        device.write_at(page, offset)
    }

    /// Initialize checkpoint blocks to zero (used during v2→v3 migration).

    pub(in crate::buffer::commit_log) fn init_checkpoint_blocks(
        device: &dyn BlockBackend,
        shard_count: usize,
        max_seq: u64,
    ) -> OnyxResult<()> {
        let empty = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq,
            used_bytes: 0,
        };
        let encoded = empty.encode();
        for i in 0..shard_count {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + i as u64 * SHARD_CHECKPOINT_SIZE;
            device.write_at(&encoded, offset)?;
        }
        Ok(())
    }

    /// Replace a durably drained buffer with an empty v3 layout.
    ///
    /// The caller must first make every applied entry durable in the metadata
    /// manifest and verify that the physical ring is empty. Replace the old
    /// superblock with a durable migration marker before publishing new
    /// checkpoint pages so a crash can never expose a valid layout whose
    /// recovery boundary is missing. The marker carries `max_seq`, allowing a
    /// restart in either intermediate state to resume without reusing sequence
    /// numbers even when the pool is opened outside `OnyxEngine`.
    pub(crate) fn reinitialize_empty_layout(
        device: &dyn BlockBackend,
        shard_count: usize,
        max_seq: u64,
    ) -> OnyxResult<()> {
        Self::validate_shard_count(shard_count)?;
        if max_seq == u64::MAX {
            return Err(OnyxError::Config(
                "buffer sequence exhausted u64 range during shard migration".into(),
            ));
        }
        let total_data = Self::total_data_bytes_v3(device.size(), shard_count)?;
        if total_data < shard_count as u64 * BLOCK_SIZE as u64 {
            return Err(OnyxError::Config(format!(
                "persistent slot device too small for {} shards",
                shard_count
            )));
        }

        let marker = LayoutMigrationMarker::new(shard_count, max_seq);
        let existing_backup = Self::read_layout_migration_backup(device)?;
        if let Some(existing) = existing_backup {
            if existing.max_seq != max_seq {
                return Err(OnyxError::Config(format!(
                    "buffer layout-migration backup has sequence floor {}, expected {max_seq}",
                    existing.max_seq
                )));
            }
        }
        // Publish the redundant copy first at the fixed shard-0 checkpoint page.
        // If this first write tears, the still-valid old superblock must fail
        // closed when no other on-disk sequence evidence survives. Every later
        // failure can recover the target and max_seq from this page.
        device.write_at(&marker.encode_checkpoint(), COMMIT_LOG_SUPERBLOCK_SIZE)?;
        device.flush()?;

        device.write_at(&marker.encode(), 0)?;
        device.flush()?;

        let empty = ShardCheckpoint {
            head_offset: 0,
            tail_offset: 0,
            max_seq,
            used_bytes: 0,
        }
        .encode();
        for i in 1..shard_count {
            let offset = COMMIT_LOG_SUPERBLOCK_SIZE + i as u64 * SHARD_CHECKPOINT_SIZE;
            device.write_at(&empty, offset)?;
        }
        device.flush()?;

        let superblock = GlobalSuperblock::new(shard_count);
        device.write_at(&superblock.encode(), 0)?;
        // Keep the extension after the final superblock becomes durable. It is
        // also a valid empty shard-0 checkpoint, and the next normal checkpoint
        // update removes it. Rewriting it here would create a second torn-write
        // window with no remaining copy of max_seq.
        device.flush()
    }

    /// Check whether the buffer device with an old shard layout has zero
    /// unflushed entries, meaning it is safe to reinitialize with a different
    /// shard count (or migrate to v3). A clean result carries the old global
    /// sequence floor that the replacement layout must preserve.
    ///
    /// V3 checkpoints are authoritative recovery boundaries. Use the same
    /// guided scan as normal open so records below a durable empty checkpoint
    /// are treated as stale history, while contiguous post-checkpoint appends
    /// are still detected. Falling back to a full-device scan here would
    /// resurrect every reclaimed record whose on-disk header remains
    /// `flushed=false` and make a clean shard-count migration impossible.

    pub(super) fn check_old_layout_empty(
        device: &Arc<dyn BlockBackend>,
        sb: &GlobalSuperblock,
    ) -> OnyxResult<Option<u64>> {
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
        let use_packed_checkpoint = sb.is_v3()
            && old_shards >= PACKED_CHECKPOINT_SLOT_COUNT
            && device.uring_target().is_none();
        let packed_load = if use_packed_checkpoint {
            Some(Self::read_packed_checkpoint(device.as_ref(), old_shards)?)
        } else {
            None
        };

        let mut consumed = 0u64;
        let mut global_max_seq = 0u64;
        let mut missing_checkpoint = false;
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
            let checkpoint = if sb.is_v3() {
                match packed_load.as_ref() {
                    Some(PackedCheckpointLoad::Packed(table)) => Some(table.checkpoints[i]),
                    Some(PackedCheckpointLoad::Corrupt) => None,
                    Some(PackedCheckpointLoad::Legacy) | None => {
                        Self::read_shard_checkpoint(device.as_ref(), i)?
                    }
                }
            } else {
                None
            };
            missing_checkpoint |= sb.is_v3() && checkpoint.is_none();
            if checkpoint.is_some_and(|checkpoint| checkpoint.used_bytes != 0) {
                tracing::info!(
                    shard = i,
                    used_bytes = checkpoint.map_or(0, |checkpoint| checkpoint.used_bytes),
                    "buffer checkpoint is non-empty — old layout must be opened and drained"
                );
                return Ok(None);
            }
            let scan = if let Some(checkpoint) = checkpoint.as_ref() {
                BufferShard::rebuild_indices_guided(
                    shard_dev.as_ref(),
                    shard_bytes,
                    checkpoint,
                    &lba_index,
                    &latest_lba_seq,
                    &pending_lba_buckets,
                    &pending,
                    &pending_count,
                )?
            } else {
                BufferShard::rebuild_indices(
                    shard_dev.as_ref(),
                    shard_bytes,
                    &lba_index,
                    &latest_lba_seq,
                    &pending_lba_buckets,
                    &pending,
                    &pending_count,
                )?
            };
            global_max_seq = global_max_seq
                .max(scan.max_seq)
                .max(checkpoint.map_or(0, |checkpoint| checkpoint.max_seq));

            if !pending.is_empty() {
                tracing::warn!(
                    shard = i,
                    pending = pending.len(),
                    "buffer shard has unflushed entries — cannot reinit"
                );
                return Ok(None);
            }
        }
        if sb.is_v3() && missing_checkpoint {
            return Err(OnyxError::Config(
                "buffer checkpoint is corrupt; refusing automatic layout reinitialization \
                 without an authoritative on-disk sequence floor"
                    .into(),
            ));
        }
        Ok(Some(global_max_seq))
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
