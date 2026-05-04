//! Batched LV3 verify-on-hit primitive for promote-on-verified-hit dedup.
//!
//! When a dedup hit fires (either from the persistent `dedup_index`
//! or from the in-memory [`CandidateCache`]) we cannot trust the hash
//! alone: the schema swap moved keys to `xxh3_64` (8 B), trading
//! cryptographic strength for memory and disk savings, and birthday
//! math at 1 PiB / 4 K (~ 2.7 × 10¹¹ unique blocks) puts the
//! expected collision count at ≈ 1900 pairs across the entire
//! dataset. Without verify, those collisions become silent data
//! corruption.
//!
//! The verifier therefore reads the *original* fragment back from
//! LV3, byte-compares it against the new write's source block, and
//! only signals a real hit on bytewise match. Mismatches are treated
//! as misses by the caller — the new write goes through the normal
//! fresh-write path and is registered as a new candidate.
//!
//! # IO design constraint (project memory `dedup_verify_io_pattern`)
//!
//! The verify path **must** submit reads through the engine's batched
//! io_uring [`ReadPool`] at high queue depth, never via serial
//! `read_blocks` per hit. Per-hit synchronous reads bottleneck the
//! whole dedup hot path long before NVMe queue capacity is reached.
//! This module therefore exposes a single batch entrypoint
//! ([`batched_verify`]) that fans every input into the pool, then
//! drains all replies before returning.
//!
//! # Failure handling
//!
//! Any per-target error (read failure, channel drop, decode failure)
//! is reported as `false` (not an error). The dedup pipeline treats
//! `false` as "this candidate is not a real duplicate", routing the
//! new write through the fresh-write path. Verify never fails the
//! whole batch — a single dead PBA must not break dedup for siblings
//! in the same batch.

use crate::error::OnyxResult;
use crate::io::read_pool::ReadPool;
use crate::meta::schema::BlockmapValue;
use crate::types::BLOCK_SIZE;

/// One verify request: read the 4 KiB block referenced by `mapping`
/// and compare the decoded payload against `expected`.
pub struct VerifyTarget<'a> {
    /// Where the original fragment lives on LV3 (PBA, slot offset,
    /// compression, unit sizes, offset_in_unit, CRC). The
    /// [`ReadPool`] uses this to read + decompress + slice out the
    /// requested 4 KiB LBA.
    pub mapping: BlockmapValue,
    /// The new write's source 4 KiB block; verify succeeds when this
    /// matches the bytes read from LV3 byte-for-byte.
    pub expected: &'a [u8],
}

impl<'a> VerifyTarget<'a> {
    pub fn new(mapping: BlockmapValue, expected: &'a [u8]) -> Self {
        Self { mapping, expected }
    }
}

/// Batched LV3 verify. Submits every target asynchronously into the
/// `ReadPool` (which folds incoming requests into io_uring batches at
/// the worker level — see [`crate::io::read_pool`] for the SQE batching
/// design), then drains every reply in submission order.
///
/// Returns one `bool` per input target: `true` when the LV3 read
/// succeeded *and* the decoded bytes match `expected` exactly,
/// `false` for any kind of mismatch or per-target error.
///
/// `targets.len()` does not need to be capped here — the pool's
/// per-worker `BATCH_MAX` already serialises submission into
/// io_uring-sized chunks, and recv is sequential so the in-flight
/// queue depth is bounded by the pool's request channel capacity.
/// Callers that want hard caps can chunk before invoking.
pub fn batched_verify(
    read_pool: &ReadPool,
    targets: &[VerifyTarget<'_>],
) -> OnyxResult<Vec<bool>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    // Submit every read up front so the read-pool workers can fold
    // them into io_uring batches. Hold the receivers in submission
    // order so per-target results are matched up correctly during
    // drain.
    let mut receivers = Vec::with_capacity(targets.len());
    for target in targets {
        match read_pool.submit_read_async(target.mapping) {
            Ok(rx) => receivers.push(Some(rx)),
            // A submission failure (channel closed, etc.) collapses
            // this target to a false-match. We still record a
            // placeholder so the result vector lines up with the
            // input.
            Err(_) => receivers.push(None),
        }
    }

    let mut results = Vec::with_capacity(targets.len());
    for (target, rx_opt) in targets.iter().zip(receivers) {
        let matches = match rx_opt {
            None => false,
            Some(rx) => match rx.recv() {
                Ok(Ok(decoded)) => verify_bytes(&decoded, target.expected),
                Ok(Err(_)) | Err(_) => false,
            },
        };
        results.push(matches);
    }
    Ok(results)
}

/// Compare a freshly-decoded LV3 payload against the caller's
/// expected block. Both slices must be exactly `BLOCK_SIZE`; any
/// shorter result is a corruption and reported as a mismatch (false)
/// rather than a panic so verify never crashes the dedup pipeline.
#[inline]
fn verify_bytes(decoded: &[u8], expected: &[u8]) -> bool {
    let block_size = BLOCK_SIZE as usize;
    decoded.len() == block_size && expected.len() == block_size && decoded == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::device::RawDevice;
    use crate::io::engine::IoEngine;
    use crate::metrics::EngineMetrics;
    use crate::types::Pba;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    const DEV_SIZE: u64 = 4 * 1024 * 1024;

    /// Build a fresh data device, an `IoEngine` for writes, and a
    /// `ReadPool` for verifies — both using the same backing file.
    /// `RawDevice` does not impl Clone, so we open the path twice.
    fn fresh_engine_and_pool() -> (NamedTempFile, IoEngine, ReadPool) {
        let tmp = NamedTempFile::new().unwrap();
        tmp.as_file().set_len(DEV_SIZE).unwrap();

        let dev = RawDevice::open_or_create(tmp.path(), DEV_SIZE).unwrap();
        let metrics = Arc::new(EngineMetrics::default());
        let engine = IoEngine::new_raw(dev, false);

        let pool_dev = RawDevice::open_or_create(tmp.path(), DEV_SIZE).unwrap();
        let pool = ReadPool::start(2, 32, &pool_dev, 0, BLOCK_SIZE, false, metrics).unwrap();
        (tmp, engine, pool)
    }

    fn passthrough_blockmap(pba: u64, crc: u32) -> BlockmapValue {
        BlockmapValue {
            pba: Pba(pba),
            compression: 0, // None = passthrough
            unit_compressed_size: BLOCK_SIZE,
            unit_original_size: BLOCK_SIZE,
            unit_lba_count: 1,
            offset_in_unit: 0,
            crc32: crc,
            slot_offset: 0,
            flags: 0,
        }
    }

    #[test]
    fn verify_empty_input_returns_empty_output() {
        let (_tmp, _engine, pool) = fresh_engine_and_pool();
        let result = batched_verify(&pool, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn verify_match_reports_true() {
        let (_tmp, engine, pool) = fresh_engine_and_pool();
        let block = vec![0xC3u8; BLOCK_SIZE as usize];
        engine.write_blocks(Pba(2), &block).unwrap();
        let crc = crc32fast::hash(&block);

        let target = VerifyTarget::new(passthrough_blockmap(2, crc), &block);
        let results = batched_verify(&pool, &[target]).unwrap();
        assert_eq!(results, vec![true]);
    }

    #[test]
    fn verify_mismatch_reports_false() {
        let (_tmp, engine, pool) = fresh_engine_and_pool();
        let stored = vec![0xC3u8; BLOCK_SIZE as usize];
        engine.write_blocks(Pba(3), &stored).unwrap();
        let crc = crc32fast::hash(&stored);

        // Caller's "expected" disagrees with what's on LV3.
        let attempted = vec![0xA5u8; BLOCK_SIZE as usize];
        let target = VerifyTarget::new(passthrough_blockmap(3, crc), &attempted);
        let results = batched_verify(&pool, &[target]).unwrap();
        assert_eq!(results, vec![false]);
    }

    #[test]
    fn verify_batch_preserves_order_with_mixed_results() {
        let (_tmp, engine, pool) = fresh_engine_and_pool();
        // Write four distinct blocks at PBAs 4..8.
        let blocks: Vec<Vec<u8>> = (0..4u8)
            .map(|i| vec![0x10 + i; BLOCK_SIZE as usize])
            .collect();
        for (i, b) in blocks.iter().enumerate() {
            engine.write_blocks(Pba(4 + i as u64), b).unwrap();
        }
        let crcs: Vec<u32> = blocks.iter().map(|b| crc32fast::hash(b)).collect();

        // Build targets where targets 0 and 2 should match, 1 and 3
        // should mismatch (we hand the wrong "expected" buffer).
        let wrong = vec![0xFFu8; BLOCK_SIZE as usize];
        let targets = vec![
            VerifyTarget::new(passthrough_blockmap(4, crcs[0]), &blocks[0]),
            VerifyTarget::new(passthrough_blockmap(5, crcs[1]), &wrong),
            VerifyTarget::new(passthrough_blockmap(6, crcs[2]), &blocks[2]),
            VerifyTarget::new(passthrough_blockmap(7, crcs[3]), &wrong),
        ];
        let results = batched_verify(&pool, &targets).unwrap();
        assert_eq!(results, vec![true, false, true, false]);
    }

    #[test]
    fn verify_batch_does_not_panic_on_unwritten_pba() {
        let (_tmp, _engine, pool) = fresh_engine_and_pool();
        // PBA 16 was never written; CRC won't match.
        let block = vec![0u8; BLOCK_SIZE as usize];
        let target = VerifyTarget::new(passthrough_blockmap(16, 12345), &block);
        let results = batched_verify(&pool, &[target]).unwrap();
        // Per-target IO/decode failure collapses to false; the batch
        // does not return an error.
        assert_eq!(results, vec![false]);
    }
}
