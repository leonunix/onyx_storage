//! Post-commit worker — Phase 2.2 of the per-volume commit architecture.
//!
//! Each commit_worker hands off "deferrable" post-commit work
//! (mark_flushed, candidate cache insert, stale dedup repairs) to its
//! paired post_commit thread. The commit_worker can then immediately
//! pick up its next job, so its hot loop becomes:
//!
//!     L2P lock → meta build → metadb commit → enqueue PostCommitJob
//!
//! instead of the original:
//!
//!     L2P lock → meta build → metadb commit
//!     → candidate.insert_many → repair_stale → mark_flushed loop
//!     → advance_tail
//!
//! Background semantics: the buffer's in-memory index keeps the
//! pending entry visible until mark_flushed drains. Reads before the
//! post_commit thread runs hit the buffer; reads after hit metadb.
//! Both paths return the same data because metadb has already
//! published the new mapping. The only cost of lag is buffer ring
//! head retention, which is bounded by the post_commit_tx queue.

use super::*;

/// Bounded backlog per commit_worker → post_commit pairing. 128 jobs
/// of slack absorbs a commit_worker doing one commit every ~150us
/// while the post_commit thread does ~800us of mark_flushed +
/// candidate work, so the queue fills only if mark_flushed itself
/// stalls.
pub(in crate::buffer::flush) const POST_COMMIT_QUEUE_CAP: usize = 128;

/// Deferrable work emitted by a commit_worker after a successful
/// metadb commit. The owning commit_worker is single-threaded, and so
/// is the paired post_commit worker, so no synchronization is needed
/// beyond the channel boundary.
pub(in crate::buffer::flush) struct PostCommitJob {
    /// Originating shard. Used by `pool.advance_tail_for_shard` so
    /// the buffer ring can release blocks for the right shard.
    pub shard_idx: usize,
    /// (seq, lba_start, lba_count) ranges to mark flushed in the
    /// buffer pool. Each tuple is one mark_flushed() call.
    pub mark_ranges: Vec<(u64, Lba, u32)>,
    /// Fresh candidate dedup pairs to insert into the RAM cache. Empty
    /// when the commit didn't carry any miss-promote candidates.
    pub candidate_pairs: Vec<(ContentHash, BlockmapValue)>,
    /// Stale dedup_index repairs (forward entries pointing at PBAs
    /// that have since moved). Replayed via metadb tx.
    pub stale_repairs: Vec<(ContentHash, DedupEntry, DedupEntry)>,
}

impl PostCommitJob {
    pub(in crate::buffer::flush) fn empty(shard_idx: usize) -> Self {
        Self {
            shard_idx,
            mark_ranges: Vec::new(),
            candidate_pairs: Vec::new(),
            stale_repairs: Vec::new(),
        }
    }

    pub(in crate::buffer::flush) fn is_empty(&self) -> bool {
        self.mark_ranges.is_empty()
            && self.candidate_pairs.is_empty()
            && self.stale_repairs.is_empty()
    }
}

impl BufferFlusher {
    /// Drain post-commit jobs serially. One thread per commit_worker;
    /// pairing keeps mark_flushed traffic for any one buffer shard
    /// inside a single thread (matches the commit_worker's per-volume
    /// FIFO). The thread exits when the channel disconnects on engine
    /// shutdown.
    pub(in crate::buffer::flush) fn post_commit_loop(
        _worker_idx: usize,
        rx: &Receiver<PostCommitJob>,
        pool: &WriteBufferPool,
        meta: &MetaStore,
        candidate: &crate::dedup::CandidateCache,
        metrics: &EngineMetrics,
    ) {
        while let Ok(job) = rx.recv() {
            if !job.mark_ranges.is_empty() {
                let mark_start = Instant::now();
                for (seq, lba_start, lba_count) in &job.mark_ranges {
                    if let Err(e) = pool.mark_flushed(*seq, *lba_start, *lba_count) {
                        tracing::warn!(
                            seq,
                            error = %e,
                            "post_commit: failed to mark entry flushed"
                        );
                    }
                }
                Self::record_elapsed(&metrics.flush_writer_mark_flushed_ns, mark_start);
            }

            if !job.candidate_pairs.is_empty() {
                let cand_start = Instant::now();
                candidate.insert_many(&job.candidate_pairs);
                Self::record_elapsed(&metrics.flush_writer_meta_candidate_ns, cand_start);
            }

            if !job.stale_repairs.is_empty() {
                let repair_start = Instant::now();
                Self::repair_stale_dedup_index(meta, metrics, &job.stale_repairs, "post_commit");
                Self::record_elapsed(&metrics.flush_writer_meta_repair_ns, repair_start);
            }

            let _ = pool.advance_tail_for_shard(job.shard_idx);
        }
    }
}
