use super::*;

fn write_window_pressure_thresholds(config: &FlushConfig) -> (u8, u8) {
    let physical = if config.buffer_write_window_physical_pressure_pct > 0 {
        config.buffer_write_window_physical_pressure_pct.min(100)
    } else if config.buffer_write_window_pressure_pct > 0 {
        config.buffer_write_window_pressure_pct.min(100)
    } else {
        80
    };
    let payload = if config.buffer_write_window_payload_pressure_pct > 0 {
        config.buffer_write_window_payload_pressure_pct.min(100)
    } else if config.buffer_write_window_physical_pressure_pct == 0
        && config.buffer_write_window_pressure_pct > 0
    {
        config.buffer_write_window_pressure_pct.min(100)
    } else {
        80
    };
    (physical, payload)
}

impl BufferFlusher {
    pub fn start(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        config: &FlushConfig,
        dedup_config: &DedupConfig,
    ) -> Self {
        Self::start_with_metrics(
            pool,
            meta,
            lifecycle,
            allocator,
            io_engine,
            None,
            config,
            dedup_config,
            Arc::new(EngineMetrics::default()),
        )
    }

    /// `read_pool` is the LV3 read pool used for dedup verify-on-hit.
    /// Pass `None` to run the dedup pipeline in trust-hash mode
    /// (xxh3_64 collisions of ~1.5e-8 may produce occasional false
    /// dedups); production deployments should always set
    /// `read_pool_workers > 0`.
    pub fn start_with_metrics(
        pool: Arc<WriteBufferPool>,
        meta: Arc<MetaStore>,
        lifecycle: Arc<VolumeLifecycleManager>,
        allocator: Arc<SpaceAllocator>,
        io_engine: Arc<IoEngine>,
        read_pool: Option<Arc<crate::io::read_pool::ReadPool>>,
        config: &FlushConfig,
        dedup_config: &DedupConfig,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        // Build a candidate cache sized from the dedup config. The
        // shard count tracks the metadb dedup_shards routing so that a
        // candidate hit and the eventual promote commit always land in
        // the same metadb shard, preserving the inline-dedup commit
        // fast path. Per-shard capacity defaults to
        // CandidateCache::DEFAULT_PER_SHARD_CAPACITY when the dedup
        // config does not pin a value. CandidateCache is itself an
        // Arc<Inner> wrapper — `.clone()` is cheap and shares the
        // same backing storage across every flusher thread that
        // captures a copy.
        let candidate = crate::dedup::CandidateCache::new(
            dedup_config
                .candidate_shards
                .unwrap_or(8)
                .next_power_of_two(),
            dedup_config
                .candidate_per_shard_capacity
                .unwrap_or(crate::dedup::candidate::DEFAULT_PER_SHARD_CAPACITY),
        );
        // Single PBA lifecycle layer shared by the flusher's cleanup path and,
        // via `pba_lifecycle()`, by the engine's lineage drain / GC reclaim /
        // dedup scanner. One instance ⇒ one retire-retry queue + one
        // `pba_reclaim_stuck` gauge.
        let pba_lifecycle = crate::space::pba_lifecycle::PbaLifecycle::new(
            allocator.clone(),
            candidate.clone(),
            metrics.clone(),
        );
        let running = Arc::new(AtomicBool::new(true));
        let in_flight = Arc::new(FlusherInFlightTracker::default());
        let lane_count = pool.shard_count().max(1);
        let compress_workers =
            Self::per_lane_worker_count(config.compress_workers.max(1), lane_count);
        let max_raw = config.coalesce_max_raw_bytes;
        let max_lbas = config.coalesce_max_lbas;
        let min_compression_savings_pct = config.min_compression_savings_pct.min(100);
        let skip_fully_superseded = config.skip_fully_superseded;
        let buffer_write_window = Duration::from_millis(config.buffer_write_window_ms);
        let (buffer_write_window_pressure_pct, buffer_write_window_payload_pressure_pct) =
            write_window_pressure_thresholds(config);
        let flush_admission_qos = Arc::new(FlushAdmissionQos::new(
            FlushAdmissionQosConfig::from_flush(config),
            pool.clone(),
            metrics.clone(),
        ));
        let packed_meta_batch_max_lbas = if config.packed_meta_batch_max_lbas == 0 {
            DEFAULT_PACKED_META_BATCH_LBA_LIMIT
        } else {
            config.packed_meta_batch_max_lbas
        };
        let commit_workers_per_volume = config
            .commit_workers_per_volume
            .max(1)
            .min(writer::NUM_COMMIT_WORKERS);
        let writer_read_active_batch_size = config
            .writer_read_active_batch_size
            .max(1)
            .min(Self::WRITER_BATCH_SIZE);
        let commit_target_lbas_per_tx = config.commit_target_lbas_per_tx.max(1);
        let commit_coalesce_lba_budget = config.commit_coalesce_lba_budget;
        let commit_retain_tail = config.commit_retain_tail;
        let commit_coalesce_timeout = Duration::from_micros(config.commit_coalesce_timeout_us);
        let packed_commit_try_drain_lba_budget = config.packed_commit_try_drain_lba_budget;
        // Collapse the onyx flag + depth knob to a single effective cap.
        // Flag off → cap=1 (sync pacing via
        // deque); flag on → cap=configured depth (4 by default).
        let commit_worker_pipeline_depth = if config.commit_worker_deferred_outcomes {
            config.commit_worker_pipeline_depth.max(1)
        } else {
            1
        };
        let dedup_enabled = dedup_config.enabled;
        let dedup_workers = Self::per_lane_worker_count(dedup_config.workers.max(1), lane_count);
        let dedup_skip_threshold = dedup_config.buffer_skip_threshold_pct;
        let dedup_pending_skip_threshold = dedup_config.pending_skip_threshold_entries;
        let mut lanes = Vec::with_capacity(lane_count);

        // Per-shard `done_tx` / `cleanup_tx` channels are created
        // below in the lane loop; we collect clones here so the
        // commit workers can route by `CommitJob.shard_idx`. Pre-size
        // the storage so the lane loop can `push` into stable
        // indices.
        let mut lane_done_txs: Vec<Sender<Vec<u64>>> = Vec::with_capacity(lane_count);
        let mut lane_cleanup_txs: Vec<Sender<CleanupBatch>> = Vec::with_capacity(lane_count);

        // Raw MPMC producer queue followed by a single aggregator and an
        // executor queue of already-formed transactions. Directly sharing the
        // raw receiver between executors made them race for individual jobs
        // and fragmented a deep backlog into tiny transactions.
        let commit_executor_count = commit_workers_per_volume;
        let commit_executor_load = Arc::new(writer::CommitExecutorLoad::new(commit_executor_count));
        let (commit_tx, commit_rx) = bounded::<writer::CommitJob>(writer::COMMIT_WORKER_QUEUE_CAP);
        let (commit_batch_tx, commit_batch_rx) = bounded::<writer::CommitBatch>(
            writer::commit_executor_queue_capacity(commit_executor_count),
        );
        let mut commit_worker_txs: Vec<Sender<writer::CommitJob>> =
            Vec::with_capacity(commit_executor_count);
        let mut commit_worker_rxs: Vec<Receiver<writer::CommitBatch>> =
            Vec::with_capacity(commit_executor_count);
        for _ in 0..commit_executor_count {
            commit_worker_txs.push(commit_tx.clone());
            commit_worker_rxs.push(commit_batch_rx.clone());
        }
        drop(commit_tx);
        drop(commit_batch_rx);

        // Post-commit pairing. One channel per commit_worker so
        // mark_flushed traffic for any one volume stays serialised
        // (matches the commit_worker's per-volume FIFO).
        let mut post_commit_txs: Vec<Sender<writer::PostCommitJob>> =
            Vec::with_capacity(commit_executor_count);
        let mut post_commit_rxs: Vec<Receiver<writer::PostCommitJob>> =
            Vec::with_capacity(commit_executor_count);
        for _ in 0..commit_executor_count {
            let (tx, rx) = bounded::<writer::PostCommitJob>(writer::POST_COMMIT_QUEUE_CAP);
            post_commit_txs.push(tx);
            post_commit_rxs.push(rx);
        }

        for shard_idx in 0..lane_count {
            // Inter-stage channel sizes — sized to keep the writer's
            // per-cycle drain (Self::WRITER_BATCH_SIZE) from starving
            // when an upstream stage briefly stalls. Multipliers picked
            // so write_rx exactly fits one full writer batch and the
            // upstream stages have ~4 batches' worth of slack.
            // Pre-2026-04-27 sizes were workers*4 (~8 slots), which
            // capped writer drain at 8 units regardless of
            // WRITER_BATCH_SIZE — bumping the const alone was a no-op.
            //
            // Stage 1 → Stage 1.5 (dedup) or Stage 2 (compress). These queues
            // must hold several complete writer batches. The former
            // `workers * 32` sizing was only 64 units with two workers, so the
            // coalescer blocked after one eighth of a 512-unit writer batch and
            // fed LV3 in increasingly fragmented waves.
            let upstream_queue_cap = Self::WRITER_BATCH_SIZE.saturating_mul(4);
            let (dedup_tx, dedup_rx) =
                bounded::<CoalesceUnit>(upstream_queue_cap.max(dedup_workers.saturating_mul(32)));
            // Stage 1.5 → Stage 2
            let (compress_tx, compress_rx) = bounded::<CoalesceUnit>(
                upstream_queue_cap.max(compress_workers.saturating_mul(32)),
            );
            // Stage 2 → Stage 3 — sized to one full writer batch so a
            // single writer cycle can drain to capacity.
            let (write_tx, write_rx) =
                bounded::<CompressedUnit>(Self::WRITER_BATCH_SIZE.max(compress_workers * 4));
            // Stage 3 → Stage 1 (feedback: completed seqs)
            let (done_tx, done_rx) = unbounded::<Vec<u64>>();
            // Writer/dedup → cleanup thread (async dead PBA reclamation)
            let (cleanup_tx, cleanup_rx) = unbounded::<CleanupBatch>();

            // Capture lane-local senders for the commit workers (they
            // route done_tx / cleanup_tx by `CommitJob.shard_idx`).
            lane_done_txs.push(done_tx.clone());
            lane_cleanup_txs.push(cleanup_tx.clone());

            let running_c = running.clone();
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let metrics_c = metrics.clone();
            let in_flight_c = in_flight.clone();
            let flush_admission_qos_c = flush_admission_qos.clone();
            let coalesce_out_tx = if dedup_enabled {
                dedup_tx.clone()
            } else {
                compress_tx.clone()
            };
            let coalesce_handle = thread::Builder::new()
                .name(format!("flusher-coalesce-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherCoalesce, shard_idx);
                    Self::coalesce_loop(
                        shard_idx,
                        &pool_c,
                        &meta_c,
                        &coalesce_out_tx,
                        &done_rx,
                        &running_c,
                        &in_flight_c,
                        &metrics_c,
                        max_raw,
                        max_lbas,
                        skip_fully_superseded,
                        buffer_write_window,
                        buffer_write_window_pressure_pct,
                        buffer_write_window_payload_pressure_pct,
                        &flush_admission_qos_c,
                    );
                })
                .expect("failed to spawn coalescer thread");

            let mut dedup_handles = Vec::new();
            if dedup_enabled {
                for worker_idx in 0..dedup_workers {
                    let rx = dedup_rx.clone();
                    let miss_tx = compress_tx.clone();
                    let running_d = running.clone();
                    let meta_d = meta.clone();
                    let pool_d = pool.clone();
                    let lifecycle_d = lifecycle.clone();
                    let allocator_d = allocator.clone();
                    let done_tx_d = done_tx.clone();
                    let metrics_d = metrics.clone();
                    let cleanup_tx_d = cleanup_tx.clone();
                    let candidate_d = candidate.clone();
                    let read_pool_d = read_pool.clone();
                    let commit_worker_txs_d = commit_worker_txs.clone();
                    let h = thread::Builder::new()
                        .name(format!("flusher-dedup-{}-{}", shard_idx, worker_idx))
                        .spawn(move || {
                            affinity::bind_current(
                                ThreadRole::FlusherDedup,
                                shard_idx * dedup_workers + worker_idx,
                            );
                            Self::dedup_loop(
                                shard_idx,
                                &rx,
                                &miss_tx,
                                &meta_d,
                                &pool_d,
                                &lifecycle_d,
                                &allocator_d,
                                &done_tx_d,
                                &running_d,
                                dedup_skip_threshold,
                                dedup_pending_skip_threshold,
                                &metrics_d,
                                &cleanup_tx_d,
                                &candidate_d,
                                read_pool_d.as_deref(),
                                &commit_worker_txs_d,
                                commit_workers_per_volume,
                                commit_worker_pipeline_depth.max(8),
                            );
                        })
                        .expect("failed to spawn dedup worker");
                    dedup_handles.push(h);
                }
            }
            drop(dedup_rx);
            drop(dedup_tx);
            drop(compress_tx);

            let mut compress_handles = Vec::with_capacity(compress_workers);
            for worker_idx in 0..compress_workers {
                let rx = compress_rx.clone();
                let tx = write_tx.clone();
                let running_w = running.clone();
                let metrics_w = metrics.clone();
                let h = thread::Builder::new()
                    .name(format!("flusher-compress-{}-{}", shard_idx, worker_idx))
                    .spawn(move || {
                        affinity::bind_current(
                            ThreadRole::FlusherCompress,
                            shard_idx * compress_workers + worker_idx,
                        );
                        Self::compress_loop(
                            &rx,
                            &tx,
                            &running_w,
                            &metrics_w,
                            min_compression_savings_pct,
                        );
                    })
                    .expect("failed to spawn compress worker");
                compress_handles.push(h);
            }
            drop(compress_rx);
            drop(write_tx);

            let running_w = running.clone();
            let pool_w = pool.clone();
            let meta_w = meta.clone();
            let lifecycle_w = lifecycle.clone();
            let allocator_w = allocator.clone();
            let io_engine_w = io_engine.clone();
            let metrics_w = metrics.clone();
            let in_flight_w = in_flight.clone();
            let candidate_w = candidate.clone();
            let commit_worker_txs_w = commit_worker_txs.clone();
            let writer_handle = thread::Builder::new()
                .name(format!("flusher-writer-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherWriter, shard_idx);
                    // Create this writer's own LV3 io_uring ring AFTER the NUMA
                    // affinity bind so the ring's pages fault in NUMA-local to the
                    // writer (don't cross NUMA). Returns None for the syscall
                    // backend or when per-shard rings are disabled — the writer
                    // then falls back to the shared backend ring.
                    let write_session = match io_engine_w.new_write_session() {
                        Ok(session) => session,
                        Err(e) => {
                            tracing::warn!(
                                shard = shard_idx,
                                error = %e,
                                "failed to create per-shard LV3 write ring; using shared ring"
                            );
                            None
                        }
                    };
                    let mut packer = Packer::new_with_lane(allocator_w.clone(), shard_idx);
                    Self::writer_loop(
                        shard_idx,
                        &write_rx,
                        &pool_w,
                        &meta_w,
                        &lifecycle_w,
                        &allocator_w,
                        &io_engine_w,
                        write_session.as_ref(),
                        &done_tx,
                        &running_w,
                        &in_flight_w,
                        &mut packer,
                        &metrics_w,
                        &cleanup_tx,
                        &candidate_w,
                        packed_meta_batch_max_lbas,
                        &commit_worker_txs_w,
                        commit_workers_per_volume,
                        writer_read_active_batch_size,
                    );
                })
                .expect("failed to spawn writer thread");

            let running_cl = running.clone();
            let pba_lifecycle_cl = pba_lifecycle.clone();
            let metrics_cl = metrics.clone();
            let cleanup_handle = thread::Builder::new()
                .name(format!("flusher-cleanup-{}", shard_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherCleanup, shard_idx);
                    Self::cleanup_loop(
                        shard_idx,
                        &cleanup_rx,
                        &pba_lifecycle_cl,
                        &running_cl,
                        &metrics_cl,
                    );
                })
                .expect("failed to spawn cleanup thread");

            lanes.push(FlusherLane {
                coalesce_handle: Some(coalesce_handle),
                dedup_handles,
                compress_handles,
                writer_handle: Some(writer_handle),
                cleanup_handle: Some(cleanup_handle),
            });
        }

        // The aggregator owns the only batch sender. It exits only after every
        // raw sender is dropped, forwarding the final partial transaction
        // before it disconnects the executor queue.
        let commit_aggregator_pool = pool.clone();
        let commit_aggregator_metrics = metrics.clone();
        let commit_aggregator_load = commit_executor_load.clone();
        let commit_aggregator_handle = thread::Builder::new()
            .name("flusher-commit-aggregator".to_string())
            .spawn(move || {
                affinity::bind_current(ThreadRole::CommitWorker, commit_executor_count);
                Self::commit_aggregator_loop(
                    commit_rx,
                    commit_batch_tx,
                    commit_aggregator_load,
                    Some(commit_aggregator_pool),
                    Some(commit_aggregator_metrics),
                    commit_retain_tail,
                    commit_target_lbas_per_tx,
                    commit_coalesce_lba_budget,
                    commit_coalesce_timeout,
                    packed_commit_try_drain_lba_budget,
                );
            })
            .expect("failed to spawn commit aggregator");

        // Spawn the commit executors now that lane channels
        // exist. Each worker indexes `lane_done_txs` / `lane_cleanup_txs`
        // by `CommitJob.shard_idx` to fire `done_tx` and queue
        // cleanup payloads back into the originating shard's lane.
        let mut commit_worker_handles: Vec<JoinHandle<()>> =
            Vec::with_capacity(commit_executor_count);
        for (worker_idx, rx) in commit_worker_rxs.into_iter().enumerate() {
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let lifecycle_c = lifecycle.clone();
            let allocator_c = allocator.clone();
            let in_flight_c = in_flight.clone();
            let metrics_c = metrics.clone();
            let candidate_c = candidate.clone();
            let lane_done_txs_c = lane_done_txs.clone();
            let lane_cleanup_txs_c = lane_cleanup_txs.clone();
            let post_commit_tx_c = post_commit_txs[worker_idx].clone();
            let commit_executor_load_c = commit_executor_load.clone();
            let h = thread::Builder::new()
                .name(format!("flusher-commit-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::CommitWorker, worker_idx);
                    Self::commit_worker_loop(
                        worker_idx,
                        &rx,
                        &commit_executor_load_c,
                        &pool_c,
                        &meta_c,
                        &lifecycle_c,
                        &allocator_c,
                        &in_flight_c,
                        &metrics_c,
                        &lane_cleanup_txs_c,
                        &candidate_c,
                        &lane_done_txs_c,
                        &post_commit_tx_c,
                        commit_target_lbas_per_tx,
                        commit_worker_pipeline_depth,
                    );
                })
                .expect("failed to spawn commit worker");
            commit_worker_handles.push(h);
        }

        // Drop our extra clones of the post_commit_txs — only the
        // commit_workers hold senders now. When the commit workers
        // exit on shutdown, these channels disconnect and the
        // post_commit threads will drain and exit.
        drop(post_commit_txs);

        let mut post_commit_handles: Vec<JoinHandle<()>> =
            Vec::with_capacity(commit_executor_count);
        for (worker_idx, rx) in post_commit_rxs.into_iter().enumerate() {
            let pool_c = pool.clone();
            let meta_c = meta.clone();
            let candidate_c = candidate.clone();
            let metrics_c = metrics.clone();
            let lane_done_txs_c = lane_done_txs.clone();
            let h = thread::Builder::new()
                .name(format!("flusher-post-commit-{}", worker_idx))
                .spawn(move || {
                    affinity::bind_current(ThreadRole::FlusherPostCommit, worker_idx);
                    Self::post_commit_loop(
                        worker_idx,
                        &rx,
                        &pool_c,
                        &meta_c,
                        &candidate_c,
                        &metrics_c,
                        &lane_done_txs_c,
                    );
                })
                .expect("failed to spawn post-commit worker");
            post_commit_handles.push(h);
        }

        Self {
            running,
            lanes,
            in_flight,
            candidate,
            pba_lifecycle,
            commit_aggregator_handle: Some(commit_aggregator_handle),
            commit_worker_handles,
            commit_worker_txs,
            post_commit_handles,
        }
    }

    /// Handle to the per-shard RAM candidate cache. Exposed so the
    /// engine can wire the cleanup hook (refcount→0 → candidate
    /// remove) and the dedup scanner can warm the cache during
    /// background rescans. Cheap clone — shares the same backing
    /// shards.
    pub fn candidate_cache(&self) -> crate::dedup::CandidateCache {
        self.candidate.clone()
    }

    /// Clone of the flusher's [`PbaLifecycle`]. The engine wires the lineage
    /// drain, GC reclaim, and dedup scanner to this single instance so they all
    /// share its retire-retry queue and `pba_reclaim_stuck` gauge.
    pub fn pba_lifecycle(&self) -> crate::space::pba_lifecycle::PbaLifecycle {
        self.pba_lifecycle.clone()
    }

    pub fn cleanup_mappings_now(&self, cleanups: &[RemapCleanup], context: &'static str) {
        Self::cleanup_dead_pbas_batch(&self.pba_lifecycle, cleanups, context);
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.join_lanes();
    }

    pub(crate) fn wait_volume_generation_idle(
        &self,
        vol_id: &str,
        vol_created_at: u64,
        timeout: Duration,
    ) -> bool {
        self.in_flight
            .wait_volume_generation_idle(vol_id, vol_created_at, timeout)
    }

    /// Wait for all pending buffer entries to be flushed, then stop.
    /// Used during graceful shutdown to ensure the buffer device is clean
    /// (e.g. before a shard count change on next startup).
    pub fn drain_and_stop(&mut self, pool: &crate::buffer::pool::WriteBufferPool) {
        let _ = self.drain_with_timeout(pool, std::time::Duration::from_secs(60));
    }

    /// Buffer-as-sole-journal Phase A: drive the flusher until every
    /// pending buffer entry has been processed (or `timeout` elapses),
    /// then stop. Returns drain statistics for callers that want to
    /// confirm the replay actually completed before accepting client
    /// IO or comparing shadow state.
    ///
    /// The "replay" semantic falls out of the existing flusher start-up
    /// behaviour: when the buffer pool is reopened from disk, any
    /// already-pending entries land in `pending_entries` and the
    /// coalescer picks them up via its `head_stuck_seq_for_shard`
    /// retry. Driving the same pipeline to quiescence is therefore
    /// equivalent to replaying the buffer-as-journal under the current
    /// metadb state.
    ///
    /// `timeout` is wall-clock; on hit we leave the flusher in a
    /// running state-machine for the caller to observe (the caller can
    /// retry `drain_with_timeout` or fall back to a forced stop). The
    /// `pending_at_exit` field on [`BufferReplayStats`] distinguishes
    /// "drained clean" (== 0) from "timed out with backlog".
    pub fn drain_with_timeout(
        &mut self,
        pool: &crate::buffer::pool::WriteBufferPool,
        timeout: std::time::Duration,
    ) -> BufferReplayStats {
        let started_at = std::time::Instant::now();
        let pending_at_start = pool.pending_count();
        let deadline = started_at + timeout;
        loop {
            let pending = pool.pending_count();
            if pending == 0 {
                tracing::info!(
                    pending_at_start,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "flusher drain complete — buffer is clean"
                );
                let stats = BufferReplayStats {
                    pending_at_start,
                    pending_at_exit: 0,
                    elapsed: started_at.elapsed(),
                    timed_out: false,
                };
                self.running.store(false, Ordering::Relaxed);
                self.join_lanes();
                return stats;
            }
            if std::time::Instant::now() > deadline {
                tracing::warn!(
                    pending,
                    pending_at_start,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "flusher drain timed out — stopping with unflushed entries"
                );
                let stats = BufferReplayStats {
                    pending_at_start,
                    pending_at_exit: pending,
                    elapsed: started_at.elapsed(),
                    timed_out: true,
                };
                self.running.store(false, Ordering::Relaxed);
                self.join_lanes();
                return stats;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    fn join_lanes(&mut self) {
        for lane in &mut self.lanes {
            if let Some(h) = lane.coalesce_handle.take() {
                let _ = h.join();
            }
            for h in lane.dedup_handles.drain(..) {
                let _ = h.join();
            }
            for h in lane.compress_handles.drain(..) {
                let _ = h.join();
            }
            if let Some(h) = lane.writer_handle.take() {
                let _ = h.join();
            }
        }
        // Shard writers have stopped. Close and join the commit pipeline in
        // producer order so no executor can exit before the aggregator has
        // forwarded its final partial batch.
        self.commit_worker_txs.clear();
        if let Some(h) = self.commit_aggregator_handle.take() {
            let _ = h.join();
        }
        for h in self.commit_worker_handles.drain(..) {
            let _ = h.join();
        }
        // post_commit threads exit when commit_worker post_commit_tx
        // senders drop (the only senders are inside
        // commit_worker stack frames, freed when the threads above
        // joined). Join them next so mark_flushed/candidate work for
        // the last batch of commits is durable before downstream
        // cleanup runs.
        for h in self.post_commit_handles.drain(..) {
            let _ = h.join();
        }
        // Per-lane cleanup workers drain after the commit workers
        // finish (commit workers may push cleanup payloads through
        // each lane's cleanup_tx during their own drain).
        for lane in &mut self.lanes {
            if let Some(h) = lane.cleanup_handle.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for BufferFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod pressure_config_tests {
    use super::*;

    #[test]
    fn legacy_pressure_value_still_controls_both_signals() {
        let config = FlushConfig {
            buffer_write_window_pressure_pct: 23,
            ..FlushConfig::default()
        };
        assert_eq!(write_window_pressure_thresholds(&config), (23, 23));
    }

    #[test]
    fn split_pressure_values_override_legacy_independently() {
        let config = FlushConfig {
            buffer_write_window_pressure_pct: 23,
            buffer_write_window_physical_pressure_pct: 40,
            buffer_write_window_payload_pressure_pct: 80,
            ..FlushConfig::default()
        };
        assert_eq!(write_window_pressure_thresholds(&config), (40, 80));
    }
}
