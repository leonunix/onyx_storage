use super::*;

impl BufferFlusher {
    pub(in crate::buffer::flush) fn compress_loop(
        rx: &Receiver<CoalesceUnit>,
        tx: &Sender<CompressedUnit>,
        running: &AtomicBool,
        metrics: &EngineMetrics,
        min_compression_savings_pct: u8,
    ) {
        // Reusable per-worker compression scratch. With `coalesce_max_raw_bytes`
        // capped at 128 KiB by config, LZ4's worst-case output is
        // ~132 KiB; 256 KiB gives comfortable headroom. Zeroed once at
        // thread start (one-shot ~50 µs) and reused for every unit
        // thereafter.
        //
        // The flamegraph attributed ~40 % of compress-thread CPU to
        // `_rjem_je_pages_purge_forced → madvise →
        // smp_call_function_many_cond` — jemalloc returning the
        // freshly-freed 30 KiB scratch allocations back to the kernel,
        // which fired a cross-CPU IPI on every purge. Holding one
        // long-lived `Box<[u8]>` per worker eliminates that churn:
        // the buffer never enters jemalloc's free-list rotation, so
        // there's no purge trigger and no IPI storm. Per-unit codec
        // work also stops paying the `vec![0; max_out]` alloc + zero
        // it used to do, and `lz4_flex::block::compress_into` writes
        // in place rather than allocating its own internal Vec.
        const COMPRESS_BUF_BYTES: usize = 256 * 1024;
        let mut compress_buf: Box<[u8]> = vec![0u8; COMPRESS_BUF_BYTES].into_boxed_slice();
        while running.load(Ordering::Relaxed) {
            let recv_start = Instant::now();
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(unit) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    let active_start = Instant::now();
                    let CoalesceUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        raw_blocks,
                        compression: algo,
                        vol_created_at,
                        seq_lba_ranges,
                        dedup_skipped,
                        block_hashes,
                        dedup_stale_repairs,
                        dedup_completion,
                    } = unit;

                    // Phase A: materialise the unit's contiguous bytes.
                    let raw_build_start = Instant::now();
                    let original_size = raw_blocks.len() * BLOCK_SIZE as usize;
                    let mut raw_data = Vec::with_capacity(original_size);
                    for block in &raw_blocks {
                        raw_data.extend_from_slice(block.bytes());
                    }
                    metrics.flush_compress_raw_build_ns.fetch_add(
                        raw_build_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );

                    // Phase B: codec. We deliberately bypass the
                    // `Box<dyn Compressor>` trait + caller-supplied dst
                    // buffer pattern. The trait implementation allocated
                    // a zero-filled `vec![0u8; max_out]` then called
                    // `lz4_flex::compress(src)` which itself allocated
                    // a `Vec<u8>` internally, then memcpy'd that into
                    // the caller's buffer. With 32 workers running this
                    // per unit and `--refill_buffers` workloads pinning
                    // ~100% bypass rate, the three 30 KiB allocations
                    // per unit (raw_data + compressed_buf + lz4 internal)
                    // plus a 30 KiB memset and a redundant memcpy were
                    // taking ~2.5 ms per unit — i.e. compress was
                    // running at ~12 MB/s per worker, ~40× under LZ4's
                    // single-thread spec. Calling the codec function
                    // directly lets it own its output Vec and skips
                    // the intermediate buffer entirely; the bypass case
                    // (incompressible random input) now just drops the
                    // codec's Vec and keeps `raw_data`.
                    let codec_start = Instant::now();
                    let mut compression_bypassed = false;
                    let (compression_byte, compressed_data) = match algo {
                        CompressionAlgo::None => (0u8, raw_data),
                        CompressionAlgo::Lz4 => {
                            let max_out = lz4_flex::block::get_maximum_output_size(original_size);
                            if max_out <= compress_buf.len() {
                                match lz4_flex::block::compress_into(
                                    &raw_data,
                                    &mut compress_buf[..max_out],
                                ) {
                                    Ok(size)
                                        if Self::compression_saves_enough(
                                            original_size,
                                            size,
                                            min_compression_savings_pct,
                                        ) =>
                                    {
                                        // Copy compressed prefix out — this is
                                        // the one alloc we accept on the success
                                        // path. Bypass (incompressible random)
                                        // input doesn't hit this branch.
                                        (algo.to_u8(), compress_buf[..size].to_vec())
                                    }
                                    _ => {
                                        compression_bypassed = true;
                                        (0u8, raw_data)
                                    }
                                }
                            } else {
                                // Fallback for unusually large units that exceed
                                // the reusable buffer (config violation; not
                                // expected in steady state).
                                let out = lz4_flex::compress(&raw_data);
                                if Self::compression_saves_enough(
                                    original_size,
                                    out.len(),
                                    min_compression_savings_pct,
                                ) {
                                    (algo.to_u8(), out)
                                } else {
                                    compression_bypassed = true;
                                    drop(out);
                                    (0u8, raw_data)
                                }
                            }
                        }
                        CompressionAlgo::Zstd { level } => {
                            match zstd::encode_all(raw_data.as_slice(), level) {
                                Ok(out)
                                    if Self::compression_saves_enough(
                                        original_size,
                                        out.len(),
                                        min_compression_savings_pct,
                                    ) =>
                                {
                                    (algo.to_u8(), out)
                                }
                                _ => {
                                    compression_bypassed = true;
                                    (0u8, raw_data)
                                }
                            }
                        }
                    };
                    metrics.flush_compress_codec_ns.fetch_add(
                        codec_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );

                    metrics.compress_units.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .compress_input_bytes
                        .fetch_add(original_size as u64, Ordering::Relaxed);
                    metrics
                        .compress_output_bytes
                        .fetch_add(compressed_data.len() as u64, Ordering::Relaxed);
                    if compression_bypassed {
                        metrics
                            .compress_bypass_units
                            .fetch_add(1, Ordering::Relaxed);
                        metrics
                            .compress_bypass_bytes
                            .fetch_add(original_size as u64, Ordering::Relaxed);
                    }

                    // Phase C: CRC32 over the final compressed (or
                    // bypass-raw) payload.
                    let crc_start = Instant::now();
                    let crc32 = crc32fast::hash(&compressed_data);
                    metrics.flush_compress_crc_ns.fetch_add(
                        crc_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );

                    let cu = CompressedUnit {
                        vol_id,
                        start_lba,
                        lba_count,
                        original_size: original_size as u32,
                        compressed_data,
                        compression: compression_byte,
                        crc32,
                        vol_created_at,
                        seq_lba_ranges,
                        block_hashes,
                        dedup_stale_repairs,
                        dedup_skipped,
                        compression_bypassed,
                        dedup_completion,
                    };

                    let len_before = tx.len();
                    let started = Instant::now();
                    let result = tx.send(cu);
                    Self::record_stage_send(
                        &metrics.flush_stage_compress_send_ns,
                        &metrics.flush_stage_compress_send_ops,
                        &metrics.flush_stage_compress_send_len_sum,
                        &metrics.flush_stage_compress_send_len_max,
                        started,
                        len_before,
                    );
                    if result.is_err() {
                        return;
                    }
                    let active_ns = active_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_active_ns
                        .fetch_add(active_ns, Ordering::Relaxed);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let idle_ns = recv_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    metrics
                        .flush_compress_worker_idle_ns
                        .fetch_add(idle_ns, Ordering::Relaxed);
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn compression_saves_enough(original_size: usize, compressed_size: usize, min_pct: u8) -> bool {
        if original_size == 0 || compressed_size >= original_size {
            return false;
        }
        if min_pct == 0 {
            return true;
        }
        let saved = original_size - compressed_size;
        saved.saturating_mul(100) >= original_size.saturating_mul(min_pct as usize)
    }
}
