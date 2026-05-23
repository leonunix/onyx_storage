use super::*;

mod coalesce;
mod compress;
mod dedup;

impl BufferFlusher {
    const DEDUP_WORKER_BATCH_MAX_UNITS: usize = 64;
    const DEDUP_HIT_COMMIT_BATCH_SIZE: usize = 1024;

    fn record_stage_send(
        send_ns: &std::sync::atomic::AtomicU64,
        send_ops: &std::sync::atomic::AtomicU64,
        len_sum: &std::sync::atomic::AtomicU64,
        len_max: &std::sync::atomic::AtomicU64,
        started: Instant,
        len_before: usize,
    ) {
        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        send_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        send_ops.fetch_add(1, Ordering::Relaxed);
        len_sum.fetch_add(len_before as u64, Ordering::Relaxed);
        crate::metrics::record_counter_max(len_max, len_before as u64);
    }
}
