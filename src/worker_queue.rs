//! A work queue served by a pool of threads, with the number of receivers
//! parked on any ONE channel bounded.
//!
//! Every shared worker pool in the engine routes through this so the bound is
//! structural rather than a coincidence of whatever the pool's size knob
//! happens to default to.  See [`MAX_RECEIVERS_PER_CHANNEL`] for why the bound
//! exists and what it cost when it did not.

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, SendError, Sender, TrySendError};

/// Receivers allowed to park on ONE crossbeam channel.
///
/// ⛔ `crossbeam_channel`'s wake path is O(number of parked receivers)
/// **inside a single mutex**.  `waker::SyncWaker` is
/// `{ inner: Mutex<Waker>, is_empty: AtomicBool }` and `Waker` keeps its
/// registered receivers in a `Vec<Entry>`: a receiver that wakes runs
/// `unregister` (linear find plus a `Vec::remove` memmove) and every `send`
/// that finds any receiver parked runs `try_select` (linear scan, one
/// `Arc<Inner>` pointer-chase per entry) — both under that one mutex.  A pool
/// whose workers are mostly idle keeps `is_empty` false, so *every* request
/// pays it.
///
/// One channel served by a large pool therefore serializes admission, and it
/// fails as a CLIFF rather than a curve, because it is positive feedback: more
/// workers park more entries, which slows admission, which idles more workers,
/// which parks more entries.  Box-measured on the direct-IO submit pool at
/// QD1024 (memory `direct_io_intake_is_the_missing_10ms`):
///
/// | pool size | one channel | grouped at 8 |
/// |---|---|---|
/// | 64 | 546 MiB/s | 546 MiB/s |
/// | 128 | **349** | **557** |
/// | 256 | **355** | **544** |
///
/// At 128 and 256, 96-97% of every request's round trip was the request
/// sitting unread in a socket buffer because the thread that should have read
/// it was blocked on that mutex.  Grouping made throughput flat across a 4x
/// range of pool size — the flatness is the result, not the peak.
pub const MAX_RECEIVERS_PER_CHANNEL: usize = 8;

/// How `workers` split into groups of at most [`MAX_RECEIVERS_PER_CHANNEL`],
/// with the remainder spread evenly instead of piling into one short group.
///
/// Always returns at least one group, so a zero-sized pool cannot produce a
/// queue with nowhere to send.
pub fn group_sizes(workers: usize) -> Vec<usize> {
    let workers = workers.max(1);
    let groups = workers.div_ceil(MAX_RECEIVERS_PER_CHANNEL);
    (0..groups)
        .map(|group| workers / groups + usize::from(group < workers % groups))
        .collect()
}

/// Warns when a pool is still built as ONE channel with more receivers parked
/// on it than the bound.
///
/// Call this at any pool construction that has not been moved to
/// [`WorkerQueue`] yet. The flusher's compress / cleanup / dedup pools default
/// to 8 / 4 / 8 receivers, i.e. at or under the bound, so grouping them would
/// be a no-op today — but their sizes are config knobs
/// (`FlushConfig::compress_pool_workers`, `cleanup_pool_workers`,
/// `DedupConfig::pool_workers`), and raising one past 8 walks straight into the
/// cliff described on [`MAX_RECEIVERS_PER_CHANNEL`]. This makes that loud
/// instead of silent.
pub fn warn_if_over_receiver_bound(pool: &str, workers: usize) {
    if workers > MAX_RECEIVERS_PER_CHANNEL {
        tracing::warn!(
            pool,
            workers,
            bound = MAX_RECEIVERS_PER_CHANNEL,
            "shared pool parks more receivers on ONE channel than the bound: \
             crossbeam's wake path is O(receivers) under a single mutex, which \
             collapses admission (box-measured 36% throughput loss going 64 -> 128 \
             on the direct-IO submit pool). Route this pool through WorkerQueue."
        );
    }
}

/// A pool's intake: several channels, each parked on by at most
/// [`MAX_RECEIVERS_PER_CHANNEL`] workers, plus a rotor across them.
///
/// ⭐ Routing is **per item**, not per producer.  A producer pinned to one
/// group for life would rebuild exactly the static partition that shared pools
/// exist to remove (see `UblkConfig::shared_io_workers`), which is the reason
/// this is a rotor and not `producer_id % groups`.
pub struct WorkerQueue<T> {
    groups: Vec<Sender<T>>,
    rotor: AtomicUsize,
}

impl<T> WorkerQueue<T> {
    /// Builds a queue for `workers` threads and returns one receiver per
    /// worker, in order — hand `receivers[i]` to worker `i`.
    ///
    /// `total_capacity` is the capacity across ALL groups (`None` = unbounded),
    /// so converting an existing single-channel pool keeps its admission
    /// capacity and moves only the wake cost.
    pub fn build(workers: usize, total_capacity: Option<usize>) -> (Self, Vec<Receiver<T>>) {
        let sizes = group_sizes(workers);
        let per_group_capacity = total_capacity.map(|cap| cap.div_ceil(sizes.len()).max(1));
        let mut groups = Vec::with_capacity(sizes.len());
        let mut receivers = Vec::with_capacity(sizes.iter().sum());
        for size in sizes {
            let (tx, rx) = match per_group_capacity {
                Some(capacity) => crossbeam_channel::bounded(capacity),
                None => crossbeam_channel::unbounded(),
            };
            groups.push(tx);
            for _ in 0..size {
                receivers.push(rx.clone());
            }
        }
        (
            Self {
                groups,
                rotor: AtomicUsize::new(0),
            },
            receivers,
        )
    }

    /// Number of underlying channels. Worth logging next to the pool size so a
    /// perf arm can prove the grouping is actually active.
    pub fn groups(&self) -> usize {
        self.groups.len()
    }

    /// Routes one item.
    ///
    /// A full group is a group whose workers are all busy AND whose queue is
    /// backed up, so rotate past it rather than queue behind it; only when
    /// every group is full do we block, which is the backpressure a single
    /// shared channel also applied. Unbounded queues never report full, so
    /// there this is exactly the rotor.
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let groups = self.groups.len();
        let start = self.rotor.fetch_add(1, Ordering::Relaxed) % groups;
        let mut item = item;
        for offset in 0..groups {
            match self.groups[(start + offset) % groups].try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => item = returned,
                Err(TrySendError::Disconnected(returned)) => return Err(SendError(returned)),
            }
        }
        self.groups[start].send(item)
    }

    /// Non-blocking route. Reports `Full` only when EVERY group is full.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let groups = self.groups.len();
        let start = self.rotor.fetch_add(1, Ordering::Relaxed) % groups;
        let mut item = item;
        for offset in 0..groups {
            match self.groups[(start + offset) % groups].try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => item = returned,
                Err(TrySendError::Disconnected(returned)) => {
                    return Err(TrySendError::Disconnected(returned))
                }
            }
        }
        Err(TrySendError::Full(item))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bound is the whole point: no channel may ever have more than
    /// `MAX_RECEIVERS_PER_CHANNEL` receivers, because that count is what
    /// crossbeam's wake path is linear in. Every worker must still get a home,
    /// and the remainder must spread rather than leave one group nearly empty.
    #[test]
    fn groups_bound_receivers_and_lose_no_worker() {
        for workers in [1usize, 4, 7, 8, 9, 32, 64, 128, 256, 1000] {
            let sizes = group_sizes(workers);
            assert_eq!(
                sizes.iter().sum::<usize>(),
                workers,
                "every worker needs a home ({workers})"
            );
            assert!(
                sizes.iter().all(|size| *size <= MAX_RECEIVERS_PER_CHANNEL),
                "group over the receiver bound for {workers}: {sizes:?}"
            );
            assert!(sizes.iter().all(|size| *size >= 1), "empty group {sizes:?}");
            let spread = sizes.iter().max().unwrap() - sizes.iter().min().unwrap();
            assert!(
                spread <= 1,
                "remainder should spread, not pile up: {sizes:?}"
            );
        }
        assert_eq!(group_sizes(0), vec![1], "a zero-sized pool must not panic");
    }

    /// `build` must hand back exactly one receiver per worker, in group order,
    /// so `receivers[i]` is worker `i`'s and no worker is left without one.
    #[test]
    fn build_returns_one_receiver_per_worker() {
        for workers in [1usize, 8, 9, 20] {
            let (queue, receivers) = WorkerQueue::<u32>::build(workers, None);
            assert_eq!(receivers.len(), workers);
            assert_eq!(queue.groups(), group_sizes(workers).len());
        }
    }

    /// Routing is per ITEM, not per producer: one producer's traffic must
    /// spread over every group, or the shared pool degenerates into the static
    /// partition it exists to replace.
    #[test]
    fn one_producer_spreads_over_every_group() {
        let workers = 24; // 3 groups of 8
        let (queue, receivers) = WorkerQueue::<u32>::build(workers, None);
        assert_eq!(queue.groups(), 3);
        for item in 0..9u32 {
            queue.send(item).unwrap();
        }
        // receivers[0], [8], [16] are the three distinct groups.
        for (group, receiver_idx) in [0usize, 8, 16].into_iter().enumerate() {
            let got: Vec<u32> = receivers[receiver_idx].try_iter().collect();
            assert_eq!(
                got.len(),
                3,
                "group {group} should have taken every third item, got {got:?}"
            );
        }
    }

    /// Total capacity must survive the split, so converting a single-channel
    /// pool moves the wake cost without moving the backpressure point.
    #[test]
    fn total_capacity_survives_the_split() {
        let (queue, receivers) = WorkerQueue::<u32>::build(16, Some(32));
        assert_eq!(queue.groups(), 2);
        for item in 0..32u32 {
            queue.try_send(item).expect("32 slots were requested");
        }
        assert!(
            queue.try_send(999).is_err(),
            "past the requested total capacity it must report full"
        );
        drop(receivers);
    }

    /// A dead pool must hand the item back rather than swallow it — callers
    /// use it to fail the request with ESHUTDOWN.
    #[test]
    fn disconnected_queue_returns_the_item() {
        let (queue, receivers) = WorkerQueue::<u32>::build(4, None);
        drop(receivers);
        match queue.send(7) {
            Err(SendError(returned)) => assert_eq!(returned, 7),
            Ok(()) => panic!("a disconnected queue must not accept work"),
        }
    }
}
