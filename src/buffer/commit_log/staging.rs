use super::*;

use std::collections::BTreeMap;

/// Bounds the number of reservations that have been assigned a stage order but
/// have not yet entered an ordered sync batch. A missing predecessor retains a
/// permit, so at most `capacity - 1` later envelopes can occupy the channel and
/// reorder map; the predecessor always has room to publish.
pub(super) struct StageWindow {
    capacity: u64,
    available: AtomicU64,
    waiters: AtomicU64,
    wait_lock: parking_lot::Mutex<()>,
    changed: parking_lot::Condvar,
}

impl StageWindow {
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        let capacity = capacity.max(1) as u64;
        Arc::new(Self {
            capacity,
            available: AtomicU64::new(capacity),
            waiters: AtomicU64::new(0),
            wait_lock: parking_lot::Mutex::new(()),
            changed: parking_lot::Condvar::new(),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<StagePermit> {
        self.available
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |available| {
                available.checked_sub(1)
            })
            .ok()
            .map(|_| StagePermit {
                window: Some(self.clone()),
            })
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        relocation_cancel: Option<&AtomicBool>,
        fault: &OnceLock<String>,
    ) -> OnyxResult<(StagePermit, Option<Duration>)> {
        if let Some(permit) = self.try_acquire() {
            if let Some(reason) = fault.get() {
                drop(permit);
                return Err(OnyxError::MetaFenced(reason.clone()));
            }
            return Ok((permit, None));
        }

        let started = Instant::now();
        let deadline = relocation_cancel.map(|_| started + RELOCATION_BACKPRESSURE_BUDGET);
        self.waiters.fetch_add(1, Ordering::Relaxed);
        let _waiter = StageWindowWaiter(&self.waiters);
        let mut wait_guard = self.wait_lock.lock();
        loop {
            if let Some(reason) = fault.get() {
                return Err(OnyxError::MetaFenced(reason.clone()));
            }
            if relocation_cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire)) {
                return Err(OnyxError::RelocationCancelled);
            }
            if let Some(permit) = self.try_acquire() {
                if let Some(reason) = fault.get() {
                    drop(permit);
                    return Err(OnyxError::MetaFenced(reason.clone()));
                }
                return Ok((permit, Some(started.elapsed())));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(OnyxError::BufferPoolFull(0));
            }
            let wait = deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                .unwrap_or(BACKPRESSURE_POLL_INTERVAL)
                .min(BACKPRESSURE_POLL_INTERVAL);
            self.changed.wait_for(&mut wait_guard, wait);
        }
    }

    fn release(&self) {
        let previous = self.available.fetch_add(1, Ordering::Release);
        assert!(
            previous < self.capacity,
            "LV2 stage-window permit released more than once"
        );
        if self.waiters.load(Ordering::Relaxed) > 0 {
            let _guard = self.wait_lock.lock();
            self.changed.notify_one();
        }
    }

    pub(super) fn wake_all(&self) {
        let _guard = self.wait_lock.lock();
        self.changed.notify_all();
    }

    #[cfg(test)]
    fn available(&self) -> u64 {
        self.available.load(Ordering::Acquire)
    }

    pub(super) fn outstanding(&self) -> usize {
        self.capacity
            .saturating_sub(self.available.load(Ordering::Acquire)) as usize
    }
}

struct StageWindowWaiter<'a>(&'a AtomicU64);

impl Drop for StageWindowWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) struct StagePermit {
    window: Option<Arc<StageWindow>>,
}

impl StagePermit {
    pub(super) fn unbounded() -> Self {
        Self { window: None }
    }
}

impl Drop for StagePermit {
    fn drop(&mut self) {
        if let Some(window) = self.window.take() {
            window.release();
        }
    }
}

pub(super) struct StageEnvelope {
    order: u64,
    staged: StagedEntry,
    _permit: StagePermit,
}

impl StageEnvelope {
    pub(super) fn new(order: u64, staged: StagedEntry, permit: StagePermit) -> Self {
        Self {
            order,
            staged,
            _permit: permit,
        }
    }

    fn seq(&self) -> u64 {
        self.staged.pending.seq
    }

    fn into_staged(self) -> StagedEntry {
        let Self {
            staged,
            _permit: permit,
            ..
        } = self;
        drop(permit);
        staged
    }
}

/// Consumer-owned reorder state for one physical buffer shard. `next_order`
/// denotes the first reservation not yet handed to the existing sync pipeline.
pub(super) struct StageReorder {
    next_order: u64,
    last_emitted_seq: Option<u64>,
    future: BTreeMap<u64, StageEnvelope>,
    metric_buffered: u64,
    gap_active: bool,
    #[cfg(any(test, feature = "diagnostic-metrics"))]
    gap_since: Option<Instant>,
    metrics: Arc<OnceLock<Arc<EngineMetrics>>>,
}

impl StageReorder {
    pub(super) fn new(metrics: Arc<OnceLock<Arc<EngineMetrics>>>) -> Self {
        Self {
            next_order: 0,
            last_emitted_seq: None,
            future: BTreeMap::new(),
            metric_buffered: 0,
            gap_active: false,
            #[cfg(any(test, feature = "diagnostic-metrics"))]
            gap_since: None,
            metrics,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.future.is_empty()
    }

    pub(super) fn has_ready(&self) -> bool {
        self.future.contains_key(&self.next_order)
    }

    pub(super) fn buffered_len(&self) -> usize {
        self.future.len()
    }

    pub(super) fn next_order(&self) -> u64 {
        self.next_order
    }

    fn push_future(&mut self, envelope: StageEnvelope) -> Result<(), String> {
        let order = envelope.order;
        let seq = envelope.seq();
        if self.future.contains_key(&order) {
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_stage_reorder_duplicate
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(format!(
                "duplicate LV2 stage order {order} while expecting {} (seq={seq})",
                self.next_order
            ));
        }
        self.future.insert(order, envelope);

        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_stage_reorder_out_of_order
                .fetch_add(1, Ordering::Relaxed);
            let current = metrics
                .buffer_stage_reorder_current
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            self.metric_buffered = self.metric_buffered.saturating_add(1);
            crate::metrics::record_counter_max(&metrics.buffer_stage_reorder_max, current);
            crate::metrics::record_counter_max(
                &metrics.buffer_stage_reorder_max_distance,
                order.saturating_sub(self.next_order),
            );
        }
        if !self.gap_active {
            self.gap_active = true;
            #[cfg(any(test, feature = "diagnostic-metrics"))]
            {
                self.gap_since = Some(Instant::now());
            }
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_stage_reorder_gap_events
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn remove_ready(&mut self) -> Option<StageEnvelope> {
        let envelope = self.future.remove(&self.next_order)?;
        if let Some(metrics) = self.metrics.get() {
            if self.metric_buffered > 0 {
                metrics
                    .buffer_stage_reorder_current
                    .fetch_sub(1, Ordering::Relaxed);
                self.metric_buffered -= 1;
            }
        }
        Some(envelope)
    }

    fn emit(
        &mut self,
        envelope: StageEnvelope,
        batch: &mut Vec<StagedEntry>,
        batch_bytes: &mut usize,
    ) -> Result<(), String> {
        if envelope.order != self.next_order {
            return Err(format!(
                "LV2 stage reorder emitted order {} while expecting {}",
                envelope.order, self.next_order
            ));
        }
        let seq = envelope.seq();
        if self.last_emitted_seq.is_some_and(|last| seq <= last) {
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_stage_reorder_stale
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(format!(
                "LV2 stage seq {seq} did not increase after {:?}",
                self.last_emitted_seq
            ));
        }
        self.next_order = self
            .next_order
            .checked_add(1)
            .ok_or_else(|| "LV2 stage order exhausted u64".to_string())?;
        self.last_emitted_seq = Some(seq);
        let staged = envelope.into_staged();
        *batch_bytes = batch_bytes.saturating_add(staged.payload.len());
        batch.push(staged);
        Ok(())
    }

    fn record_resolved_gap(&mut self) {
        if !self.gap_active {
            return;
        }
        self.gap_active = false;
        #[cfg(any(test, feature = "diagnostic-metrics"))]
        if let (Some(started), Some(metrics)) = (self.gap_since.take(), self.metrics.get()) {
            metrics.buffer_stage_reorder_gap_wait_ns.fetch_add(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
    }

    fn refresh_gap(&mut self) {
        if self.future.is_empty() || self.has_ready() {
            return;
        }
        if !self.gap_active {
            self.gap_active = true;
            #[cfg(any(test, feature = "diagnostic-metrics"))]
            {
                self.gap_since = Some(Instant::now());
            }
            if let Some(metrics) = self.metrics.get() {
                metrics
                    .buffer_stage_reorder_gap_events
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn drain(
        &mut self,
        rx: &Receiver<StageEnvelope>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<StagedEntry>, String> {
        let max_entries = max_entries.max(1);
        let max_bytes = max_bytes.max(1);
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;

        while batch.len() < max_entries && batch_bytes < max_bytes {
            if let Some(envelope) = self.remove_ready() {
                self.record_resolved_gap();
                self.emit(envelope, &mut batch, &mut batch_bytes)?;
                continue;
            }

            let envelope = match rx.try_recv() {
                Ok(envelope) => envelope,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            };
            if envelope.order < self.next_order {
                if let Some(metrics) = self.metrics.get() {
                    metrics
                        .buffer_stage_reorder_stale
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(format!(
                    "stale LV2 stage order {} while expecting {} (seq={})",
                    envelope.order,
                    self.next_order,
                    envelope.seq()
                ));
            }
            if envelope.order == self.next_order {
                self.record_resolved_gap();
                self.emit(envelope, &mut batch, &mut batch_bytes)?;
            } else {
                self.push_future(envelope)?;
            }
        }
        self.refresh_gap();
        Ok(batch)
    }
}

impl Drop for StageReorder {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "diagnostic-metrics"))]
        if let (Some(started), Some(metrics)) = (self.gap_since.take(), self.metrics.get()) {
            metrics.buffer_stage_reorder_gap_wait_ns.fetch_add(
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
        if self.metric_buffered == 0 {
            return;
        }
        if let Some(metrics) = self.metrics.get() {
            metrics
                .buffer_stage_reorder_current
                .fetch_sub(self.metric_buffered, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(
        window: &Arc<StageWindow>,
        order: u64,
        seq: u64,
        payload_len: usize,
    ) -> StageEnvelope {
        let permit = window
            .try_acquire()
            .expect("test stage window must have capacity");
        let payload: Arc<[u8]> = vec![seq as u8; payload_len].into();
        let pending = Arc::new(PendingEntry::test_entry(
            seq,
            "stage-test",
            Lba(order),
            1,
            payload.clone(),
            1,
            None,
        ));
        StageEnvelope::new(
            order,
            StagedEntry {
                pending,
                payload,
                staged_at: Instant::now(),
            },
            permit,
        )
    }

    fn seqs(entries: &[StagedEntry]) -> Vec<u64> {
        entries.iter().map(|entry| entry.pending.seq).collect()
    }

    #[test]
    fn stage_window_bounds_all_unemitted_orders() {
        let window = StageWindow::new(2);
        let first = window.try_acquire().unwrap();
        let second = window.try_acquire().unwrap();

        assert_eq!(window.available(), 0);
        assert!(window.try_acquire().is_none());

        drop(first);
        let replacement = window.try_acquire().unwrap();
        assert_eq!(window.available(), 0);

        drop(second);
        drop(replacement);
        assert_eq!(window.available(), 2);
    }

    #[test]
    fn stage_window_fault_rejects_fast_and_waiting_acquires() {
        let window = StageWindow::new(1);
        let held = window.try_acquire().unwrap();
        let fault = Arc::new(OnceLock::new());
        let waiting_window = window.clone();
        let waiting_fault = fault.clone();
        let (started_tx, started_rx) = bounded(1);
        let waiting = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            waiting_window.acquire(None, &waiting_fault)
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(10));

        fault.set("stage fault".to_string()).unwrap();
        window.wake_all();
        assert!(matches!(
            waiting.join().unwrap(),
            Err(OnyxError::MetaFenced(reason)) if reason == "stage fault"
        ));
        assert_eq!(window.available(), 0);

        drop(held);
        assert!(matches!(
            window.acquire(None, &fault),
            Err(OnyxError::MetaFenced(reason)) if reason == "stage fault"
        ));
        assert_eq!(window.available(), 1);
    }

    #[test]
    fn stage_reorder_holds_future_entries_until_gap_closes() {
        let window = StageWindow::new(3);
        let (tx, rx) = bounded(3);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));

        tx.send(envelope(&window, 2, 30, 4)).unwrap();
        assert!(reorder.drain(&rx, 8, 1024).unwrap().is_empty());
        assert_eq!(reorder.next_order(), 0);
        assert_eq!(reorder.buffered_len(), 1);
        assert_eq!(window.available(), 2, "future entry retains its permit");

        tx.send(envelope(&window, 0, 10, 4)).unwrap();
        let first = reorder.drain(&rx, 8, 1024).unwrap();
        assert_eq!(seqs(&first), vec![10]);
        assert_eq!(reorder.next_order(), 1);
        assert_eq!(reorder.buffered_len(), 1);

        tx.send(envelope(&window, 1, 20, 4)).unwrap();
        let rest = reorder.drain(&rx, 8, 1024).unwrap();
        assert_eq!(seqs(&rest), vec![20, 30]);
        assert_eq!(reorder.next_order(), 3);
        assert!(reorder.is_empty());
        assert_eq!(window.available(), 3);
    }

    #[test]
    fn stage_reorder_preserves_ready_prefix_across_batch_cap() {
        let window = StageWindow::new(3);
        let (tx, rx) = bounded(3);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));

        tx.send(envelope(&window, 2, 3, 4)).unwrap();
        tx.send(envelope(&window, 1, 2, 4)).unwrap();
        assert!(reorder.drain(&rx, 2, 1024).unwrap().is_empty());

        tx.send(envelope(&window, 0, 1, 4)).unwrap();
        let first = reorder.drain(&rx, 2, 1024).unwrap();
        assert_eq!(seqs(&first), vec![1, 2]);
        assert!(reorder.has_ready());
        assert_eq!(reorder.next_order(), 2);
        assert_eq!(window.available(), 2);

        let second = reorder.drain(&rx, 2, 1024).unwrap();
        assert_eq!(seqs(&second), vec![3]);
        assert!(!reorder.has_ready());
        assert_eq!(window.available(), 3);
    }

    #[test]
    fn stage_reorder_preserves_ready_prefix_across_byte_cap() {
        let window = StageWindow::new(3);
        let (tx, rx) = bounded(3);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));

        tx.send(envelope(&window, 2, 3, 4)).unwrap();
        tx.send(envelope(&window, 1, 2, 4)).unwrap();
        assert!(reorder.drain(&rx, 8, 4).unwrap().is_empty());

        tx.send(envelope(&window, 0, 1, 4)).unwrap();
        assert_eq!(seqs(&reorder.drain(&rx, 8, 4).unwrap()), vec![1]);
        assert!(reorder.has_ready());
        assert_eq!(window.available(), 1);

        assert_eq!(seqs(&reorder.drain(&rx, 8, 4).unwrap()), vec![2]);
        assert!(reorder.has_ready());
        assert_eq!(seqs(&reorder.drain(&rx, 8, 4).unwrap()), vec![3]);
        assert_eq!(window.available(), 3);
    }

    #[test]
    fn stage_reorder_rejects_duplicate_and_stale_orders() {
        let window = StageWindow::new(3);
        let (tx, rx) = bounded(3);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));

        tx.send(envelope(&window, 2, 3, 4)).unwrap();
        assert!(reorder.drain(&rx, 8, 1024).unwrap().is_empty());
        tx.send(envelope(&window, 2, 4, 4)).unwrap();
        let duplicate = reorder.drain(&rx, 8, 1024).unwrap_err();
        assert!(duplicate.contains("duplicate LV2 stage order 2"));
        assert_eq!(reorder.buffered_len(), 1);

        drop(reorder);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));
        tx.send(envelope(&window, 0, 1, 4)).unwrap();
        assert_eq!(seqs(&reorder.drain(&rx, 8, 1024).unwrap()), vec![1]);
        tx.send(envelope(&window, 0, 2, 4)).unwrap();
        let stale = reorder.drain(&rx, 8, 1024).unwrap_err();
        assert!(stale.contains("stale LV2 stage order 0"));
    }

    #[test]
    fn stage_reorder_rejects_non_increasing_seq() {
        let window = StageWindow::new(2);
        let (tx, rx) = bounded(2);
        let mut reorder = StageReorder::new(Arc::new(OnceLock::new()));

        tx.send(envelope(&window, 0, 10, 4)).unwrap();
        tx.send(envelope(&window, 1, 9, 4)).unwrap();
        let error = reorder.drain(&rx, 8, 1024).unwrap_err();
        assert!(error.contains("LV2 stage seq 9 did not increase"));
        assert_eq!(reorder.next_order(), 1);
    }
}
