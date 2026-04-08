use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::buffer::pool::WriteBufferPool;
use crate::error::{OnyxError, OnyxResult};
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::metrics::EngineMetrics;
use crate::types::{Lba, ZoneId};
use crate::zone::worker::ZoneWorker;

/// IO request dispatched to a zone worker
pub enum ZoneIoRequest {
    Write {
        vol_id: String,
        start_lba: Lba,
        lba_count: u32,
        data: Vec<u8>,
        vol_created_at: u64,
        reply: crossbeam_channel::Sender<OnyxResult<()>>,
    },
    Read {
        vol_id: String,
        lba: Lba,
        vol_created_at: u64,
        reply: crossbeam_channel::Sender<OnyxResult<Option<Vec<u8>>>>,
    },
    Shutdown,
}

struct ZoneHandle {
    sender: Sender<ZoneIoRequest>,
    thread: Option<JoinHandle<()>>,
}

/// Routes read IO requests to per-zone worker threads based on LBA.
/// Aligned writes bypass the worker threads and append directly into the
/// durable write buffer to keep the foreground path thinner.
pub struct ZoneManager {
    handles: Vec<ZoneHandle>,
    zone_size_blocks: u64,
    zone_count: u32,
    buffer_pool: Arc<WriteBufferPool>,
    metrics: Arc<EngineMetrics>,
}

impl ZoneManager {
    pub fn new(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
    ) -> OnyxResult<Self> {
        Self::new_with_metrics(
            zone_count,
            zone_size_blocks,
            meta,
            buffer_pool,
            io_engine,
            Arc::new(EngineMetrics::default()),
        )
    }

    pub fn new_with_metrics(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        metrics: Arc<EngineMetrics>,
    ) -> OnyxResult<Self> {
        let mut handles = Vec::with_capacity(zone_count as usize);

        for i in 0..zone_count {
            let (sender, receiver) = bounded::<ZoneIoRequest>(256);
            let zone_id = ZoneId(i);
            let meta = meta.clone();
            let buffer_pool = buffer_pool.clone();
            let io_engine = io_engine.clone();
            let metrics = metrics.clone();

            let thread = thread::Builder::new()
                .name(format!("zone-worker-{}", i))
                .spawn(move || {
                    let worker = ZoneWorker::new_with_metrics(
                        zone_id,
                        meta,
                        buffer_pool,
                        io_engine,
                        metrics,
                    );
                    Self::worker_loop(worker, receiver);
                })
                .map_err(|e| OnyxError::Config(format!("failed to spawn zone thread: {}", e)))?;

            handles.push(ZoneHandle {
                sender,
                thread: Some(thread),
            });
        }

        tracing::info!(zone_count, zone_size_blocks, "zone manager started");

        Ok(Self {
            handles,
            zone_size_blocks,
            zone_count,
            buffer_pool,
            metrics,
        })
    }

    fn worker_loop(worker: ZoneWorker, receiver: Receiver<ZoneIoRequest>) {
        tracing::debug!(zone = worker.zone_id.0, "zone worker started");

        for request in receiver {
            match request {
                ZoneIoRequest::Write {
                    vol_id,
                    start_lba,
                    lba_count,
                    data,
                    vol_created_at,
                    reply,
                } => {
                    let start = Instant::now();
                    let result =
                        worker.handle_write(&vol_id, start_lba, lba_count, &data, vol_created_at);
                    worker.record_write_ns(start);
                    let _ = reply.send(result);
                }
                ZoneIoRequest::Read {
                    vol_id,
                    lba,
                    vol_created_at,
                    reply,
                } => {
                    let result = worker.handle_read_with_generation(&vol_id, lba, vol_created_at);
                    let _ = reply.send(result);
                }
                ZoneIoRequest::Shutdown => {
                    tracing::debug!(zone = worker.zone_id.0, "zone worker shutting down");
                    break;
                }
            }
        }
    }

    /// Determine which zone handles a given LBA
    pub fn zone_for_lba(&self, lba: Lba) -> ZoneId {
        let zone_idx = (lba.0 / self.zone_size_blocks) % self.zone_count as u64;
        ZoneId(zone_idx as u32)
    }

    /// Submit a write IO covering one or more contiguous LBAs.
    /// Automatically splits at zone boundaries to preserve the "same zone = serial" invariant.
    pub fn submit_write(
        &self,
        vol_id: &str,
        start_lba: Lba,
        lba_count: u32,
        data: &[u8],
        vol_created_at: u64,
    ) -> OnyxResult<()> {
        let total_start = Instant::now();
        let result = (|| {
            let chunks = if lba_count == 0 {
                0
            } else {
                let first_zone = start_lba.0 / self.zone_size_blocks;
                let last_lba = start_lba.0 + lba_count as u64 - 1;
                let last_zone = last_lba / self.zone_size_blocks;
                last_zone.saturating_sub(first_zone) + 1
            };

            self.buffer_pool
                .append(vol_id, start_lba, lba_count, &data, vol_created_at)?;

            self.metrics
                .zone_write_dispatches
                .fetch_add(chunks, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .zone_write_lbas
                .fetch_add(lba_count as u64, std::sync::atomic::Ordering::Relaxed);
            if chunks > 1 {
                self.metrics
                    .zone_write_split_ops
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            Ok(())
        })();

        self.metrics.zone_submit_write_ns.fetch_add(
            total_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        result
    }

    /// Submit a read IO. Blocks until the zone worker processes it.
    ///
    /// Returns `Ok(Some(data))` if mapped, `Ok(None)` if unmapped, `Err` on failure.
    pub fn submit_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        self.submit_read_with_generation(vol_id, lba, 0)
    }

    pub fn submit_read_with_generation(
        &self,
        vol_id: &str,
        lba: Lba,
        vol_created_at: u64,
    ) -> OnyxResult<Option<Vec<u8>>> {
        let zone = self.zone_for_lba(lba);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        self.handles[zone.0 as usize]
            .sender
            .send(ZoneIoRequest::Read {
                vol_id: vol_id.to_string(),
                lba,
                vol_created_at,
                reply: reply_tx,
            })
            .map_err(|_| OnyxError::Ublk("zone worker channel closed".into()))?;

        self.metrics
            .zone_read_dispatches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        reply_rx
            .recv()
            .map_err(|_| OnyxError::Ublk("zone worker reply channel closed".into()))?
    }

    /// Graceful shutdown: send shutdown to all workers and wait.
    pub fn shutdown(&mut self) -> OnyxResult<()> {
        for handle in &self.handles {
            let _ = handle.sender.send(ZoneIoRequest::Shutdown);
        }
        for handle in &mut self.handles {
            if let Some(thread) = handle.thread.take() {
                let _ = thread.join();
            }
        }
        tracing::info!("zone manager stopped");
        Ok(())
    }

    pub fn zone_count(&self) -> u32 {
        self.zone_count
    }
}

impl Drop for ZoneManager {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
