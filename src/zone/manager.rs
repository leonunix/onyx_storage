use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::buffer::pool::WriteBufferPool;
use crate::error::{OnyxError, OnyxResult};
use crate::io::engine::IoEngine;
use crate::meta::store::MetaStore;
use crate::types::{CompressionAlgo, Lba, ZoneId};
use crate::zone::worker::ZoneWorker;

/// IO request dispatched to a zone worker
pub enum ZoneIoRequest {
    Write {
        vol_id: String,
        lba: Lba,
        data: Vec<u8>,
        reply: crossbeam_channel::Sender<OnyxResult<()>>,
    },
    Read {
        vol_id: String,
        lba: Lba,
        reply: crossbeam_channel::Sender<OnyxResult<Option<Vec<u8>>>>,
    },
    Shutdown,
}

struct ZoneHandle {
    sender: Sender<ZoneIoRequest>,
    thread: Option<JoinHandle<()>>,
}

/// Routes IO requests to per-zone worker threads based on LBA.
pub struct ZoneManager {
    handles: Vec<ZoneHandle>,
    zone_size_blocks: u64,
    zone_count: u32,
}

impl ZoneManager {
    pub fn new(
        zone_count: u32,
        zone_size_blocks: u64,
        meta: Arc<MetaStore>,
        buffer_pool: Arc<WriteBufferPool>,
        io_engine: Arc<IoEngine>,
        compression: CompressionAlgo,
    ) -> OnyxResult<Self> {
        let mut handles = Vec::with_capacity(zone_count as usize);

        for i in 0..zone_count {
            let (sender, receiver) = bounded::<ZoneIoRequest>(256);
            let zone_id = ZoneId(i);
            let meta = meta.clone();
            let buffer_pool = buffer_pool.clone();
            let io_engine = io_engine.clone();

            let thread = thread::Builder::new()
                .name(format!("zone-worker-{}", i))
                .spawn(move || {
                    let worker =
                        ZoneWorker::new(zone_id, meta, buffer_pool, io_engine, compression);
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
        })
    }

    fn worker_loop(worker: ZoneWorker, receiver: Receiver<ZoneIoRequest>) {
        tracing::debug!(zone = worker.zone_id.0, "zone worker started");

        for request in receiver {
            match request {
                ZoneIoRequest::Write {
                    vol_id,
                    lba,
                    data,
                    reply,
                } => {
                    let result = worker.handle_write(&vol_id, lba, &data);
                    let _ = reply.send(result);
                }
                ZoneIoRequest::Read { vol_id, lba, reply } => {
                    let result = worker.handle_read(&vol_id, lba);
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

    /// Submit a write IO. Blocks until the zone worker processes it.
    pub fn submit_write(&self, vol_id: &str, lba: Lba, data: Vec<u8>) -> OnyxResult<()> {
        let zone = self.zone_for_lba(lba);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        self.handles[zone.0 as usize]
            .sender
            .send(ZoneIoRequest::Write {
                vol_id: vol_id.to_string(),
                lba,
                data,
                reply: reply_tx,
            })
            .map_err(|_| OnyxError::Ublk("zone worker channel closed".into()))?;

        reply_rx
            .recv()
            .map_err(|_| OnyxError::Ublk("zone worker reply channel closed".into()))?
    }

    /// Submit a read IO. Blocks until the zone worker processes it.
    ///
    /// Returns `Ok(Some(data))` if mapped, `Ok(None)` if unmapped, `Err` on failure.
    pub fn submit_read(&self, vol_id: &str, lba: Lba) -> OnyxResult<Option<Vec<u8>>> {
        let zone = self.zone_for_lba(lba);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        self.handles[zone.0 as usize]
            .sender
            .send(ZoneIoRequest::Read {
                vol_id: vol_id.to_string(),
                lba,
                reply: reply_tx,
            })
            .map_err(|_| OnyxError::Ublk("zone worker channel closed".into()))?;

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
