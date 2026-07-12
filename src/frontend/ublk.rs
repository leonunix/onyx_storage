// ublk frontend: exposes a Linux block device via the ublk kernel module.
// This module is only compiled on Linux (cfg(target_os = "linux")).

use std::cell::{Cell, RefCell};
use std::os::fd::RawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{sys, BufDesc, UblkError, UblkFlags, UblkIORes, UblkUringData};
use onyx_metadb::VolumeOrdinal;

use std::sync::atomic::Ordering;

use crossbeam_channel::{Receiver, Sender};
use io_uring::{opcode, types};

use crate::buffer::pool::BufferAppendTicket;
use crate::config::UblkConfig;
use crate::error::{OnyxError, OnyxResult};
use crate::types::{Lba, VolumeConfig, BLOCK_SIZE, SECTOR_SIZE};
use crate::zone::manager::ZoneManager;

/// ublk target that routes IO through the ZoneManager
pub struct OnyxUblkTarget {
    config: UblkConfig,
    zone_manager: Arc<ZoneManager>,
    vol_id: String,
    vol_ord: VolumeOrdinal,
    device_size_bytes: u64,
    vol_created_at: u64,
}

#[derive(Clone)]
struct IoWorkerContext {
    zone_manager: Arc<ZoneManager>,
    vol_id: String,
    vol_ord: VolumeOrdinal,
    vol_created_at: u64,
    block_size: u64,
    sector_size: u64,
}

struct QueuedIo {
    tag: u16,
    op: u32,
    start_sector: u64,
    nr_sectors: u32,
    data: QueuedIoData,
    queued_at: Instant,
}

enum QueuedIoData {
    Owned(Vec<u8>),
    /// Direct pointer into libublk's per-tag queue buffer.
    ///
    /// This is only used for READ requests: the worker has to write the
    /// response into libublk's buffer before the queue thread completes the
    /// tag. WRITE requests are copied into [`QueuedIoData::Owned`] before
    /// leaving the queue callback so their source bytes cannot be affected by
    /// libublk buffer reuse after the callback returns.
    Direct {
        ptr: usize,
        len: usize,
    },
}

struct CompletedIo {
    tag: u16,
    op: u32,
    res: i32,
    elapsed_ns: u64,
    queue_wait_ns: u64,
    worker_ns: u64,
    completed_at: Instant,
}

enum IoWorkerResult {
    Complete(i32),
    AwaitDurability {
        res: i32,
        tickets: Vec<BufferAppendTicket>,
    },
}

struct PendingDurableIo {
    tag: u16,
    op: u32,
    res: i32,
    queued_at: Instant,
    queue_wait_ns: u64,
    worker_ns: u64,
    tickets: Vec<BufferAppendTicket>,
}

const OPPORTUNISTIC_COMPLETION_DRAIN_MAX: usize = 8;

impl IoWorkerContext {
    fn handle_io_deferred(
        &self,
        op: u32,
        start_sector: u64,
        nr_sectors: u32,
        io_slice: &mut [u8],
    ) -> IoWorkerResult {
        let offset_bytes = start_sector * self.sector_size;
        let io_bytes = nr_sectors as u64 * self.sector_size;
        let io_len = io_bytes as usize;
        if op == sys::UBLK_IO_OP_WRITE
            && offset_bytes % self.block_size == 0
            && io_bytes % self.block_size == 0
            && io_len <= io_slice.len()
        {
            let start_lba = Lba(offset_bytes / self.block_size);
            let lba_count = (io_bytes / self.block_size) as u32;
            return match self.zone_manager.submit_write_deferred(
                &self.vol_id,
                start_lba,
                lba_count,
                &io_slice[..io_len],
                self.vol_created_at,
            ) {
                Ok(tickets) => IoWorkerResult::AwaitDurability {
                    res: io_bytes as i32,
                    tickets,
                },
                Err(err) => {
                    tracing::error!(
                        lba = start_lba.0,
                        count = lba_count,
                        error = %err,
                        "deferred write failed"
                    );
                    IoWorkerResult::Complete(-(libc::EIO as i32))
                }
            };
        }
        IoWorkerResult::Complete(self.handle_io(op, start_sector, nr_sectors, io_slice))
    }

    fn handle_io(&self, op: u32, start_sector: u64, nr_sectors: u32, io_slice: &mut [u8]) -> i32 {
        let offset_bytes = start_sector * self.sector_size;
        let io_bytes = nr_sectors as u64 * self.sector_size;
        let io_len = io_bytes as usize;

        if matches!(op, sys::UBLK_IO_OP_READ | sys::UBLK_IO_OP_WRITE) && io_len > io_slice.len() {
            tracing::error!(
                io_len,
                buf_len = io_slice.len(),
                "ublk request exceeds queue buffer size"
            );
            return -(libc::EIO as i32);
        }

        match op {
            sys::UBLK_IO_OP_READ => {
                self.handle_read(offset_bytes, io_bytes, &mut io_slice[..io_len])
            }
            sys::UBLK_IO_OP_WRITE => self.handle_write(offset_bytes, io_bytes, &io_slice[..io_len]),
            sys::UBLK_IO_OP_FLUSH => 0,
            sys::UBLK_IO_OP_DISCARD => self.handle_discard(offset_bytes, io_bytes),
            _ => -(libc::ENOTSUP as i32),
        }
    }

    fn handle_read(&self, offset_bytes: u64, io_bytes: u64, out: &mut [u8]) -> i32 {
        if offset_bytes % self.block_size == 0 && io_bytes % self.block_size == 0 {
            let start_lba = Lba(offset_bytes / self.block_size);
            let lba_count = (io_bytes / self.block_size) as u32;
            return match self.zone_manager.submit_reads_with_ordinal(
                &self.vol_id,
                Some(self.vol_ord),
                start_lba,
                lba_count,
                self.vol_created_at,
                out,
            ) {
                Ok(()) => io_bytes as i32,
                Err(e) => {
                    tracing::error!(
                        start_lba = start_lba.0,
                        lba_count,
                        error = %e,
                        "batched read failed"
                    );
                    -(libc::EIO as i32)
                }
            };
        }

        let mut buf_offset = 0usize;
        let mut remaining = io_bytes;
        let mut cur_offset = offset_bytes;
        let mut status = io_bytes as i32;

        while remaining > 0 {
            let block_lba = Lba(cur_offset / self.block_size);
            let offset_in_block = (cur_offset % self.block_size) as usize;
            let avail_in_block = self.block_size as usize - offset_in_block;
            let copy_len = (remaining as usize).min(avail_in_block);

            match self.zone_manager.submit_read_with_generation(
                &self.vol_id,
                block_lba,
                self.vol_created_at,
            ) {
                Ok(Some(data)) => {
                    let src_end = (offset_in_block + copy_len).min(data.len());
                    let actual_copy = src_end.saturating_sub(offset_in_block);
                    if actual_copy > 0 {
                        out[buf_offset..buf_offset + actual_copy]
                            .copy_from_slice(&data[offset_in_block..offset_in_block + actual_copy]);
                    }
                    if actual_copy < copy_len {
                        out[buf_offset + actual_copy..buf_offset + copy_len].fill(0);
                    }
                }
                Ok(None) => {
                    out[buf_offset..buf_offset + copy_len].fill(0);
                }
                Err(e) => {
                    tracing::error!(lba = block_lba.0, error = %e, "read failed");
                    status = -(libc::EIO as i32);
                    break;
                }
            }

            buf_offset += copy_len;
            cur_offset += copy_len as u64;
            remaining -= copy_len as u64;
        }

        status
    }

    fn handle_write(&self, offset_bytes: u64, io_bytes: u64, req: &[u8]) -> i32 {
        if offset_bytes % self.block_size == 0 && io_bytes % self.block_size == 0 {
            let start_lba = Lba(offset_bytes / self.block_size);
            let lba_count = (io_bytes / self.block_size) as u32;
            return if let Err(e) = self.zone_manager.submit_write(
                &self.vol_id,
                start_lba,
                lba_count,
                req,
                self.vol_created_at,
            ) {
                tracing::error!(
                    lba = start_lba.0,
                    count = lba_count,
                    error = %e,
                    "write failed"
                );
                -(libc::EIO as i32)
            } else {
                io_bytes as i32
            };
        }

        let mut buf_offset = 0usize;
        let mut remaining = io_bytes;
        let mut cur_offset = offset_bytes;
        let mut status = io_bytes as i32;

        while remaining > 0 {
            let block_lba = Lba(cur_offset / self.block_size);
            let offset_in_block = (cur_offset % self.block_size) as usize;
            let avail_in_block = self.block_size as usize - offset_in_block;
            let write_len = (remaining as usize).min(avail_in_block);

            let mut block = match self.zone_manager.submit_read_with_generation(
                &self.vol_id,
                block_lba,
                self.vol_created_at,
            ) {
                Ok(Some(data)) => {
                    let mut b = data;
                    b.resize(self.block_size as usize, 0);
                    b
                }
                Ok(None) => vec![0u8; self.block_size as usize],
                Err(e) => {
                    tracing::error!(lba = block_lba.0, error = %e, "RMW read failed");
                    status = -(libc::EIO as i32);
                    break;
                }
            };

            block[offset_in_block..offset_in_block + write_len]
                .copy_from_slice(&req[buf_offset..buf_offset + write_len]);

            if let Err(e) = self.zone_manager.submit_write(
                &self.vol_id,
                block_lba,
                1,
                &block,
                self.vol_created_at,
            ) {
                tracing::error!(lba = block_lba.0, error = %e, "write failed");
                status = -(libc::EIO as i32);
                break;
            }

            buf_offset += write_len;
            cur_offset += write_len as u64;
            remaining -= write_len as u64;
        }

        status
    }

    fn handle_discard(&self, offset_bytes: u64, io_bytes: u64) -> i32 {
        let start_lba = Lba(offset_bytes / self.block_size);
        let lba_count = (io_bytes / self.block_size) as u32;
        if lba_count == 0 {
            return 0;
        }

        match self
            .zone_manager
            .submit_discard(&self.vol_id, start_lba, lba_count)
        {
            Ok(()) => io_bytes as i32,
            Err(e) => {
                tracing::error!(
                    lba = start_lba.0,
                    count = lba_count,
                    error = %e,
                    "discard failed"
                );
                -(libc::EIO as i32)
            }
        }
    }
}

fn eventfd_write(fd: RawFd) {
    let value = 1u64.to_ne_bytes();
    let rc = unsafe { libc::write(fd, value.as_ptr().cast(), value.len()) };
    if rc < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EAGAIN) {
            tracing::warn!(error = %err, "failed to wake ublk queue eventfd");
        }
    }
}

fn drain_eventfd(fd: RawFd) {
    loop {
        let mut value = 0u64;
        let rc = unsafe {
            libc::read(
                fd,
                (&mut value as *mut u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if rc == std::mem::size_of::<u64>() as isize {
            continue;
        }
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EAGAIN) {
                tracing::warn!(error = %err, "failed to drain ublk queue eventfd");
            }
        }
        break;
    }
}

fn submit_eventfd_poll(q: &UblkQueue, event_fd: RawFd, qid: u16) {
    let sqe = opcode::PollAdd::new(types::Fd(event_fd), libc::POLLIN as _)
        .build()
        .user_data(
            UblkUringData::Target as u64 | ((sys::UBLK_IO_OP_FLUSH as u64) << 16) | qid as u64,
        );
    if let Err(err) = q.ublk_submit_sqe_sync(sqe) {
        tracing::error!(error = ?err, "failed to submit ublk eventfd poll");
    }
}

fn record_completed_io_metrics(
    ctx: &IoWorkerContext,
    op: u32,
    res: i32,
    elapsed_ns: u64,
    queue_wait_ns: u64,
    worker_ns: u64,
    completion_wait_ns: u64,
) {
    if res <= 0 {
        return;
    }

    let bytes = res as u64;
    let metrics = ctx.zone_manager.metrics();
    match op {
        sys::UBLK_IO_OP_READ => {
            metrics.volume_read_ops.fetch_add(1, Ordering::Relaxed);
            metrics
                .volume_read_bytes
                .fetch_add(bytes, Ordering::Relaxed);
            metrics
                .volume_read_total_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            metrics
                .ublk_read_queue_wait_ns
                .fetch_add(queue_wait_ns, Ordering::Relaxed);
            metrics
                .ublk_read_worker_ns
                .fetch_add(worker_ns, Ordering::Relaxed);
            metrics
                .ublk_read_completion_wait_ns
                .fetch_add(completion_wait_ns, Ordering::Relaxed);
        }
        sys::UBLK_IO_OP_WRITE => {
            metrics.record_ublk_write_stages(queue_wait_ns, worker_ns, completion_wait_ns);
            metrics.volume_write_ops.fetch_add(1, Ordering::Relaxed);
            metrics
                .volume_write_bytes
                .fetch_add(bytes, Ordering::Relaxed);
            metrics
                .volume_write_total_ns
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            metrics
                .ublk_write_queue_wait_ns
                .fetch_add(queue_wait_ns, Ordering::Relaxed);
            metrics
                .ublk_write_worker_ns
                .fetch_add(worker_ns, Ordering::Relaxed);
            metrics
                .ublk_write_completion_wait_ns
                .fetch_add(completion_wait_ns, Ordering::Relaxed);
        }
        _ => {}
    }
}

fn spawn_queue_workers(
    qid: u16,
    workers: usize,
    ctx: IoWorkerContext,
    request_rx: Receiver<QueuedIo>,
    completion_tx: Sender<CompletedIo>,
    durability_tx: Sender<PendingDurableIo>,
    event_fd: RawFd,
) -> Vec<JoinHandle<()>> {
    (0..workers)
        .map(|worker_idx| {
            let rx = request_rx.clone();
            let tx = completion_tx.clone();
            let durable_tx = durability_tx.clone();
            let ctx = ctx.clone();
            thread::Builder::new()
                .name(format!("ublk-q{qid}-worker-{worker_idx}"))
                .spawn(move || {
                    // qid * workers + worker_idx: decodable back to qid for
                    // NUMA pod routing (a plain sum is lossy).
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::Ublk,
                        qid as usize * workers + worker_idx,
                    );
                    while let Ok(mut req) = rx.recv() {
                        let worker_start = Instant::now();
                        let queue_wait_ns = req.queued_at.elapsed().as_nanos() as u64;
                        let result = match &mut req.data {
                            QueuedIoData::Owned(data) => ctx.handle_io_deferred(
                                req.op,
                                req.start_sector,
                                req.nr_sectors,
                                data.as_mut_slice(),
                            ),
                            QueuedIoData::Direct { ptr, len } => {
                                let io_slice = unsafe {
                                    std::slice::from_raw_parts_mut(*ptr as *mut u8, *len)
                                };
                                ctx.handle_io_deferred(
                                    req.op,
                                    req.start_sector,
                                    req.nr_sectors,
                                    io_slice,
                                )
                            }
                        };
                        let worker_ns = worker_start.elapsed().as_nanos() as u64;
                        let res = match result {
                            IoWorkerResult::Complete(res) => res,
                            IoWorkerResult::AwaitDurability { res, tickets } => {
                                if durable_tx
                                    .send(PendingDurableIo {
                                        tag: req.tag,
                                        op: req.op,
                                        res,
                                        queued_at: req.queued_at,
                                        queue_wait_ns,
                                        worker_ns,
                                        tickets,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                                continue;
                            }
                        };
                        let completed = CompletedIo {
                            tag: req.tag,
                            op: req.op,
                            res,
                            elapsed_ns: req.queued_at.elapsed().as_nanos() as u64,
                            queue_wait_ns,
                            worker_ns,
                            completed_at: Instant::now(),
                        };
                        if tx.send(completed).is_err() {
                            break;
                        }
                        eventfd_write(event_fd);
                    }
                })
                .unwrap_or_else(|err| panic!("failed to spawn ublk worker: {err}"))
        })
        .collect()
}

fn spawn_durability_dispatcher(
    qid: u16,
    rx: Receiver<PendingDurableIo>,
    completion_tx: Sender<CompletedIo>,
    event_fd: RawFd,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("ublk-q{qid}-durable"))
        .spawn(move || {
            crate::affinity::bind_current(crate::affinity::ThreadRole::Ublk, qid as usize);
            let mut pending = Vec::<PendingDurableIo>::new();
            let mut input_open = true;
            while input_open || !pending.is_empty() {
                if pending.is_empty() {
                    match rx.recv() {
                        Ok(item) => pending.push(item),
                        Err(_) => input_open = false,
                    }
                } else {
                    match rx.recv_timeout(std::time::Duration::from_micros(25)) {
                        Ok(item) => pending.push(item),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            input_open = false
                        }
                    }
                }
                while let Ok(item) = rx.try_recv() {
                    pending.push(item);
                }

                let mut idx = 0;
                while idx < pending.len() {
                    if pending[idx]
                        .tickets
                        .iter()
                        .all(BufferAppendTicket::is_durable)
                    {
                        let item = pending.swap_remove(idx);
                        for ticket in item.tickets {
                            ticket.finish();
                        }
                        let completed = CompletedIo {
                            tag: item.tag,
                            op: item.op,
                            res: item.res,
                            elapsed_ns: item.queued_at.elapsed().as_nanos() as u64,
                            queue_wait_ns: item.queue_wait_ns,
                            worker_ns: item.worker_ns,
                            completed_at: Instant::now(),
                        };
                        if completion_tx.send(completed).is_err() {
                            return;
                        }
                        eventfd_write(event_fd);
                    } else {
                        idx += 1;
                    }
                }
            }
        })
        .unwrap_or_else(|err| panic!("failed to spawn ublk durability dispatcher: {err}"))
}

impl OnyxUblkTarget {
    pub fn new(
        config: &UblkConfig,
        zone_manager: Arc<ZoneManager>,
        vol: &VolumeConfig,
    ) -> OnyxResult<Self> {
        let vol_ord = zone_manager.volume_ordinal(&vol.id.0)?;
        Ok(Self {
            config: config.clone(),
            zone_manager,
            vol_id: vol.id.0.clone(),
            vol_ord,
            device_size_bytes: vol.size_bytes,
            vol_created_at: vol.created_at,
        })
    }

    /// Kill a running ublk device by its kernel device ID.
    /// Safe to call from any thread — creates a temporary control handle.
    pub fn kill_device(dev_id: u32) -> OnyxResult<()> {
        let ctrl = UblkCtrl::new_simple(dev_id as i32).map_err(|e| {
            OnyxError::Ublk(format!("failed to open ublk ctrl {}: {:?}", dev_id, e))
        })?;
        ctrl.kill_dev()
            .map_err(|e| OnyxError::Ublk(format!("failed to kill ublk dev {}: {:?}", dev_id, e)))?;
        Ok(())
    }

    /// Start the ublk device. Blocks until the device is stopped.
    /// Reports the kernel-assigned device ID via `dev_id_tx` before blocking.
    pub fn run(&self, dev_id_tx: Option<std::sync::mpsc::Sender<u32>>) -> OnyxResult<()> {
        let nr_queues = self.config.nr_queues;
        let depth = self.config.queue_depth;
        let io_buf_bytes = self.config.io_buf_bytes;
        let dev_size = self.device_size_bytes;
        let dev_name = format!("onyx-{}", self.vol_id);

        let sess = UblkCtrlBuilder::default()
            .name(&dev_name)
            .nr_queues(nr_queues)
            .depth(depth)
            .io_buf_bytes(io_buf_bytes)
            .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .map_err(|e| OnyxError::Ublk(format!("failed to create ublk ctrl: {:?}", e)))?;

        let tgt_init = move |dev: &mut UblkDev| {
            let info = dev.dev_info;
            dev.tgt.dev_size = dev_size;
            dev.tgt.params = sys::ublk_params {
                types: sys::UBLK_PARAM_TYPE_BASIC | sys::UBLK_PARAM_TYPE_DISCARD,
                basic: sys::ublk_param_basic {
                    attrs: sys::UBLK_ATTR_VOLATILE_CACHE,
                    logical_bs_shift: 9,
                    physical_bs_shift: 12,
                    io_opt_shift: 12,
                    io_min_shift: 12,
                    max_sectors: info.max_io_buf_bytes >> 9,
                    dev_sectors: dev_size >> 9,
                    ..Default::default()
                },
                discard: sys::ublk_param_discard {
                    discard_alignment: BLOCK_SIZE as u32,
                    discard_granularity: BLOCK_SIZE as u32,
                    max_discard_sectors: (2 * 1024 * 1024 / SECTOR_SIZE) as u32, // 2MB
                    max_write_zeroes_sectors: 0,
                    max_discard_segments: 1,
                    reserved0: 0,
                },
                ..Default::default()
            };
            Ok::<(), UblkError>(())
        };

        let worker_ctx = IoWorkerContext {
            zone_manager: self.zone_manager.clone(),
            vol_id: self.vol_id.clone(),
            vol_ord: self.vol_ord,
            vol_created_at: self.vol_created_at,
            block_size: BLOCK_SIZE as u64,
            sector_size: SECTOR_SIZE as u64,
        };
        let queue_workers = self.config.queue_workers.max(1);

        let q_handler = move |qid: u16, dev: &UblkDev| {
            crate::affinity::bind_current(
                crate::affinity::ThreadRole::Ublk,
                qid as usize * queue_workers,
            );
            let bufs = Rc::new(RefCell::new(dev.alloc_queue_io_bufs()));
            let io_bufs = bufs.clone();
            let queue_thread_bound = Rc::new(Cell::new(false));
            let (request_tx, request_rx) = crossbeam_channel::unbounded::<QueuedIo>();
            let (completion_tx, completion_rx) = crossbeam_channel::unbounded::<CompletedIo>();
            let (durability_tx, durability_rx) = crossbeam_channel::unbounded::<PendingDurableIo>();
            let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if event_fd < 0 {
                tracing::error!(
                    error = %std::io::Error::last_os_error(),
                    "failed to create ublk queue eventfd"
                );
                return;
            }
            let workers = queue_workers;
            let durability_handle =
                spawn_durability_dispatcher(qid, durability_rx, completion_tx.clone(), event_fd);
            let worker_handles = spawn_queue_workers(
                qid,
                workers,
                worker_ctx.clone(),
                request_rx,
                completion_tx,
                durability_tx.clone(),
                event_fd,
            );
            let submit_tx = request_tx.clone();
            let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
                if !queue_thread_bound.get() {
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::Ublk,
                        qid as usize * queue_workers,
                    );
                    queue_thread_bound.set(true);
                }

                let complete_ready = |drain_wakeup: bool, max_completions: Option<usize>| {
                    if drain_wakeup {
                        drain_eventfd(event_fd);
                    }
                    let bufs = io_bufs.borrow();
                    let mut completed = 0usize;
                    while let Ok(done) = completion_rx.try_recv() {
                        let completion_wait_ns = done.completed_at.elapsed().as_nanos() as u64;
                        let elapsed_ns = done.elapsed_ns.saturating_add(completion_wait_ns);
                        record_completed_io_metrics(
                            &worker_ctx,
                            done.op,
                            done.res,
                            elapsed_ns,
                            done.queue_wait_ns,
                            done.worker_ns,
                            completion_wait_ns,
                        );

                        if let Err(err) = q.complete_io_cmd_unified(
                            done.tag,
                            BufDesc::Slice(bufs[done.tag as usize].as_slice()),
                            Ok(UblkIORes::Result(done.res)),
                        ) {
                            tracing::error!(error = ?err, "ublk completion failed");
                        }
                        completed += 1;
                        if max_completions.is_some_and(|limit| completed >= limit) {
                            break;
                        }
                    }
                };

                if tag == qid && _io.is_tgt_io() {
                    complete_ready(true, None);
                    submit_eventfd_poll(q, event_fd, qid);
                    return;
                }
                complete_ready(false, Some(OPPORTUNISTIC_COMPLETION_DRAIN_MAX));
                let iod = q.get_iod(tag);
                let queued_at = Instant::now();
                let op = iod.op_flags & 0xFF;
                let start_sector = iod.start_sector;
                let nr_sectors = iod.nr_sectors;
                let io_bytes = nr_sectors as u64 * SECTOR_SIZE as u64;
                let io_len = io_bytes as usize;
                if matches!(op, sys::UBLK_IO_OP_READ | sys::UBLK_IO_OP_WRITE) {
                    let mut bufs = io_bufs.borrow_mut();
                    let io_buf = &mut bufs[tag as usize];
                    let io_slice = io_buf.as_mut_slice();
                    if io_len > io_slice.len() {
                        tracing::error!(
                            tag,
                            io_len,
                            buf_len = io_slice.len(),
                            "ublk request exceeds queue buffer size"
                        );
                        if let Err(err) = q.complete_io_cmd_unified(
                            tag,
                            BufDesc::Slice(io_buf.as_slice()),
                            Ok(UblkIORes::Result(-(libc::EIO as i32))),
                        ) {
                            tracing::error!(error = ?err, "ublk completion failed");
                        }
                        return;
                    }

                    let data = if op == sys::UBLK_IO_OP_WRITE {
                        QueuedIoData::Owned(io_slice[..io_len].to_vec())
                    } else {
                        QueuedIoData::Direct {
                            ptr: io_slice.as_mut_ptr() as usize,
                            len: io_len,
                        }
                    };

                    let queued = QueuedIo {
                        tag,
                        op,
                        start_sector,
                        nr_sectors,
                        data,
                        queued_at,
                    };
                    drop(bufs);
                    if submit_tx.send(queued).is_err() {
                        tracing::warn!(qid, tag, "ublk queue workers stopped");
                        let bufs = io_bufs.borrow();
                        if let Err(err) = q.complete_io_cmd_unified(
                            tag,
                            BufDesc::Slice(bufs[tag as usize].as_slice()),
                            Ok(UblkIORes::Result(-(libc::EIO as i32))),
                        ) {
                            tracing::error!(error = ?err, "ublk completion failed");
                        }
                    }
                    return;
                }

                let queued = QueuedIo {
                    tag,
                    op,
                    start_sector,
                    nr_sectors,
                    data: QueuedIoData::Owned(Vec::new()),
                    queued_at,
                };
                if submit_tx.send(queued).is_err() {
                    tracing::warn!(qid, tag, "ublk queue workers stopped");
                    let bufs = io_bufs.borrow();
                    if let Err(err) = q.complete_io_cmd_unified(
                        tag,
                        BufDesc::Slice(bufs[tag as usize].as_slice()),
                        Ok(UblkIORes::Result(-(libc::EIO as i32))),
                    ) {
                        tracing::error!(error = ?err, "ublk completion failed");
                    }
                }
            };

            let queue = match {
                let bufs = bufs.borrow();
                UblkQueue::new(qid, dev)
                    .unwrap()
                    .submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs)))
            } {
                Ok(q) => q,
                Err(e) => {
                    tracing::error!(error = ?e, "submit_fetch_commands_unified failed");
                    return;
                }
            };

            submit_eventfd_poll(&queue, event_fd, qid);
            queue.wait_and_handle_io(io_handler);
            drop(request_tx);
            for handle in worker_handles {
                let _ = handle.join();
            }
            drop(durability_tx);
            let _ = durability_handle.join();
            unsafe {
                libc::close(event_fd);
            }
        };

        let dev_handler = move |ctrl: &UblkCtrl| {
            let id = ctrl.dev_info().dev_id;
            tracing::info!(dev_id = id, "ublk device ready");
            if let Some(tx) = dev_id_tx {
                let _ = tx.send(id);
            }
        };

        sess.run_target(tgt_init, q_handler, dev_handler)
            .map_err(|e| OnyxError::Ublk(format!("ublk run_target failed: {:?}", e)))?;

        Ok(())
    }
}
