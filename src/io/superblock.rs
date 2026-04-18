use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::device::RawDevice;
use crate::io::uring::{IoUringSession, UringOp};
use crate::types::{BLOCK_SIZE, RESERVED_BLOCKS};

// ──────────────────────────── Constants ────────────────────────────

pub const DATA_SUPERBLOCK_MAGIC: u32 = 0x4F4C_5633; // "OLV3"
pub const DATA_SUPERBLOCK_VERSION: u32 = 1;

/// `DataSuperblock::flags` bit: set when the engine has performed a graceful
/// shutdown (flusher drained, cleanup_tx empty, metadata consistent). Absence
/// on startup triggers the dirty-recovery path: scan the per-volume blockmap
/// CFs and rebuild RocksDB refcount from ground truth.
pub const FLAG_CLEAN_SHUTDOWN: u64 = 1 << 0;

pub const HEARTBEAT_MAGIC: u32 = 0x4F48_4254; // "OHBT"
pub const HEARTBEAT_VERSION: u32 = 1;

pub const HA_LOCK_MAGIC: u32 = 0x4F48_414C; // "OHAL"
pub const HA_LOCK_VERSION: u32 = 1;

const SUPERBLOCK_HEADER_SIZE: usize = 64;
const HEARTBEAT_HEADER_SIZE: usize = 36;
const HA_LOCK_HEADER_SIZE: usize = 44;

// ──────────────────────────── DataSuperblock ────────────────────────────

/// LV3 data device superblock (block 0, 4096 bytes).
///
/// Layout (little-endian):
///    0.. 4: magic (0x4F4C_5633)
///    4.. 8: version (1)
///    8..24: device_uuid (16 bytes, v4 random)
///   24..32: device_size_bytes
///   32..40: creation_timestamp (epoch nanos)
///   40..44: reserved_blocks (currently 8)
///   44..48: crc32 (over bytes 0..44 + 48..64 with CRC zeroed)
///   48..56: format_timestamp (epoch nanos)
///   56..64: flags (reserved, 0)
///   64..4096: zero
#[derive(Debug, Clone, Copy)]
pub struct DataSuperblock {
    pub magic: u32,
    pub version: u32,
    pub device_uuid: [u8; 16],
    pub device_size_bytes: u64,
    pub creation_timestamp: u64,
    pub reserved_blocks: u32,
    pub crc32: u32,
    pub format_timestamp: u64,
    pub flags: u64,
}

impl DataSuperblock {
    /// Create a new superblock for a fresh device.
    pub fn new(device_size_bytes: u64) -> Self {
        let now = current_time_nanos();
        let uuid = generate_uuid_v4();
        let mut sb = Self {
            magic: DATA_SUPERBLOCK_MAGIC,
            version: DATA_SUPERBLOCK_VERSION,
            device_uuid: uuid,
            device_size_bytes,
            creation_timestamp: now,
            reserved_blocks: RESERVED_BLOCKS as u32,
            crc32: 0,
            format_timestamp: now,
            flags: 0,
        };
        sb.update_crc();
        sb
    }

    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..24].copy_from_slice(&self.device_uuid);
        buf[24..32].copy_from_slice(&self.device_size_bytes.to_le_bytes());
        buf[32..40].copy_from_slice(&self.creation_timestamp.to_le_bytes());
        buf[40..44].copy_from_slice(&self.reserved_blocks.to_le_bytes());
        buf[44..48].copy_from_slice(&self.crc32.to_le_bytes());
        buf[48..56].copy_from_slice(&self.format_timestamp.to_le_bytes());
        buf[56..64].copy_from_slice(&self.flags.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4096]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != DATA_SUPERBLOCK_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != DATA_SUPERBLOCK_VERSION {
            return None;
        }

        let mut crc_buf = [0u8; SUPERBLOCK_HEADER_SIZE];
        crc_buf.copy_from_slice(&buf[0..SUPERBLOCK_HEADER_SIZE]);
        crc_buf[44..48].fill(0); // zero CRC field
        let expected_crc = crc32fast::hash(&crc_buf);

        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&buf[8..24]);

        let sb = Self {
            magic,
            version,
            device_uuid: uuid,
            device_size_bytes: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            creation_timestamp: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            reserved_blocks: u32::from_le_bytes(buf[40..44].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
            format_timestamp: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
            flags: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
        };
        if sb.crc32 != expected_crc {
            return None;
        }
        Some(sb)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; SUPERBLOCK_HEADER_SIZE];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..24].copy_from_slice(&self.device_uuid);
        tmp[24..32].copy_from_slice(&self.device_size_bytes.to_le_bytes());
        tmp[32..40].copy_from_slice(&self.creation_timestamp.to_le_bytes());
        tmp[40..44].copy_from_slice(&self.reserved_blocks.to_le_bytes());
        // 44..48 stays zero (CRC slot)
        tmp[48..56].copy_from_slice(&self.format_timestamp.to_le_bytes());
        tmp[56..64].copy_from_slice(&self.flags.to_le_bytes());
        self.crc32 = crc32fast::hash(&tmp);
    }

    /// Test whether the previous run exited via graceful shutdown.
    pub fn is_clean_shutdown(&self) -> bool {
        self.flags & FLAG_CLEAN_SHUTDOWN != 0
    }

    /// Flip the `FLAG_CLEAN_SHUTDOWN` bit. Caller must follow with
    /// `update_crc()` + `write_superblock()` for the change to be durable.
    pub fn set_clean_shutdown(&mut self, clean: bool) {
        if clean {
            self.flags |= FLAG_CLEAN_SHUTDOWN;
        } else {
            self.flags &= !FLAG_CLEAN_SHUTDOWN;
        }
    }

    /// Format UUID as standard hex string (e.g. "550e8400-e29b-41d4-a716-446655440000").
    pub fn uuid_string(&self) -> String {
        let u = &self.device_uuid;
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7],
            u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15],
        )
    }
}

// ──────────────────────────── HeartbeatBlock ────────────────────────────

/// Heartbeat block (block 1, 4096 bytes). Written periodically by the active node.
///
/// Layout (little-endian):
///    0.. 4: magic (0x4F48_4254)
///    4.. 8: version (1)
///    8..16: timestamp_nanos (epoch nanos)
///   16..24: node_id
///   24..32: sequence (monotonically increasing)
///   32..36: crc32 (over bytes 0..32 with CRC zeroed)
///   36..4096: zero
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatBlock {
    pub magic: u32,
    pub version: u32,
    pub timestamp_nanos: u64,
    pub node_id: u64,
    pub sequence: u64,
    pub crc32: u32,
}

impl HeartbeatBlock {
    pub fn new(node_id: u64, sequence: u64) -> Self {
        let mut hb = Self {
            magic: HEARTBEAT_MAGIC,
            version: HEARTBEAT_VERSION,
            timestamp_nanos: current_time_nanos(),
            node_id,
            sequence,
            crc32: 0,
        };
        hb.update_crc();
        hb
    }

    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.timestamp_nanos.to_le_bytes());
        buf[16..24].copy_from_slice(&self.node_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.sequence.to_le_bytes());
        buf[32..36].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4096]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != HEARTBEAT_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != HEARTBEAT_VERSION {
            return None;
        }

        let mut crc_buf = [0u8; HEARTBEAT_HEADER_SIZE];
        crc_buf.copy_from_slice(&buf[0..HEARTBEAT_HEADER_SIZE]);
        crc_buf[32..36].fill(0);
        let expected_crc = crc32fast::hash(&crc_buf);

        let hb = Self {
            magic,
            version,
            timestamp_nanos: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            node_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            sequence: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
        };
        if hb.crc32 != expected_crc {
            return None;
        }
        Some(hb)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; HEARTBEAT_HEADER_SIZE];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..16].copy_from_slice(&self.timestamp_nanos.to_le_bytes());
        tmp[16..24].copy_from_slice(&self.node_id.to_le_bytes());
        tmp[24..32].copy_from_slice(&self.sequence.to_le_bytes());
        // 32..36 stays zero (CRC slot)
        self.crc32 = crc32fast::hash(&tmp);
    }

    /// Seconds elapsed since this heartbeat was written.
    pub fn age_secs(&self) -> f64 {
        let now = current_time_nanos();
        (now.saturating_sub(self.timestamp_nanos)) as f64 / 1_000_000_000.0
    }
}

// ──────────────────────────── HaLockBlock ────────────────────────────

/// HA lock block (block 2, 4096 bytes). Written when a node acquires ownership.
///
/// Layout (little-endian):
///    0.. 4: magic (0x4F48_414C)
///    4.. 8: version (1)
///    8..16: owner_node_id
///   16..24: fence_token (monotonically increasing, prevents split-brain)
///   24..32: lease_start_nanos (epoch nanos)
///   32..40: lease_duration_nanos
///   40..44: crc32 (over bytes 0..40 with CRC zeroed)
///   44..4096: zero
#[derive(Debug, Clone, Copy)]
pub struct HaLockBlock {
    pub magic: u32,
    pub version: u32,
    pub owner_node_id: u64,
    pub fence_token: u64,
    pub lease_start_nanos: u64,
    pub lease_duration_nanos: u64,
    pub crc32: u32,
}

impl HaLockBlock {
    pub fn new(owner_node_id: u64, fence_token: u64, lease_duration: Duration) -> Self {
        let mut lock = Self {
            magic: HA_LOCK_MAGIC,
            version: HA_LOCK_VERSION,
            owner_node_id,
            fence_token,
            lease_start_nanos: current_time_nanos(),
            lease_duration_nanos: lease_duration.as_nanos() as u64,
            crc32: 0,
        };
        lock.update_crc();
        lock
    }

    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.owner_node_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.fence_token.to_le_bytes());
        buf[24..32].copy_from_slice(&self.lease_start_nanos.to_le_bytes());
        buf[32..40].copy_from_slice(&self.lease_duration_nanos.to_le_bytes());
        buf[40..44].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 4096]) -> Option<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != HA_LOCK_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != HA_LOCK_VERSION {
            return None;
        }

        let mut crc_buf = [0u8; HA_LOCK_HEADER_SIZE];
        crc_buf.copy_from_slice(&buf[0..HA_LOCK_HEADER_SIZE]);
        crc_buf[40..44].fill(0);
        let expected_crc = crc32fast::hash(&crc_buf);

        let lock = Self {
            magic,
            version,
            owner_node_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            fence_token: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            lease_start_nanos: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            lease_duration_nanos: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            crc32: u32::from_le_bytes(buf[40..44].try_into().unwrap()),
        };
        if lock.crc32 != expected_crc {
            return None;
        }
        Some(lock)
    }

    pub fn update_crc(&mut self) {
        let mut tmp = [0u8; HA_LOCK_HEADER_SIZE];
        tmp[0..4].copy_from_slice(&self.magic.to_le_bytes());
        tmp[4..8].copy_from_slice(&self.version.to_le_bytes());
        tmp[8..16].copy_from_slice(&self.owner_node_id.to_le_bytes());
        tmp[16..24].copy_from_slice(&self.fence_token.to_le_bytes());
        tmp[24..32].copy_from_slice(&self.lease_start_nanos.to_le_bytes());
        tmp[32..40].copy_from_slice(&self.lease_duration_nanos.to_le_bytes());
        // 40..44 stays zero (CRC slot)
        self.crc32 = crc32fast::hash(&tmp);
    }

    /// Whether the lease has expired based on current time.
    pub fn is_expired(&self) -> bool {
        let now = current_time_nanos();
        now > self
            .lease_start_nanos
            .saturating_add(self.lease_duration_nanos)
    }

    /// Remaining lease time. Returns zero if expired.
    pub fn remaining(&self) -> Duration {
        let now = current_time_nanos();
        let expiry = self
            .lease_start_nanos
            .saturating_add(self.lease_duration_nanos);
        Duration::from_nanos(expiry.saturating_sub(now))
    }
}

// ──────────────────────────── Device I/O helpers ────────────────────────────

/// Read and validate the DataSuperblock from block 0.
/// Returns `Ok(None)` if block 0 is all zeros (fresh device) or has invalid magic/CRC.
pub fn read_superblock(device: &RawDevice) -> OnyxResult<Option<DataSuperblock>> {
    let min_size = RESERVED_BLOCKS * BLOCK_SIZE as u64;
    if device.size() < min_size {
        return Err(OnyxError::Config(format!(
            "LV3 device too small: {} bytes < {} bytes minimum (need {} reserved blocks)",
            device.size(),
            min_size,
            RESERVED_BLOCKS,
        )));
    }

    let mut buf = [0u8; 4096];
    device.read_at(&mut buf, 0)?;
    Ok(DataSuperblock::from_bytes(&buf))
}

/// Write a DataSuperblock to block 0.
pub fn write_superblock(device: &RawDevice, sb: &DataSuperblock) -> OnyxResult<()> {
    let bytes = sb.to_bytes();
    let mut aligned = AlignedBuf::new(BLOCK_SIZE as usize, false)?;
    aligned.as_mut_slice().copy_from_slice(&bytes);
    device.write_at(aligned.as_slice(), 0)?;
    device.sync()?;
    Ok(())
}

/// Format a fresh LV3 device: write superblock to block 0, zero blocks 1-7.
pub fn format_device(device: &RawDevice) -> OnyxResult<DataSuperblock> {
    let sb = DataSuperblock::new(device.size());

    // Write superblock (block 0)
    write_superblock(device, &sb)?;

    // Zero blocks 1 through RESERVED_BLOCKS-1
    let zero_bytes = ((RESERVED_BLOCKS - 1) * BLOCK_SIZE as u64) as usize;
    let mut zeros = AlignedBuf::new(zero_bytes, false)?;
    // AlignedBuf is already zeroed
    let _ = zeros.as_mut_slice(); // ensure mutable access
    device.write_at(zeros.as_slice(), BLOCK_SIZE as u64)?;
    device.sync()?;

    tracing::info!(
        uuid = sb.uuid_string(),
        device_size = sb.device_size_bytes,
        reserved_blocks = sb.reserved_blocks,
        "LV3 device formatted"
    );

    Ok(sb)
}

/// Read the heartbeat block from block 1.
pub fn read_heartbeat(device: &RawDevice) -> OnyxResult<Option<HeartbeatBlock>> {
    let mut buf = [0u8; 4096];
    device.read_at(&mut buf, BLOCK_SIZE as u64)?;
    Ok(HeartbeatBlock::from_bytes(&buf))
}

/// Read the HA lock block from block 2.
pub fn read_ha_lock(device: &RawDevice) -> OnyxResult<Option<HaLockBlock>> {
    let mut buf = [0u8; 4096];
    device.read_at(&mut buf, 2 * BLOCK_SIZE as u64)?;
    Ok(HaLockBlock::from_bytes(&buf))
}

/// Write the HA lock block to block 2.
pub fn write_ha_lock(device: &RawDevice, lock: &HaLockBlock) -> OnyxResult<()> {
    let bytes = lock.to_bytes();
    let mut aligned = AlignedBuf::new(BLOCK_SIZE as usize, false)?;
    aligned.as_mut_slice().copy_from_slice(&bytes);
    device.write_at(aligned.as_slice(), 2 * BLOCK_SIZE as u64)?;
    device.sync()?;
    Ok(())
}

// ──────────────────────────── HeartbeatWriter ────────────────────────────

/// Background thread that periodically writes a heartbeat block to LV3 block 1.
pub struct HeartbeatWriter {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HeartbeatWriter {
    /// Start a heartbeat writer that uses classic pwrite + fsync.
    pub fn start(device: RawDevice, node_id: u64, interval: Duration) -> Self {
        Self::start_inner(device, node_id, interval, None)
    }

    /// Start a heartbeat writer that submits write+fdatasync as one io_uring batch.
    pub fn start_uring(
        device: RawDevice,
        node_id: u64,
        interval: Duration,
        session: Arc<IoUringSession>,
    ) -> Self {
        Self::start_inner(device, node_id, interval, Some(session))
    }

    fn start_inner(
        device: RawDevice,
        node_id: u64,
        interval: Duration,
        session: Option<Arc<IoUringSession>>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        let handle = thread::Builder::new()
            .name("heartbeat-writer".into())
            .spawn(move || {
                Self::heartbeat_loop(&device, node_id, interval, &running_clone, session.as_ref());
            })
            .expect("failed to spawn heartbeat writer thread");

        Self {
            running,
            handle: Some(handle),
        }
    }

    fn heartbeat_loop(
        device: &RawDevice,
        node_id: u64,
        interval: Duration,
        running: &AtomicBool,
        session: Option<&Arc<IoUringSession>>,
    ) {
        let sequence = AtomicU64::new(1);

        // Pre-allocate aligned buffer for the heartbeat block
        let mut aligned = match AlignedBuf::new(BLOCK_SIZE as usize, false) {
            Ok(buf) => buf,
            Err(e) => {
                tracing::error!(error = %e, "heartbeat: failed to allocate aligned buffer");
                return;
            }
        };

        while running.load(Ordering::Relaxed) {
            let seq = sequence.fetch_add(1, Ordering::Relaxed);
            let hb = HeartbeatBlock::new(node_id, seq);
            let bytes = hb.to_bytes();
            aligned.as_mut_slice().copy_from_slice(&bytes);

            if let Some(session) = session {
                let fd = device.as_raw_fd();
                let offset = device.base_offset() + BLOCK_SIZE as u64;
                let ops = [
                    UringOp::Write {
                        fd,
                        ptr: aligned.as_ptr(),
                        len: BLOCK_SIZE,
                        offset,
                    },
                    UringOp::FsyncDataBarrier { fd },
                ];
                match unsafe { session.submit_batch(&ops) } {
                    Ok(results) => {
                        let write_ok = results[0].bytes() == Some(BLOCK_SIZE);
                        if !write_ok {
                            tracing::warn!(
                                errno = ?results[0].errno(),
                                bytes = ?results[0].bytes(),
                                "heartbeat: io_uring write failed"
                            );
                        } else if let Some(errno) = results[1].errno() {
                            tracing::warn!(errno, "heartbeat: io_uring fdatasync failed");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "heartbeat: io_uring submit failed");
                    }
                }
            } else if let Err(e) = device.write_at(aligned.as_slice(), BLOCK_SIZE as u64) {
                tracing::warn!(error = %e, "heartbeat: write failed");
            } else if let Err(e) = device.sync() {
                tracing::warn!(error = %e, "heartbeat: sync failed");
            }

            // Sleep in short increments to allow fast shutdown
            let mut remaining = interval;
            while remaining > Duration::ZERO && running.load(Ordering::Relaxed) {
                let sleep_step = remaining.min(Duration::from_millis(500));
                thread::sleep(sleep_step);
                remaining = remaining.saturating_sub(sleep_step);
            }
        }

        tracing::info!("heartbeat writer stopped");
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HeartbeatWriter {
    fn drop(&mut self) {
        self.stop();
    }
}

// ──────────────────────────── Utilities ────────────────────────────

fn current_time_nanos() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

/// Generate a UUID v4 (random) by reading 16 bytes from /dev/urandom.
fn generate_uuid_v4() -> [u8; 16] {
    let mut uuid = [0u8; 16];
    // Read from /dev/urandom
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut uuid);
    } else {
        // Fallback: use timestamp-based pseudo-random
        let now = current_time_nanos();
        uuid[0..8].copy_from_slice(&now.to_le_bytes());
        let pid = std::process::id();
        uuid[8..12].copy_from_slice(&pid.to_le_bytes());
        let thread_id = format!("{:?}", std::thread::current().id());
        let hash = crc32fast::hash(thread_id.as_bytes());
        uuid[12..16].copy_from_slice(&hash.to_le_bytes());
    }

    // Set version 4 and variant bits per RFC 4122
    uuid[6] = (uuid[6] & 0x0F) | 0x40; // version 4
    uuid[8] = (uuid[8] & 0x3F) | 0x80; // variant 1

    uuid
}
