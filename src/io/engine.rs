use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::io::device::RawDevice;
use crate::types::{Pba, BLOCK_SIZE, RESERVED_BLOCKS};

/// IO engine for reading/writing raw data blocks on LV3.
///
/// LV3 slots are pure payload — no on-disk header. All metadata (compression,
/// crc32, original_size) lives in RocksDB BlockmapValue, which is crash-consistent
/// via WriteBatch. This allows a full 4096-byte payload per slot.
///
/// PBA addresses are translated by `pba_offset` (default `RESERVED_BLOCKS`)
/// so that PBA 0 from the allocator maps to device offset `pba_offset * BLOCK_SIZE`.
/// Blocks 0..pba_offset are reserved for superblock, heartbeat, and HA lock.
pub struct IoEngine {
    data_device: RawDevice,
    use_hugepages: bool,
    block_size: u32,
    pba_offset: u64,
}

impl IoEngine {
    /// Create an IoEngine with standard PBA offset (RESERVED_BLOCKS).
    /// PBA 0 maps to device offset `RESERVED_BLOCKS * BLOCK_SIZE`.
    pub fn new(data_device: RawDevice, use_hugepages: bool) -> Self {
        Self {
            data_device,
            use_hugepages,
            block_size: BLOCK_SIZE,
            pba_offset: RESERVED_BLOCKS,
        }
    }

    /// Create an IoEngine without PBA offset (PBA 0 = device offset 0).
    /// For testing only — production code should use `new()`.
    pub fn new_raw(data_device: RawDevice, use_hugepages: bool) -> Self {
        Self {
            data_device,
            use_hugepages,
            block_size: BLOCK_SIZE,
            pba_offset: 0,
        }
    }

    /// Write raw payload to LV3 at the given PBA slot.
    /// Payload is zero-padded to BLOCK_SIZE if shorter.
    pub fn write_block(&self, pba: Pba, payload: &[u8]) -> OnyxResult<()> {
        if payload.len() > self.block_size as usize {
            return Err(OnyxError::Compress(format!(
                "payload too large: {} > {}",
                payload.len(),
                self.block_size
            )));
        }

        let offset = (pba.0 + self.pba_offset) * self.block_size as u64;
        let mut buf = AlignedBuf::new(self.block_size as usize, self.use_hugepages)?;
        let slice = buf.as_mut_slice();
        slice[..payload.len()].copy_from_slice(payload);
        // Rest is already zero (AlignedBuf is zeroed)

        self.data_device.write_at(buf.as_slice(), offset)?;
        Ok(())
    }

    /// Read raw payload from LV3 at the given PBA slot.
    /// Returns exactly `size` bytes (must be <= BLOCK_SIZE).
    pub fn read_block(&self, pba: Pba, size: usize) -> OnyxResult<Vec<u8>> {
        if size > self.block_size as usize {
            return Err(OnyxError::Compress(format!(
                "requested read size {} > block_size {}",
                size, self.block_size
            )));
        }

        let offset = (pba.0 + self.pba_offset) * self.block_size as u64;
        let mut buf = AlignedBuf::new(self.block_size as usize, self.use_hugepages)?;
        self.data_device.read_at(buf.as_mut_slice(), offset)?;

        Ok(buf.as_slice()[..size].to_vec())
    }

    /// Write payload spanning multiple contiguous 4KB slots starting at `pba`.
    /// Last slot is zero-padded to BLOCK_SIZE.
    pub fn write_blocks(&self, pba: Pba, payload: &[u8]) -> OnyxResult<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let bs = self.block_size as usize;
        let total_size = ((payload.len() + bs - 1) / bs) * bs; // round up
        let offset = (pba.0 + self.pba_offset) * self.block_size as u64;

        let mut buf = AlignedBuf::new(total_size, self.use_hugepages)?;
        let slice = buf.as_mut_slice();
        slice[..payload.len()].copy_from_slice(payload);
        // Padding is already zero

        self.data_device.write_at(buf.as_slice(), offset)?;
        Ok(())
    }

    /// Read `size` bytes spanning multiple contiguous 4KB slots starting at `pba`.
    pub fn read_blocks(&self, pba: Pba, size: usize) -> OnyxResult<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let bs = self.block_size as usize;
        let read_size = ((size + bs - 1) / bs) * bs; // round up to block boundary
        let offset = (pba.0 + self.pba_offset) * self.block_size as u64;

        let mut buf = AlignedBuf::new(read_size, self.use_hugepages)?;
        self.data_device.read_at(buf.as_mut_slice(), offset)?;

        Ok(buf.as_slice()[..size].to_vec())
    }

    pub fn device_size(&self) -> u64 {
        self.data_device.size()
    }

    pub fn total_blocks(&self) -> u64 {
        self.data_device.size() / self.block_size as u64 - self.pba_offset
    }

    pub fn sync(&self) -> OnyxResult<()> {
        self.data_device.sync()
    }
}
