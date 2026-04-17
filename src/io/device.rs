use std::fs::{File, OpenOptions};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::error::{OnyxError, OnyxResult};
use crate::io::aligned::AlignedBuf;
use crate::types::BLOCK_SIZE;

/// Raw device/file handle for O_DIRECT IO
pub struct RawDevice {
    file: File,
    size_bytes: u64,
    base_offset: u64,
    path: PathBuf,
    direct_io: bool,
}

impl RawDevice {
    fn log_direct_fallback(&self, op: &str, offset: u64, len: usize, reason: &str) {
        tracing::warn!(
            op,
            path = %self.path.display(),
            base_offset = self.base_offset,
            global_offset = offset,
            len,
            reason,
            "direct-io device falling back to buffered io"
        );
    }

    /// Open a device or file for O_DIRECT read/write.
    /// Falls back to buffered IO if O_DIRECT is not supported (e.g., regular files in tests).
    pub fn open(path: &Path) -> OnyxResult<Self> {
        let (file, direct_io) = Self::open_direct(path)?;
        let size_bytes = Self::get_size(&file, path)?;

        Ok(Self {
            file,
            size_bytes,
            base_offset: 0,
            path: path.to_path_buf(),
            direct_io,
        })
    }

    /// Open a file, create if it doesn't exist, with a specified size.
    /// Useful for testing with regular files.
    pub fn open_or_create(path: &Path, size_bytes: u64) -> OnyxResult<Self> {
        if !path.exists() {
            let f = File::create(path).map_err(|e| OnyxError::Device {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
            f.set_len(size_bytes).map_err(|e| OnyxError::Device {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
        }

        let (file, direct_io) = Self::open_direct(path)?;

        Ok(Self {
            file,
            size_bytes,
            base_offset: 0,
            path: path.to_path_buf(),
            direct_io,
        })
    }

    fn open_direct(path: &Path) -> OnyxResult<(File, bool)> {
        let metadata = std::fs::metadata(path).map_err(|e| OnyxError::Device {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;

        if !metadata.file_type().is_file() {
            if let Ok(f) = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
            {
                return Ok((f, true));
            }
        }

        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| OnyxError::Device {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
        Ok((f, false))
    }

    fn get_size(file: &File, path: &Path) -> OnyxResult<u64> {
        let metadata = file.metadata().map_err(|e| OnyxError::Device {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;

        if metadata.file_type().is_file() {
            Ok(metadata.len())
        } else {
            // Block device — use seek to end
            use std::io::Seek;
            let mut f = file.try_clone().map_err(|e| OnyxError::Device {
                path: path.to_path_buf(),
                reason: e.to_string(),
            })?;
            f.seek(std::io::SeekFrom::End(0))
                .map_err(|e| OnyxError::Device {
                    path: path.to_path_buf(),
                    reason: e.to_string(),
                })
        }
    }

    /// Read exactly `buf.len()` bytes at the given offset.
    /// Loops on short reads to guarantee the full buffer is filled.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        let offset = self.translate_offset(offset, buf.len())?;
        if self.direct_io {
            if !Self::is_direct_io_offset_aligned(offset)
                || !Self::is_direct_io_len_aligned(buf.len())
            {
                self.log_direct_fallback("read", offset, buf.len(), "unaligned offset or length");
                return self.buffered_read_at(buf, offset);
            }
            if !Self::is_direct_io_ptr_aligned(buf.as_ptr()) {
                let mut aligned = AlignedBuf::new(buf.len(), false)?;
                self.read_exact_file(
                    self.file.try_clone().map_err(|e| OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!("clone failed: {}", e),
                    })?,
                    aligned.as_mut_slice(),
                    offset,
                )?;
                buf.copy_from_slice(&aligned.as_slice()[..buf.len()]);
                return Ok(());
            }
        }

        self.read_exact_file(
            self.file.try_clone().map_err(|e| OnyxError::Device {
                path: self.path.clone(),
                reason: format!("clone failed: {}", e),
            })?,
            buf,
            offset,
        )
    }

    /// Write exactly `buf.len()` bytes at the given offset.
    /// Loops on short writes to guarantee the full buffer is written.
    pub fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        let offset = self.translate_offset(offset, buf.len())?;
        if self.direct_io {
            if !Self::is_direct_io_offset_aligned(offset)
                || !Self::is_direct_io_len_aligned(buf.len())
            {
                self.log_direct_fallback("write", offset, buf.len(), "unaligned offset or length");
                return self.buffered_write_at(buf, offset);
            }
            if !Self::is_direct_io_ptr_aligned(buf.as_ptr()) {
                let mut aligned = AlignedBuf::new(buf.len(), false)?;
                aligned.as_mut_slice()[..buf.len()].copy_from_slice(buf);
                return self.write_exact_file(
                    self.file.try_clone().map_err(|e| OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!("clone failed: {}", e),
                    })?,
                    aligned.as_slice(),
                    offset,
                );
            }
        }

        self.write_exact_file(
            self.file.try_clone().map_err(|e| OnyxError::Device {
                path: self.path.clone(),
                reason: format!("clone failed: {}", e),
            })?,
            buf,
            offset,
        )
    }

    fn read_exact_file(&self, file: File, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        let mut done = 0usize;
        while done < buf.len() {
            match file.read_at(&mut buf[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!(
                            "read_at offset={}: unexpected EOF after {} of {} bytes",
                            offset,
                            done,
                            buf.len()
                        ),
                    });
                }
                Ok(n) => done += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!("read_at offset={}: {}", offset, e),
                    });
                }
            }
        }
        Ok(())
    }

    fn write_exact_file(&self, file: File, buf: &[u8], offset: u64) -> OnyxResult<()> {
        let mut done = 0usize;
        while done < buf.len() {
            match file.write_at(&buf[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!(
                            "write_at offset={}: zero-length write after {} of {} bytes",
                            offset,
                            done,
                            buf.len()
                        ),
                    });
                }
                Ok(n) => done += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(OnyxError::Device {
                        path: self.path.clone(),
                        reason: format!("write_at offset={}: {}", offset, e),
                    });
                }
            }
        }
        Ok(())
    }

    fn buffered_handle(&self) -> OnyxResult<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|e| OnyxError::Device {
                path: self.path.clone(),
                reason: e.to_string(),
            })
    }

    fn reopen_with_same_mode(&self) -> OnyxResult<File> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if self.direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }
        opts.open(&self.path).map_err(|e| OnyxError::Device {
            path: self.path.clone(),
            reason: e.to_string(),
        })
    }

    fn buffered_read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        self.read_exact_file(self.buffered_handle()?, buf, offset)
    }

    fn buffered_write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        self.write_exact_file(self.buffered_handle()?, buf, offset)
    }

    fn is_direct_io_offset_aligned(offset: u64) -> bool {
        offset % BLOCK_SIZE as u64 == 0
    }

    fn is_direct_io_len_aligned(len: usize) -> bool {
        len % BLOCK_SIZE as usize == 0
    }

    fn is_direct_io_ptr_aligned(ptr: *const u8) -> bool {
        (ptr as usize) % BLOCK_SIZE as usize == 0
    }

    pub fn sync(&self) -> OnyxResult<()> {
        self.file.sync_all().map_err(|e| OnyxError::Device {
            path: self.path.clone(),
            reason: format!("sync: {}", e),
        })
    }

    pub fn size(&self) -> u64 {
        self.size_bytes
    }

    pub fn slice(&self, base_offset: u64, size_bytes: u64) -> OnyxResult<Self> {
        let end = base_offset
            .checked_add(size_bytes)
            .ok_or_else(|| OnyxError::Device {
                path: self.path.clone(),
                reason: format!(
                    "slice overflow: base_offset={} size_bytes={}",
                    base_offset, size_bytes
                ),
            })?;
        if end > self.size_bytes {
            return Err(OnyxError::Device {
                path: self.path.clone(),
                reason: format!(
                    "slice out of bounds: base_offset={} size_bytes={} device_size={}",
                    base_offset, size_bytes, self.size_bytes
                ),
            });
        }

        Ok(Self {
            file: self.reopen_with_same_mode()?,
            size_bytes,
            base_offset: self.base_offset + base_offset,
            path: self.path.clone(),
            direct_io: self.direct_io,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Raw file descriptor for io_uring SQE construction.
    pub fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    fn translate_offset(&self, offset: u64, len: usize) -> OnyxResult<u64> {
        let len = len as u64;
        let end = offset.checked_add(len).ok_or_else(|| OnyxError::Device {
            path: self.path.clone(),
            reason: format!("offset overflow: offset={} len={}", offset, len),
        })?;
        if end > self.size_bytes {
            return Err(OnyxError::Device {
                path: self.path.clone(),
                reason: format!(
                    "out-of-bounds IO: offset={} len={} view_size={}",
                    offset, len, self.size_bytes
                ),
            });
        }
        Ok(self.base_offset + offset)
    }
}
