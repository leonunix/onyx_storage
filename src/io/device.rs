use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{OnyxError, OnyxResult};

/// Raw device/file handle for O_DIRECT IO
pub struct RawDevice {
    file: File,
    size_bytes: u64,
    path: PathBuf,
    direct_io: bool,
}

impl RawDevice {
    /// Open a device or file for O_DIRECT read/write.
    /// Falls back to buffered IO if O_DIRECT is not supported (e.g., regular files on macOS).
    pub fn open(path: &Path) -> OnyxResult<Self> {
        let (file, direct_io) = Self::open_direct(path)?;
        let size_bytes = Self::get_size(&file, path)?;

        Ok(Self {
            file,
            size_bytes,
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
            path: path.to_path_buf(),
            direct_io,
        })
    }

    #[cfg(target_os = "linux")]
    fn open_direct(path: &Path) -> OnyxResult<(File, bool)> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(f) => Ok((f, true)),
            Err(_) => {
                // Fallback to buffered
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
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn open_direct(path: &Path) -> OnyxResult<(File, bool)> {
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
            let size = f
                .seek(std::io::SeekFrom::End(0))
                .map_err(|e| OnyxError::Device {
                    path: path.to_path_buf(),
                    reason: e.to_string(),
                })?;
            Ok(size)
        }
    }

    /// Read exactly `buf.len()` bytes at the given offset.
    /// Loops on short reads to guarantee the full buffer is filled.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> OnyxResult<()> {
        use std::os::unix::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            match self.file.read_at(&mut buf[done..], offset + done as u64) {
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

    /// Write exactly `buf.len()` bytes at the given offset.
    /// Loops on short writes to guarantee the full buffer is written.
    pub fn write_at(&self, buf: &[u8], offset: u64) -> OnyxResult<()> {
        use std::os::unix::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            match self.file.write_at(&buf[done..], offset + done as u64) {
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

    pub fn sync(&self) -> OnyxResult<()> {
        self.file.sync_all().map_err(|e| OnyxError::Device {
            path: self.path.clone(),
            reason: format!("sync: {}", e),
        })
    }

    pub fn size(&self) -> u64 {
        self.size_bytes
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }
}
