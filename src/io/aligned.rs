use std::alloc::{self, Layout};

use crate::error::{OnyxError, OnyxResult};
use crate::types::BLOCK_SIZE;

/// 4KB-aligned buffer for O_DIRECT IO.
/// Optionally backed by hugepages.
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
    is_hugepage: bool,
}

// SAFETY: AlignedBuf owns its memory and the pointer is not shared
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate an aligned buffer of the given size.
    /// Size will be rounded up to a multiple of BLOCK_SIZE.
    pub fn new(size: usize, _use_hugepages: bool) -> OnyxResult<Self> {
        let aligned_size = round_up(size, BLOCK_SIZE as usize);
        if aligned_size == 0 {
            return Err(OnyxError::Config("cannot allocate zero-size buffer".into()));
        }

        #[cfg(target_os = "linux")]
        if _use_hugepages {
            return Self::alloc_hugepage(aligned_size);
        }

        Self::alloc_regular(aligned_size)
    }

    fn alloc_regular(size: usize) -> OnyxResult<Self> {
        let layout = Layout::from_size_align(size, BLOCK_SIZE as usize)
            .map_err(|e| OnyxError::Config(format!("invalid layout: {}", e)))?;

        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(OnyxError::Io(std::io::Error::from(
                std::io::ErrorKind::OutOfMemory,
            )));
        }

        Ok(Self {
            ptr,
            len: size,
            layout,
            is_hugepage: false,
        })
    }

    #[cfg(target_os = "linux")]
    fn alloc_hugepage(size: usize) -> OnyxResult<Self> {
        use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
        use std::num::NonZeroUsize;

        let nz_size = NonZeroUsize::new(size)
            .ok_or_else(|| OnyxError::Config("cannot mmap zero bytes".into()))?;

        let ptr = unsafe {
            mmap_anonymous(
                None,
                nz_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_HUGETLB,
            )
        };

        match ptr {
            Ok(p) => {
                let layout = Layout::from_size_align(size, BLOCK_SIZE as usize).unwrap();
                Ok(Self {
                    ptr: p.as_ptr() as *mut u8,
                    len: size,
                    layout,
                    is_hugepage: true,
                })
            }
            Err(_) => {
                // Fallback to regular allocation
                tracing::warn!("hugepage allocation failed, falling back to regular memory");
                Self::alloc_regular(size)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.is_hugepage {
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = nix::sys::mman::munmap(
                    std::ptr::NonNull::new(self.ptr as *mut _).unwrap(),
                    self.len,
                );
            }
        } else {
            unsafe {
                alloc::dealloc(self.ptr, self.layout);
            }
        }
    }
}

pub fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}
