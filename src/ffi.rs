//! C FFI exports for onyx-storage.
//!
//! All functions use integer error codes. Call `onyx_strerror()` for details.
//! Thread-local storage holds the last error message.

use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};

use crate::config::OnyxConfig;
use crate::engine::OnyxEngine;
use crate::error::OnyxError;
use crate::types::CompressionAlgo;
use crate::volume::OnyxVolume;

pub const ONYX_OK: c_int = 0;
pub const ONYX_ERR_NULL: c_int = -1;
pub const ONYX_ERR_INVALID: c_int = -2;
pub const ONYX_ERR_IO: c_int = -3;
pub const ONYX_ERR_NOTFOUND: c_int = -4;
pub const ONYX_ERR_NOSPACE: c_int = -5;
pub const ONYX_ERR_CONFIG: c_int = -6;
pub const ONYX_ERR_INTERNAL: c_int = -7;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() =
            CString::new(msg).unwrap_or_else(|_| CString::new("unknown error").unwrap());
    });
}

fn error_code(e: &OnyxError) -> c_int {
    match e {
        OnyxError::Io(_) => ONYX_ERR_IO,
        OnyxError::Meta(_) => ONYX_ERR_IO,
        OnyxError::SpaceExhausted => ONYX_ERR_NOSPACE,
        OnyxError::VolumeNotFound(_) => ONYX_ERR_NOTFOUND,
        OnyxError::Config(_) => ONYX_ERR_CONFIG,
        OnyxError::OutOfBounds { .. } => ONYX_ERR_INVALID,
        OnyxError::InvalidLba { .. } => ONYX_ERR_INVALID,
        OnyxError::VolumeDeleted(_) => ONYX_ERR_NOTFOUND,
        _ => ONYX_ERR_INTERNAL,
    }
}

fn handle_error(e: OnyxError) -> c_int {
    let code = error_code(&e);
    set_last_error(&e.to_string());
    code
}

fn compression_from_int(v: c_int) -> CompressionAlgo {
    match v {
        1 => CompressionAlgo::Lz4,
        2 => CompressionAlgo::Zstd { level: 3 },
        _ => CompressionAlgo::None,
    }
}

// --- Engine ---

/// Open the storage engine from a TOML config file.
/// Returns NULL on error (call `onyx_strerror()` for details).
///
/// Compression is per-volume (set at create_volume time), not engine-wide.
#[no_mangle]
pub extern "C" fn onyx_engine_open(config_path: *const c_char) -> *mut OnyxEngine {
    if config_path.is_null() {
        set_last_error("config_path is null");
        return std::ptr::null_mut();
    }
    let path = match unsafe { CStr::from_ptr(config_path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("config_path is not valid UTF-8");
            return std::ptr::null_mut();
        }
    };
    let config = match OnyxConfig::load(std::path::Path::new(path)) {
        Ok(c) => c,
        Err(e) => {
            handle_error(e);
            return std::ptr::null_mut();
        }
    };
    match OnyxEngine::open(&config) {
        Ok(engine) => Box::into_raw(Box::new(engine)),
        Err(e) => {
            handle_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Open engine in meta-only mode (no data device needed).
/// Only volume management operations are available.
#[no_mangle]
pub extern "C" fn onyx_engine_open_meta(config_path: *const c_char) -> *mut OnyxEngine {
    if config_path.is_null() {
        set_last_error("config_path is null");
        return std::ptr::null_mut();
    }
    let path = match unsafe { CStr::from_ptr(config_path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("config_path is not valid UTF-8");
            return std::ptr::null_mut();
        }
    };
    let config = match OnyxConfig::load(std::path::Path::new(path)) {
        Ok(c) => c,
        Err(e) => {
            handle_error(e);
            return std::ptr::null_mut();
        }
    };
    match OnyxEngine::open_meta_only(&config) {
        Ok(engine) => Box::into_raw(Box::new(engine)),
        Err(e) => {
            handle_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Close the engine and free resources.
#[no_mangle]
pub extern "C" fn onyx_engine_close(engine: *mut OnyxEngine) {
    if !engine.is_null() {
        let _ = unsafe { Box::from_raw(engine) };
    }
}

// --- Volume management ---

/// Create a new volume.
/// `compression`: 0=None, 1=LZ4, 2=ZSTD
#[no_mangle]
pub extern "C" fn onyx_create_volume(
    engine: *mut OnyxEngine,
    name: *const c_char,
    size_bytes: u64,
    compression: c_int,
) -> c_int {
    if engine.is_null() || name.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let engine = unsafe { &*engine };
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("name is not valid UTF-8");
            return ONYX_ERR_INVALID;
        }
    };
    let algo = compression_from_int(compression);
    match engine.create_volume(name, size_bytes, algo) {
        Ok(()) => ONYX_OK,
        Err(e) => handle_error(e),
    }
}

/// Delete a volume.
#[no_mangle]
pub extern "C" fn onyx_delete_volume(engine: *mut OnyxEngine, name: *const c_char) -> c_int {
    if engine.is_null() || name.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let engine = unsafe { &*engine };
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("name is not valid UTF-8");
            return ONYX_ERR_INVALID;
        }
    };
    match engine.delete_volume(name) {
        Ok(_) => ONYX_OK,
        Err(e) => handle_error(e),
    }
}

/// List volumes as JSON. Caller must free `*out_json` with `onyx_free_string()`.
#[no_mangle]
pub extern "C" fn onyx_list_volumes(engine: *mut OnyxEngine, out_json: *mut *mut c_char) -> c_int {
    if engine.is_null() || out_json.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let engine = unsafe { &*engine };
    match engine.list_volumes() {
        Ok(volumes) => {
            let entries: Vec<serde_json::Value> = volumes
                .iter()
                .map(|v| {
                    serde_json::json!({
                        "name": v.id.0,
                        "size_bytes": v.size_bytes,
                        "compression": format!("{:?}", v.compression),
                        "zone_count": v.zone_count,
                    })
                })
                .collect();
            let json = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".into());
            match CString::new(json) {
                Ok(cs) => {
                    unsafe { *out_json = cs.into_raw() };
                    ONYX_OK
                }
                Err(_) => {
                    set_last_error("JSON contains null byte");
                    ONYX_ERR_INTERNAL
                }
            }
        }
        Err(e) => handle_error(e),
    }
}

// --- Volume IO ---

/// Open a volume for IO. Returns NULL on error.
#[no_mangle]
pub extern "C" fn onyx_open_volume(
    engine: *mut OnyxEngine,
    name: *const c_char,
) -> *mut OnyxVolume {
    if engine.is_null() || name.is_null() {
        set_last_error("null pointer");
        return std::ptr::null_mut();
    }
    let engine = unsafe { &*engine };
    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("name is not valid UTF-8");
            return std::ptr::null_mut();
        }
    };
    match engine.open_volume(name) {
        Ok(vol) => Box::into_raw(Box::new(vol)),
        Err(e) => {
            handle_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Write data to a volume at a byte offset.
#[no_mangle]
pub extern "C" fn onyx_volume_write(
    vol: *mut OnyxVolume,
    offset: u64,
    buf: *const u8,
    len: u64,
) -> c_int {
    if vol.is_null() || buf.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let vol = unsafe { &*vol };
    let data = unsafe { std::slice::from_raw_parts(buf, len as usize) };
    match vol.write(offset, data) {
        Ok(()) => ONYX_OK,
        Err(e) => handle_error(e),
    }
}

/// Read data from a volume at a byte offset into caller-provided buffer.
#[no_mangle]
pub extern "C" fn onyx_volume_read(
    vol: *mut OnyxVolume,
    offset: u64,
    buf: *mut u8,
    len: u64,
) -> c_int {
    if vol.is_null() || buf.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let vol = unsafe { &*vol };
    match vol.read(offset, len as usize) {
        Ok(data) => {
            let copy_len = data.len().min(len as usize);
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), buf, copy_len);
            }
            ONYX_OK
        }
        Err(e) => handle_error(e),
    }
}

/// Discard (TRIM) a byte range on a volume.
/// Only full 4KB blocks within the range are discarded.
#[no_mangle]
pub extern "C" fn onyx_volume_discard(vol: *mut OnyxVolume, offset: u64, len: u64) -> c_int {
    if vol.is_null() {
        set_last_error("null pointer");
        return ONYX_ERR_NULL;
    }
    let vol = unsafe { &*vol };
    match vol.discard(offset, len) {
        Ok(()) => ONYX_OK,
        Err(e) => handle_error(e),
    }
}

/// Close a volume handle.
#[no_mangle]
pub extern "C" fn onyx_volume_close(vol: *mut OnyxVolume) {
    if !vol.is_null() {
        let _ = unsafe { Box::from_raw(vol) };
    }
}

// --- Utility ---

/// Get the last error message (thread-local). Returns a pointer valid until the next error.
#[no_mangle]
pub extern "C" fn onyx_strerror() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

/// Free a string allocated by onyx (e.g., from `onyx_list_volumes`).
#[no_mangle]
pub extern "C" fn onyx_free_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}
