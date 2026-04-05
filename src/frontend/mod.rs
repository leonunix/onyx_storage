#[cfg(target_os = "linux")]
pub mod ublk;

#[cfg(not(target_os = "linux"))]
pub mod ublk {
    // Stub for non-Linux platforms (ublk is Linux-only)
}
