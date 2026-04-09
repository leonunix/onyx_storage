//! SIGHUP signal handler for config reload.
//!
//! Uses a static AtomicBool flag that is async-signal-safe to set.
//! The main loop polls `take_reload_flag()` to check if a reload was requested.

use std::sync::atomic::{AtomicBool, Ordering};

static RELOAD_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Install the SIGHUP handler. Call once at startup.
///
/// On non-Linux platforms this is a no-op (ublk is Linux-only anyway).
#[cfg(target_os = "linux")]
pub fn install_signal_handlers() {
    use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};

    extern "C" fn handle_sighup(_: libc::c_int) {
        RELOAD_REQUESTED.store(true, Ordering::SeqCst);
    }

    let action = SigAction::new(
        SigHandler::Handler(handle_sighup),
        SaFlags::SA_RESTART,
        SigSet::empty(),
    );
    unsafe {
        let _ = sigaction(Signal::SIGHUP, &action);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn install_signal_handlers() {
    // No-op on non-Linux platforms.
}

/// Check and clear the reload flag. Returns true if SIGHUP was received
/// since the last call.
pub fn take_reload_flag() -> bool {
    RELOAD_REQUESTED.swap(false, Ordering::SeqCst)
}
