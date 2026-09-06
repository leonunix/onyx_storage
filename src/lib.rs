#[cfg(not(target_os = "linux"))]
compile_error!("onyx-storage only supports Linux");

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Compile a diagnostic metrics block only for tests or an explicitly
/// instrumented build. Keep correctness counters and control-loop inputs out
/// of this macro: the default production build erases its body completely.
#[cfg(any(test, feature = "diagnostic-metrics"))]
macro_rules! diagnostic_metrics {
    ($($body:tt)*) => {{ $($body)* }};
}

#[cfg(not(any(test, feature = "diagnostic-metrics")))]
macro_rules! diagnostic_metrics {
    ($($body:tt)*) => {{}};
}

pub(crate) use diagnostic_metrics;

pub mod config;
pub mod error;
pub mod types;

pub mod affinity;
pub mod buffer;
pub mod chunklet_isolation;
pub mod chunklet_ops;
pub mod chunklet_pool;
pub mod chunklet_watchdog;
pub mod compress;
pub mod dedup;
pub mod direct_io;
pub mod frontend;
pub mod gc;
pub mod io;
pub mod lifecycle;
pub mod mem;
pub mod meta;
pub mod metrics;
pub mod numa;
pub mod packer;
pub mod space;
pub mod zone;

pub mod engine;
pub mod ffi;
pub mod service;
pub mod signal;
pub mod volume;
pub mod worker_queue;
