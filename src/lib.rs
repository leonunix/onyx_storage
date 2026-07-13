#[cfg(not(target_os = "linux"))]
compile_error!("onyx-storage only supports Linux");

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
