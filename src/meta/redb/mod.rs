//! Paged L1/L2 blockmap on redb.
//!
//! Replaces per-volume RocksDB CF blockmaps with a single redb file holding:
//! - T_VOLUMES:       vol_id → VolumeRoot { l1_size, gen }
//! - T_L1:            (vol_id, l1_idx) → L1Entry { page_id, gen }
//! - T_L2_PAGES:      page_id → encoded L2Page (256 entries per page, 1 MiB user data)
//! - T_PAGE_REFS:     page_id → u32 refcount (COW, v1 always 1)
//! - T_PAGE_FREE:     page_id → () (freelist)
//! - T_PAGE_NEXT_ID:  "" → u64 monotonic allocator
//!
//! See design doc: `.claude/plans/redb_paged_blockmap.md`.

pub mod page;
pub mod schema;
pub mod store;

#[cfg(test)]
mod tests;

pub use page::{L2Page, LBAS_PER_PAGE, L2_PAGE_RAW_MAX};
pub use schema::{L1Entry, VolumeRoot};
pub use store::RedbStore;
