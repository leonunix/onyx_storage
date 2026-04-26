# Onyx metadb Integration Plan

Branch: `feature/metadb-integration`

Goal: replace the RocksDB-backed `MetaStore` with `onyx-metadb` while keeping
the upper Onyx write/read/GC/dedup call shape stable during the migration.

## Current State

- Onyx still opens RocksDB through `src/meta/store.rs` and submodules.
- The hot callers already depend on a narrow `MetaStore` surface:
  volume lifecycle, L2P blockmap lookups, atomic remap/write helpers,
  refcount inspection, dedup lookup/update, scans, durability sync, and memory
  metrics.
- `metadb/` is a separate crate and has the Onyx-oriented primitives needed by
  the adapter: `Db::get`, `Db::multi_get`, `Db::range`, `Transaction::l2p_remap`,
  `Db::range_delete`, `Db::cleanup_dedup_for_dead_pbas`, refcount iterators,
  dedup iterators, volume lifecycle, and WAL-backed atomic commits.
- Phase A still has a standalone benchmark/soak gate pending in
  `metadb/docs/ONYX_INTEGRATION_PLAN.md`. Onyx-side adapter work can start on
  this branch, but the final RocksDB removal is gated on that sign-off.

## Migration Shape

### 1. Add an explicit backend boundary

Introduce a private backend layer under `src/meta/`:

- `src/meta/backend/mod.rs`
- `src/meta/backend/rocks.rs`
- `src/meta/backend/metadb.rs`

`MetaStore` remains the public type used by engine, zone, flusher, GC, and
dedup code. Internally it delegates to either backend. This lets us switch one
method family at a time without touching every caller in the same commit.

### 2. Preserve Onyx value encodings

Keep `BlockmapValue` as Onyx's canonical 28-byte L2P value. The metadb adapter
converts with:

- Onyx `BlockmapValue` -> `onyx_metadb::L2pValue([u8; 28])`
- `onyx_metadb::L2pValue` -> Onyx `BlockmapValue`
- Onyx `DedupEntry` -> `onyx_metadb::DedupValue`
- `onyx_metadb::DedupValue` -> Onyx `DedupEntry`

No semantic reinterpretation belongs in the adapter beyond this byte-level
translation and freed-PBA extent sizing.

### 3. Replace atomic write paths first

The risky RocksDB code is the cross-table WriteBatch logic. Migrate these first:

- `atomic_batch_write`
- `atomic_batch_write_packed`
- `atomic_batch_write_multi`
- `atomic_batch_dedup_hits`
- `delete_blockmap_range`
- volume delete cleanup

The metadb path should submit remap/range-delete intent and consume
`ApplyOutcome` to free PBAs through `SpaceAllocator`. Onyx owns physical free
extent sizing by decoding the previous `BlockmapValue`.

### 4. Move reads and scans

After write-path parity tests pass, move the lower-risk operations:

- `get_mapping`
- `multi_get_mappings`
- `get_mappings_range`
- `scan_all_blockmap_entries`
- `count_blockmap_refs_for_pba`
- `get_refcount` / `multi_get_refcounts`
- dedup point/multi gets and skipped-block scans

### 5. Retire RocksDB configuration

Rename configuration in a compatibility step:

- accept old `meta.rocksdb_path` as an alias while warning once
- introduce `meta.path` for metadb
- map `block_cache_mb` to `page_cache_bytes`
- keep `wal_dir` only if metadb grows a matching separate-WAL option

When all tests use `meta.path`, remove the RocksDB dependency from the root
crate.

## Test Gate

Before deleting RocksDB code:

- Port the existing `src/meta/store/*` tests to run against both backends.
- Keep the current flusher/dedup/GC regression tests intact; those are the
  known refcount-drift tripwires.
- Run `cargo test` at the Onyx root.
- Run `cargo test` in `metadb/`.
- Run the metadb soak gate from `metadb/docs/ONYX_INTEGRATION_PLAN.md`.
- Run at least one Onyx end-to-end write/read/delete-volume cycle with metadb.

## First Commits

1. Add `onyx-metadb` as an optional path dependency and introduce the backend
   module skeleton.
2. Move the existing RocksDB implementation behind `backend::rocks` with no
   behavior change.
3. Add value conversion tests for `BlockmapValue` and `DedupEntry`.
4. Implement metadb open/config plumbing.
5. Port read-only calls.
6. Port remap/write/dedup-hit calls and keep the old backend available until
   parity tests are green.
