# Onyx metadb Integration Status

Branch: `feature/metadb-integration`

Onyx now uses the in-tree `onyx-metadb` crate as the only metadata backend.
The public `MetaStore` surface is preserved for engine, zone, flusher, GC, and
dedup callers, but its implementation delegates directly to metadb.

## Completed

- Root dependency switched to mandatory `onyx_metadb`.
- Legacy metadata backend files removed from `src/meta/`.
- `MetaConfig` uses `meta.path`.
- Blockmap, refcount, dedup, delete-range, delete-volume, scan, and diagnostic
  adapters route through metadb.
- Dedup cleanup runs when range/delete operations free PBAs.
- Deleted-volume blockmap reads preserve the old unmapped semantics.
- Batched write and dedup-hit return values match the existing `MetaStore`
  contract.

## Verification

- `cargo check`
- `cargo test --lib`
- `cargo test --test test_meta`
- `cargo test --test test_engine`
- `cargo test --test test_integration`
- Remaining integration suites were run after the switch:
  `test_packer`, `test_perf`, `test_space`, `test_stability`,
  `test_superblock`, `test_types`, and `test_zone`.

## Follow-Up

- Add a metadb-native blockmap/refcount inspection tool to replace the removed
  ldb-based probe.
- Run the metadb soak gate from `metadb/docs/ONYX_INTEGRATION_PLAN.md` before
  treating the branch as production-ready.
