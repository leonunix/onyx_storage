# Onyx Phase 2 — Snapshot lifecycle + rich per-volume stats

This doc is the **coordination contract** between the onyx engine track (Rust) and
the dashboard track (Go + Vue). It is authoritative for the Unix-socket IPC commands
and the JSON shapes both sides build to. Design rationale lives inline.

## Why

Phase 1 delivered the write/read/dedup/GC data plane. Phase 2 makes onyx usable as a
*volume product*: point-in-time snapshots (create / delete / restore / clone) and
"pure-storage"-style at-a-glance capacity numbers (logical size, real used space,
dedup ratio, compress ratio) surfaced through the CLI, the IPC socket, and the
dashboard.

## What metadb already gives us (no metadb change needed for create/list/delete/clone)

`onyx_metadb::Db` (path dep) already implements the COW primitives — verified in
`metadb/src/db/snapshot.rs` + `metadb/src/db/volume.rs`:

| Need | metadb API | Returns |
|------|------------|---------|
| create snapshot | `Db::take_snapshot(vol_ord: u16) -> Result<SnapshotId>` | `SnapshotId = u64` |
| list snapshots | `Db::snapshots()` / `Db::snapshots_for(vol_ord)` | `Vec<SnapshotEntry>` |
| drop snapshot | `Db::drop_snapshot(id) -> Result<Option<DropReport>>` | `DropReport.freed_pbas: Vec<Pba>` |
| clone → new vol | `Db::clone_volume(src_snap_id) -> Result<VolumeOrdinal>` | new `VolumeOrdinal` |
| diff | `Db::diff(a,b)` / `Db::diff_with_current(snap)` | `Vec<DiffEntry>` |

`SnapshotEntry { id, vol_ord, l2p_roots_page, created_lsn, l2p_shard_roots }`.

`DropReport.freed_pbas` are PBAs whose refcount hit 0 — identical in kind to the
lineage-GC-surfaced freed PBAs. They MUST be `coalesce_free_pbas_to_extents` +
`PbaLifecycle::retire_committed` (NOT direct-free), then reclaimed by `GcRunner`
Gate 1/2. Reuse the exact pattern in `src/engine/lineage.rs::drain_once`. Direct-free
re-introduces the premature-free CRC (rc==0 proof does not cover rc-untracked
packed/multi-LBA L2P sharing).

### Restore — two semantics, both shipped

1. **Clone-to-new-volume** (`snapshot-clone`): `clone_volume(snap_id)` → register the
   new ordinal under a new volume name in the onyx catalog. Original untouched.
2. **In-place rollback** (`snapshot-restore`): rewind the *same* volume to the snapshot
   via metadb's `Db::restore_volume_to_snapshot(snap_id)` (`metadb/src/db/snapshot.rs`).
   A root-swap was rejected: `drop_snapshot` surfaces zero PBAs (Phase 5 hands physical
   reclaim to the dead-list / lineage GC), so a root-swap would **leak** the diverged
   data PBAs (live now, never dead-listed). Instead restore = `diff_with_current` →
   replay the inverse (changed LBAs back to their snapshot value, post-snapshot adds
   deleted) in **one atomic transaction** through the soaked `commit_ops` remap path;
   the diverged PBAs dead-list and reclaim exactly like an overwrite. Three subtleties
   the implementation handles (each has a regression test in `metadb/tests/db_restore.rs`):
   - **Forced sync before diff**: `diff_with_current` reads the committed `tree.root()`;
     onyx commits via the *staged* path (tree fold deferred to BFG sync), so restore
     does `flush_with_gate(Forced)` first or the diff misses the writes and no-ops.
   - **Seq-strip on the rollback remap**: the snapshot value carries an old commit seq;
     `seq_guard_rejects` would drop it over a higher-seq overwrite, so restore re-stamps
     each value with `with_seq(0)` (the guard-bypass sentinel) to apply unconditionally.
   - **Buffer read-cache invalidation**: a flushed onyx buffer entry stays read-serving
     until its ring slot is reclaimed; `WriteBufferPool::purge_volume` now clears the
     volume's `lba_index` unconditionally so post-restore reads fall through to metadb.
   onyx quiesces the volume first (must be **stopped**: reject if a live handle is open,
   `purge_volume`, drain the flusher, `sync_durable`).

## Onyx-side identity model

metadb identifies snapshots by numeric `SnapshotId`. Users name them. The onyx
**snapshot catalog** (extends `onyx-volume-catalog.bin`, version 1→2) maps
`(volume_name, snap_name) -> { snapshot_id, vol_ord, created_lsn, created_at, size_bytes }`.
v1 catalog files load with an empty snapshot set; persist always writes v2.

## IPC contract (Unix socket, text protocol — mirrors existing `cmd args\n … ok\n`)

Human/CLI commands:

```
snapshot-create <volume> <snap>            -> ok <snapshot_id>\n
snapshot-delete <volume> <snap>            -> ok <freed_blocks>\n
snapshot-list [volume]                     -> "<snap> <id> <created_at> <size_bytes>\n"… ok\n
snapshot-clone <volume> <snap> <new_vol>   -> ok <new_vol>\n
snapshot-restore <volume> <snap>           -> ok\n   (in-place; volume must be stopped)
```

JSON commands (dashboard):

```
snapshots-json [volume]   -> [ {volume,name,snapshot_id,created_at,created_lsn,size_bytes} ]\nok\n
                             (created_at = epoch SECONDS in JSON; stored internally as epoch nanos)
volume-usage <volume>     -> { volume, logical_size_bytes, mapped_lbas, mapped_bytes,
                               physical_bytes, unique_blocks, dedup_ratio, compress_ratio,
                               data_reduction_ratio, computed_at }\nok\n
```

`volume-usage` is **cold data**. The underlying L2P scan is O(live entries) — seconds on
a large volume — so the engine serves it from a per-volume TTL cache (60 s) and only
rescans when stale/missing. `computed_at` (epoch seconds) is the "as of" stamp; the
dashboard should show it and treat the numbers as cold, not live. Call it on the volume
**detail** view, never in a hot list poll. `mapped_bytes` = live L2P entries × block_size.
`physical_bytes` = Σ unique compressed units; ratios are derived in the scan and are
self-consistent (`data_reduction = dedup × compress`).

### Stats surfaced in existing JSON (extended, additive — never remove fields)

- `volumes-json` per-volume object gains: `logical_size_bytes`, and (once counters
  land) `physical_bytes`, `dedup_ratio`, `compress_ratio`. Existing `metrics` (IO)
  unchanged.
- `metrics-json` / `status-json` gain engine-wide aggregates: `compress_ratio`,
  `dedup_hit_rate`, `data_reduction_ratio`, `logical_used_bytes`, `physical_used_bytes`.
  (Engine-wide compress in/out bytes and dedup hits/misses already exist in
  `EngineMetrics`; aggregates are computed at serialization.)

Per-volume dedup/compress are inherently approximate (dedup is cross-volume global) —
attribute to the volume that issued the write; surface the global numbers as the
authoritative reduction.

## A4 — `Db::restore_volume_to_snapshot` (in-place rollback): SHIPPED

Implemented as a diff-replay over the soaked `commit_ops` path (see "Restore — two
semantics, both shipped" above), **not** a root-swap. metadb method
`restore_volume_to_snapshot(snap_id)` (`metadb/src/db/snapshot.rs`) =
`flush_with_gate(Forced)` → `diff_with_current` → one transaction of
`l2p_remap(value.with_seq(0))` / `delete` (chunked at `RESTORE_MAX_OPS_PER_TX=16384`
for huge diffs). onyx: `MetadbBackend/MetaStore::restore_snapshot` resolve the catalog
`snapshot_id`; `engine.restore_snapshot` quiesces (reject-if-open + `purge_volume` +
flusher-idle + `sync_durable`), then restores and bumps `snapshot_restore_ops`. Tests:
`metadb/tests/db_restore.rs` (7) + `tests/test_engine.rs::full_engine_restore_snapshot_rolls_back`.

## Dashboard surface (track B)

- Backend (`dashboard/backend`): `OnyxService` gains `CreateSnapshot/ListSnapshots/
  DeleteSnapshot/CloneSnapshot/RestoreSnapshot/VolumeUsage` calling the socket commands
  above; routes under `/api/v1/volumes/{name}/snapshots` (+ `/restore`, `/clone`);
  new RBAC perms `snapshots:read` / `snapshots:write`; audit each mutation.
- Frontend (`dashboard/frontend`): snapshot panel on the volume view (list + create +
  delete + clone + restore-with-confirm), and capacity StatCards (logical / used /
  physical / dedup× / compress×) on overview + volume detail. Light blue-green theme.
```
