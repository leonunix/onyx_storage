# OS-Level Stability Testing

This project already has unit tests and integration tests inside `cargo test`, but those stay mostly inside a process-local harness. For Linux `ublk` validation, we also want a long-running external soak that exercises:

- concurrent mixed read/write traffic
- variable IO sizes
- long runtime, such as 48 hours
- periodic service restart and recovery
- exact byte-for-byte verification of logical data

## Why Python Here

The recommended language for the OS-level harness is Python, not because the workload itself needs Python performance, but because the problem is orchestration-heavy:

- start and stop `onyx-storage`
- recreate a test volume
- discover `/dev/ublkbN` through the IPC socket
- coordinate restarts during load
- preserve logs, recent events, and failure evidence

Rust is still a good fit for in-process model tests. Python is the simpler and faster choice for a host-level soak harness.

## Harness

The main entry point is:

`[scripts/os_integrity_stress.py](/root/onyx_storage/scripts/os_integrity_stress.py)`

What it does:

- recreates a dedicated test volume
- starts `onyx-storage` for that volume only
- opens the exposed `/dev/ublkbN` block device
- runs concurrent workers with mixed random reads, writes, and `fsync`
- keeps a compact shadow model of every 4 KiB logical block
- verifies reads against the shadow model during the run
- performs periodic random sample scrubs
- gracefully restarts the service on a fixed cadence
- performs a full-image scrub at the end
- optionally restarts once more and scrubs again to verify persistence across reopen

The shadow model is metadata-only, not a second 400 GiB mirror file. Each 4 KiB logical block stores an 8-byte state record, then expected bytes are regenerated on demand. That keeps the validator practical for large volumes and long runs.

## Recommended 48-Hour Soak

Example for your `config/vdb-detailed.toml` environment:

```bash
mkdir -p /var/tmp/onyx-soak-48h

python3 scripts/os_integrity_stress.py \
  --repo-root /root/onyx_storage \
  --config config/vdb-detailed.toml \
  --engine-cmd "target/release/onyx-storage" \
  --run-dir /var/tmp/onyx-soak-48h \
  --volume soak-48h \
  --volume-size 320g \
  --duration 48h \
  --workers 16 \
  --io-sizes 4k,8k,16k,32k,64k,128k,256k,512k,1m \
  --hotset-ratio 0.10 \
  --hotset-probability 0.70 \
  --sample-scrub-interval 60s \
  --restart-interval 2h \
  --verify-chunk 4m
```

Why `320g` on a ~`400g` data area:

- leaves allocator and GC some headroom
- avoids turning the first soak into a pure full-capacity test
- still exercises large logical images and long-lived metadata

## Suggested Matrix

Run at least these three soak variants:

1. Mixed baseline
   Use the example above with `lz4`, restart every 2 hours, 16 workers.

2. High-contention overwrite
   Keep the same size, but raise `--hotset-probability` to `0.90` and reduce `--hotset-ratio` to `0.02`.

3. Metadata stress
   Use `--compression zstd`, keep restarts enabled, and reduce `--io-sizes` to `4k,8k,16k,32k,64k`.

## Failure Artifacts

The harness writes everything into `--run-dir`:

- `engine.log`: engine stdout/stderr
- `events.jsonl`: operation timeline and lifecycle events
- `summary.json`: rolling stats and final result
- `recent-events.json`: last events before failure
- `shadow-state.bin`: compact logical-image metadata used for verification

If the harness reports any mismatch, treat it as a data-integrity bug first, not as a benchmark anomaly.

## Recommended Workflow

Before the first long soak:

```bash
cargo build --release
python3 -m py_compile scripts/os_integrity_stress.py
```

Then start with a short smoke run:

```bash
python3 scripts/os_integrity_stress.py \
  --repo-root /root/onyx_storage \
  --config config/vdb-detailed.toml \
  --engine-cmd "target/release/onyx-storage" \
  --run-dir /var/tmp/onyx-soak-smoke \
  --volume soak-smoke \
  --volume-size 32g \
  --duration 20m \
  --workers 8 \
  --restart-interval 10m
```

If that stays clean, move to the 48-hour run.

## Watching Metrics In Dashboard

For your use case, the best split is:

- `os_integrity_stress.py` manages `onyx-storage`
- dashboard only observes and controls through the same Unix socket

Do not start the dashboard `engine` component through `dev.sh` while the soak harness is running, otherwise both sides would try to manage the same `onyx-storage` process.

Use the same config file for both:

- soak harness: `config/vdb-detailed.toml`
- dashboard backend: `ONYX_STORAGE_CONFIG=config/vdb-detailed.toml`

Recommended workflow:

1. Build the engine and prepare the environment.
2. Start dashboard backend and frontend.
3. Start the soak harness.
4. Open the dashboard and watch `Overview` and `Metrics`.

If you want one command for the whole setup, `dev.sh` now supports a dedicated `test` target:

```bash
./dev.sh --config config/vdb-detailed.toml start test 24h
```

What that does:

- starts dashboard backend
- starts dashboard frontend
- starts the OS soak harness
- lets the soak harness own `onyx-storage`

Defaults used by `dev.sh start test`:

- volume name: `soak-volume`
- volume size: `320g`
- engine command: `target/release/onyx-storage`
- workers: host CPU count

You can override them with environment variables:

```bash
ONYX_TEST_VOLUME=soak-48h \
ONYX_TEST_VOLUME_SIZE=320g \
ONYX_TEST_WORKERS=16 \
ONYX_TEST_EXTRA_ARGS="--restart-interval 2h --sample-scrub-interval 60s" \
./dev.sh --config config/vdb-detailed.toml start test 48h
```

Start the dashboard separately from the soak run:

```bash
./dev.sh --config config/vdb-detailed.toml start backend
./dev.sh --config config/vdb-detailed.toml start frontend
```

Then start the soak harness in another terminal:

```bash
python3 scripts/os_integrity_stress.py \
  --repo-root /root/onyx_storage \
  --config config/vdb-detailed.toml \
  --engine-cmd "target/release/onyx-storage" \
  --run-dir /var/tmp/onyx-soak-48h \
  --volume soak-48h \
  --volume-size 320g \
  --duration 48h
```

Open the dashboard here:

- frontend: `http://localhost:5173`
- backend API used by the frontend proxy: `http://localhost:8010`

The dashboard will fetch:

- engine overview via `status-json`
- global metrics via `metrics-json`
- per-volume metrics via `volumes-json`

That means while the soak test is running, you can watch:

- buffer fill and pending entries
- read/write ops and bytes
- compression ratio
- dedup hit rate
- flush / GC / dedup error counters
- per-volume IO counters

If you only want the dashboard for observation, `backend + frontend` is enough.
Leave `engine` to the soak harness.
