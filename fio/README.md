# Onyx fio external ioengine

This Rust plugin sends fio IO directly to the Onyx Direct IO Unix socket and
does not create or use a ublk device. Each fio job opens one socket session.
Only the small `src/fio_bridge.c` ABI adapter includes fio's private headers;
the protocol client and completion handling are implemented in Rust.

The fio external-engine ABI is private. Build the plugin against the source
tree for the exact fio version that will load it:

```bash
fio --version
cd /path/to/matching/fio/source && ./configure
make -C fio FIO_SOURCE_DIR=/path/to/matching/fio/source
```

Normal release builds compile expensive per-IO stage timing out of both the
engine and plugin. Build an instrumented pair only while diagnosing latency:

```bash
make engine-release CARGO_FEATURES=diagnostic-metrics
make fio-engine FIO_SOURCE_DIR=/path/to/matching/fio/source \
  CARGO_FEATURES=diagnostic-metrics
```

The response wire format remains version 3 in either mode. Without the feature,
stage latency fields are zero (unmeasured), while correctness, error, capacity,
and engine control-loop metrics remain enabled.

The protocol framing has offline tests that need no fio source tree:

```bash
cargo test --manifest-path fio/Cargo.toml
```

Run the canonical mixed workload against an already-running Onyx service:

```bash
fio --name=onyx-mix \
  --ioengine="$PWD/fio/onyx_fio_engine.so" \
  --onyx_socket=/tmp/onyx-storage-nvme.sock \
  --onyx_volume=fio-volume \
  --rw=randrw --rwmixread=70 --bsrange=4k-32k \
  --iodepth=16 --numjobs=16 \
  --time_based=1 --runtime=1800 --group_reporting=1 --refill_buffers
```

`onyx_socket` is the control-socket path; the plugin appends `.io`.

⚠ `--refill_buffers` matters here for the same reason it does over ublk: the
default buffer reuse lets lz4 compress the payload ~128x and the throughput
number becomes meaningless.

Current protocol constraints:

- Linux and Unix sockets only.
- Every IO must be a **multiple of 4 KiB at a 4 KiB-aligned offset**, up to
  **128 KiB** per IO. The engine's aligned path takes any block count, so the
  ceiling is only a bound on how much memory one session may pin; raising
  `MAX_DIRECT_IO_BYTES` means redoing the arithmetic in its doc comment.
- `bs` and `ba` are checked against that at **job init**, with a message
  naming what is wrong — not as an `EINVAL` on the first IO, which fio would
  report as a device error.
- Buffers are sized from the job's own `max_bs`, so a `bs=4k` job does not
  pay for a `bs=128k` job's frames.
- `trim` is supported (`--rw=trimwrite`, `--trim_percentage`), and goes to the
  same `OnyxVolume::discard` the ublk frontend uses.
- `fsync` / `fdatasync` are **completed locally without touching the wire**.
  This is correct rather than a shortcut: the foreground `append()` blocks
  until that sequence has completed its LV2 `fdatasync`, so a write fio has
  already reaped is already durable. ⚠ The consequence is that `--fsync=N`
  **measures nothing** on this engine — do not read a number out of it.
- `size` is optional: a successful `HELLO` reports the volume capacity, which
  the plugin hands to fio through `get_file_size`. An explicit `size=` still
  wins, and a failed capacity probe just puts fio back to requiring one.
- `iodepth` is limited to 256 per job by the server protocol, and this is not
  going to change: the only stable measurement point on the box is QD256 as
  `j16d16`, i.e. 16 per job, so 256 is already 16x the headroom needed. Past
  QD1024 the system is in congestion collapse regardless of frontend.
- `numjobs` creates one independent Direct IO session per job (server maximum:
  64 sessions).
- **Protocol version 3.** The plugin and the engine must agree on the version;
  a mismatch fails at `HELLO` with `EPROTO` rather than reporting wrong
  numbers. Within a matching version the session is self-describing — the
  `HELLO` capability payload carries the block size and IO ceiling, and the
  plugin refuses to start if it cannot honour them — so this is a version
  requirement, not the older "must be built from the same tree" rule.

## The `onyx-stage` latency ledger

At job cleanup each session prints one line per direction to stderr:

```
onyx-stage write n=… accounted=…% rtt_ns … stage_ns … intake_ns … total_ns …
  resp_queue_ns … egress_ns … [within total] queue_ns … engine_ns … durable_ns … dispatch_ns …
```

`stage` / `intake` / `total` / `resp_queue` / `egress` are disjoint and span
the whole round trip, in the order an IO walks them:

| segment | window | measured by |
|---|---|---|
| `stage` | `queue()` staged it -> `commit()` wrote it | client `Instant` |
| `intake` | client `write` -> server finished reading the request | client stamp vs server `CLOCK_MONOTONIC` |
| `total` | request read -> response built | server `Instant` |
| `resp_queue` | response built -> writer thread's `write` | server `Instant` |
| `egress` | server `write` -> client finished reading the response | server stamp vs client `CLOCK_MONOTONIC` |

`accounted` is their sum over `rtt`, and it is the number to read first: the
breakdown is only trustworthy at ~100%. `queue` / `engine` / `durable` /
`dispatch` subdivide `total` and must not be added on top of it.

`intake` and `egress` need both processes on one machine, since they difference
two `CLOCK_MONOTONIC` readings taken in different processes. They report 0
rather than a fabricated value when that does not hold.
