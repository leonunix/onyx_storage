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

Run a 4 KiB random-write workload against an already-running Onyx service:

```bash
fio --name=onyx-randwrite \
  --ioengine="$PWD/fio/onyx_fio_engine.so" \
  --onyx_socket=/tmp/onyx-storage-nvme.sock \
  --onyx_volume=myvolume \
  --rw=randwrite --bs=4k --iodepth=32 --numjobs=1 \
  --size=100G --time_based=1 --runtime=60 --group_reporting=1
```

`onyx_socket` is the control-socket path; the plugin appends `.io`. `size` is
required because this is a diskless engine and fio cannot discover the volume
size through protocol version 2.

Current protocol constraints:

- Linux and Unix sockets only.
- Reads and writes only; trim and flush are rejected.
- Exactly 4 KiB per IO, with 4 KiB-aligned offsets.
- `iodepth` is limited to 256 per job by the server protocol.
- `numjobs` creates one independent Direct IO session per job (server maximum:
  64 sessions).
- The plugin and the engine must be built from the same tree. Protocol version
  2 changed both header sizes, so a stale `.so` against a new engine (or the
  reverse) fails at `HELLO` with `EPROTO` rather than reporting wrong numbers.

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
