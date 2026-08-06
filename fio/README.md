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
size through protocol version 1.

Current protocol constraints:

- Linux and Unix sockets only.
- Reads and writes only; trim and flush are rejected.
- Exactly 4 KiB per IO, with 4 KiB-aligned offsets.
- `iodepth` is limited to 256 per job by the server protocol.
- `numjobs` creates one independent Direct IO session per job (server maximum:
  64 sessions).
