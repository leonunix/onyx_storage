import argparse
import json
import pathlib
import tempfile
import unittest

from scripts.os_integrity_stress import BLOCK_SIZE, HarnessError, IntegrityHarness


class ShortReadDevice:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.path = pathlib.Path("/dev/fake")

    def read(self, offset: int, length: int) -> bytes:
        return self.payload

    def write(self, offset: int, data: bytes) -> int:
        return len(data)

    def fsync(self) -> None:
        return None

    def close(self) -> None:
        return None


class OsIntegrityStressTests(unittest.TestCase):
    def make_harness(self) -> IntegrityHarness:
        self.tempdir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.tempdir.name)
        config_path = root / "config.toml"
        config_path.write_text('[service]\nsocket_path = "/tmp/onyx-test.sock"\n', encoding="utf-8")
        args = argparse.Namespace(
            config=str(config_path),
            repo_root=str(root),
            socket_path=None,
            run_dir=str(root / "run"),
            volume="vol",
            volume_size=BLOCK_SIZE * 8,
            compression="lz4",
            duration_secs=1,
            workers=1,
            io_sizes_blocks=[1],
            blocks_per_lock=1,
            hotset_ratio=1.0,
            hotset_probability=1.0,
            hot_windows=1,
            write_probability=0.0,
            read_probability=1.0,
            sample_scrub_interval_secs=3600,
            restart_interval_secs=0,
            report_interval_secs=3600,
            verify_chunk_bytes=BLOCK_SIZE,
            seed=1234,
            final_restart_verify=False,
            startup_timeout_secs=1,
            startup_drain_timeout_secs=0,
            operation_log_stride=0,
            write_mode_weights=[10, 35, 25, 30],
            engine_cmd="/bin/true",
        )
        harness = IntegrityHarness(args)
        harness.device = ShortReadDevice(b"")
        return harness

    def tearDown(self) -> None:
        tempdir = getattr(self, "tempdir", None)
        if tempdir is not None:
            tempdir.cleanup()

    def test_worker_short_read_is_failure_during_active_run(self) -> None:
        harness = self.make_harness()
        try:
            harness.worker_loop(2)
            self.assertEqual(harness.failure, "worker-2: short read: expected 4096, got 0")
            self.assertEqual(harness.stats.snapshot().io_errors, 1)
        finally:
            harness.teardown()

    def test_worker_short_read_is_ignored_after_normal_stop_requested(self) -> None:
        harness = self.make_harness()
        try:
            harness.normal_stop_requested.set()
            harness.worker_loop(2)
            self.assertIsNone(harness.failure)
            self.assertEqual(harness.stats.snapshot().io_errors, 0)
            self.assertTrue(
                any(
                    event.get("event") == "ignored-shutdown-io-error"
                    and event.get("context") == "worker-2"
                    for event in harness.event_log.recent()
                )
            )
        finally:
            harness.teardown()

    def test_teardown_device_not_open_is_ignored(self) -> None:
        harness = self.make_harness()
        try:
            harness.teardown_started.set()
            harness._handle_thread_exception("worker-2", HarnessError("device not open"))
            self.assertIsNone(harness.failure)
            self.assertEqual(harness.stats.snapshot().io_errors, 0)
        finally:
            harness.teardown()

    def test_summary_reports_average_and_recent_bandwidth(self) -> None:
        harness = self.make_harness()
        try:
            start_time = 100.0
            harness.stats.add(
                write_ops=4,
                read_ops=2,
                flush_ops=1,
                write_bytes=4 * BLOCK_SIZE,
                read_bytes=2 * BLOCK_SIZE,
            )
            first = harness._build_summary(start_time, now=110.0)
            self.assertEqual(first["write_bw"], first["write_bw_avg"])
            self.assertEqual(first["recent_window_secs"], 10.0)
            self.assertEqual(first["recent_write_ops"], 4)
            self.assertEqual(first["recent_read_ops"], 2)
            self.assertEqual(first["recent_write_bytes"], 4 * BLOCK_SIZE)
            self.assertEqual(first["recent_read_bytes"], 2 * BLOCK_SIZE)

            harness.stats.add(
                write_ops=3,
                read_ops=1,
                flush_ops=2,
                write_bytes=3 * BLOCK_SIZE,
                read_bytes=BLOCK_SIZE,
            )
            second = harness._build_summary(start_time, now=115.0)
            self.assertEqual(second["recent_window_secs"], 5.0)
            self.assertEqual(second["recent_write_ops"], 3)
            self.assertEqual(second["recent_read_ops"], 1)
            self.assertEqual(second["recent_flush_ops"], 2)
            self.assertEqual(second["recent_write_bytes"], 3 * BLOCK_SIZE)
            self.assertEqual(second["recent_read_bytes"], BLOCK_SIZE)
            self.assertIn("write_bw_recent", second)
            self.assertIn("read_bw_recent", second)
        finally:
            harness.teardown()

    def test_write_summary_persists_recent_bandwidth_fields(self) -> None:
        harness = self.make_harness()
        try:
            harness.stats.add(write_ops=1, write_bytes=BLOCK_SIZE)
            harness.write_summary(start_time=0.0, final=False)
            summary_path = pathlib.Path(harness.run_dir) / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertIn("write_bw_avg", summary)
            self.assertIn("write_bw_recent", summary)
            self.assertIn("recent_window_secs", summary)
            self.assertFalse(summary["final"])
        finally:
            harness.teardown()


if __name__ == "__main__":
    unittest.main()
