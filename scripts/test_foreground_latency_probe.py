#!/usr/bin/env python3

import contextlib
import io
import json
import pathlib
import sys
import unittest
from unittest import mock

import foreground_latency_probe as probe


class ForegroundLatencyProbeTests(unittest.TestCase):
    def test_metrics_stream_calls_only_metrics_endpoint_and_emits_proxy(self) -> None:
        snapshots = iter(
            (
                {
                    "buffer_throttle_count": 10,
                    "buffer_throttle_us_total": 100,
                    "buffer_throttle_us_max": 50,
                    "buffer_backend_debt_throttle_count": 2,
                    "buffer_backend_debt_throttle_us_total": 30,
                    "buffer_backend_debt_throttle_us_max": 20,
                    "flush_writer_meta_commits": 100,
                    "flush_writer_meta_lbas": 1_000,
                },
                {
                    "buffer_throttle_count": 13,
                    "buffer_throttle_us_total": 160,
                    "buffer_throttle_us_max": 55,
                    "buffer_backend_debt_throttle_count": 4,
                    "buffer_backend_debt_throttle_us_total": 50,
                    "buffer_backend_debt_throttle_us_max": 25,
                    "flush_writer_meta_commits": 104,
                    "flush_writer_meta_lbas": 1_080,
                },
            )
        )
        calls: list[str] = []

        def fake_command(
            _path: pathlib.Path, name: str, _timeout: float
        ) -> dict[str, int]:
            calls.append(name)
            return next(snapshots)

        output = io.StringIO()
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    "foreground_latency_probe.py",
                    "--stream",
                    "metrics",
                    "--count",
                    "2",
                    "--interval",
                    "0",
                ],
            ),
            mock.patch.object(probe, "command", side_effect=fake_command),
            mock.patch.object(probe, "process_id") as process_id,
            mock.patch.object(probe, "schedstat") as schedstat,
            contextlib.redirect_stdout(output),
        ):
            probe.main()

        self.assertEqual(calls, ["metrics-json", "metrics-json"])
        process_id.assert_not_called()
        schedstat.assert_not_called()
        raw = output.getvalue()
        self.assertEqual(raw.count("\n"), 1)
        self.assertNotIn(": ", raw)
        row = json.loads(raw)
        self.assertEqual(
            row["schema"], "onyx-foreground-latency-probe-metrics-v1"
        )
        self.assertEqual(row["stream"], "metrics")
        self.assertGreater(row["elapsed"], 0)
        self.assertEqual(
            set(row["fetch_window"]),
            {
                "sample_started_epoch",
                "metrics_started_epoch",
                "metrics_ended_epoch",
                "metrics_duration_ms",
                "sample_ended_epoch",
                "metrics_counter_elapsed_s",
            },
        )
        self.assertEqual(row["fetch_window"]["metrics_counter_elapsed_s"], row["elapsed"])
        counter_interval = row["counter_interval"]
        self.assertLess(
            counter_interval["started_epoch"], counter_interval["ended_epoch"]
        )
        self.assertEqual(
            counter_interval["ended_epoch"] - counter_interval["started_epoch"],
            counter_interval["epoch_elapsed_seconds"],
        )
        self.assertEqual(counter_interval["elapsed_seconds"], row["elapsed"])
        self.assertEqual(
            counter_interval["monotonic_elapsed_seconds"], row["elapsed"]
        )
        self.assertGreater(counter_interval["elapsed_seconds"], 0)
        self.assertGreater(counter_interval["epoch_elapsed_seconds"], 0)
        self.assertLessEqual(
            row["fetch_window"]["metrics_started_epoch"],
            row["fetch_window"]["metrics_ended_epoch"],
        )
        throttle = row["write_throttle"]
        self.assertEqual(throttle["interval_seconds"], row["elapsed"])
        self.assertEqual(throttle["count_absolute"], 13)
        self.assertEqual(throttle["count_delta"], 3)
        self.assertAlmostEqual(throttle["count_rate_s"], 3 / row["elapsed"])
        self.assertEqual(throttle["wait_us_absolute"], 160)
        self.assertEqual(throttle["wait_us_delta"], 60)
        self.assertAlmostEqual(throttle["wait_us_rate_s"], 60 / row["elapsed"])
        self.assertEqual(throttle["wait_max_us_lifetime"], 55)
        self.assertEqual(throttle["backend_debt_count_absolute"], 4)
        self.assertEqual(throttle["backend_debt_count_delta"], 2)
        self.assertAlmostEqual(
            throttle["backend_debt_count_rate_s"], 2 / row["elapsed"]
        )
        self.assertEqual(throttle["backend_debt_wait_us_absolute"], 50)
        self.assertEqual(throttle["backend_debt_wait_us_delta"], 20)
        self.assertAlmostEqual(
            throttle["backend_debt_wait_us_rate_s"], 20 / row["elapsed"]
        )
        self.assertEqual(throttle["backend_debt_wait_max_us_lifetime"], 25)
        proxy = row["writer_commit_proxy"]
        self.assertEqual(proxy["classification"], "proxy")
        self.assertFalse(proxy["is_actual_commit_completion"])
        self.assertEqual(proxy["source"], "metrics-json.flush_writer_meta_commits")
        self.assertEqual(proxy["lbas_source"], "metrics-json.flush_writer_meta_lbas")
        self.assertEqual(proxy["completed_tx_absolute"], 104)
        self.assertEqual(proxy["completed_tx_delta"], 4)
        self.assertEqual(proxy["lbas_absolute"], 1_080)
        self.assertEqual(proxy["lbas_delta"], 80)
        self.assertEqual(proxy["interval_seconds"], row["elapsed"])
        self.assertAlmostEqual(
            proxy["completed_tx_rate_s"], 4 / row["elapsed"]
        )

    def test_status_stream_calls_only_status_endpoint_and_is_authoritative(self) -> None:
        statuses = iter(
            (
                {
                    "status": {
                        "metadb_memory": {
                            "commit_success": 100,
                            "commit_attempts": 101,
                            "commit_ops": 1_000,
                            "pending_dispatch": 1,
                        },
                        "buffer_next_seq": 101,
                        "buffer_applied_frontier": 99,
                        "buffer_durable_seq": 98,
                        "metadb_durable_buffer_seq": 98,
                        "buffer_pending_entries": 2,
                        "buffer_fill_pct": 3,
                        "buffer_physical_fill_pct": 4,
                        "buffer_shards": [],
                    }
                },
                {
                    "status": {
                        "metadb_memory": {
                            "commit_success": 104,
                            "commit_attempts": 106,
                            "commit_ops": 1_080,
                            "pending_dispatch": 2,
                        },
                        "buffer_next_seq": 111,
                        "buffer_applied_frontier": 106,
                        "buffer_durable_seq": 102,
                        "metadb_durable_buffer_seq": 102,
                        "buffer_pending_entries": 5,
                        "buffer_fill_pct": 6,
                        "buffer_physical_fill_pct": 7,
                        "buffer_shards": [],
                    }
                },
            )
        )
        calls: list[str] = []

        def fake_command(
            _path: pathlib.Path, name: str, _timeout: float
        ) -> dict[str, object]:
            calls.append(name)
            return next(statuses)

        output = io.StringIO()
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    "foreground_latency_probe.py",
                    "--stream",
                    "status",
                    "--pid",
                    "123",
                    "--count",
                    "2",
                    "--interval",
                    "0",
                ],
            ),
            mock.patch.object(probe, "command", side_effect=fake_command),
            mock.patch.object(probe, "schedstat", side_effect=({}, {})) as schedstat,
            contextlib.redirect_stdout(output),
        ):
            probe.main()

        self.assertEqual(calls, ["status-json", "status-json"])
        self.assertEqual(schedstat.call_args_list, [mock.call(123), mock.call(123)])
        raw = output.getvalue()
        self.assertEqual(raw.count("\n"), 1)
        row = json.loads(raw)
        self.assertEqual(
            row["schema"], "onyx-foreground-latency-probe-status-v1"
        )
        self.assertEqual(row["stream"], "status")
        self.assertGreater(row["elapsed"], 0)
        self.assertEqual(
            set(row["fetch_window"]),
            {
                "sample_started_epoch",
                "status_started_epoch",
                "status_ended_epoch",
                "status_duration_ms",
                "sample_ended_epoch",
                "status_counter_elapsed_s",
            },
        )
        self.assertEqual(row["fetch_window"]["status_counter_elapsed_s"], row["elapsed"])
        counter_interval = row["counter_interval"]
        self.assertLess(
            counter_interval["started_epoch"], counter_interval["ended_epoch"]
        )
        self.assertEqual(
            counter_interval["ended_epoch"] - counter_interval["started_epoch"],
            counter_interval["epoch_elapsed_seconds"],
        )
        self.assertEqual(counter_interval["elapsed_seconds"], row["elapsed"])
        self.assertEqual(
            counter_interval["monotonic_elapsed_seconds"], row["elapsed"]
        )
        self.assertGreater(counter_interval["elapsed_seconds"], 0)
        self.assertGreater(counter_interval["epoch_elapsed_seconds"], 0)
        authoritative = row["authoritative_commit"]
        self.assertTrue(authoritative["available"])
        self.assertEqual(authoritative["classification"], "authoritative")
        self.assertTrue(authoritative["is_actual_commit_completion"])
        self.assertEqual(
            authoritative["source"],
            "status-json.status.metadb_memory.commit_success",
        )
        self.assertEqual(authoritative["completed_tx_absolute"], 104)
        self.assertEqual(authoritative["completed_tx_delta"], 4)
        self.assertEqual(authoritative["interval_seconds"], row["elapsed"])
        self.assertAlmostEqual(
            authoritative["completed_tx_rate_s"], 4 / row["elapsed"]
        )
        self.assertEqual(row["authoritative_metadb"]["completed_tx"], 4)
        self.assertEqual(row["authoritative_metadb"]["pending"]["pending_dispatch"], 2)
        self.assertEqual(row["pending"]["buffer_pending_entries"], 5)
        self.assertEqual(row["pending"]["metadb"]["pending_dispatch"], 2)
        self.assertEqual(row["durability"]["next_seq_advance"], 10)

    def test_default_combined_stream_preserves_endpoint_order_and_schema(self) -> None:
        calls: list[str] = []

        def fake_command(
            _path: pathlib.Path, name: str, _timeout: float
        ) -> dict[str, object]:
            calls.append(name)
            return {"status": {}} if name == "status-json" else {}

        output = io.StringIO()
        with (
            mock.patch.object(
                sys,
                "argv",
                [
                    "foreground_latency_probe.py",
                    "--pid",
                    "123",
                    "--count",
                    "2",
                    "--interval",
                    "0",
                ],
            ),
            mock.patch.object(probe, "command", side_effect=fake_command),
            mock.patch.object(probe, "schedstat", side_effect=({}, {})),
            contextlib.redirect_stdout(output),
        ):
            probe.main()

        self.assertEqual(
            calls,
            ["metrics-json", "status-json", "metrics-json", "status-json"],
        )
        row = json.loads(output.getvalue())
        self.assertNotIn("stream", row)
        self.assertEqual(
            list(row),
            [
                "ts",
                "ts_monotonic",
                "elapsed",
                "fetch_window",
                "write_p99_ns",
                "lv2_durable_stages",
                "lv2_percentiles_are_bucket_upper_bounds",
                "commit_worker",
                "flush_writer_meta",
                "metadb",
                "durability",
                "flush_qos",
                "write_throttle",
                "chunklet_io_scheduler",
                "chunklet_pd_io_scheduler",
                "chunklet_io_execution",
                "buffer_sync",
                "shards",
                "scheduler",
            ],
        )
        self.assertEqual(
            list(row["fetch_window"]),
            [
                "sample_started_epoch",
                "metrics_started_epoch",
                "metrics_ended_epoch",
                "metrics_duration_ms",
                "status_started_epoch",
                "status_ended_epoch",
                "status_duration_ms",
                "sample_ended_epoch",
                "metrics_counter_elapsed_s",
                "status_counter_elapsed_s",
            ],
        )

    def test_split_stream_baseline_does_not_emit(self) -> None:
        for stream in ("metrics", "status"):
            output = io.StringIO()
            with (
                mock.patch.object(
                    probe,
                    "command",
                    return_value={"status": {}} if stream == "status" else {},
                ),
                mock.patch.object(probe, "schedstat", return_value={}),
                contextlib.redirect_stdout(output),
            ):
                if stream == "metrics":
                    probe.run_metrics_stream(
                        pathlib.Path("unused.sock"),
                        count=1,
                        interval=0,
                        timeout=3.0,
                    )
                else:
                    probe.run_status_stream(
                        pathlib.Path("unused.sock"),
                        123,
                        count=1,
                        interval=0,
                        timeout=3.0,
                    )
            self.assertEqual(output.getvalue(), "")

    def test_qualification_counters_reject_resets(self) -> None:
        old_throttle = {
            "buffer_throttle_count": 2,
            "buffer_throttle_us_total": 10,
            "buffer_throttle_us_max": 5,
            "buffer_backend_debt_throttle_count": 0,
            "buffer_backend_debt_throttle_us_total": 0,
            "buffer_backend_debt_throttle_us_max": 0,
        }
        with self.assertRaisesRegex(RuntimeError, "buffer_throttle_count"):
            probe.write_throttle_interval_summary(
                {**old_throttle, "buffer_throttle_count": 1},
                old_throttle,
                1.0,
            )
        with self.assertRaisesRegex(RuntimeError, "flush_writer_meta_commits"):
            probe.writer_commit_proxy_summary(
                {"flush_writer_meta_commits": 1, "flush_writer_meta_lbas": 10},
                {"flush_writer_meta_commits": 2, "flush_writer_meta_lbas": 10},
                1.0,
            )
        with self.assertRaisesRegex(RuntimeError, "commit_success"):
            probe.authoritative_commit_summary(
                {"metadb_memory": {"commit_success": 1}},
                {"metadb_memory": {"commit_success": 2}},
                1.0,
            )

    def test_fetch_timing_and_elapsed_are_strict(self) -> None:
        monotonic_values = iter((10.0, 10.25))
        epoch_values = iter((100.0, 100.25))
        with (
            mock.patch.object(probe.time, "monotonic", side_effect=monotonic_values),
            mock.patch.object(probe.time, "time", side_effect=epoch_values),
            mock.patch.object(probe, "command", return_value={"counter": 7}),
        ):
            result = probe.timed_command(
                pathlib.Path("unused.sock"), "metrics-json", 3.0
            )

        self.assertEqual(
            result,
            probe.FetchObservation(
                value={"counter": 7},
                started_monotonic=10.0,
                started_epoch=100.0,
                ended_monotonic=10.25,
                ended_epoch=100.25,
            ),
        )
        self.assertEqual(probe.strict_counter_elapsed(10.25, 10.0), 0.25)
        with self.assertRaisesRegex(RuntimeError, "did not advance"):
            probe.strict_counter_elapsed(10.0, 10.0)
        with self.assertRaisesRegex(RuntimeError, "did not advance"):
            probe.strict_counter_elapsed(9.0, 10.0)
        interval = probe.counter_interval_summary(
            previous_ended_monotonic=10.0,
            previous_ended_epoch=100.0,
            current_ended_monotonic=10.5,
            current_ended_epoch=101.25,
        )
        self.assertEqual(
            interval,
            {
                "started_epoch": 100.0,
                "ended_epoch": 101.25,
                "elapsed_seconds": 0.5,
                "monotonic_elapsed_seconds": 0.5,
                "epoch_elapsed_seconds": 1.25,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "epoch clock did not advance"):
            probe.counter_interval_summary(
                previous_ended_monotonic=10.0,
                previous_ended_epoch=100.0,
                current_ended_monotonic=10.5,
                current_ended_epoch=100.0,
            )

    def test_split_stream_cadence_is_anchored_to_sample_start(self) -> None:
        monotonic_values = iter((10.0, 10.1, 10.2, 10.25))
        epoch_values = iter((100.0, 100.1, 100.2, 100.25))
        with (
            mock.patch.object(probe.time, "monotonic", side_effect=monotonic_values),
            mock.patch.object(probe.time, "time", side_effect=epoch_values),
            mock.patch.object(probe, "command", return_value={}),
            mock.patch.object(probe.time, "sleep") as sleep,
        ):
            probe.run_metrics_stream(
                pathlib.Path("unused.sock"),
                count=1,
                interval=1.0,
                timeout=3.0,
            )

        sleep.assert_called_once_with(0.75)

    def test_split_stream_command_exceptions_fail_the_stream(self) -> None:
        for stream in ("metrics", "status"):
            with self.subTest(stream=stream):
                with (
                    mock.patch.object(
                        probe,
                        "command",
                        side_effect=RuntimeError(f"{stream} fetch failed"),
                    ),
                    mock.patch.object(probe, "schedstat", return_value={}),
                ):
                    with self.assertRaisesRegex(RuntimeError, "fetch failed"):
                        if stream == "metrics":
                            probe.run_metrics_stream(
                                pathlib.Path("unused.sock"),
                                count=1,
                                interval=0,
                                timeout=3.0,
                            )
                        else:
                            probe.run_status_stream(
                                pathlib.Path("unused.sock"),
                                123,
                                count=1,
                                interval=0,
                                timeout=3.0,
                            )

    def test_authoritative_counter_availability_transitions_fail(self) -> None:
        cases = (
            ({}, {"metadb_memory": {"commit_success": 2}}),
            ({"metadb_memory": {"commit_success": 2}}, {}),
        )
        for current, previous in cases:
            with self.subTest(current=current, previous=previous):
                with self.assertRaisesRegex(RuntimeError, "availability changed"):
                    probe.authoritative_commit_summary(current, previous, 1.0)
        unavailable = probe.authoritative_commit_summary({}, {}, 1.0)
        self.assertFalse(unavailable["available"])
        self.assertIsNone(unavailable["is_actual_commit_completion"])

    def test_split_qualification_counters_require_exact_nonnegative_ints(self) -> None:
        source = "metrics-json.flush_writer_meta_commits"
        with self.assertRaisesRegex(RuntimeError, "counter missing"):
            probe.strict_split_counter({}, "flush_writer_meta_commits", source)
        for value in (True, "1", -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    RuntimeError, "not a nonnegative integer"
                ):
                    probe.strict_split_counter(
                        {"flush_writer_meta_commits": value},
                        "flush_writer_meta_commits",
                        source,
                    )
        with self.assertRaisesRegex(RuntimeError, "counter missing"):
            probe.writer_commit_proxy_summary(
                {
                    "flush_writer_meta_commits": 10,
                    "flush_writer_meta_lbas": 100,
                },
                {"flush_writer_meta_lbas": 90},
                1.0,
            )

    def test_metadb_interval_uses_completed_tx_and_started_ops_semantics(self) -> None:
        old_meta = {
            "commit_attempts": 10,
            "commit_success": 8,
            "commit_errors": 1,
            "commit_empty": 2,
            "commit_ops": 100,
            "last_applied_lsn": 1_000,
            "commit_apply_gate_wait_us": 20,
            "commit_apply_rc_wait_us": 30,
            "flush_calls_forced": 4,
            "flush_l2p_fold_us": 1_000,
        }
        current_meta = {
            **old_meta,
            "commit_attempts": 14,
            "commit_success": 11,
            "commit_errors": 2,
            "commit_empty": 3,
            "commit_ops": 160,
            "last_applied_lsn": 1_003,
            "commit_apply_gate_wait_us": 25,
            "commit_apply_rc_wait_us": 50,
            "flush_calls_forced": 5,
            "flush_l2p_fold_us": 1_700,
            "checkpoint_sync_bfg": 41,
            "checkpoint_sync_kind": 1,
            "checkpoint_sync_phase": 6,
            "checkpoint_sync_transition_seq": 99,
            "checkpoint_sync_started_unix_us": 1_700_000_000_000_000,
            "checkpoint_sync_phase_started_unix_us": 1_700_000_001_000_000,
            "checkpoint_quiesce_bfg": 42,
            "checkpoint_quiesce_phase": 2,
            "checkpoint_quiesce_transition_seq": 12,
            "checkpoint_quiesce_started_unix_us": 1_700_000_000_500_000,
            "checkpoint_quiesce_phase_started_unix_us": 1_700_000_001_500_000,
        }

        result = probe.metadb_interval_summary(
            {"metadb_memory": current_meta},
            {"metadb_memory": old_meta},
            2.0,
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["attempted_tx"], 4)
        self.assertEqual(result["attempted_tx_s"], 2.0)
        self.assertEqual(result["completed_tx"], 3)
        self.assertEqual(result["completed_tx_s"], 1.5)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(result["empty_tx"], 1)
        self.assertEqual(result["started_ops"], 60)
        self.assertEqual(result["started_ops_s"], 30.0)
        self.assertEqual(result["last_applied_lsn_advance"], 3)
        self.assertEqual(result["wait_us_delta"]["commit_apply_gate_wait_us"], 5)
        self.assertEqual(result["wait_us_delta"]["commit_apply_rc_wait_us"], 20)
        checkpoint = result["checkpoint"]
        self.assertEqual(checkpoint["forced_counter_delta"], 1)
        self.assertEqual(checkpoint["completed_phase_us_delta"]["flush_l2p_fold_us"], 700)
        self.assertEqual(
            checkpoint["sync_cycle"],
            {
                "available": True,
                "bfg": 41,
                "kind": 1,
                "code": 6,
                "name": "io",
                "transition_seq": 99,
                "cycle_started_unix_us": 1_700_000_000_000_000,
                "phase_started_unix_us": 1_700_000_001_000_000,
            },
        )
        self.assertEqual(
            checkpoint["quiesce"],
            {
                "available": True,
                "bfg": 42,
                "code": 2,
                "name": "await_sync",
                "transition_seq": 12,
                "cycle_started_unix_us": 1_700_000_000_500_000,
                "phase_started_unix_us": 1_700_000_001_500_000,
            },
        )

    def test_metadb_interval_saturates_reset_counters(self) -> None:
        result = probe.metadb_interval_summary(
            {"metadb_memory": {"commit_success": 2, "commit_ops": 3}},
            {"metadb_memory": {"commit_success": 10, "commit_ops": 20}},
            1.0,
        )
        self.assertEqual(result["completed_tx"], 0)
        self.assertEqual(result["started_ops"], 0)

    def test_checkpoint_lanes_are_unavailable_before_first_transition(self) -> None:
        result = probe.metadb_interval_summary(
            {
                "metadb_memory": {
                    "commit_success": 1,
                    "checkpoint_sync_transition_seq": 0,
                    "checkpoint_quiesce_transition_seq": 0,
                }
            },
            {"metadb_memory": {"commit_success": 0}},
            1.0,
        )

        checkpoint = result["checkpoint"]
        self.assertFalse(checkpoint["sync_cycle"]["available"])
        self.assertFalse(checkpoint["quiesce"]["available"])
        self.assertEqual(checkpoint["sync_cycle"]["cycle_started_unix_us"], 0)
        self.assertEqual(checkpoint["quiesce"]["cycle_started_unix_us"], 0)

    def test_metadb_interval_accepts_missing_legacy_status(self) -> None:
        self.assertEqual(
            probe.metadb_interval_summary({}, {}, 1.0),
            {"available": False},
        )

    def test_flush_writer_meta_is_completion_side_rate(self) -> None:
        result = probe.flush_writer_meta_summary(
            {"flush_writer_meta_commits": 8, "flush_writer_meta_lbas": 160},
            {"flush_writer_meta_commits": 5, "flush_writer_meta_lbas": 100},
            2.0,
        )
        self.assertEqual(result["completed_tx"], 3)
        self.assertEqual(result["completed_tx_s"], 1.5)
        self.assertEqual(result["completed_lbas"], 60)
        self.assertEqual(result["completed_lbas_s"], 30.0)

    def test_durability_separates_applied_and_checkpoint_frontiers(self) -> None:
        previous = {
            "buffer_next_seq": 100,
            "buffer_applied_frontier": 95,
            "buffer_durable_seq": 90,
            "metadb_durable_buffer_seq": 90,
        }
        current = {
            "buffer_next_seq": 120,
            "buffer_applied_frontier": 110,
            "buffer_durable_seq": 90,
            "metadb_durable_buffer_seq": 90,
            "buffer_pending_entries": 7,
            "buffer_fill_pct": 4,
            "buffer_physical_fill_pct": 30,
            "buffer_shards": [{"fill_pct": 27}, {"fill_pct": 30}],
        }

        result = probe.durability_summary(current, previous)

        self.assertEqual(result["next_seq_advance"], 20)
        self.assertEqual(result["applied_frontier_advance"], 15)
        self.assertEqual(result["durable_seq_advance"], 0)
        self.assertEqual(result["metadb_durable_seq_advance"], 0)
        self.assertEqual(result["allocated_minus_applied"], 9)
        self.assertEqual(result["applied_minus_durable"], 20)
        self.assertEqual(result["physical_fill_pct"], 30)
        self.assertEqual(result["max_shard_fill_pct"], 30)

    def test_shard_interval_reports_manifest_release_and_oldest_pending(self) -> None:
        previous = {
            "buffer_shards": [
                {
                    "shard_idx": 0,
                    "capacity_bytes": 1_000,
                    "head_offset": 900,
                    "tail_offset": 800,
                    "used_bytes": 100,
                    "append_ops": 10,
                    "append_bytes": 1_048_576,
                    "reserve_wait_ns": 10,
                    "release_calls": 2,
                    "released_entries": 20,
                    "released_bytes": 200,
                }
            ]
        }
        current = {
            "buffer_shards": [
                {
                    "shard_idx": 0,
                    "capacity_bytes": 1_000,
                    "head_offset": 100,
                    "tail_offset": 950,
                    "used_bytes": 150,
                    "append_ops": 14,
                    "append_bytes": 3_145_728,
                    "reserve_wait_ns": 2_000_010,
                    "fill_pct": 15,
                    "head_seq": 42,
                    "head_block_reason": "awaiting_manifest",
                    "head_residency_ms": 5_000,
                    "oldest_pending_seq": 77,
                    "oldest_pending_age_ms": 250,
                    "release_calls": 3,
                    "released_entries": 28,
                    "released_bytes": 350,
                    "last_release_cap": 41,
                }
            ]
        }

        result = probe.shard_interval_summaries(current, previous, 2.0)[0]

        self.assertEqual(result["head_delta"], 200)
        self.assertEqual(result["tail_delta"], 150)
        self.assertEqual(result["iops"], 2.0)
        self.assertEqual(result["mib_s"], 1.0)
        self.assertEqual(result["release_calls_delta"], 1)
        self.assertEqual(result["released_entries_delta"], 8)
        self.assertEqual(result["released_bytes_delta"], 150)
        self.assertEqual(result["head_block_reason"], "awaiting_manifest")
        self.assertEqual(result["oldest_pending_seq"], 77)
        self.assertEqual(result["last_release_cap"], 41)


if __name__ == "__main__":
    unittest.main()
