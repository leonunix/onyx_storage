#!/usr/bin/env python3

import unittest

import foreground_latency_probe as probe


class ForegroundLatencyProbeTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
