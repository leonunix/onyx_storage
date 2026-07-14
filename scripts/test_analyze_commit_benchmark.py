#!/usr/bin/env python3

import unittest

import analyze_commit_benchmark as analyzer


class AnalyzeCommitBenchmarkTests(unittest.TestCase):
    def test_optional_counter_delta_accepts_legacy_snapshots(self) -> None:
        self.assertIsNone(analyzer.optional_counter_delta({}, {}, "new_counter"))

    def test_combines_single_and_range_remap_work(self) -> None:
        before_metrics = {
            "flush_writer_meta_lbas": 100,
            "flush_writer_meta_commits": 10,
            "flush_commit_worker_jobs": 5,
            "flush_commit_worker_queue_wait_ns": 1_000_000,
            "flush_commit_worker_aggregator_residence_ns": 750_000,
            "flush_commit_worker_executor_queue_wait_ns": 250_000,
        }
        after_metrics = {
            "flush_writer_meta_lbas": 110,
            "flush_writer_meta_commits": 12,
            "flush_commit_worker_jobs": 7,
            "flush_commit_worker_queue_wait_ns": 3_000_000,
            "flush_commit_worker_aggregator_residence_ns": 2_250_000,
            "flush_commit_worker_executor_queue_wait_ns": 750_000,
        }
        before_status = {
            "commit_apply_us": 100,
            "apply_refcount_us": 30,
            "apply_l2p_remap_count": 2,
            "apply_l2p_remap_us": 10,
            "apply_l2p_remap_range_count": 1,
            "apply_l2p_remap_range_lbas": 4,
            "apply_l2p_remap_range_us": 20,
        }
        after_status = {
            "commit_apply_us": 200,
            "apply_refcount_us": 60,
            "apply_l2p_remap_count": 8,
            "apply_l2p_remap_us": 40,
            "apply_l2p_remap_range_count": 3,
            "apply_l2p_remap_range_lbas": 8,
            "apply_l2p_remap_range_us": 50,
        }
        fio = {
            "jobs": [
                {
                    "error": 0,
                    "write": {
                        "iops": 1234.0,
                        "clat_ns": {"percentile": {"99.000000": 2_000_000}},
                    },
                }
            ]
        }

        summary = analyzer.build_summary(
            before_metrics,
            after_metrics,
            fio,
            before_status,
            after_status,
            None,
        )

        self.assertEqual(summary["apply_lbas"], 10)
        self.assertEqual(summary["apply_totals_us"]["l2p"], 60)
        self.assertEqual(summary["l2p_apply_us_per_lba"], 6.0)
        self.assertEqual(summary["queue_wait_ms_per_job"], 1.0)
        self.assertEqual(summary["aggregator_residence_ms_per_job"], 0.75)
        self.assertEqual(summary["executor_queue_wait_ms_per_job"], 0.25)
        self.assertEqual(
            summary["l2p_remap_breakdown"],
            {
                "plain_remap_ops": 6,
                "range_ops": 2,
                "range_lbas": 4,
                "lbas_per_range_op": 2.0,
                "total_lbas": 10,
            },
        )


if __name__ == "__main__":
    unittest.main()
