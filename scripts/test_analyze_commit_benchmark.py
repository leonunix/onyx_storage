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
            "apply_refcount_batch_count": 10,
            "apply_refcount_batch_actions": 100,
            "apply_refcount_batch_pbas": 80,
            "apply_refcount_breakdown_sampled_pbas": 40,
            "apply_refcount_pba_grouping_us": 100,
            "apply_refcount_actions_sort_us": 40,
            "apply_refcount_actions_sort_sampled_actions": 20,
            "apply_refcount_stage_sampled_us": 500,
            "apply_refcount_pbas_materialize_us": 10,
            "apply_refcount_base_page_lookup_us": 200,
            "apply_refcount_base_profiled_pbas": 40,
            "apply_refcount_base_page_runs": 10,
            "apply_refcount_base_hole_runs": 2,
            "apply_refcount_base_overlay_runs": 1,
            "apply_refcount_base_clean_runs": 7,
            "apply_refcount_base_output_init_us": 5,
            "apply_refcount_base_inner_lock_wait_us": 6,
            "apply_refcount_base_page_resolve_us": 7,
            "apply_refcount_base_request_materialize_us": 8,
            "apply_refcount_base_cache_probe_us": 9,
            "apply_refcount_base_decode_us": 10,
            "apply_refcount_base_lookup_attempts": 90,
            "apply_refcount_epoch_retries": 10,
            "apply_refcount_fold_lock_wait_us": 30,
            "apply_refcount_slot_lock_wait_us": 40,
            "apply_refcount_pending_slot_scan_us": 300,
            "apply_refcount_delta_merge_us": 400,
            "flush_rc_fold_service_us": 1000,
            "flush_rc_fold_validate_us": 100,
            "flush_rc_fold_stage_us": 600,
            "flush_rc_fold_remove_us": 200,
        }
        after_status = {
            "commit_apply_us": 200,
            "apply_refcount_us": 60,
            "apply_l2p_remap_count": 8,
            "apply_l2p_remap_us": 40,
            "apply_l2p_remap_range_count": 3,
            "apply_l2p_remap_range_lbas": 8,
            "apply_l2p_remap_range_us": 50,
            "apply_refcount_batch_count": 12,
            "apply_refcount_batch_actions": 130,
            "apply_refcount_batch_pbas": 100,
            "apply_refcount_breakdown_sampled_pbas": 50,
            "apply_refcount_pba_grouping_us": 160,
            "apply_refcount_actions_sort_us": 55,
            "apply_refcount_actions_sort_sampled_actions": 25,
            "apply_refcount_stage_sampled_us": 620,
            "apply_refcount_pbas_materialize_us": 14,
            "apply_refcount_base_page_lookup_us": 250,
            "apply_refcount_base_profiled_pbas": 50,
            "apply_refcount_base_page_runs": 14,
            "apply_refcount_base_hole_runs": 3,
            "apply_refcount_base_overlay_runs": 2,
            "apply_refcount_base_clean_runs": 9,
            "apply_refcount_base_output_init_us": 7,
            "apply_refcount_base_inner_lock_wait_us": 9,
            "apply_refcount_base_page_resolve_us": 10,
            "apply_refcount_base_request_materialize_us": 12,
            "apply_refcount_base_cache_probe_us": 14,
            "apply_refcount_base_decode_us": 16,
            "apply_refcount_base_lookup_attempts": 113,
            "apply_refcount_epoch_retries": 13,
            "apply_refcount_fold_lock_wait_us": 37,
            "apply_refcount_slot_lock_wait_us": 49,
            "apply_refcount_pending_slot_scan_us": 370,
            "apply_refcount_delta_merge_us": 480,
            "flush_rc_fold_service_us": 1500,
            "flush_rc_fold_validate_us": 140,
            "flush_rc_fold_stage_us": 900,
            "flush_rc_fold_remove_us": 300,
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
        refcount_batch = summary["refcount_batch_breakdown"]
        self.assertEqual(refcount_batch["base_lookup_attempts"], 23)
        self.assertEqual(refcount_batch["epoch_retries"], 3)
        self.assertEqual(refcount_batch["fold_lock_wait_us"], 7)
        self.assertEqual(refcount_batch["slot_lock_wait_us"], 9)
        self.assertEqual(refcount_batch["measured_stage_total_us"], 216)
        self.assertEqual(refcount_batch["actions_sort_us"], 15)
        self.assertEqual(refcount_batch["actions_sort_sampled_actions"], 5)
        self.assertEqual(refcount_batch["actions_sort_us_per_sampled_action"], 3.0)
        self.assertEqual(refcount_batch["stage_sampled_us"], 120)
        self.assertEqual(refcount_batch["pbas_materialize_us"], 4)
        self.assertEqual(refcount_batch["base_profile"]["pbas"], 10)
        self.assertEqual(refcount_batch["base_profile"]["page_runs"], 4)
        self.assertEqual(
            refcount_batch["base_profile"]["phase_us"]["cache_probe"], 5
        )
        self.assertEqual(
            refcount_batch["base_profile"]["phase_us_per_pba"]["decode"], 0.6
        )
        self.assertEqual(
            summary["rc_fold_profile"],
            {
                "service_us": 500,
                "phase_us": {"validate": 40, "stage": 300, "remove": 100},
                "unattributed_us": 60,
            },
        )
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
