"""P03 acceptance tests with hand-computable synthetic runs."""

import json
import math
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from ghawtrace import features
from ghawtrace.feature_pipeline import prepare_features


RUN_SCHEMA = pa.schema([
    ("run_id", pa.int64()), ("repo_id", pa.int64()), ("workflow_path", pa.string()),
    ("workflow_id", pa.int64()), ("created_at", pa.string()), ("updated_at", pa.string()),
    ("event", pa.string()), ("status", pa.string()), ("conclusion", pa.string()),
    ("run_attempts", pa.int32()),
])


def run(run_id, created, updated="2026-01-05T00:05:00Z", repo_id=100, path=".github/workflows/alpha.yml",
        workflow_id=7, event="schedule", status="completed", conclusion="success", attempts=1):
    return {
        "run_id": run_id, "repo_id": repo_id, "workflow_path": path, "workflow_id": workflow_id,
        "created_at": created, "updated_at": updated, "event": event, "status": status,
        "conclusion": conclusion, "run_attempts": attempts,
    }


class P03TestBase(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve()

    def write_runs(self, rows, name="runs.parquet"):
        path = self.root / name
        pq.write_table(pa.Table.from_pylist(rows, schema=RUN_SCHEMA), path)
        return path

    def cutoffs(self, entries):
        """Write a cutoff Parquet with cutoff_id, cutoff_utc and optional scoping."""
        path = self.root / "cutoffs.parquet"
        pq.write_table(pa.Table.from_pylist(entries, schema=pa.schema([
            ("cutoff_id", pa.string()), ("cutoff_utc", pa.string()),
            ("repo_id", pa.int64()), ("workflow_path", pa.string()),
        ])), path)
        return path

    def profile_row(self, tables, benchmark):
        rows = [row for row in tables["workflow_profile"].to_pylist() if row["workflow_path"].endswith(benchmark)]
        self.assertEqual(len(rows), 1)
        return rows[0]


class ProfileMetricsTest(P03TestBase):
    """P03-T01: hand-computed denominators, zero denominators and missing time."""

    def setUp(self):
        super().setUp()
        rows = [
            run(1, "2026-01-05T00:30:00Z", "2026-01-05T00:35:00Z", event="schedule", conclusion="success"),
            run(2, "2026-01-05T12:00:00Z", "2026-01-05T12:10:00Z", workflow_id=9,
                conclusion="failure", attempts=2),
            run(3, "2026-01-07T05:00:00Z", "2026-01-07T05:00:01Z", event="workflow_dispatch",
                conclusion="skipped"),
            run(4, "2026-01-12T00:00:00Z", "2026-01-12T00:00:00Z", conclusion="success"),
            run(5, "2026-01-06T23:00:00Z", None, path=".github/workflows/beta.yml", workflow_id=None,
                event="push", status=None, conclusion="skipped", attempts=None),
        ]
        self.tables = features.build_feature_tables(self.write_runs(rows))
        self.alpha = self.profile_row(self.tables, "alpha.yml")
        self.beta = self.profile_row(self.tables, "beta.yml")

    def test_event_proportions_and_hours(self):
        self.assertEqual(self.alpha["event_type_count"], 2)
        self.assertEqual(self.alpha["top_event"], "schedule")
        self.assertAlmostEqual(self.alpha["top_event_share"], 0.75)
        self.assertAlmostEqual(self.alpha["schedule_share"], 0.75)
        self.assertAlmostEqual(self.alpha["dispatch_share"], 0.25)
        self.assertAlmostEqual(self.alpha["webhook_share"], 0.0)
        self.assertAlmostEqual(self.alpha["event_schedule_share"], 0.75)
        self.assertAlmostEqual(self.alpha["event_workflow_dispatch_share"], 0.25)
        self.assertAlmostEqual(self.alpha["event_entropy_bits"], 0.8112781244591328)
        self.assertAlmostEqual(self.alpha["hour_00_utc_share"], 0.5)
        self.assertAlmostEqual(self.alpha["hour_05_utc_share"], 0.25)
        self.assertAlmostEqual(self.alpha["hour_12_utc_share"], 0.25)
        self.assertEqual(self.alpha["peak_hour_utc"], 0)
        self.assertAlmostEqual(self.alpha["peak_hour_share"], 0.5)
        self.assertAlmostEqual(self.alpha["hour_entropy_bits"], 1.5)
        self.assertAlmostEqual(self.alpha["night_share_utc"], 0.75)
        total = sum(self.alpha[f"hour_{hour:02d}_utc_share"] for hour in range(24))
        self.assertAlmostEqual(total, 1.0)

    def test_activity_and_interval_metrics(self):
        self.assertEqual(self.alpha["run_count"], 4)
        self.assertEqual(self.alpha["observation_span_days"], 8)
        self.assertEqual(self.alpha["active_day_count"], 3)
        self.assertAlmostEqual(self.alpha["active_day_rate"], 0.375)
        self.assertAlmostEqual(self.alpha["mean_runs_per_active_day"], 4 / 3)
        self.assertEqual(self.alpha["max_daily_runs"], 2)
        self.assertEqual(self.alpha["first_week_utc"].isoformat(), "2026-01-05")
        self.assertEqual(self.alpha["last_week_utc"].isoformat(), "2026-01-12")
        self.assertEqual(self.alpha["workflow_id"], 7)
        self.assertEqual(self.alpha["workflow_id_variants"], 2)
        self.assertTrue(self.alpha["workflow_id_conflict"])
        self.assertEqual(self.alpha["interval_count"], 3)
        self.assertAlmostEqual(self.alpha["interval_mean_seconds"], 201_000.0)
        self.assertAlmostEqual(self.alpha["interval_min_seconds"], 41_400.0)
        self.assertAlmostEqual(self.alpha["interval_median_seconds"], 147_600.0)
        self.assertAlmostEqual(self.alpha["interval_p90_seconds"], 360_720.0)
        self.assertAlmostEqual(self.alpha["interval_max_seconds"], 414_000.0)
        self.assertTrue(self.alpha["interval_estimable"])

    def test_conclusions_retries_and_elapsed(self):
        self.assertEqual(self.alpha["success_count"], 2)
        self.assertEqual(self.alpha["failure_count"], 1)
        self.assertEqual(self.alpha["skipped_count"], 1)
        self.assertAlmostEqual(self.alpha["skipped_rate"], 0.25)
        self.assertAlmostEqual(self.alpha["failure_rate"], 1 / 3)
        self.assertTrue(self.alpha["failure_rate_estimable"])
        self.assertEqual(self.alpha["attempts_known_count"], 4)
        self.assertEqual(self.alpha["total_attempts"], 5)
        self.assertAlmostEqual(self.alpha["mean_attempts"], 1.25)
        self.assertEqual(self.alpha["retried_run_count"], 1)
        self.assertAlmostEqual(self.alpha["retry_rate"], 0.25)
        self.assertEqual(self.alpha["max_attempts"], 2)
        self.assertEqual(self.alpha["elapsed_known_count"], 4)
        self.assertEqual(self.alpha["elapsed_missing_count"], 0)
        self.assertEqual(self.alpha["elapsed_zero_count"], 1)
        self.assertAlmostEqual(self.alpha["elapsed_mean_seconds"], 225.25)
        self.assertAlmostEqual(self.alpha["elapsed_median_seconds"], 150.5)
        self.assertAlmostEqual(self.alpha["elapsed_p90_seconds"], 510.0)
        self.assertAlmostEqual(self.alpha["elapsed_p99_seconds"], 591.0)
        self.assertAlmostEqual(self.alpha["elapsed_max_seconds"], 600.0)
        self.assertTrue(self.alpha["elapsed_estimable"])

    def test_single_run_and_missing_time_are_marked(self):
        self.assertEqual(self.beta["run_count"], 1)
        self.assertEqual(self.beta["observation_span_days"], 1)
        self.assertAlmostEqual(self.beta["active_day_rate"], 1.0)
        self.assertIsNone(self.beta["workflow_id"])
        self.assertEqual(self.beta["workflow_id_variants"], 0)
        self.assertFalse(self.beta["workflow_id_conflict"])
        self.assertIsNone(self.beta["failure_rate"])
        self.assertFalse(self.beta["failure_rate_estimable"])
        self.assertEqual(self.beta["attempts_known_count"], 0)
        self.assertIsNone(self.beta["mean_attempts"])
        self.assertIsNone(self.beta["retry_rate"])
        self.assertFalse(self.beta["retry_rate_estimable"])
        self.assertIsNone(self.beta["max_attempts"])
        self.assertEqual(self.beta["elapsed_known_count"], 0)
        self.assertEqual(self.beta["elapsed_missing_count"], 1)
        for name in ("elapsed_mean_seconds", "elapsed_median_seconds", "elapsed_p90_seconds",
                     "elapsed_p99_seconds", "elapsed_max_seconds"):
            self.assertIsNone(self.beta[name])
        self.assertFalse(self.beta["elapsed_estimable"])
        self.assertEqual(self.beta["interval_count"], 0)
        for name in ("interval_mean_seconds", "interval_min_seconds", "interval_median_seconds",
                     "interval_p90_seconds", "interval_max_seconds"):
            self.assertIsNone(self.beta[name])
        self.assertFalse(self.beta["interval_estimable"])
        self.assertEqual(self.beta["peak_hour_utc"], 23)
        self.assertAlmostEqual(self.beta["night_share_utc"], 0.0)
        self.assertEqual(self.beta["top_event"], "push")

    def test_negative_elapsed_is_missing_but_counted(self):
        rows = [
            run(1, "2026-01-05T00:10:00Z", "2026-01-05T00:09:00Z"),
            run(2, "2026-01-05T00:20:00Z", "2026-01-05T00:30:00Z"),
        ]
        tables = features.build_feature_tables(self.write_runs(rows, "negative.parquet"))
        profile = tables["workflow_profile"].to_pylist()[0]
        self.assertEqual(profile["elapsed_negative_count"], 1)
        self.assertEqual(profile["elapsed_known_count"], 1)
        self.assertEqual(profile["elapsed_missing_count"], 1)
        self.assertAlmostEqual(profile["elapsed_median_seconds"], 600.0)

    def test_week_panel_elapsed_and_retry_counts(self):
        week = {
            (row["workflow_path"], row["week_start_utc"].isoformat()): row
            for row in self.tables["workflow_week"].to_pylist()
        }
        first = week[(".github/workflows/alpha.yml", "2026-01-05")]
        self.assertAlmostEqual(first["elapsed_median_seconds"], 300.0)
        self.assertAlmostEqual(first["elapsed_p90_seconds"], 540.0)
        self.assertEqual(first["retried_run_count"], 1)
        second = week[(".github/workflows/alpha.yml", "2026-01-12")]
        self.assertAlmostEqual(second["elapsed_median_seconds"], 0.0)
        self.assertEqual(second["retried_run_count"], 0)
        self.assertIsNone(week[(".github/workflows/beta.yml", "2026-01-05")]["elapsed_median_seconds"])

    def test_unknown_conclusion_and_event_are_bucketed(self):
        rows = [run(1, "2026-01-05T01:00:00Z", "2026-01-05T01:01:00Z", event=None, conclusion=None)]
        tables = features.build_feature_tables(self.write_runs(rows, "unknown.parquet"))
        profile = tables["workflow_profile"].to_pylist()[0]
        self.assertEqual(profile["unknown_conclusion_count"], 1)
        self.assertIsNone(profile["failure_rate"])
        self.assertFalse(profile["failure_rate_estimable"])
        self.assertAlmostEqual(profile["event_unknown_share"], 1.0)
        self.assertEqual(profile["top_event"], "unknown")
        self.assertEqual(profile["event_type_count"], 1)

    def test_event_mix_keeps_exact_counts(self):
        rows = {
            (row["repo_id"], row["workflow_path"], row["event"]): row["run_count"]
            for row in self.tables["workflow_event_mix"].to_pylist()
        }
        self.assertEqual(rows[(100, ".github/workflows/alpha.yml", "schedule")], 3)
        self.assertEqual(rows[(100, ".github/workflows/alpha.yml", "workflow_dispatch")], 1)
        self.assertEqual(rows[(100, ".github/workflows/beta.yml", "push")], 1)
        self.assertEqual(sum(rows.values()), 5)

    def test_null_rates_stay_null_and_never_nan(self):
        rows = [
            run(1, "2026-01-05T01:00:00Z", "2026-01-05T01:01:00Z", conclusion="skipped"),
            run(2, "2026-01-05T02:00:00Z", "2026-01-05T02:01:00Z", conclusion="cancelled"),
            run(3, "2026-01-12T01:00:00Z", "2026-01-12T01:01:00Z", conclusion="skipped"),
        ]
        tables = features.build_feature_tables(self.write_runs(rows, "null-rates.parquet"))
        profile = tables["workflow_profile"].to_pylist()[0]
        self.assertIsNone(profile["failure_rate"])
        self.assertFalse(profile["failure_rate_estimable"])
        self.assertAlmostEqual(profile["skipped_rate"], 2 / 3)
        asof = {row["cutoff_id"]: row for row in tables["workflow_asof"].to_pylist()}
        self.assertEqual(asof["2026-01-12"]["n_prior_outcomes"], 2)
        self.assertIsNone(asof["2026-01-12"]["prior_failure_rate"])
        self.assertAlmostEqual(asof["2026-01-12"]["prior_skip_rate"], 0.5)
        for table in (tables["workflow_profile"], tables["workflow_week"], tables["workflow_asof"]):
            for row in table.to_pylist():
                for name, value in row.items():
                    if isinstance(value, float):
                        self.assertFalse(math.isnan(value), f"{name} is NaN")
        self.assertTrue(tables["checks"]["no_unmasked_nan"]["ok"])


class WeekPanelTest(P03TestBase):
    """P03-T03: continuous UTC weeks, zero weeks, before-first and truncated weeks."""

    def setUp(self):
        super().setUp()
        rows = [
            run(1, "2026-01-05T01:00:00Z", path=".github/workflows/stable.yml"),
            run(2, "2026-01-05T02:00:00Z", path=".github/workflows/stable.yml"),
            run(3, "2026-01-12T01:00:00Z", path=".github/workflows/stable.yml"),
            run(4, "2026-01-13T01:00:00Z", path=".github/workflows/stable.yml"),
            run(5, "2026-01-19T01:00:00Z", path=".github/workflows/stable.yml"),
            run(6, "2026-01-06T01:00:00Z", path=".github/workflows/sparse.yml"),
            run(7, "2026-01-27T01:00:00Z", path=".github/workflows/sparse.yml"),
            run(8, "2026-01-20T01:00:00Z", path=".github/workflows/late.yml"),
            run(9, "2026-01-27T02:00:00Z", path=".github/workflows/late.yml"),
        ]
        self.tables = features.build_feature_tables(self.write_runs(rows))

    def panel(self, name):
        rows = [row for row in self.tables["workflow_week"].to_pylist() if row["workflow_path"].endswith(name)]
        return rows

    def test_panel_grid_states_and_zero_weeks(self):
        stable = self.panel("stable.yml")
        self.assertEqual([row["week_start_utc"].isoformat() for row in stable],
                         ["2026-01-05", "2026-01-12", "2026-01-19", "2026-01-26"])
        self.assertEqual([row["observation_state"] for row in stable],
                         ["partial", "complete", "complete", "partial"])
        self.assertEqual([row["run_count"] for row in stable], [2, 2, 1, 0])
        self.assertEqual([row["active_day_count"] for row in stable], [1, 2, 1, 0])
        self.assertEqual([row["success_count"] for row in stable], [2, 2, 1, 0])

        sparse = self.panel("sparse.yml")
        self.assertEqual([row["run_count"] for row in sparse], [1, 0, 0, 1])
        self.assertEqual(sparse[1]["active_day_count"], 0)
        self.assertIsNone(sparse[1]["elapsed_median_seconds"])

    def test_before_first_observation_is_unknown(self):
        late = self.panel("late.yml")
        self.assertEqual([row["observation_state"] for row in late],
                         ["before_first_observation", "before_first_observation", "complete", "partial"])
        self.assertEqual([row["run_count"] for row in late], [None, None, 1, 1])
        self.assertEqual([row["active_day_count"] for row in late], [None, None, 1, 1])
        self.assertEqual([row["success_count"] for row in late], [None, None, 1, 1])
        self.assertIsNone(late[0]["elapsed_median_seconds"])

    def test_week_counts_reconcile(self):
        week = self.tables["workflow_week"]
        self.assertEqual(len(week), 3 * 4)
        self.assertEqual(sum(row["run_count"] or 0 for row in week.to_pylist()), 9)
        self.assertTrue(self.tables["checks"]["week_per_workflow_sums"]["ok"])

    def test_utc_monday_alignment(self):
        rows = [
            run(1, "2026-01-04T23:59:59Z", path=".github/workflows/sunday.yml"),
            run(2, "2026-01-05T00:00:00Z", path=".github/workflows/sunday.yml"),
        ]
        tables = features.build_feature_tables(self.write_runs(rows, "monday.parquet"))
        weeks = [row["week_start_utc"].isoformat() for row in tables["workflow_week"].to_pylist()]
        self.assertEqual(weeks, ["2025-12-29", "2026-01-05"])
        self.assertEqual([row["run_count"] for row in tables["workflow_week"].to_pylist()], [1, 1])


class AsOfFeatureTest(P03TestBase):
    """P03-T04: cutoffs only see records and results observable at that time."""

    ROWS = [
        run(1, "2026-01-05T00:30:00Z", "2026-01-05T00:35:00Z", conclusion="success"),
        run(2, "2026-01-05T12:00:00Z", "2026-01-05T12:10:00Z", conclusion="failure", attempts=2),
        run(3, "2026-01-07T05:00:00Z", "2026-01-07T05:00:01Z", event="workflow_dispatch",
            conclusion="skipped"),
        run(4, "2026-01-12T00:00:00Z", "2026-01-12T00:00:00Z", conclusion="success"),
        run(5, "2026-01-06T23:00:00Z", None, path=".github/workflows/beta.yml", workflow_id=None,
            event="push", status=None, conclusion="skipped", attempts=None),
    ]

    def build(self, rows, cutoffs):
        path = self.write_runs(rows, f"runs-{len(rows)}.parquet")
        return features.build_asof(features.read_runs(path), features.group_workflows(features.read_runs(path)),
                                   features.load_cutoffs(cutoffs))

    def asof_rows(self, table, path_suffix):
        return [row for row in table.to_pylist() if row["workflow_path"].endswith(path_suffix)]

    def test_outcome_visibility_and_window(self):
        cutoffs = self.cutoffs([
            {"cutoff_id": "before", "cutoff_utc": "2026-01-05T00:31:00Z"},
            {"cutoff_id": "after-first", "cutoff_utc": "2026-01-05T00:36:00Z"},
            {"cutoff_id": "third-run", "cutoff_utc": "2026-01-08T00:00:00Z"},
            {"cutoff_id": "monday", "cutoff_utc": "2026-01-12T00:00:00Z"},
        ])
        table = self.build(self.ROWS, cutoffs)
        alpha = {row["cutoff_id"]: row for row in self.asof_rows(table, "alpha.yml")}
        self.assertEqual([row["cutoff_id"] for row in self.asof_rows(table, "alpha.yml")],
                         ["before", "after-first", "third-run", "monday"])

        before = alpha["before"]
        self.assertEqual(before["n_prior_runs"], 1)
        self.assertEqual(before["n_prior_outcomes"], 0)
        self.assertIsNone(before["prior_skip_rate"])
        self.assertIsNone(before["prior_elapsed_median_seconds"])
        self.assertTrue(before["has_prior_runs"])
        self.assertFalse(before["has_prior_outcomes"])

        after_first = alpha["after-first"]
        self.assertEqual(after_first["n_prior_runs"], 1)
        self.assertEqual(after_first["n_prior_outcomes"], 1)
        self.assertAlmostEqual(after_first["prior_skip_rate"], 0.0)
        self.assertAlmostEqual(after_first["prior_elapsed_median_seconds"], 300.0)
        self.assertIsNone(after_first["prior_mean_gap_seconds"])

        third = alpha["third-run"]
        self.assertEqual(third["n_prior_runs"], 3)
        self.assertEqual(third["n_prior_outcomes"], 3)
        self.assertAlmostEqual(third["prior_skip_rate"], 1 / 3)
        self.assertAlmostEqual(third["prior_failure_rate"], 0.5)
        self.assertAlmostEqual(third["prior_retry_rate"], 1 / 3)
        self.assertAlmostEqual(third["prior_elapsed_median_seconds"], 300.0)
        self.assertAlmostEqual(third["prior_elapsed_p90_seconds"], 540.0)
        self.assertAlmostEqual(third["prior_elapsed_max_seconds"], 600.0)
        self.assertAlmostEqual(third["prior_mean_gap_seconds"], 94_500.0)
        self.assertAlmostEqual(third["prior_max_gap_seconds"], 147_600.0)
        self.assertEqual(third["prior_active_day_count"], 2)
        self.assertAlmostEqual(third["prior_window_days"], (2 * 86_400 + 23.5 * 3_600) / 86_400)
        self.assertEqual(third["window_start_utc"], alpha["third-run"]["window_start_utc"])
        self.assertEqual(third["window_end_utc"].isoformat(), "2026-01-08T00:00:00+00:00")

        monday = alpha["monday"]
        self.assertEqual(monday["n_prior_runs"], 3)
        self.assertEqual(monday["n_prior_outcomes"], 3)

    def test_beta_has_no_observable_outcome(self):
        cutoffs = self.cutoffs([{"cutoff_id": "monday", "cutoff_utc": "2026-01-12T00:00:00Z"}])
        table = self.build(self.ROWS, cutoffs)
        beta, = self.asof_rows(table, "beta.yml")
        self.assertEqual(beta["n_prior_runs"], 1)
        self.assertEqual(beta["n_prior_outcomes"], 0)
        self.assertFalse(beta["has_prior_outcomes"])
        self.assertIsNone(beta["prior_elapsed_median_seconds"])
        self.assertIsNone(beta["prior_mean_gap_seconds"])

    def test_appending_future_records_does_not_change_old_cutoffs(self):
        cutoffs = self.cutoffs([
            {"cutoff_id": "c1", "cutoff_utc": "2026-01-08T00:00:00Z"},
            {"cutoff_id": "c2", "cutoff_utc": "2026-01-12T00:00:00Z"},
            {"cutoff_id": "future", "cutoff_utc": "2026-02-03T00:00:00Z"},
        ])
        old = self.build(self.ROWS, cutoffs)
        future_rows = self.ROWS + [run(6, "2026-02-02T00:00:00Z", "2026-02-02T00:00:10Z",
                                       conclusion="failure")]
        new = self.build(future_rows, cutoffs)
        old_rows = {(row["workflow_path"], row["cutoff_id"]): row for row in old.to_pylist()}
        new_rows = {(row["workflow_path"], row["cutoff_id"]): row for row in new.to_pylist()}
        self.assertEqual(set(new_rows), set(old_rows))
        for key in old_rows:
            if key[1] != "future":
                self.assertEqual(old_rows[key], new_rows[key], key)
        self.assertEqual(new_rows[(".github/workflows/alpha.yml", "future")]["n_prior_runs"], 5)

    def test_scoped_cutoff_only_covers_one_workflow(self):
        cutoffs = self.cutoffs([{
            "cutoff_id": "beta-only", "cutoff_utc": "2026-01-12T00:00:00Z",
            "repo_id": 100, "workflow_path": ".github/workflows/beta.yml",
        }])
        table = self.build(self.ROWS, cutoffs)
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(table.to_pylist()[0]["workflow_path"], ".github/workflows/beta.yml")

    def test_unknown_scoped_workflow_is_rejected(self):
        cutoffs = self.cutoffs([{
            "cutoff_id": "bad", "cutoff_utc": "2026-01-12T00:00:00Z",
            "repo_id": 100, "workflow_path": ".github/workflows/missing.yml",
        }])
        with self.assertRaises(ValueError):
            self.build(self.ROWS, cutoffs)

    def test_cutoff_grid_can_be_disabled(self):
        tables = features.build_feature_tables(self.write_runs(self.ROWS), cutoff_grid="none")
        self.assertEqual(len(tables["workflow_asof"]), 0)
        self.assertEqual(tables["workflow_asof"].schema, features.ASOF_SCHEMA)


class PipelineTest(P03TestBase):
    """P03-T02/T03: reproducible batches, reuse and input validation."""

    def test_batch_is_reproducible_and_reused(self):
        source = self.root / "source"
        source.mkdir()
        rows = [
            run(1, "2026-01-05T01:00:00Z"), run(2, "2026-01-12T01:00:00Z"),
            run(3, "2026-01-06T01:00:00Z", path=".github/workflows/beta.yml"),
        ]
        pq.write_table(pa.Table.from_pylist(rows, schema=RUN_SCHEMA), source / "runs_input.parquet")
        pq.write_table(pa.Table.from_pylist([{"run_id": 1, "repo_id": 100, "attempt": 1, "tar_path": "x",
                                              "tar_member": "1/attempt-1.zip", "archive_bytes": 1}]),
                       source / "attempt_index.parquet")
        target, reused = prepare_features(source, self.root / "output")
        self.assertFalse(reused)
        manifest = json.loads((target / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "success")
        self.assertEqual(manifest["outputs"]["workflow_profile.parquet"]["rows"], 2)
        self.assertEqual(manifest["outputs"]["workflow_week.parquet"]["rows"], 2 * 2)
        self.assertEqual(manifest["steps"][0]["output_rows"], 3)
        self.assertEqual(sorted(path.name for path in target.iterdir()),
                         ["manifest.json", "workflow_asof.parquet", "workflow_event_mix.parquet",
                          "workflow_profile.parquet", "workflow_week.parquet"])
        original = (target / "manifest.json").read_bytes()
        self.assertEqual(prepare_features(source, self.root / "output"), (target, True))
        self.assertEqual((target / "manifest.json").read_bytes(), original)
        self.assertEqual(list((self.root / "output").iterdir()), [target])

    def test_cli_reports_batch_directory(self):
        rows = [run(1, "2026-01-05T01:00:00Z")]
        path = self.write_runs(rows)
        result = subprocess.run(
            [sys.executable, "-m", "ghawtrace.feature_pipeline", "--runs-input", str(path),
             "--output-root", str(self.root / "cli")],
            capture_output=True, text=True, check=True,
        )
        payload = json.loads(result.stdout)
        self.assertTrue(Path(payload["batch_dir"]).is_dir())
        self.assertFalse(payload["reused"])

    def test_duplicate_or_null_keys_are_rejected(self):
        duplicated = [run(1, "2026-01-05T01:00:00Z"), run(1, "2026-01-05T02:00:00Z")]
        with self.assertRaises(ValueError):
            features.build_feature_tables(self.write_runs(duplicated, "duplicate.parquet"))
        missing_created = [run(1, None)]
        with self.assertRaises(ValueError):
            features.build_feature_tables(self.write_runs(missing_created, "missing.parquet"))
        bad_time = [run(1, "2026-01-05 01:00:00")]
        with self.assertRaises(ValueError):
            features.build_feature_tables(self.write_runs(bad_time, "bad.parquet"))
        null_path = [run(1, "2026-01-05T01:00:00Z", path=None)]
        with self.assertRaises(ValueError):
            features.build_feature_tables(self.write_runs(null_path, "null-path.parquet"))

    def test_output_inside_input_is_rejected(self):
        rows = [run(1, "2026-01-05T01:00:00Z")]
        source = self.root / "source"
        source.mkdir()
        pq.write_table(pa.Table.from_pylist(rows, schema=RUN_SCHEMA), source / "runs_input.parquet")
        with self.assertRaises(ValueError):
            prepare_features(source, source / "features")

    def test_empty_input_is_rejected(self):
        empty = self.write_runs([], "empty.parquet")
        with self.assertRaises(ValueError):
            features.build_feature_tables(empty)

    def test_clean_timestamp_table_is_accepted(self):
        schema = pa.schema([
            ("run_id", pa.int64()), ("repo_id", pa.int64()), ("workflow_path", pa.string()),
            ("workflow_id", pa.int64()),
            ("created_at_utc", pa.timestamp("us", tz="UTC")),
            ("updated_at_utc", pa.timestamp("us", tz="UTC")),
            ("event", pa.string()), ("status", pa.string()), ("conclusion", pa.string()),
            ("run_attempts", pa.int32()),
        ])
        rows = [
            {"run_id": 1, "repo_id": 100, "workflow_path": ".github/workflows/alpha.yml",
             "workflow_id": 7, "created_at_utc": datetime(2026, 1, 5, 0, 30, tzinfo=timezone.utc),
             "updated_at_utc": datetime(2026, 1, 5, 0, 35, tzinfo=timezone.utc),
             "event": "schedule", "status": "completed", "conclusion": "success", "run_attempts": 1},
        ]
        path = self.root / "clean.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        tables = features.build_feature_tables(path)
        profile = tables["workflow_profile"].to_pylist()[0]
        self.assertEqual(profile["run_count"], 1)
        self.assertAlmostEqual(profile["elapsed_median_seconds"], 300.0)
        self.assertEqual(profile["first_week_utc"].isoformat(), "2026-01-05")


if __name__ == "__main__":
    unittest.main()
