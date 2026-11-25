# -*- coding: utf-8 -*-
import importlib
import importlib.machinery
import importlib.util
import inspect
import json
import os
import sys
import tempfile
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
P02_DIR = REPO_ROOT / "ghawtrace"

_COMPAT_SHIMS_INSTALLED = False
_USED_PYSPARK_STUB = False


def _install_compat_shims():
    global _COMPAT_SHIMS_INSTALLED, _USED_PYSPARK_STUB
    if _COMPAT_SHIMS_INSTALLED:
        return

    if "urllib2" not in sys.modules:
        try:
            import urllib2 as _real_urllib2  # noqa: F401
        except ImportError:
            urllib2_mod = types.ModuleType("urllib2")
            urllib2_mod.__spec__ = importlib.machinery.ModuleSpec("urllib2", loader=None)
            urllib2_mod.urlopen = urllib.request.urlopen
            urllib2_mod.Request = urllib.request.Request
            urllib2_mod.URLError = urllib.error.URLError
            urllib2_mod.HTTPError = urllib.error.HTTPError
            sys.modules["urllib2"] = urllib2_mod

    has_real = False
    try:
        has_real = importlib.util.find_spec("pyspark") is not None
    except ValueError:
        has_real = False

    if not has_real:
        _USED_PYSPARK_STUB = True
        pyspark_mod = types.ModuleType("pyspark")
        pyspark_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark", loader=None)
        pyspark_mod.__path__ = []

        sql_mod = types.ModuleType("pyspark.sql")
        sql_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql", loader=None)
        sql_mod.__path__ = []

        functions_mod = types.ModuleType("pyspark.sql.functions")
        functions_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.functions", loader=None)

        types_mod = types.ModuleType("pyspark.sql.types")
        types_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.types", loader=None)

        window_mod = types.ModuleType("pyspark.sql.window")
        window_mod.__spec__ = importlib.machinery.ModuleSpec("pyspark.sql.window", loader=None)

        class _DummySparkContext(object):
            appId = "local-stub-app"
            applicationId = "local-stub-app"

        class _DummyWriter(object):
            def mode(self, save_mode):
                return self

            def parquet(self, path):
                Path(path).mkdir(parents=True, exist_ok=True)

        class _DummyDataFrame(object):
            def __init__(self, data, schema=None):
                self._data = data
                self._schema = schema
                self.write = _DummyWriter()

        class _DummySparkSession(object):
            class builder(object):
                @classmethod
                def appName(cls, name):
                    return cls

                @classmethod
                def master(cls, url):
                    return cls

                @classmethod
                def config(cls, *args, **kwargs):
                    return cls

                @classmethod
                def enableHiveSupport(cls):
                    return cls

                @classmethod
                def getOrCreate(cls):
                    return _DummySparkSession()

            def __init__(self):
                self.sparkContext = _DummySparkContext()

            def createDataFrame(self, data, schema=None):
                return _DummyDataFrame(data, schema)

            def stop(self):
                pass

        class _DummyCol(object):
            def __init__(self, *args, **kwargs):
                pass

            def cast(self, *args, **kwargs):
                return self

            def desc(self):
                return self

            def asc(self):
                return self

        class _DummyWindowSpec(object):
            def partitionBy(self, *args, **kwargs):
                return self

            def orderBy(self, *args, **kwargs):
                return self

            def rowsBetween(self, *args, **kwargs):
                return self

            def rangeBetween(self, *args, **kwargs):
                return self

        class _DummyWindow(object):
            unboundedPreceding = -1
            unboundedFollowing = 1
            currentRow = 0

            @staticmethod
            def partitionBy(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def orderBy(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def rowsBetween(*args, **kwargs):
                return _DummyWindowSpec()

            @staticmethod
            def rangeBetween(*args, **kwargs):
                return _DummyWindowSpec()

        def _udf(fn, return_type=None):
            return fn

        sql_mod.SparkSession = _DummySparkSession
        sql_mod.functions = functions_mod
        sql_mod.Window = _DummyWindow
        window_mod.Window = _DummyWindow
        window_mod.WindowSpec = _DummyWindowSpec
        functions_mod.udf = _udf
        functions_mod.col = lambda *a, **kw: _DummyCol(*a, **kw)
        types_mod.DoubleType = lambda: "double"
        types_mod.StringType = lambda: "string"
        pyspark_mod.sql = sql_mod

        for mod_name, mod_obj in [
            ("pyspark", pyspark_mod),
            ("pyspark.sql", sql_mod),
            ("pyspark.sql.functions", functions_mod),
            ("pyspark.sql.types", types_mod),
            ("pyspark.sql.window", window_mod),
        ]:
            sys.modules[mod_name] = mod_obj

    _COMPAT_SHIMS_INSTALLED = True


def _load_p02_module(module_name, file_path):
    _install_compat_shims()
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


convert_mod = _load_p02_module("convert_for_spark2", P02_DIR / "convert_for_spark2.py")
clean_mod = _load_p02_module("clean_runs", P02_DIR / "clean_runs.py")
bench_mod = _load_p02_module("partition_bench", P02_DIR / "partition_bench.py")


def _make_sample_table(num_rows=2):
    cols = {
        "run_id": [101 + i for i in range(num_rows)],
        "repo_id": [1001 for _ in range(num_rows)],
        "workflow_id": [2001 for _ in range(num_rows)],
        "workflow_path": [".github/workflows/ci.yml" for _ in range(num_rows)],
        "created_at": ["2026-01-01T00:00:00.900Z" for _ in range(num_rows)],
        "updated_at": ["2026-01-01T00:00:01.100Z" for _ in range(num_rows)],
        "event": ["push" for _ in range(num_rows)],
        "status": ["completed" for _ in range(num_rows)],
        "conclusion": ["success" for _ in range(num_rows)],
        "run_attempts": [1 for _ in range(num_rows)],
        "log_archive_count": [1 for _ in range(num_rows)],
        "log_file_count": [2 for _ in range(num_rows)],
        "empty_archive_count": [0 for _ in range(num_rows)],
        "github_metadata_json": ['{"extra": true}' for _ in range(num_rows)],
        "head_sha": ["abc123" for _ in range(num_rows)],
    }
    return pa.table(cols)


def _run_convert_main(args):
    orig_argv = sys.argv
    arg_list = [str(a) for a in args]
    try:
        sys.argv = ["convert_for_spark2.py"] + arg_list
        params = list(inspect.signature(convert_mod.main).parameters.values())
        required_params = [
            p for p in params
            if p.default is inspect.Parameter.empty
            and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        if len(required_params) >= 1:
            rc = convert_mod.main(arg_list)
        else:
            rc = convert_mod.main()
        return 0 if rc is None else int(rc)
    except SystemExit as exc:
        return 0 if exc.code in (None, 0) else int(exc.code)
    finally:
        sys.argv = orig_argv


class TestConvertForSpark2Regression(unittest.TestCase):
    def test_rejects_same_input_and_output_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp) / "shard_dir"
            work_dir.mkdir()
            pq.write_table(_make_sample_table(2), work_dir / "part-000.parquet")
            before_hash = convert_mod._sha256_file(work_dir / "part-000.parquet")

            rc = _run_convert_main([str(work_dir), str(work_dir)])
            self.assertNotEqual(rc, 0)
            self.assertEqual(convert_mod._sha256_file(work_dir / "part-000.parquet"), before_hash)

    def test_rejects_overlapping_input_output_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            in_dir = Path(tmp) / "input"
            out_dir = in_dir / "nested_output"
            in_dir.mkdir(parents=True)
            pq.write_table(_make_sample_table(1), in_dir / "part-000.parquet")

            rc = _run_convert_main([str(in_dir), str(out_dir)])
            self.assertNotEqual(rc, 0)

    def test_limit_one_isolated_batch_has_exact_single_shard_and_preserves_source_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            in_dir = Path(tmp) / "raw_shards"
            out_full = Path(tmp) / "out_full"
            out_sample = Path(tmp) / "out_sample_limit1"
            in_dir.mkdir()

            shard_a = in_dir / "shard_000.parquet"
            shard_b = in_dir / "shard_001.parquet"
            pq.write_table(_make_sample_table(2), shard_a)
            pq.write_table(_make_sample_table(3), shard_b)

            hash_a_before = convert_mod._sha256_file(shard_a)
            hash_b_before = convert_mod._sha256_file(shard_b)

            rc_full = _run_convert_main([str(in_dir), str(out_full)])
            self.assertEqual(rc_full, 0)
            self.assertEqual(
                sorted(p.name for p in out_full.glob("*.parquet")),
                ["shard_000.parquet", "shard_001.parquet"],
            )

            rc_sample = _run_convert_main([str(in_dir), str(out_sample), "1"])
            self.assertEqual(rc_sample, 0)
            out_sample_shards = sorted(p.name for p in out_sample.glob("*.parquet"))
            self.assertEqual(out_sample_shards, ["shard_000.parquet"])

            manifest = json.loads((out_sample / "_conversion_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["shard_count"], 1)
            self.assertEqual(manifest["shards"][0]["source_sha256"], hash_a_before)

            self.assertEqual(convert_mod._sha256_file(shard_a), hash_a_before)
            self.assertEqual(convert_mod._sha256_file(shard_b), hash_b_before)


class TestCleanRunsTimestampAndPrecision(unittest.TestCase):
    def test_cross_second_fractional_duration_is_not_truncated_to_one_second(self):
        _, t_start = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:00.900Z")
        _, t_end = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:01.100Z")
        self.assertIsNotNone(t_start)
        self.assertIsNotNone(t_end)
        delta = round(t_end - t_start, 6)
        self.assertAlmostEqual(delta, 0.2, places=6)

    def test_same_second_subsecond_inversion_is_negative_delta(self):
        _, t_start = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:05.500Z")
        _, t_end = clean_mod.parse_utc_timestamp_py("2026-01-01T00:00:05.200Z")
        self.assertIsNotNone(t_start)
        self.assertIsNotNone(t_end)
        self.assertLess(t_end - t_start, 0.0)

    def test_invalid_or_empty_timestamp_returns_none_tuple(self):
        self.assertEqual(clean_mod.parse_utc_timestamp_py(None), (None, None))
        self.assertEqual(clean_mod.parse_utc_timestamp_py(""), (None, None))
        self.assertEqual(clean_mod.parse_utc_timestamp_py("not-a-timestamp"), (None, None))


class TestPartitionBenchTelemetryFailure(unittest.TestCase):
    def test_null_totals_marks_metrics_unavailable_instead_of_zero(self):
        totals = bench_mod._null_totals()
        self.assertIsNone(totals["stage_count"])
        self.assertIsNone(totals["total_tasks"])
        self.assertIsNone(totals["executor_run_time_ms"])
        self.assertIsNone(totals["shuffle_read_bytes"])
        self.assertIsNone(totals["shuffle_write_bytes"])

    def test_fetch_spark_rest_returns_error_dict_on_unreachable_endpoint(self):
        res = bench_mod.fetch_spark_rest("http://127.0.0.1:1", "app-test-0001", "stages")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)


class TestPySparkEndToEndIfAvailable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.has_pyspark = not _USED_PYSPARK_STUB

    def test_clean_runs_retains_unfinished_and_nulls_inverted_elapsed_seconds(self):
        if not self.has_pyspark:
            self.skipTest("pyspark not installed in local Python environment")

        spark_sql_mod = importlib.import_module("pyspark.sql")
        SparkSession = spark_sql_mod.SparkSession

        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test-p02-clean-runs")
            .getOrCreate()
        )
        try:
            rows = [
                (1, 10, 100, ".github/workflows/a.yml",
                 "2026-01-01T00:00:00.900Z", "2026-01-01T00:00:01.100Z",
                 "push", "completed", "success", 1, 1, 1, 0),
                (2, 10, 101, ".github/workflows/b.yml",
                 "2026-01-01T00:01:00.000Z", "2026-01-01T00:01:02.000Z",
                 "push", "queued", None, 1, 0, 0, 0),
                (3, 10, 102, ".github/workflows/c.yml",
                 "2026-01-01T00:02:00.500Z", "2026-01-01T00:02:00.200Z",
                 "pull_request", "completed", "failure", 1, 0, 0, 0),
                (4, 10, 103, "",
                 "2026-01-01T00:03:00.000Z", "2026-01-01T00:03:01.000Z",
                 "push", "completed", "success", 1, 0, 0, 0),
            ]
            cols = clean_mod.P01_RUN_COLUMNS
            in_df = spark.createDataFrame(rows, cols)

            with tempfile.TemporaryDirectory() as tmp:
                in_dir = os.path.join(tmp, "in_parquet")
                out_dir = os.path.join(tmp, "out_clean")
                report_path = os.path.join(tmp, "report.json")
                in_df.write.mode("overwrite").parquet(in_dir)

                orig_argv = sys.argv
                try:
                    sys.argv = ["clean_runs.py", in_dir, out_dir, report_path]
                    clean_mod.main()
                finally:
                    sys.argv = orig_argv

                rep = json.loads(Path(report_path).read_text(encoding="utf-8"))
                self.assertTrue(rep["reconciled_equals_input"])
                self.assertEqual(rep["retained_rows"], 3)
                self.assertEqual(rep["total_quarantined_rows"], 1)
                self.assertEqual(rep["retained_unfinished_runs"]["total_rows"], 1)
                self.assertEqual(rep["quality_statistics"]["time_inversion_count"], 1)

                clean_rows = {
                    r["run_id"]: r
                    for r in spark.read.parquet(os.path.join(out_dir, "runs_clean")).collect()
                }
                self.assertIn(2, clean_rows)
                self.assertAlmostEqual(clean_rows[1]["elapsed_seconds"], 0.2, places=5)
                self.assertIsNone(clean_rows[3]["elapsed_seconds"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()