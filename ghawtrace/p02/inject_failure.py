# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import json
import glob
import shutil
import tempfile

try:
    import importlib.util
    def load_module(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod
except ImportError:
    import imp
    def load_module(name, path):
        return imp.load_source(name, path)

HERE = os.path.dirname(os.path.abspath(__file__))
clean_runs_path = os.environ.get("CLEAN_RUNS_PATH") or os.path.join(HERE, "clean_runs.py")
if not os.path.isfile(clean_runs_path):
    print("ERROR: clean_runs.py not found at", clean_runs_path)
    sys.exit(2)
print("[setup] loading clean_runs from:", clean_runs_path)
clean_mod = load_module("clean_runs", clean_runs_path)

from pyspark.sql import SparkSession

ROWS = [
    (1, 10, None, ".github/workflows/a.yml", "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (2, 10, 100, ".github/workflows/a.yml", "2026-01-01T00:01:00Z", "2026-01-01T00:01:02Z",
     "push", None, None, 1, 0, 0, 0),
    (3, 10, 100, None, "2026-01-01T00:02:00Z", "2026-01-01T00:02:01Z",
     "push", "completed", "success", 1, 0, 0, 0),
    (4, 10, 100, ".github/workflows/b.yml", "2026-01-01T00:03:00Z", "2026-01-01T00:03:01Z",
     "push", "completed", "failure", 1, 0, 0, 0),
]


def make_input(spark, path):
    df = spark.createDataFrame(ROWS, clean_mod.P01_RUN_COLUMNS)
    df.write.mode("overwrite").parquet("file://" + path)


def run_clean(input_dir, output_dir, report_path):
    orig_argv = sys.argv
    orig_stop = _spark_ref[0].stop
    _spark_ref[0].stop = lambda: None
    try:
        sys.argv = ["clean_runs.py", "file://" + input_dir, "file://" + output_dir, report_path]
        clean_mod.main()
    finally:
        sys.argv = orig_argv
        _spark_ref[0].stop = orig_stop


def read_current_token(output_dir):
    p = os.path.join(output_dir, "current", "_SUCCESS")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        try:
            return json.load(f).get("batch_token")
        except Exception:
            return None


def current_is_complete(output_dir):
    cur = os.path.join(output_dir, "current")
    if not os.path.isdir(cur):
        return False, "current dir missing"
    if not os.path.exists(os.path.join(cur, "_SUCCESS")):
        return False, "current/_SUCCESS missing"
    for sub in ("runs_clean", "runs_quarantine"):
        d = os.path.join(cur, sub)
        if not os.path.isdir(d):
            return False, "%s missing" % sub
        parts = glob.glob(os.path.join(d, "part-*.parquet"))
        if not parts:
            return False, "%s has no parquet" % sub
    return True, "ok"


_spark_ref = [None]


def main():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("inject-failure")
             .getOrCreate())
    _spark_ref[0] = spark

    # 4 个故障点：前 3 个应保留旧批次；第 4 个暴露 rename 窗口期
    fault_points = [
        "after_batch_clean_write",
        "after_batch_success",
        "after_stage_success",
        "after_rename_current_to_backup",
    ]

    results = []

    try:
        for fp in fault_points:
            tmp = tempfile.mkdtemp(prefix="p02_ft_%s_" % fp)
            try:
                input_dir = os.path.join(tmp, "in")
                output_dir = os.path.join(tmp, "out")
                rep1 = os.path.join(tmp, "rep1.json")
                rep2 = os.path.join(tmp, "rep2.json")

                make_input(spark, input_dir)

                if "P02_FAULT_INJECT" in os.environ:
                    del os.environ["P02_FAULT_INJECT"]
                run_clean(input_dir, output_dir, rep1)
                token1 = read_current_token(output_dir)
                ok1, msg1 = current_is_complete(output_dir)
                if not ok1 or not token1:
                    results.append((fp, False, "first run not usable: " + msg1))
                    continue

                os.environ["P02_FAULT_INJECT"] = fp
                failed = False
                try:
                    run_clean(input_dir, output_dir, rep2)
                except Exception as e:
                    failed = True
                    err_msg = str(e)[:120]
                finally:
                    if "P02_FAULT_INJECT" in os.environ:
                        del os.environ["P02_FAULT_INJECT"]

                if not failed:
                    results.append((fp, False, "expected failure but succeeded"))
                    continue

                token_after = read_current_token(output_dir)
                ok2, msg2 = current_is_complete(output_dir)

                if token_after != token1:
                    results.append((fp, False,
                                    "current changed: before=%s after=%s" % (token1, token_after)))
                elif not ok2:
                    results.append((fp, False,
                                    "current incomplete after fault: " + msg2))
                else:
                    results.append((fp, True,
                                    "current intact: %s; fault: %s" % (token1, err_msg)))
            finally:
                shutil.rmtree(tmp, True)
    finally:
        spark.stop()

    print("=" * 60)
    print("FAULT INJECTION RESULTS")
    for fp, ok, msg in results:
        print(("PASS" if ok else "FAIL"), "-", fp, "-", msg)
    print("=" * 60)
    all_pass = all(ok for _, ok, _ in results)
    print("RESULT:", "ALL PASS" if all_pass else "SOME FAILED")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
