# -*- coding: utf-8 -*-
"""Fault injection acceptance for publish robustness (review item 3 / item 2).

Run from repo root on a Spark 2.x node:

    spark-submit --master local[1] \
      --py-files ghawtrace/p02/clean_runs.py \
      ghawtrace/p02/inject_failure.py

Env:
    CLEAN_RUNS_PATH  override; default: ./clean_runs.py (same dir)
"""
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
InjectedFault = getattr(clean_mod, "InjectedFault", None)
if InjectedFault is None:
    print("ERROR: clean_runs.py does not export InjectedFault.")
    print("Make sure _maybe_inject_fault raises InjectedFault(stage_name).")
    sys.exit(2)

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

FAULT_POINTS = [
    "after_batch_clean_write",
    "after_batch_success",
    "after_stage_success",
    "after_rename_current_to_backup",
]

_spark_ref = [None]


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


def _local(p):
    return p[len("file://"):] if p.startswith("file://") else p


def verify_rep1_paths(spark, rep1_path):
    """From the first report, read both tables at their fixed paths and
    return (batch_token, clean_rows, quarantine_rows) or (None, msg)."""
    with open(rep1_path) as f:
        r1 = json.load(f)
    old_clean = r1["clean_path"]
    old_q = r1["quarantine_path"]
    old_token = r1["batch_token"]

    clean_rows = spark.read.parquet(old_clean).count()
    q_path = _local(old_q)
    q_rows = spark.read.parquet(old_q).count() if os.path.isdir(q_path) else 0

    success_file = _local(old_clean.rsplit("/", 1)[0] + "/_SUCCESS")
    with open(success_file) as f:
        success_dict = json.load(f)
    success_token = success_dict.get("batch_token")

    if success_token != old_token:
        return None, "rep1 _SUCCESS token mismatch: %s != %s" % (success_token, old_token)
    if clean_rows <= 0:
        return None, "rep1 clean_path has 0 rows: %s" % old_clean
    return (old_token, clean_rows, q_rows), "ok"


def run_one_fault_point(spark, fp):
    """Returns (ok, message). On PASS, message carries stage/batch_token/rows."""
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
            return False, "first run not usable: " + msg1

        os.environ["P02_FAULT_INJECT"] = fp
        fault_hit = False
        fault_mismatch = None
        other_exc = None
        try:
            run_clean(input_dir, output_dir, rep2)
        except InjectedFault as e:
            if getattr(e, "stage_name", None) == fp:
                fault_hit = True
            else:
                fault_mismatch = getattr(e, "stage_name", "<missing>")
        except Exception as e:
            other_exc = "%s: %s" % (type(e).__name__, str(e)[:200])
        finally:
            if "P02_FAULT_INJECT" in os.environ:
                del os.environ["P02_FAULT_INJECT"]

        if fault_mismatch is not None:
            return False, "InjectedFault for wrong stage: %s" % fault_mismatch
        if other_exc is not None:
            return False, "unexpected non-injected exception: %s" % other_exc
        if not fault_hit:
            return False, "no exception raised; injection point not reached"

        info, msg = verify_rep1_paths(spark, rep1)
        if info is None:
            return False, msg

        batch_token, clean_rows, q_rows = info
        token_after = read_current_token(output_dir)
        if token_after != token1:
            return False, "current changed: before=%s after=%s" % (token1, token_after)
        ok2, msg2 = current_is_complete(output_dir)
        if not ok2:
            return False, "current incomplete after fault: " + msg2

        return True, (
            "stage=%s; batch_token=%s; clean_rows=%d; quarantine_rows=%d"
            % (fp, batch_token, clean_rows, q_rows)
        )
    finally:
        shutil.rmtree(tmp, True)


def run_negative_regression(spark):
    """Non-injection failure before publish must NOT count as a fault hit.

    Simulates an ordinary input error on the second run: delete the input
    parquet so clean_runs.py fails before reaching any publish stage.
    Expected:
      - clean_runs.py raises a non-InjectedFault exception
      - current/ still points to the first batch, intact
    """
    tmp = tempfile.mkdtemp(prefix="p02_neg_")
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
            return False, "negative regression: first run not usable: " + msg1

        # Corrupt input: remove all parquet files
        for name in os.listdir(input_dir):
            if name.endswith(".parquet"):
                os.remove(os.path.join(input_dir, name))

        got_injected = False
        got_other = None
        try:
            run_clean(input_dir, output_dir, rep2)
        except InjectedFault:
            got_injected = True
        except Exception as e:
            got_other = "%s: %s" % (type(e).__name__, str(e)[:200])

        ok_cur, msg_cur = current_is_complete(output_dir)
        cur_token = read_current_token(output_dir)

        print("-" * 60)
        print("NEGATIVE REGRESSION: input corrupted, no fault injection")
        print("  got_InjectedFault       :", got_injected, "(expected False)")
        print("  got_other_exception     :", got_other, "(expected non-None)")
        print("  current_intact          :", ok_cur, msg_cur)
        print("  current_token_unchanged :", cur_token == token1)

        ok = (
            (not got_injected)
            and (got_other is not None)
            and ok_cur
            and (cur_token == token1)
        )
        return ok, ("input_corrupted: InjectedFault=%s, other_exc=%s, current_unchanged=%s"
                    % (got_injected, "yes" if got_other else "no", cur_token == token1))
    finally:
        shutil.rmtree(tmp, True)


def main():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("inject-failure")
             .getOrCreate())
    _spark_ref[0] = spark

    results = []
    try:
        for fp in FAULT_POINTS:
            ok, msg = run_one_fault_point(spark, fp)
            results.append((fp, ok, msg))
        neg_ok, neg_msg = run_negative_regression(spark)
        results.append(("input_corrupted", neg_ok, neg_msg))
    finally:
        spark.stop()

    print("=" * 60)
    print("FAULT INJECTION RESULTS")
    for fp, ok, msg in results:
        print(("PASS" if ok else "FAIL"), "-", fp, "-", msg)
    print("=" * 60)

    all_pass = bool(results) and all(ok for _, ok, _ in results)
    print("RESULT:", "ALL PASS" if all_pass else "SOME FAILED")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()