"""P01 acceptance tests with synthetic data, separate from the course sample."""

from collections import Counter
from datetime import date, datetime, timedelta
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

from ghawtrace.inputs import RUN_INPUT_SCHEMA
from ghawtrace.pipeline import prepare_batch


class PipelineAcceptanceTest(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve()
        self.source = self.root / "source"
        self.output = self.root / "output"
        for name in ("runs", "logs"):
            (self.source / name).mkdir(parents=True)
        rows = [
            {
                "run_id": run_id, "repo_id": 1, "workflow_id": 11,
                "workflow_path": ".github/workflows/cohort.yml", "created_at": "2026-09-01T00:00:00Z",
                "updated_at": "2026-09-01T00:01:00Z", "conclusion": "success", "run_attempts": 1,
            }
            for run_id in range(10, 32)
        ]
        rows.append({
            **rows[0], "run_id": 1, "workflow_id": 12,
            "workflow_path": ".github/workflows/singleton.yml",
            "conclusion": "failure", "updated_at": None,
        })
        rows[0].update(conclusion="skipped")
        rows[1].update(run_attempts=2)
        self.shard = self.source / "runs" / "part-00000.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RUN_INPUT_SCHEMA), self.shard)
        archive = BytesIO()
        with ZipFile(archive, "w") as zipped:
            zipped.writestr("1_agent.txt", "synthetic event\nsecond synthetic event\n")
        payload = archive.getvalue()
        with tarfile.open(self.source / "logs" / "1.tar", "w") as tar:
            for member in ("11/attempt-2.zip", "11/attempt-1.zip", "12/attempt-1.zip"):
                entry = tarfile.TarInfo(member)
                entry.size = len(payload)
                tar.addfile(entry, BytesIO(payload))

    def command(self, mode):
        return [
            sys.executable, "-m", "ghawtrace.pipeline",
            "--input-root", str(self.source), "--output-root", str(self.output), "--mode", mode,
        ]

    def test_sample_coverage_and_repeat_selection(self):
        first, reused = prepare_batch(self.source, self.output, "sample")
        second, _ = prepare_batch(self.source, self.root / "other-output", "sample")
        self.assertFalse(reused)
        self.assertEqual(first.name, second.name)
        manifest = first / "sample_manifest.json"
        self.assertEqual(manifest.read_bytes(), (second / manifest.name).read_bytes())
        sample = json.loads(manifest.read_text())
        self.assertEqual(sample["source"], "provided_inputs")
        self.assertEqual(sample["anchors"], {
            "single_run_workflow": 1, "skipped": 10, "failure": 1,
            "with_archive": 11, "without_archive": 1, "multiple_archived_attempts": 11,
        })
        self.assertEqual(sample["run_ids"], [1, *range(10, 32)])
        self.assertEqual(sample["cohorts"][0]["run_count"], 22)

    def test_contract_joins_conserve_run_counts(self):
        target, _ = prepare_batch(self.source, self.output, "sample")
        runs = pq.read_table(target / "runs_input.parquet").to_pylist()
        attempts = pq.read_table(target / "attempt_index.parquet").to_pylist()
        workflow_counts = Counter((r["repo_id"], r["workflow_path"]) for r in runs)
        weeks = Counter()
        for run in runs:
            created = datetime.fromisoformat(run["created_at"]).date()
            utc_monday = created - timedelta(days=created.weekday())
            weeks[(run["repo_id"], run["workflow_path"], utc_monday)] += 1
        self.assertTrue(all(key[-1] == date(2026, 8, 31) for key in weeks))
        self.assertEqual(sum(workflow_counts.values()), len(runs))
        self.assertEqual(sum(weeks.values()), len(runs))
        counts = Counter(row["run_id"] for row in attempts)
        self.assertEqual(counts, {11: 2, 12: 1})
        # One evidence row per attempt / member / line. Two evidence lines on attempt 1.
        evidence = [(11, 1, "1_agent.txt", 1), (11, 1, "1_agent.txt", 2), (11, 2, "1_agent.txt", 1)]
        self.assertEqual(len(evidence), len(set(evidence)))
        self.assertTrue(
            set(key[:2] for key in evidence) <= {(row["run_id"], row["attempt"]) for row in attempts}
        )
        evidence_by_run = Counter(key[0] for key in evidence)
        joined = [(row["run_id"], counts.get(row["run_id"]), evidence_by_run.get(row["run_id"])) for row in runs]
        self.assertEqual(len(joined), len({row[0] for row in joined}))
        self.assertEqual(len(joined), len(runs))
        self.assertIn((11, 2, 3), joined)
        self.assertIn((1, None, None), joined)

    def test_full_rerun_reuses_batch_and_records_counts(self):
        target, reused = prepare_batch(self.source, self.output, "full")
        self.assertFalse(reused)
        original = (target / "manifest.json").read_bytes()
        self.assertEqual(prepare_batch(self.source, self.output, "full"), (target, True))
        self.assertEqual((target / "manifest.json").read_bytes(), original)
        self.assertEqual(list(self.output.iterdir()), [target])
        result = json.loads(original)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["parameters"]["mode"], "full")
        self.assertEqual(result["outputs"]["runs_input.parquet"]["rows"], 23)
        self.assertEqual(result["outputs"]["attempt_index.parquet"]["rows"], 3)
        self.assertEqual(len(result["inputs"]), 2)

    def test_interrupted_process_does_not_publish_partial_outputs(self):
        # Exit without cleanup after both Parquet files have been written.
        code = """
import os
import sys
from pathlib import Path
from ghawtrace import pipeline
from unittest.mock import patch

with patch.object(pipeline, "write_json", side_effect=lambda *args: os._exit(23)):
    pipeline.prepare_batch(Path(sys.argv[1]), Path(sys.argv[2]), "full")
"""
        status = subprocess.run([sys.executable, "-c", code, str(self.source), str(self.output)])
        self.assertEqual(status.returncode, 23)
        self.assertEqual(list(self.output.glob("full-*")), [])
        stage, = list(self.output.glob(".full-*"))
        self.assertTrue((stage / "runs_input.parquet").exists())
        self.assertTrue((stage / "attempt_index.parquet").exists())
        self.assertFalse((stage / "manifest.json").exists())
        subprocess.run(self.command("full"), capture_output=True, check=True)
        target, = list(self.output.glob("full-*"))
        self.assertEqual(json.loads((target / "manifest.json").read_text())["status"], "success")

    def test_concurrent_writers_publish_one_batch(self):
        with subprocess.Popen(self.command("full"), stdout=subprocess.PIPE) as first:
            second = subprocess.run(self.command("full"), capture_output=True, check=True)
            first_output, _ = first.communicate(timeout=30)
        self.assertEqual(first.returncode, 0)
        results = [json.loads(first_output), json.loads(second.stdout)]
        self.assertEqual(results[0]["batch_dir"], results[1]["batch_dir"])
        self.assertEqual(sorted(result["reused"] for result in results), [False, True])
        self.assertEqual(len(list(self.output.iterdir())), 1)


if __name__ == "__main__":
    unittest.main()
