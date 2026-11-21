"""Prepare a reproducible P01 input batch."""

import argparse
from datetime import datetime, timezone
import errno
import hashlib
import json
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from .inputs import (
    ATTEMPT_INDEX_SCHEMA, SAMPLE_MAX_RUNS, SAMPLE_MIN_RUNS, SAMPLE_WORKFLOWS,
    describe_sample, index_attempts, select_sample, write_runs,
)


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_snapshot(path):
    info = path.stat()
    return info.st_ino, info.st_size, info.st_mtime_ns


def find_inputs(root):
    shards = sorted((root / "runs").glob("*.parquet"))
    archives = sorted((root / "logs").glob("*.tar"))
    if not shards or not archives:
        raise ValueError("Input root must contain runs/*.parquet and logs/*.tar (unpack both RAR files first)")
    return shards, archives


def verify_inputs(root, shards, archives, snapshots):
    if find_inputs(root) != (shards, archives) or any(
        file_snapshot(path) != snapshot for path, snapshot in snapshots.items()
    ):
        raise ValueError("Input files changed during the run. Retry using a stable input directory")


def existing_batch(path, batch_id, expected_outputs):
    if not path.exists():
        return False
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        outputs = manifest["outputs"]
        if (
            manifest["status"] == "success" and manifest["batch_id"] == batch_id
            and set(outputs) == expected_outputs
            and all((path / name).stat().st_size == outputs[name]["bytes"] for name in expected_outputs)
        ):
            return True
    except (OSError, ValueError, TypeError, KeyError):
        pass
    raise ValueError(f"Output already exists without a valid completed batch: {path}")


def prepare_batch(input_root, output_root, mode):
    if mode not in ("sample", "full"):
        raise ValueError(f"Unknown mode: {mode}")
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    if any(output_root.is_relative_to(input_root / name) for name in ("runs", "logs")):
        raise ValueError("Output root must be outside the source runs/ and logs/ directories")
    shards, archives = find_inputs(input_root)
    paths = [*shards, *archives]
    snapshots = {path: file_snapshot(path) for path in paths}
    inventory = [
        {"path": path.relative_to(input_root).as_posix(), "bytes": snapshots[path][1], "sha256": sha256_file(path)}
        for path in paths
    ]
    verify_inputs(input_root, shards, archives, snapshots)
    parameters = {"interface_version": 1, "mode": mode}
    if mode == "sample":
        parameters.update(
            sample_rule_version=1, max_workflows=SAMPLE_WORKFLOWS,
            min_workflow_runs=SAMPLE_MIN_RUNS, max_workflow_runs=SAMPLE_MAX_RUNS,
        )
    fingerprint = json.dumps({"inputs": inventory, "parameters": parameters}, sort_keys=True).encode()
    batch_id = f"{mode}-{hashlib.sha256(fingerprint).hexdigest()}"
    target = output_root / batch_id
    expected = {"runs_input.parquet", "attempt_index.parquet"}
    if mode == "sample":
        expected.add("sample_manifest.json")
    if existing_batch(target, batch_id, expected):
        return target, True

    output_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{batch_id}-", dir=output_root) as temp:
        staging = Path(temp)
        attempts = index_attempts(archives)
        steps = [
            {"step": "inventory", "input_files": len(paths), "output_files": len(inventory)},
            {"step": "index_attempts", "input_tar_files": len(archives), "output_rows": len(attempts)},
        ]
        sample = select_sample(shards, attempts) if mode == "sample" else None
        read, written, run_ids = write_runs(shards, staging / "runs_input.parquet", sample)
        selected_attempts = attempts
        outputs = {"runs_input.parquet": {"rows": written}}
        if sample:
            selected_attempts = [row for row in attempts if row["run_id"] in run_ids]
            description = describe_sample(sample, run_ids)
            write_json(staging / "sample_manifest.json", description)
            outputs["sample_manifest.json"] = {"run_ids": len(run_ids)}
            steps.append({
                "step": "select_sample", "input_rows": sample.scanned_runs,
                "output_run_ids": len(run_ids), "coverage_cases": len(sample.anchors),
            })
        pq.write_table(pa.Table.from_pylist(selected_attempts, schema=ATTEMPT_INDEX_SCHEMA), staging / "attempt_index.parquet")
        outputs["attempt_index.parquet"] = {"rows": len(selected_attempts)}
        steps.extend([
            {"step": "write_runs", "input_rows": read, "output_rows": written},
            {"step": "write_attempts", "input_rows": len(attempts), "output_rows": len(selected_attempts)},
        ])
        verify_inputs(input_root, shards, archives, snapshots)
        for name in outputs:
            outputs[name]["bytes"] = (staging / name).stat().st_size
        write_json(staging / "manifest.json", {
            "batch_id": batch_id, "parameters": parameters, "inputs": inventory, "steps": steps,
            "outputs": outputs, "status": "success",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        try:
            staging.rename(target)
        except OSError as exc:
            if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY) or not existing_batch(target, batch_id, expected):
                raise
            return target, True
    return target, False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("sample", "full"))
    args = parser.parse_args()
    try:
        target, reused = prepare_batch(args.input_root, args.output_root, args.mode)
    except (OSError, ValueError, tarfile.TarError) as exc:
        parser.exit(1, f"P01: {exc}\n")
    print(json.dumps({"batch_dir": str(target), "reused": reused}, ensure_ascii=False))


if __name__ == "__main__":
    main()
