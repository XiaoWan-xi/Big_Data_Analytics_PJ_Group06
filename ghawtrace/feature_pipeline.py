"""Prepare a reproducible P03 feature batch (profile, weekly panel, as-of history)."""

import argparse
from datetime import datetime, timezone
import errno
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from .features import build_feature_tables, resolve_input


PROFILE_FILE = "workflow_profile.parquet"
EVENT_MIX_FILE = "workflow_event_mix.parquet"
WEEK_FILE = "workflow_week.parquet"
ASOF_FILE = "workflow_asof.parquet"
OUTPUT_FILES = (PROFILE_FILE, EVENT_MIX_FILE, WEEK_FILE, ASOF_FILE)


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


def inventory(root, files):
    """Describe the input files relative to their root directory."""
    root = root if root.is_dir() else root.parent
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in files
    ]


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


def prepare_features(runs_input, output_root, cutoff_grid="weekly", cutoffs_path=None):
    """Build every P03 table under ``output_root/p03-<hash>`` and publish atomically."""
    runs_input = Path(runs_input).resolve()
    output_root = Path(output_root).resolve()
    if cutoffs_path is not None:
        cutoff_grid = "explicit"
    elif cutoff_grid not in ("weekly", "none"):
        raise ValueError(f"Unknown cutoff grid: {cutoff_grid}")
    if output_root == runs_input or (runs_input.is_dir() and output_root.is_relative_to(runs_input)):
        raise ValueError("Output root must be outside the input directory")
    files = resolve_input(runs_input)
    snapshots = {path: file_snapshot(path) for path in files}
    inputs = inventory(runs_input, files)
    parameters = {"interface_version": 1, "cutoff_grid": cutoff_grid}
    if cutoffs_path is not None:
        cutoffs_file = Path(cutoffs_path).resolve()
        if not cutoffs_file.is_file():
            raise ValueError(f"Cutoff table does not exist: {cutoffs_file}")
        parameters["cutoffs"] = {
            "path": cutoffs_file.name,
            "bytes": cutoffs_file.stat().st_size,
            "sha256": sha256_file(cutoffs_file),
        }
    fingerprint = json.dumps({"inputs": inputs, "parameters": parameters}, sort_keys=True).encode()
    batch_id = f"p03-{hashlib.sha256(fingerprint).hexdigest()}"
    target = output_root / batch_id
    if existing_batch(target, batch_id, set(OUTPUT_FILES)):
        return target, True

    output_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{batch_id}-", dir=output_root) as temp:
        staging = Path(temp)
        tables = build_feature_tables(
            runs_input, cutoff_grid=cutoff_grid if cutoffs_path is None else "none",
            cutoffs_path=cutoffs_path,
        )
        for path, snapshot in snapshots.items():
            if file_snapshot(path) != snapshot:
                raise ValueError("Input files changed during the run. Retry using a stable input directory")
        outputs = {}
        for name, key in (
            (PROFILE_FILE, "workflow_profile"), (EVENT_MIX_FILE, "workflow_event_mix"),
            (WEEK_FILE, "workflow_week"), (ASOF_FILE, "workflow_asof"),
        ):
            table = tables[key]
            pq.write_table(table, staging / name)
            outputs[name] = {"rows": len(table), "bytes": (staging / name).stat().st_size}
        steps = [
            {"step": "read_runs", "input_files": len(files), "output_rows": tables["runs"].row_count,
             "workflows": len(tables["workflows"]), "weeks": tables["weeks"]},
            {"step": "workflow_profile", "input_rows": tables["runs"].row_count,
             "output_rows": len(tables["workflow_profile"])},
            {"step": "workflow_event_mix", "output_rows": len(tables["workflow_event_mix"])},
            {"step": "workflow_week", "output_rows": len(tables["workflow_week"]), "weeks": tables["weeks"]},
            {"step": "workflow_asof", "output_rows": len(tables["workflow_asof"]),
             "cutoffs": len(tables["cutoffs"])},
            {"step": "reconcile", "checks": len(tables["checks"]),
             "failed": sum(not check["ok"] for check in tables["checks"].values())},
        ]
        write_json(staging / "manifest.json", {
            "batch_id": batch_id,
            "parameters": parameters,
            "inputs": inputs,
            "steps": steps,
            "checks": tables["checks"],
            "outputs": outputs,
            "status": "success",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        try:
            staging.rename(target)
        except OSError as exc:
            if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY) or not existing_batch(target, batch_id, set(OUTPUT_FILES)):
                raise
            return target, True
    return target, False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-input", required=True, type=Path,
                        help="P01 batch directory, runs_input.parquet or a directory of Parquet shards")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--cutoff-grid", choices=("weekly", "none"), default="weekly")
    parser.add_argument("--cutoffs", type=Path, help="Optional cutoff Parquet with cutoff_id and cutoff_utc")
    args = parser.parse_args()
    try:
        target, reused = prepare_features(args.runs_input, args.output_root, args.cutoff_grid, args.cutoffs)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"P03: {exc}\n")
    print(json.dumps({"batch_dir": str(target), "reused": reused}, ensure_ascii=False))


if __name__ == "__main__":
    main()
