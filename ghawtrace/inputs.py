"""Read the teacher-provided Parquet shards and index nested log archives."""

from collections import Counter
from dataclasses import dataclass
import re
import tarfile

import pyarrow as pa
import pyarrow.parquet as pq


RUN_INPUT_SCHEMA = pa.schema([
    ("run_id", pa.int64()),
    ("repo_id", pa.int64()),
    ("workflow_id", pa.int64()),
    ("workflow_path", pa.string()),
    ("created_at", pa.string()),
    ("updated_at", pa.string()),
    ("event", pa.string()),
    ("status", pa.string()),
    ("conclusion", pa.string()),
    ("run_attempts", pa.int32()),
    ("log_archive_count", pa.int32()),
    ("log_file_count", pa.int32()),
    ("empty_archive_count", pa.int32()),
])
ATTEMPT_INDEX_SCHEMA = pa.schema([
    pa.field("run_id", pa.int64(), nullable=False),
    pa.field("repo_id", pa.int64(), nullable=False),
    pa.field("attempt", pa.int32(), nullable=False),
    pa.field("tar_path", pa.string(), nullable=False),
    pa.field("tar_member", pa.string(), nullable=False),
    pa.field("archive_bytes", pa.int64(), nullable=False),
])
ATTEMPT_MEMBER = re.compile(r"([1-9][0-9]*)/attempt-([1-9][0-9]*)\.zip")
SAMPLE_CASES = (
    "single_run_workflow", "skipped", "failure",
    "with_archive", "without_archive", "multiple_archived_attempts",
)
SAMPLE_WORKFLOWS = 12
SAMPLE_MIN_RUNS = 20
SAMPLE_MAX_RUNS = 150


def iter_run_batches(shards, columns=RUN_INPUT_SCHEMA.names):
    expected = pa.schema([RUN_INPUT_SCHEMA.field(name) for name in columns])
    for shard in shards:
        with pq.ParquetFile(shard) as source:
            actual = source.schema_arrow
            bad_fields = [
                field.name for field in expected
                if field.name not in actual.names or actual.field(field.name).type != field.type
            ]
            if bad_fields:
                raise ValueError(f"{shard.name}: missing or incompatible fields {bad_fields}")
            for batch in source.iter_batches(columns=columns, batch_size=32_768):
                yield pa.RecordBatch.from_arrays(batch.columns, schema=expected)


def index_attempts(archives):
    rows = []
    seen = set()
    for archive in archives:
        repo_id = int(archive.stem)
        if repo_id < 1:
            raise ValueError(f"Invalid repository ID in archive filename: {archive.name}")
        with tarfile.open(archive, mode="r:") as source:
            for member in source:
                if member.isdir():
                    continue
                match = ATTEMPT_MEMBER.fullmatch(member.name)
                if not match or not member.isfile():
                    raise ValueError(f"Invalid attempt member in {archive.name}: {member.name}")
                run_id, attempt = map(int, match.groups())
                key = (run_id, attempt)
                if key in seen:
                    raise ValueError(f"Duplicate attempt key: {key}")
                seen.add(key)
                rows.append({
                    "run_id": run_id, "repo_id": repo_id, "attempt": attempt,
                    "tar_path": f"logs/{archive.name}", "tar_member": member.name,
                    "archive_bytes": member.size,
                })
    return sorted(rows, key=lambda row: (row["run_id"], row["attempt"]))


@dataclass(frozen=True)
class Sample:
    anchors: dict[str, int]
    workflows: dict[tuple[int, str], int]
    scanned_runs: int


def select_sample(shards, attempts):
    attempt_counts = Counter(row["run_id"] for row in attempts)
    workflow_counts = Counter()
    first_ids = {}
    with_archives = set()
    anchors = {}
    scanned = 0
    for batch in iter_run_batches(shards, ("run_id", "repo_id", "workflow_path", "conclusion")):
        scanned += batch.num_rows
        for run_id, repo_id, path, conclusion in zip(*(column.to_pylist() for column in batch.columns)):
            if run_id is None:
                continue
            archived = attempt_counts[run_id]
            workflow = (repo_id, path)
            if repo_id is not None and path:
                workflow_counts[workflow] += 1
                first_ids[workflow] = min(first_ids.get(workflow, run_id), run_id)
                if archived:
                    with_archives.add(workflow)
            case = "with_archive" if archived else "without_archive"
            anchors[case] = min(anchors.get(case, run_id), run_id)
            if conclusion in ("skipped", "failure"):
                anchors[conclusion] = min(anchors.get(conclusion, run_id), run_id)
            if archived > 1:
                case = "multiple_archived_attempts"
                anchors[case] = min(anchors.get(case, run_id), run_id)

    singletons = [first_ids[key] for key, count in workflow_counts.items() if count == 1]
    if singletons:
        anchors["single_run_workflow"] = min(singletons)
    missing = sorted(set(SAMPLE_CASES) - anchors.keys())
    if missing:
        raise ValueError(f"Sample needs both course datasets with coverage for {missing}")
    workflows = [
        key for key in sorted(with_archives)
        if SAMPLE_MIN_RUNS <= workflow_counts[key] <= SAMPLE_MAX_RUNS
    ][:SAMPLE_WORKFLOWS]
    return Sample(anchors, {key: workflow_counts[key] for key in workflows}, scanned)


def write_runs(shards, output_path, sample):
    read_count = written_count = 0
    selected_ids = set()
    anchors = set(sample.anchors.values()) if sample else set()
    with pq.ParquetWriter(output_path, RUN_INPUT_SCHEMA) as writer:
        for batch in iter_run_batches(shards):
            read_count += batch.num_rows
            if sample:
                ids = batch.column("run_id").to_pylist()
                keys = zip(batch.column("repo_id").to_pylist(), batch.column("workflow_path").to_pylist())
                mask = [
                    run_id is not None and (run_id in anchors or key in sample.workflows)
                    for run_id, key in zip(ids, keys)
                ]
                selected_ids.update(run_id for run_id, keep in zip(ids, mask) if keep)
                batch = batch.filter(pa.array(mask, type=pa.bool_()))
            if batch.num_rows:
                writer.write_batch(batch)
                written_count += batch.num_rows
    if sample and (len(selected_ids) != written_count or not anchors <= selected_ids):
        raise ValueError("The selected sample has duplicate IDs or lacks a coverage anchor")
    return read_count, written_count, selected_ids


def describe_sample(sample, run_ids):
    return {
        "source": "provided_inputs",
        "anchors": sample.anchors,
        "single_run_workflow_count": 1,
        "cohorts": [
            {"repo_id": repo_id, "workflow_path": path, "run_count": count}
            for (repo_id, path), count in sorted(sample.workflows.items())
        ],
        "run_ids": sorted(run_ids),
    }
