"""P03 workflow features: profiles, continuous UTC weekly panels and as-of history.

All metrics are computed from the P01/P02 run table on UTC timestamps.  The
denominators are fixed here so that P04 (clustering), P05 (run scoring) and P07
(joint scoring) reuse identical values:

* event, hour and conclusion shares divide by all runs of the workflow.
* ``skipped_rate`` divides skipped runs by all runs; ``failure_rate`` divides
  failures by ``success + failure`` only and is null when that denominator is 0.
* ``retry_rate`` divides completed runs with ``run_attempts > 1`` by completed
  runs with a known attempt count; ``total_attempts`` and ``retried_run_count``
  keep those counts.  In-progress runs do not have a final attempt count.
* ``elapsed_seconds`` is ``updated_at - created_at`` for completed runs.  A run
  without an update timestamp, with ``updated_at < created_at``, or that is not
  completed has a null elapsed value and is counted in ``elapsed_missing_count``;
  completed negative rows are also counted as ``elapsed_negative_count``.
* intervals use the gap between consecutive run creation times of one workflow;
  a single-run workflow has no gap and every interval feature is null with
  ``interval_estimable = false``.
* ``active_day_rate`` divides distinct UTC days with a run by the inclusive
  calendar-day span from the first to the last run of the workflow.
* percentiles are exact linear-interpolation percentiles (``numpy.percentile``,
  method ``linear``) over observed values, not approximate quantiles.

The weekly panel covers every UTC week from the first to the last week of the
input.  ``observation_state`` marks the first and last weeks of the input as
``partial`` (the collection window truncates them), weeks before the workflow's
first observed run as ``before_first_observation`` with null counts, and every
other week as ``complete`` even when ``run_count`` is 0.  For partially observed
input (a P01 sample), only workflows listed as complete cohorts in
``sample_manifest.json`` are treated this way; other workflows have observed
weeks marked ``partial`` and unobserved weeks ``unknown`` with null counts.

The as-of table materialises history features for explicit scoring cutoffs.
Activity features only use runs created before the cutoff.  A known result
requires the run to be completed, to have a non-null conclusion, and to have
``updated_at`` before the cutoff; elapsed and attempt features require a
completed run whose update is visible.  Appending later runs can therefore never
change an earlier cutoff row.  Global cutoffs only emit workflows already visible
at the cutoff; explicitly scoped cutoffs may emit a cold-start row with
``has_prior_runs = false``.
"""

from dataclasses import dataclass
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

US_PER_SECOND = 1_000_000
US_PER_DAY = 86_400_000_000
EPOCH = date(1970, 1, 1)

#: Raw GitHub ``event`` values observed in the dataset; other values are bucketed.
EVENTS = (
    "issue_comment", "schedule", "pull_request_target", "pull_request", "issues",
    "check_run", "workflow_run", "pull_request_review_comment", "workflow_dispatch",
    "push", "deployment_status", "check_suite", "pull_request_review", "discussion",
    "discussion_comment", "release", "repository_dispatch", "create",
)
#: Conclusion buckets; ``other`` collects unrecognised conclusion strings and
#: ``unknown`` collects null conclusions.
CONCLUSIONS = (
    "success", "failure", "skipped", "cancelled", "action_required", "startup_failure",
)
CONCLUSION_OTHER = len(CONCLUSIONS)
CONCLUSION_UNKNOWN = CONCLUSION_OTHER + 1
CONCLUSION_BUCKETS = len(CONCLUSIONS) + 2
EVENT_OTHER = len(EVENTS)
EVENT_UNKNOWN = EVENT_OTHER + 1
EVENT_BUCKETS = len(EVENTS) + 2
HOURS = 24
NIGHT_HOURS = 6
PERCENTILES = (50, 90, 99)
WEEK_STATE_BEFORE = 0
WEEK_STATE_PARTIAL = 1
WEEK_STATE_COMPLETE = 2
WEEK_STATE_UNKNOWN = 3
WEEK_STATES = ("before_first_observation", "partial", "complete", "unknown")
DISPATCH_EVENTS = ("workflow_dispatch", "repository_dispatch")


def _as_int64(values, mask=None):
    return pa.array(np.asarray(values, dtype=np.int64), pa.int64(), mask=mask)


def _as_float64(values, mask=None):
    data = np.asarray(values, dtype=np.float64)
    if mask is not None and np.any(mask):
        data = np.where(mask, np.nan, data)
    return pa.array(data, pa.float64(), mask=mask)


def _as_timestamp(micros, mask=None):
    return _as_int64(micros, mask).cast(pa.timestamp("us", tz="UTC"))


def _as_date(days, mask=None):
    return _as_int64(days, mask).cast(pa.int32()).cast(pa.date32())


def _parse_utc(column, name):
    """Parse a UTC timestamp column into int64 microseconds."""
    if pa.types.is_timestamp(column.type):
        return column.cast(pa.timestamp("us", tz="UTC")).cast(pa.int64())
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        parsed = pc.strptime(column, format="%Y-%m-%dT%H:%M:%SZ", unit="us", error_is_null=True)
        failed = parsed.null_count - column.null_count
        if failed:
            raise ValueError(f"{name}: {failed} values are not UTC 'YYYY-MM-DDTHH:MM:SSZ' timestamps")
        return parsed.cast(pa.int64())
    raise ValueError(f"{name}: expected a string or timestamp column, got {column.type}")


def _encode(column):
    """Dictionary-encode a string column, returning int32 codes and the dictionary."""
    encoded = pc.dictionary_encode(column.combine_chunks())
    return encoded.indices.to_numpy(), encoded.dictionary.to_pylist()


def _lookup(values, known, other_bucket, unknown_bucket):
    """Map dictionary values to fixed category codes."""
    table = np.full(len(values), other_bucket, dtype=np.int32)
    index = {value: position for position, value in enumerate(known)}
    for position, value in enumerate(values):
        if value in index:
            table[position] = index[value]
        elif value == "unknown":
            table[position] = unknown_bucket
    return table


def resolve_input(path):
    """Expand a P01 batch directory, a partitioned directory or a Parquet file."""
    path = Path(path)
    if path.is_dir():
        batch_file = path / "runs_input.parquet"
        if batch_file.is_file():
            files = [batch_file]
        else:
            files = sorted(file for file in path.rglob("*.parquet") if file.is_file())
    elif path.is_file():
        files = [path]
    else:
        raise ValueError(f"Input path does not exist: {path}")
    if not files:
        raise ValueError(f"No Parquet shards found in {path}")
    return files


def _open_dataset(path, files):
    """Open exactly ``files``; hive partition columns are kept when available."""
    path = Path(path)
    base = path if path.is_dir() else path.parent
    try:
        return ds.dataset(
            [str(file) for file in files], format="parquet",
            partitioning="hive", partition_base_dir=str(base),
        )
    except (pa.ArrowInvalid, ValueError):
        return ds.dataset([str(file) for file in files], format="parquet")


@dataclass(frozen=True)
class RunData:
    """Run rows sorted by workflow and creation time; null elapsed uses -1."""

    run_id: np.ndarray
    repo_id: np.ndarray
    workflow_id: np.ndarray
    created_us: np.ndarray
    updated_us: np.ndarray
    has_updated: np.ndarray
    elapsed_us: np.ndarray
    negative_elapsed: np.ndarray
    attempts: np.ndarray
    has_attempts: np.ndarray
    is_open: np.ndarray
    is_completed: np.ndarray
    has_conclusion: np.ndarray
    day: np.ndarray
    week: np.ndarray
    hour: np.ndarray
    event_codes: np.ndarray
    event_values: list
    conclusion_codes: np.ndarray
    conclusion_values: list
    path_codes: np.ndarray
    paths: list

    @property
    def row_count(self):
        return len(self.run_id)


@dataclass(frozen=True)
class WorkflowIndex:
    """Contiguous per-workflow slices of the sorted run table."""

    repo_id: np.ndarray
    path: list
    starts: np.ndarray
    sizes: np.ndarray
    first_week: np.ndarray
    last_week: np.ndarray
    first_day: np.ndarray
    last_day: np.ndarray

    def __len__(self):
        return len(self.starts)


def read_runs(path, files=None):
    """Read a run table, validate its keys and return the derived Arrow table."""
    input_path = Path(path)
    files = resolve_input(input_path) if files is None else list(files)
    if not files:
        raise ValueError(f"No Parquet shards found in {input_path}")
    dataset = _open_dataset(input_path, files)
    names = set(dataset.schema.names)
    if not {"run_id", "repo_id", "workflow_path"} <= names:
        raise ValueError("Run input must contain run_id, repo_id and workflow_path")
    created_name = "created_at_utc" if "created_at_utc" in names else "created_at"
    updated_name = "updated_at_utc" if "updated_at_utc" in names else "updated_at"
    if created_name not in names:
        raise ValueError("Run input must contain created_at_utc or created_at")
    columns = ["run_id", "repo_id", "workflow_path", created_name]
    for name in ("workflow_id", "event", "status", "conclusion", "run_attempts", updated_name):
        if name in names:
            columns.append(name)
    table = dataset.to_table(columns=sorted(set(columns)))
    return normalize_runs(table, created_name, updated_name if updated_name in names else None)


def normalize_runs(table, created_name="created_at", updated_name="updated_at"):
    """Validate keys and build the derived UTC columns used by every feature."""
    for name in ("run_id", "repo_id", "workflow_path"):
        if table.column(name).null_count:
            raise ValueError(f"{name} has {table.column(name).null_count} null values")
    ids = table.column("run_id").to_numpy()
    if len(np.unique(ids)) != len(ids):
        raise ValueError("run_id is not unique; the run table must have one row per run")

    created_column = _parse_utc(table.column(created_name), created_name)
    if created_column.null_count:
        raise ValueError(f"{created_name} has {created_column.null_count} null values")
    created_us = created_column.to_numpy()
    if updated_name is None:
        updated_us = np.zeros(len(created_us), np.int64)
        has_updated = np.zeros(len(created_us), bool)
    else:
        updated_column = _parse_utc(table.column(updated_name), updated_name)
        has_updated = ~updated_column.is_null().to_numpy()
        updated_us = updated_column.fill_null(0).to_numpy()
    negative = has_updated & (updated_us < created_us)
    elapsed = np.full(len(created_us), -1, np.int64)
    complete = has_updated & ~negative
    elapsed[complete] = updated_us[complete] - created_us[complete]

    if "run_attempts" in table.column_names:
        attempts_column = table.column("run_attempts")
        has_attempts = ~attempts_column.is_null().to_numpy()
        attempts = attempts_column.fill_null(0).to_numpy().astype(np.int64)
        has_attempts &= attempts >= 1
        attempts = np.where(has_attempts, attempts, 0)
    else:
        attempts = np.zeros(len(created_us), np.int64)
        has_attempts = np.zeros(len(created_us), bool)

    if "status" in table.column_names:
        status_codes, status_values = _encode(pc.fill_null(table.column("status"), "unknown"))
        completed_by_value = np.array([value == "completed" for value in status_values])
        open_by_value = np.array([value not in ("completed", "unknown") for value in status_values])
        is_completed = completed_by_value[status_codes]
        is_open = open_by_value[status_codes]
    else:
        is_completed = np.ones(len(created_us), bool)
        is_open = np.zeros(len(created_us), bool)

    if "conclusion" in table.column_names:
        has_conclusion = ~table.column("conclusion").is_null().to_numpy()
        conclusion_column = pc.fill_null(table.column("conclusion"), "unknown")
    else:
        has_conclusion = np.zeros(len(created_us), bool)
        conclusion_column = pa.chunked_array([pa.array(["unknown"] * len(created_us), pa.string())])

    if "event" in table.column_names:
        event_column = pc.fill_null(table.column("event"), "unknown")
    else:
        event_column = pa.chunked_array([pa.array(["unknown"] * len(created_us), pa.string())])

    day = created_us // US_PER_DAY
    week = day - ((day + 3) % 7)  # 1970-01-01 was a Thursday
    hour = (created_us // (US_PER_SECOND * 3600)) % HOURS

    runs = pa.table({
        "run_id": table.column("run_id").cast(pa.int64()),
        "repo_id": table.column("repo_id").cast(pa.int64()),
        "workflow_path": table.column("workflow_path"),
        "workflow_id": (
            table.column("workflow_id").cast(pa.int64()) if "workflow_id" in table.column_names
            else pa.array([None] * len(created_us), pa.int64())
        ),
        "created_us": pa.array(created_us, pa.int64()),
        "updated_us": _as_int64(updated_us, ~has_updated),
        "has_updated": pa.array(has_updated, pa.bool_()),
        "elapsed_us": _as_int64(elapsed, elapsed < 0),
        "negative_elapsed": pa.array(negative, pa.bool_()),
        "attempts": pa.array(attempts, pa.int64()),
        "has_attempts": pa.array(has_attempts, pa.bool_()),
        "is_open": pa.array(is_open, pa.bool_()),
        "is_completed": pa.array(is_completed, pa.bool_()),
        "has_conclusion": pa.array(has_conclusion, pa.bool_()),
        "day": pa.array(day, pa.int64()),
        "week": pa.array(week, pa.int64()),
        "hour": pa.array(hour, pa.int8()),
        "event": pc.dictionary_encode(event_column),
        "conclusion": pc.dictionary_encode(conclusion_column),
    })
    runs = runs.sort_by([
        ("repo_id", "ascending"), ("workflow_path", "ascending"),
        ("created_us", "ascending"), ("run_id", "ascending"),
    ])
    return _to_run_data(runs)


def _to_run_data(runs):
    path_codes, paths = _encode(runs.column("workflow_path"))
    event_codes, event_values = _encode(runs.column("event"))
    conclusion_codes, conclusion_values = _encode(runs.column("conclusion"))
    return RunData(
        run_id=runs.column("run_id").to_numpy(),
        repo_id=runs.column("repo_id").to_numpy(),
        workflow_id=runs.column("workflow_id").fill_null(-1).to_numpy(),
        created_us=runs.column("created_us").to_numpy(),
        updated_us=runs.column("updated_us").fill_null(0).to_numpy(),
        has_updated=runs.column("has_updated").to_numpy(),
        elapsed_us=runs.column("elapsed_us").fill_null(-1).to_numpy(),
        negative_elapsed=runs.column("negative_elapsed").to_numpy(),
        attempts=runs.column("attempts").to_numpy(),
        has_attempts=runs.column("has_attempts").to_numpy(),
        is_open=runs.column("is_open").to_numpy(),
        is_completed=runs.column("is_completed").to_numpy(),
        has_conclusion=runs.column("has_conclusion").to_numpy(),
        day=runs.column("day").to_numpy(),
        week=runs.column("week").to_numpy(),
        hour=runs.column("hour").to_numpy(),
        event_codes=event_codes.astype(np.int32),
        event_values=event_values,
        conclusion_codes=conclusion_codes.astype(np.int32),
        conclusion_values=conclusion_values,
        path_codes=path_codes.astype(np.int32),
        paths=paths,
    )


def group_workflows(runs):
    """Group the sorted runs by (repo_id, exact workflow_path)."""
    change = np.flatnonzero((np.diff(runs.repo_id) != 0) | (np.diff(runs.path_codes) != 0)) + 1
    starts = np.concatenate(([0], change)).astype(np.int64)
    sizes = np.diff(np.concatenate((starts, [runs.row_count]))).astype(np.int64)
    ends = starts + sizes - 1
    return WorkflowIndex(
        repo_id=runs.repo_id[starts],
        path=[runs.paths[code] for code in runs.path_codes[starts]],
        starts=starts,
        sizes=sizes,
        first_week=runs.week[starts],
        last_week=runs.week[ends],
        first_day=runs.day[starts],
        last_day=runs.day[ends],
    )


def _share(count, total):
    return float(count) / total if total else None


def _percentile_values(values):
    """Return (median, p90, p99) or three nulls for an empty sample."""
    if not len(values):
        return None, None, None
    median, p90, p99 = np.percentile(values, PERCENTILES)
    return float(median), float(p90), float(p99)


def _workflow_id_stats(workflow_id):
    known = workflow_id[workflow_id >= 0]
    if not len(known):
        return None, 0
    return int(known.min()), int(len(np.unique(known)))


def _profile_schema():
    return pa.schema([
        ("repo_id", pa.int64()), ("workflow_path", pa.string()),
        ("workflow_id", pa.int64()), ("workflow_id_variants", pa.int32()),
        ("workflow_id_conflict", pa.bool_()),
        ("run_count", pa.int64()),
        ("first_seen_utc", pa.timestamp("us", tz="UTC")),
        ("last_seen_utc", pa.timestamp("us", tz="UTC")),
        ("first_week_utc", pa.date32()), ("last_week_utc", pa.date32()),
        ("observation_span_days", pa.int32()),
        ("active_day_count", pa.int32()), ("active_day_rate", pa.float64()),
        ("mean_runs_per_active_day", pa.float64()), ("max_daily_runs", pa.int32()),
        ("success_count", pa.int64()), ("failure_count", pa.int64()),
        ("skipped_count", pa.int64()), ("cancelled_count", pa.int64()),
        ("action_required_count", pa.int64()), ("startup_failure_count", pa.int64()),
        ("other_conclusion_count", pa.int64()), ("unknown_conclusion_count", pa.int64()),
        ("skipped_rate", pa.float64()), ("failure_rate", pa.float64()),
        ("failure_rate_estimable", pa.bool_()), ("open_run_count", pa.int64()),
        ("attempts_known_count", pa.int64()), ("total_attempts", pa.int64()),
        ("mean_attempts", pa.float64()), ("retried_run_count", pa.int64()),
        ("retry_rate", pa.float64()), ("retry_rate_estimable", pa.bool_()),
        ("max_attempts", pa.int32()),
        ("elapsed_known_count", pa.int64()), ("elapsed_missing_count", pa.int64()),
        ("elapsed_negative_count", pa.int64()), ("elapsed_zero_count", pa.int64()),
        ("elapsed_mean_seconds", pa.float64()), ("elapsed_median_seconds", pa.float64()),
        ("elapsed_p90_seconds", pa.float64()), ("elapsed_p99_seconds", pa.float64()),
        ("elapsed_max_seconds", pa.float64()), ("elapsed_estimable", pa.bool_()),
        ("interval_count", pa.int64()), ("interval_mean_seconds", pa.float64()),
        ("interval_min_seconds", pa.float64()), ("interval_median_seconds", pa.float64()),
        ("interval_p90_seconds", pa.float64()), ("interval_max_seconds", pa.float64()),
        ("interval_estimable", pa.bool_()),
        *[(f"hour_{hour:02d}_utc_share", pa.float64()) for hour in range(HOURS)],
        ("peak_hour_utc", pa.int8()), ("peak_hour_share", pa.float64()),
        ("hour_entropy_bits", pa.float64()), ("night_share_utc", pa.float64()),
        ("event_type_count", pa.int32()), ("top_event", pa.string()),
        ("top_event_share", pa.float64()), ("event_entropy_bits", pa.float64()),
        ("schedule_share", pa.float64()), ("dispatch_share", pa.float64()),
        ("webhook_share", pa.float64()),
        *[(f"event_{event}_share", pa.float64()) for event in EVENTS],
        ("event_other_share", pa.float64()), ("event_unknown_share", pa.float64()),
    ])


EVENT_MIX_SCHEMA = pa.schema([
    ("repo_id", pa.int64()), ("workflow_path", pa.string()),
    ("event", pa.string()), ("run_count", pa.int64()), ("run_share", pa.float64()),
])
WEEK_SCHEMA = pa.schema([
    ("repo_id", pa.int64()), ("workflow_path", pa.string()),
    ("week_start_utc", pa.date32()), ("observation_state", pa.string()),
    ("run_count", pa.int64()), ("active_day_count", pa.int32()),
    ("success_count", pa.int64()), ("failure_count", pa.int64()),
    ("skipped_count", pa.int64()), ("cancelled_count", pa.int64()),
    ("action_required_count", pa.int64()), ("startup_failure_count", pa.int64()),
    ("other_conclusion_count", pa.int64()), ("unknown_conclusion_count", pa.int64()),
    ("retried_run_count", pa.int64()),
    ("elapsed_median_seconds", pa.float64()), ("elapsed_p90_seconds", pa.float64()),
])
ASOF_SCHEMA = pa.schema([
    ("repo_id", pa.int64()), ("workflow_path", pa.string()),
    ("cutoff_id", pa.string()), ("cutoff_utc", pa.timestamp("us", tz="UTC")),
    ("window_start_utc", pa.timestamp("us", tz="UTC")),
    ("window_end_utc", pa.timestamp("us", tz="UTC")),
    ("prior_window_days", pa.float64()),
    ("n_prior_runs", pa.int64()), ("n_prior_outcomes", pa.int64()),
    ("n_prior_known_attempts", pa.int64()),
    ("prior_active_day_count", pa.int32()), ("prior_runs_per_day", pa.float64()),
    ("prior_skip_rate", pa.float64()), ("prior_failure_rate", pa.float64()),
    ("prior_retry_rate", pa.float64()),
    ("prior_elapsed_median_seconds", pa.float64()),
    ("prior_elapsed_p90_seconds", pa.float64()),
    ("prior_elapsed_max_seconds", pa.float64()),
    ("prior_mean_gap_seconds", pa.float64()), ("prior_max_gap_seconds", pa.float64()),
    ("has_prior_runs", pa.bool_()), ("has_prior_outcomes", pa.bool_()),
])


def _nullable_int(values, arrow_type=pa.int64()):
    mask = np.array([value is None for value in values], bool)
    data = np.array([0 if value is None else value for value in values], np.int64)
    if pa.types.is_int32(arrow_type):
        return pa.array(data.astype(np.int32), pa.int32(), mask=mask)
    if pa.types.is_int8(arrow_type):
        return pa.array(data.astype(np.int8), pa.int8(), mask=mask)
    return pa.array(data, pa.int64(), mask=mask)


def _nullable_float(values):
    mask = np.array([value is None for value in values], bool)
    data = np.array([np.nan if value is None else value for value in values], np.float64)
    return pa.array(data, pa.float64(), mask=mask)


def _entropy(counts, total):
    probabilities = counts[counts > 0] / total
    return float(-(probabilities * np.log2(probabilities)).sum())


def build_profile(runs, workflows):
    """Build the per-workflow profile and the exact event mix table."""
    conclusion_category = _lookup(
        runs.conclusion_values, CONCLUSIONS, CONCLUSION_OTHER, CONCLUSION_UNKNOWN
    )[runs.conclusion_codes]
    event_category = _lookup(runs.event_values, EVENTS, EVENT_OTHER, EVENT_UNKNOWN)[runs.event_codes]
    count = len(workflows)
    fields = (
        "workflow_id", "workflow_id_variants", "workflow_id_conflict",
        "run_count", "first_seen_us", "last_seen_us", "first_week", "last_week",
        "observation_span_days", "active_day_count", "active_day_rate",
        "mean_runs_per_active_day", "max_daily_runs",
        "success_count", "failure_count", "skipped_count", "cancelled_count",
        "action_required_count", "startup_failure_count", "other_conclusion_count",
        "unknown_conclusion_count", "skipped_rate", "failure_rate", "failure_rate_estimable",
        "open_run_count", "attempts_known_count", "total_attempts", "mean_attempts",
        "retried_run_count", "retry_rate", "retry_rate_estimable", "max_attempts",
        "elapsed_known_count", "elapsed_missing_count", "elapsed_negative_count",
        "elapsed_zero_count", "elapsed_mean_seconds", "elapsed_median_seconds",
        "elapsed_p90_seconds", "elapsed_p99_seconds", "elapsed_max_seconds",
        "elapsed_estimable", "interval_count", "interval_mean_seconds",
        "interval_min_seconds", "interval_median_seconds", "interval_p90_seconds",
        "interval_max_seconds", "interval_estimable", "peak_hour_utc", "peak_hour_share",
        "hour_entropy_bits", "night_share_utc", "event_type_count", "top_event",
        "top_event_share", "event_entropy_bits", "schedule_share", "dispatch_share",
        "webhook_share",
    )
    values = {name: [] for name in fields}
    hour_counts = np.zeros((count, HOURS), np.int64)
    event_shares = np.zeros((count, len(EVENTS)), np.float64)
    event_other = np.zeros(count, np.float64)
    event_unknown = np.zeros(count, np.float64)
    mix = {name: [] for name in ("repo_id", "workflow_path", "event", "run_count", "run_share")}
    event_position = {value: position for position, value in enumerate(EVENTS)}

    for workflow in range(count):
        start = int(workflows.starts[workflow])
        size = int(workflows.sizes[workflow])
        run_slice = slice(start, start + size)
        created = runs.created_us[run_slice]
        days = runs.day[run_slice]
        hours = np.bincount(runs.hour[run_slice], minlength=HOURS)
        hour_counts[workflow] = hours
        peak_hour = int(np.argmax(hours))

        raw_events = np.bincount(runs.event_codes[run_slice], minlength=len(runs.event_values))
        for position in np.flatnonzero(raw_events):
            mix["repo_id"].append(int(workflows.repo_id[workflow]))
            mix["workflow_path"].append(workflows.path[workflow])
            mix["event"].append(runs.event_values[position])
            mix["run_count"].append(int(raw_events[position]))
            mix["run_share"].append(float(raw_events[position]) / size)
        categories = np.bincount(event_category[run_slice], minlength=EVENT_BUCKETS)
        event_shares[workflow] = categories[:len(EVENTS)] / size
        event_other[workflow] = float(categories[EVENT_OTHER]) / size
        event_unknown[workflow] = float(categories[EVENT_UNKNOWN]) / size
        top_count = int(raw_events.max())
        top_event = min(runs.event_values[position] for position in np.flatnonzero(raw_events == top_count))

        conclusions = np.bincount(conclusion_category[run_slice], minlength=CONCLUSION_BUCKETS)
        success, failure, skipped, cancelled, action_required, startup_failure, other, unknown = conclusions.tolist()

        attempts = runs.attempts[run_slice]
        known_mask = runs.has_attempts[run_slice] & runs.is_completed[run_slice]
        attempts_known = int(known_mask.sum())
        if attempts_known:
            total_attempts = int(attempts[known_mask].sum())
            retried = int((attempts[known_mask] > 1).sum())
            max_attempts = int(attempts[known_mask].max())
        else:
            total_attempts = retried = 0
            max_attempts = None

        elapsed = runs.elapsed_us[run_slice]
        valid_elapsed = (elapsed >= 0) & runs.is_completed[run_slice]
        elapsed_known = int(valid_elapsed.sum())
        elapsed_seconds = elapsed[valid_elapsed] / US_PER_SECOND
        median, p90, p99 = _percentile_values(elapsed_seconds)
        elapsed_mean = float(elapsed_seconds.mean()) if elapsed_known else None
        elapsed_max = float(elapsed_seconds.max()) if elapsed_known else None
        elapsed_zero = int((elapsed_seconds == 0).sum())

        gaps = np.diff(created) / US_PER_SECOND
        if len(gaps):
            interval_mean = float(gaps.mean())
            interval_min = float(gaps.min())
            interval_max = float(gaps.max())
            interval_median, interval_p90, _ = _percentile_values(gaps)
        else:
            interval_mean = interval_min = interval_max = interval_median = interval_p90 = None

        day_changes = np.flatnonzero(np.diff(days) != 0)
        active_days = len(day_changes) + 1
        span_days = int(days[-1] - days[0]) + 1
        daily_lengths = np.diff(np.concatenate(([0], day_changes + 1, [size])))
        workflow_id, variants = _workflow_id_stats(runs.workflow_id[run_slice])

        values["workflow_id"].append(workflow_id)
        values["workflow_id_variants"].append(variants)
        values["workflow_id_conflict"].append(variants > 1)
        values["run_count"].append(size)
        values["first_seen_us"].append(int(created[0]))
        values["last_seen_us"].append(int(created[-1]))
        values["first_week"].append(int(workflows.first_week[workflow]))
        values["last_week"].append(int(workflows.last_week[workflow]))
        values["observation_span_days"].append(span_days)
        values["active_day_count"].append(active_days)
        values["active_day_rate"].append(active_days / span_days)
        values["mean_runs_per_active_day"].append(size / active_days)
        values["max_daily_runs"].append(int(daily_lengths.max()))
        values["success_count"].append(success)
        values["failure_count"].append(failure)
        values["skipped_count"].append(skipped)
        values["cancelled_count"].append(cancelled)
        values["action_required_count"].append(action_required)
        values["startup_failure_count"].append(startup_failure)
        values["other_conclusion_count"].append(other)
        values["unknown_conclusion_count"].append(unknown)
        values["skipped_rate"].append(skipped / size)
        failure_denominator = success + failure
        values["failure_rate"].append(failure / failure_denominator if failure_denominator else None)
        values["failure_rate_estimable"].append(failure_denominator > 0)
        values["open_run_count"].append(int(runs.is_open[run_slice].sum()))
        values["attempts_known_count"].append(attempts_known)
        values["total_attempts"].append(total_attempts)
        values["mean_attempts"].append(total_attempts / attempts_known if attempts_known else None)
        values["retried_run_count"].append(retried)
        values["retry_rate"].append(retried / attempts_known if attempts_known else None)
        values["retry_rate_estimable"].append(attempts_known > 0)
        values["max_attempts"].append(max_attempts)
        values["elapsed_known_count"].append(elapsed_known)
        values["elapsed_missing_count"].append(size - elapsed_known)
        values["elapsed_negative_count"].append(
            int((runs.negative_elapsed[run_slice] & runs.is_completed[run_slice]).sum())
        )
        values["elapsed_zero_count"].append(elapsed_zero)
        values["elapsed_mean_seconds"].append(elapsed_mean)
        values["elapsed_median_seconds"].append(median)
        values["elapsed_p90_seconds"].append(p90)
        values["elapsed_p99_seconds"].append(p99)
        values["elapsed_max_seconds"].append(elapsed_max)
        values["elapsed_estimable"].append(elapsed_known > 0)
        values["interval_count"].append(len(gaps))
        values["interval_mean_seconds"].append(interval_mean)
        values["interval_min_seconds"].append(interval_min)
        values["interval_median_seconds"].append(interval_median)
        values["interval_p90_seconds"].append(interval_p90)
        values["interval_max_seconds"].append(interval_max)
        values["interval_estimable"].append(len(gaps) > 0)
        values["peak_hour_utc"].append(peak_hour)
        values["peak_hour_share"].append(float(hours[peak_hour]) / size)
        values["hour_entropy_bits"].append(_entropy(hours, size))
        values["night_share_utc"].append(float(hours[:NIGHT_HOURS].sum()) / size)
        values["event_type_count"].append(int((raw_events > 0).sum()))
        values["top_event"].append(top_event)
        values["top_event_share"].append(top_count / size)
        values["event_entropy_bits"].append(_entropy(raw_events, size))
        schedule_share = float(event_shares[workflow, event_position["schedule"]])
        dispatch_share = float(sum(event_shares[workflow, event_position[event]] for event in DISPATCH_EVENTS))
        values["schedule_share"].append(schedule_share)
        values["dispatch_share"].append(dispatch_share)
        values["webhook_share"].append(max(0.0, 1.0 - schedule_share - dispatch_share - event_unknown[workflow]))

    columns = {
        "repo_id": pa.array(workflows.repo_id, pa.int64()),
        "workflow_path": pa.array(workflows.path, pa.string()),
        "workflow_id": _nullable_int(values["workflow_id"]),
        "workflow_id_variants": pa.array(np.asarray(values["workflow_id_variants"], np.int32), pa.int32()),
        "workflow_id_conflict": pa.array(np.asarray(values["workflow_id_conflict"], bool), pa.bool_()),
        "run_count": pa.array(np.asarray(values["run_count"], np.int64), pa.int64()),
        "first_seen_utc": _as_timestamp(np.asarray(values["first_seen_us"], np.int64)),
        "last_seen_utc": _as_timestamp(np.asarray(values["last_seen_us"], np.int64)),
        "first_week_utc": _as_date(np.asarray(values["first_week"], np.int64)),
        "last_week_utc": _as_date(np.asarray(values["last_week"], np.int64)),
        "observation_span_days": pa.array(np.asarray(values["observation_span_days"], np.int32), pa.int32()),
        "active_day_count": pa.array(np.asarray(values["active_day_count"], np.int32), pa.int32()),
        "active_day_rate": pa.array(np.asarray(values["active_day_rate"], np.float64), pa.float64()),
        "mean_runs_per_active_day": pa.array(np.asarray(values["mean_runs_per_active_day"], np.float64), pa.float64()),
        "max_daily_runs": pa.array(np.asarray(values["max_daily_runs"], np.int32), pa.int32()),
        "success_count": pa.array(np.asarray(values["success_count"], np.int64), pa.int64()),
        "failure_count": pa.array(np.asarray(values["failure_count"], np.int64), pa.int64()),
        "skipped_count": pa.array(np.asarray(values["skipped_count"], np.int64), pa.int64()),
        "cancelled_count": pa.array(np.asarray(values["cancelled_count"], np.int64), pa.int64()),
        "action_required_count": pa.array(np.asarray(values["action_required_count"], np.int64), pa.int64()),
        "startup_failure_count": pa.array(np.asarray(values["startup_failure_count"], np.int64), pa.int64()),
        "other_conclusion_count": pa.array(np.asarray(values["other_conclusion_count"], np.int64), pa.int64()),
        "unknown_conclusion_count": pa.array(np.asarray(values["unknown_conclusion_count"], np.int64), pa.int64()),
        "skipped_rate": pa.array(np.asarray(values["skipped_rate"], np.float64), pa.float64()),
        "failure_rate": _nullable_float(values["failure_rate"]),
        "failure_rate_estimable": pa.array(np.asarray(values["failure_rate_estimable"], bool), pa.bool_()),
        "open_run_count": pa.array(np.asarray(values["open_run_count"], np.int64), pa.int64()),
        "attempts_known_count": pa.array(np.asarray(values["attempts_known_count"], np.int64), pa.int64()),
        "total_attempts": pa.array(np.asarray(values["total_attempts"], np.int64), pa.int64()),
        "mean_attempts": _nullable_float(values["mean_attempts"]),
        "retried_run_count": pa.array(np.asarray(values["retried_run_count"], np.int64), pa.int64()),
        "retry_rate": _nullable_float(values["retry_rate"]),
        "retry_rate_estimable": pa.array(np.asarray(values["retry_rate_estimable"], bool), pa.bool_()),
        "max_attempts": _nullable_int(values["max_attempts"], pa.int32()),
        "elapsed_known_count": pa.array(np.asarray(values["elapsed_known_count"], np.int64), pa.int64()),
        "elapsed_missing_count": pa.array(np.asarray(values["elapsed_missing_count"], np.int64), pa.int64()),
        "elapsed_negative_count": pa.array(np.asarray(values["elapsed_negative_count"], np.int64), pa.int64()),
        "elapsed_zero_count": pa.array(np.asarray(values["elapsed_zero_count"], np.int64), pa.int64()),
        "elapsed_mean_seconds": _nullable_float(values["elapsed_mean_seconds"]),
        "elapsed_median_seconds": _nullable_float(values["elapsed_median_seconds"]),
        "elapsed_p90_seconds": _nullable_float(values["elapsed_p90_seconds"]),
        "elapsed_p99_seconds": _nullable_float(values["elapsed_p99_seconds"]),
        "elapsed_max_seconds": _nullable_float(values["elapsed_max_seconds"]),
        "elapsed_estimable": pa.array(np.asarray(values["elapsed_estimable"], bool), pa.bool_()),
        "interval_count": pa.array(np.asarray(values["interval_count"], np.int64), pa.int64()),
        "interval_mean_seconds": _nullable_float(values["interval_mean_seconds"]),
        "interval_min_seconds": _nullable_float(values["interval_min_seconds"]),
        "interval_median_seconds": _nullable_float(values["interval_median_seconds"]),
        "interval_p90_seconds": _nullable_float(values["interval_p90_seconds"]),
        "interval_max_seconds": _nullable_float(values["interval_max_seconds"]),
        "interval_estimable": pa.array(np.asarray(values["interval_estimable"], bool), pa.bool_()),
        "peak_hour_utc": pa.array(np.asarray(values["peak_hour_utc"], np.int8), pa.int8()),
        "peak_hour_share": pa.array(np.asarray(values["peak_hour_share"], np.float64), pa.float64()),
        "hour_entropy_bits": pa.array(np.asarray(values["hour_entropy_bits"], np.float64), pa.float64()),
        "night_share_utc": pa.array(np.asarray(values["night_share_utc"], np.float64), pa.float64()),
        "event_type_count": pa.array(np.asarray(values["event_type_count"], np.int32), pa.int32()),
        "top_event": pa.array(values["top_event"], pa.string()),
        "top_event_share": pa.array(np.asarray(values["top_event_share"], np.float64), pa.float64()),
        "event_entropy_bits": pa.array(np.asarray(values["event_entropy_bits"], np.float64), pa.float64()),
        "schedule_share": pa.array(np.asarray(values["schedule_share"], np.float64), pa.float64()),
        "dispatch_share": pa.array(np.asarray(values["dispatch_share"], np.float64), pa.float64()),
        "webhook_share": pa.array(np.asarray(values["webhook_share"], np.float64), pa.float64()),
    }
    for hour in range(HOURS):
        columns[f"hour_{hour:02d}_utc_share"] = pa.array(
            hour_counts[:, hour] / np.asarray(values["run_count"], np.float64), pa.float64()
        )
    for position, event in enumerate(EVENTS):
        columns[f"event_{event}_share"] = pa.array(event_shares[:, position], pa.float64())
    columns["event_other_share"] = pa.array(event_other, pa.float64())
    columns["event_unknown_share"] = pa.array(event_unknown, pa.float64())
    profile = pa.Table.from_pydict(columns, schema=_profile_schema())
    event_mix = pa.Table.from_pydict(mix, schema=EVENT_MIX_SCHEMA)
    return profile, event_mix


def build_week_panel(runs, workflows, fully_observed=None):
    """Build the dense workflow x UTC-week panel for the global observation window.

    ``fully_observed`` is the set of ``(repo_id, workflow_path)`` keys whose input
    contains every run (full input, or the complete-workflow cohorts of a P01
    sample).  Other workflows have observed weeks marked ``partial`` and every
    unobserved week ``unknown`` with null counts, so a sample cannot invent
    silent weeks that the full data shows as active.
    """
    count = len(workflows)
    week_base = int(runs.week.min())
    week_max = int(runs.week.max())
    n_weeks = int((week_max - week_base) // 7) + 1
    cells = count * n_weeks
    conclusion_category = _lookup(
        runs.conclusion_values, CONCLUSIONS, CONCLUSION_OTHER, CONCLUSION_UNKNOWN
    )[runs.conclusion_codes]
    if fully_observed is None:
        fully = np.ones(count, bool)
    else:
        keys = set(fully_observed)
        fully = np.array([
            (int(repo), path) in keys
            for repo, path in zip(workflows.repo_id.tolist(), workflows.path)
        ], bool)

    run_count = np.zeros(cells, np.int64)
    active_days = np.zeros(cells, np.int32)
    conclusions = np.zeros((cells, CONCLUSION_BUCKETS), np.int64)
    retried = np.zeros(cells, np.int64)
    elapsed_median = np.full(cells, np.nan, np.float64)
    elapsed_p90 = np.full(cells, np.nan, np.float64)

    for workflow in range(count):
        start = int(workflows.starts[workflow])
        size = int(workflows.sizes[workflow])
        run_slice = slice(start, start + size)
        base = workflow * n_weeks
        local = (runs.week[run_slice] - week_base) // 7
        run_count[base:base + n_weeks] = np.bincount(local, minlength=n_weeks)
        flat = local * CONCLUSION_BUCKETS + conclusion_category[run_slice]
        conclusions[base:base + n_weeks] = np.bincount(
            flat, minlength=n_weeks * CONCLUSION_BUCKETS
        ).reshape(n_weeks, CONCLUSION_BUCKETS)
        retry_mask = (
            runs.is_completed[run_slice]
            & runs.has_attempts[run_slice]
            & (runs.attempts[run_slice] > 1)
        )
        retried[base:base + n_weeks] = np.bincount(local[retry_mask], minlength=n_weeks)
        pairs = np.unique(local * 100_000 + runs.day[run_slice])
        active_days[base:base + n_weeks] = np.bincount(
            (pairs // 100_000).astype(np.int64), minlength=n_weeks
        )
        elapsed = runs.elapsed_us[run_slice]
        completed = runs.is_completed[run_slice]
        segment_starts = np.concatenate(([0], np.flatnonzero(np.diff(local) != 0) + 1))
        segment_ends = np.concatenate((segment_starts[1:], [size]))
        for segment_start, segment_end in zip(segment_starts, segment_ends):
            values = elapsed[segment_start:segment_end]
            values = values[(values >= 0) & completed[segment_start:segment_end]]
            if not len(values):
                continue
            median, p90, _ = _percentile_values(values / US_PER_SECOND)
            cell = base + int(local[segment_start])
            elapsed_median[cell] = median
            elapsed_p90[cell] = p90

    state = np.full(cells, WEEK_STATE_COMPLETE, np.int8)
    week_offsets = np.arange(n_weeks, dtype=np.int64) * 7
    for workflow in range(count):
        base = workflow * n_weeks
        first_local = int((workflows.first_week[workflow] - week_base) // 7)
        state[base:base + first_local] = WEEK_STATE_BEFORE
        if fully[workflow]:
            if state[base] != WEEK_STATE_BEFORE:
                state[base] = WEEK_STATE_PARTIAL
            if state[base + n_weeks - 1] != WEEK_STATE_BEFORE:
                state[base + n_weeks - 1] = WEEK_STATE_PARTIAL
        else:
            tail = slice(base + first_local, base + n_weeks)
            state[tail] = np.where(
                run_count[tail] > 0, WEEK_STATE_PARTIAL, WEEK_STATE_UNKNOWN
            )

    blank = (state == WEEK_STATE_BEFORE) | (state == WEEK_STATE_UNKNOWN)
    weekday = np.tile(week_base + week_offsets, count)
    paths = np.repeat(np.asarray(workflows.path, dtype=object), n_weeks)
    columns = {
        "repo_id": pa.array(np.repeat(workflows.repo_id, n_weeks), pa.int64()),
        "workflow_path": pa.array(paths.tolist(), pa.string()),
        "week_start_utc": _as_date(weekday),
        "observation_state": pa.array(np.asarray(WEEK_STATES, dtype=object)[state].tolist(), pa.string()),
        "run_count": pa.array(run_count, pa.int64(), mask=blank),
        "active_day_count": pa.array(active_days, pa.int32(), mask=blank),
        "success_count": pa.array(conclusions[:, 0], pa.int64(), mask=blank),
        "failure_count": pa.array(conclusions[:, 1], pa.int64(), mask=blank),
        "skipped_count": pa.array(conclusions[:, 2], pa.int64(), mask=blank),
        "cancelled_count": pa.array(conclusions[:, 3], pa.int64(), mask=blank),
        "action_required_count": pa.array(conclusions[:, 4], pa.int64(), mask=blank),
        "startup_failure_count": pa.array(conclusions[:, 5], pa.int64(), mask=blank),
        "other_conclusion_count": pa.array(conclusions[:, 6], pa.int64(), mask=blank),
        "unknown_conclusion_count": pa.array(conclusions[:, 7], pa.int64(), mask=blank),
        "retried_run_count": pa.array(retried, pa.int64(), mask=blank),
        "elapsed_median_seconds": _as_float64(elapsed_median, blank | np.isnan(elapsed_median)),
        "elapsed_p90_seconds": _as_float64(elapsed_p90, blank | np.isnan(elapsed_p90)),
    }
    return pa.Table.from_pydict(columns, schema=WEEK_SCHEMA)


def _iso_date(days):
    return (EPOCH + timedelta(days=int(days))).isoformat()


def weekly_cutoffs(runs):
    """One global cutoff at every UTC Monday 00:00 covered by the input."""
    week_base = int(runs.week.min())
    week_max = int(runs.week.max())
    days = np.arange(week_base, week_max + 1, 7, dtype=np.int64)
    return pa.table({
        "cutoff_id": pa.array([_iso_date(day) for day in days], pa.string()),
        "cutoff_us": pa.array(days * US_PER_DAY, pa.int64()),
        "repo_id": pa.array([None] * len(days), pa.int64()),
        "workflow_path": pa.array([None] * len(days), pa.string()),
    })


def load_coverage(path, sample_manifest=None):
    """Return the fully observed workflow keys and the sample manifest path.

    A P01 sample contains all runs for the 12 cohort workflows but only one
    anchor run for the workflows used to cover specific cases.  Only the cohort
    keys can be treated as fully observed when building the weekly panel.
    """
    input_path = Path(path)
    manifest_path = None
    if sample_manifest is not None:
        manifest_path = Path(sample_manifest).resolve()
        if not manifest_path.is_file():
            raise ValueError(f"Sample manifest does not exist: {manifest_path}")
    elif input_path.is_dir() and (input_path / "sample_manifest.json").is_file():
        manifest_path = input_path / "sample_manifest.json"
    if manifest_path is None:
        return None, None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"Cannot read sample manifest {manifest_path}: {exc}") from exc
    cohorts = data.get("cohorts") if isinstance(data, dict) else None
    if not isinstance(cohorts, list):
        raise ValueError(f"Sample manifest {manifest_path} has no cohorts list")
    keys = set()
    for item in cohorts:
        if not isinstance(item, dict) or item.get("repo_id") is None or item.get("workflow_path") is None:
            raise ValueError(f"Invalid cohort entry in {manifest_path}: {item!r}")
        keys.add((int(item["repo_id"]), str(item["workflow_path"])))
    return keys, manifest_path


def load_cutoffs(path):
    """Load an explicit cutoff table with optional per-workflow scoping."""
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    names = set(table.column_names)
    if "cutoff_id" not in names:
        raise ValueError("Cutoff table must contain cutoff_id")
    time_name = next((name for name in ("cutoff_utc", "cutoff_at", "cutoff") if name in names), None)
    if time_name is None:
        raise ValueError("Cutoff table must contain cutoff_utc (UTC timestamp or ISO-8601 string)")
    scoped = ("repo_id" in names) != ("workflow_path" in names)
    if scoped:
        raise ValueError("Cutoff scoping needs both repo_id and workflow_path, or neither")
    ids = table.column("cutoff_id").cast(pa.string())
    if ids.null_count:
        raise ValueError("cutoff_id must not be null")
    if len(set(ids.to_pylist())) != len(ids):
        raise ValueError("cutoff_id must be unique")
    cutoff_us = _parse_utc(table.column(time_name), time_name)
    if cutoff_us.null_count:
        raise ValueError(f"{time_name} must not be null")
    if "repo_id" in names:
        repo_column = table.column("repo_id").cast(pa.int64())
        path_column = table.column("workflow_path").cast(pa.string())
    else:
        repo_column = pa.array([None] * len(ids), pa.int64())
        path_column = pa.array([None] * len(ids), pa.string())
    for cutoff_id, repo, workflow_path in zip(ids.to_pylist(), repo_column.to_pylist(), path_column.to_pylist()):
        if (repo is None) != (workflow_path is None):
            raise ValueError(
                f"Cutoff {cutoff_id} must set both repo_id and workflow_path or neither"
            )
    return pa.table({
        "cutoff_id": ids,
        "cutoff_us": cutoff_us,
        "repo_id": repo_column,
        "workflow_path": path_column,
    })


def _asof_history(created, elapsed, attempts, conclusion_category,
                  day_changes, gap_starts, k, final, elapsed_visible, attempts_visible):
    """Return history features for the first ``k`` runs of one workflow."""
    window_start = int(created[0])
    active_days = int(np.searchsorted(day_changes, k, side="left")) + 1
    if k > 1:
        mean_gap = (int(created[k - 1]) - window_start) / US_PER_SECOND / (k - 1)
        max_gap = float(gap_starts[k - 2]) / US_PER_SECOND
    else:
        mean_gap = max_gap = None
    outcomes = int(final.sum())
    if outcomes:
        counts = np.bincount(conclusion_category[:k][final], minlength=CONCLUSION_BUCKETS)
        skip_rate = float(counts[2]) / outcomes
        failure_denominator = int(counts[0]) + int(counts[1])
        failure_rate = float(counts[1]) / failure_denominator if failure_denominator else None
    else:
        skip_rate = failure_rate = None
    known_count = int(attempts_visible.sum())
    if known_count:
        visible_attempts = attempts[:k][attempts_visible]
        retry_rate = float((visible_attempts > 1).sum()) / known_count
    else:
        retry_rate = None
    if elapsed_visible.any():
        elapsed_seconds = elapsed[:k][elapsed_visible] / US_PER_SECOND
        elapsed_median = float(np.median(elapsed_seconds))
        elapsed_p90 = float(np.percentile(elapsed_seconds, 90))
        elapsed_max = float(elapsed_seconds.max())
    else:
        elapsed_median = elapsed_p90 = elapsed_max = None
    return {
        "k": k,
        "outcomes": outcomes,
        "window_start": window_start,
        "active_days": active_days,
        "mean_gap": mean_gap,
        "max_gap": max_gap,
        "skip_rate": skip_rate,
        "failure_rate": failure_rate,
        "retry_rate": retry_rate,
        "known_attempts": known_count,
        "elapsed_median": elapsed_median,
        "elapsed_p90": elapsed_p90,
        "elapsed_max": elapsed_max,
    }


def build_asof(runs, workflows, cutoffs):
    """Materialise history features at explicit scoring cutoffs.

    Global cutoffs only produce rows for workflows already visible at the
    cutoff (at least one run created before it).  Explicitly scoped cutoffs are
    always emitted, so a requested cold start can be inspected with
    ``has_prior_runs = false``.
    """
    conclusion_category = _lookup(
        runs.conclusion_values, CONCLUSIONS, CONCLUSION_OTHER, CONCLUSION_UNKNOWN
    )[runs.conclusion_codes]
    cutoff_ids = cutoffs.column("cutoff_id").to_pylist()
    cutoff_us = cutoffs.column("cutoff_us").to_numpy()
    scoped_repo = cutoffs.column("repo_id").to_pylist()
    scoped_path = cutoffs.column("workflow_path").to_pylist()
    for index, (repo, path) in enumerate(zip(scoped_repo, scoped_path)):
        if (repo is None) != (path is None):
            raise ValueError(
                f"Cutoff {cutoff_ids[index]} must set both repo_id and workflow_path or neither"
            )
    global_indexes = np.array(
        [index for index, repo in enumerate(scoped_repo) if repo is None], np.int64
    )
    workflow_index = {
        key: index for index, key in enumerate(zip(workflows.repo_id.tolist(), workflows.path))
    }
    scoped = {}
    for index, (repo, path) in enumerate(zip(scoped_repo, scoped_path)):
        if repo is None:
            continue
        key = (int(repo), path)
        if key not in workflow_index:
            raise ValueError(f"Cutoff {cutoff_ids[index]} references an unknown workflow: {key}")
        scoped.setdefault(workflow_index[key], []).append(index)

    kept = []
    total = 0
    for workflow in range(len(workflows)):
        first_created = int(runs.created_us[workflows.starts[workflow]])
        scoped_indexes = set(scoped.get(workflow, []))
        indexes = np.concatenate(
            (global_indexes, np.asarray(scoped.get(workflow, []), np.int64))
        ).astype(np.int64)
        indexes = indexes[np.argsort(cutoff_us[indexes], kind="stable")]
        selected = [
            int(index) for index in indexes
            if index in scoped_indexes or cutoff_us[index] > first_created
        ]
        kept.append(selected)
        total += len(selected)

    columns = {
        "repo_id": np.empty(total, np.int64),
        "workflow_path": [],
        "cutoff_id": [],
        "cutoff_us": np.empty(total, np.int64),
        "window_start": np.zeros(total, np.int64),
        "window_days": np.full(total, np.nan, np.float64),
        "n_prior_runs": np.zeros(total, np.int64),
        "n_prior_outcomes": np.zeros(total, np.int64),
        "n_prior_known_attempts": np.zeros(total, np.int64),
        "active_days": np.zeros(total, np.int64),
        "runs_per_day": np.full(total, np.nan, np.float64),
        "skip_rate": np.full(total, np.nan, np.float64),
        "failure_rate": np.full(total, np.nan, np.float64),
        "retry_rate": np.full(total, np.nan, np.float64),
        "elapsed_median": np.full(total, np.nan, np.float64),
        "elapsed_p90": np.full(total, np.nan, np.float64),
        "elapsed_max": np.full(total, np.nan, np.float64),
        "mean_gap": np.full(total, np.nan, np.float64),
        "max_gap": np.full(total, np.nan, np.float64),
    }
    position = 0
    for workflow, selected in enumerate(kept):
        start = int(workflows.starts[workflow])
        size = int(workflows.sizes[workflow])
        run_slice = slice(start, start + size)
        created = runs.created_us[run_slice]
        updated = runs.updated_us[run_slice]
        has_updated = runs.has_updated[run_slice]
        elapsed = runs.elapsed_us[run_slice]
        attempts = runs.attempts[run_slice]
        has_attempts = runs.has_attempts[run_slice]
        completed = runs.is_completed[run_slice]
        known_conclusion = runs.has_conclusion[run_slice]
        conclusion = conclusion_category[run_slice]
        days = runs.day[run_slice]
        day_changes = np.flatnonzero(np.diff(days) != 0) + 1
        gap_starts = np.maximum.accumulate(np.diff(created)) if size > 1 else np.empty(0, np.int64)
        previous = None
        previous_key = None
        for index in selected:
            cutoff = int(cutoff_us[index])
            columns["repo_id"][position] = int(workflows.repo_id[workflow])
            columns["workflow_path"].append(workflows.path[workflow])
            columns["cutoff_id"].append(cutoff_ids[index])
            columns["cutoff_us"][position] = cutoff
            k = int(np.searchsorted(created, cutoff, side="left"))
            if k:
                updated_visible = has_updated[:k] & (updated[:k] < cutoff)
                final = completed[:k] & known_conclusion[:k] & updated_visible
                elapsed_visible = completed[:k] & updated_visible & (elapsed[:k] >= 0)
                attempts_visible = completed[:k] & updated_visible & has_attempts[:k]
                key = (
                    k, int(final.sum()), int(elapsed_visible.sum()),
                    int(attempts_visible.sum()),
                )
                if key != previous_key:
                    previous = _asof_history(
                        created, elapsed, attempts, conclusion,
                        day_changes, gap_starts, k, final, elapsed_visible, attempts_visible,
                    )
                    previous_key = key
                columns["window_start"][position] = previous["window_start"]
                window_days = (cutoff - previous["window_start"]) / US_PER_DAY
                columns["window_days"][position] = window_days
                columns["n_prior_runs"][position] = previous["k"]
                columns["n_prior_outcomes"][position] = previous["outcomes"]
                columns["n_prior_known_attempts"][position] = previous["known_attempts"]
                columns["active_days"][position] = previous["active_days"]
                columns["runs_per_day"][position] = previous["k"] / window_days if window_days > 0 else np.nan
                for name in (
                    "skip_rate", "failure_rate", "retry_rate", "elapsed_median",
                    "elapsed_p90", "elapsed_max", "mean_gap", "max_gap",
                ):
                    value = previous[name]
                    if value is not None:
                        columns[name][position] = value
            position += 1
    if position != total:
        raise AssertionError(f"as-of row budget mismatch: {position} != {total}")

    no_prior = columns["n_prior_runs"] == 0

    def missing(name):
        """Null mask for a float column: undefined values are stored as NaN."""
        return np.isnan(columns[name])

    table_columns = {
        "repo_id": pa.array(columns["repo_id"], pa.int64()),
        "workflow_path": pa.array(columns["workflow_path"], pa.string()),
        "cutoff_id": pa.array(columns["cutoff_id"], pa.string()),
        "cutoff_utc": _as_timestamp(columns["cutoff_us"]),
        "window_start_utc": _as_timestamp(columns["window_start"], no_prior),
        "window_end_utc": _as_timestamp(columns["cutoff_us"]),
        "prior_window_days": _as_float64(columns["window_days"], missing("window_days")),
        "n_prior_runs": pa.array(columns["n_prior_runs"], pa.int64()),
        "n_prior_outcomes": pa.array(columns["n_prior_outcomes"], pa.int64()),
        "n_prior_known_attempts": pa.array(columns["n_prior_known_attempts"], pa.int64()),
        "prior_active_day_count": pa.array(columns["active_days"].astype(np.int32), pa.int32(), mask=no_prior),
        "prior_runs_per_day": _as_float64(columns["runs_per_day"], missing("runs_per_day")),
        "prior_skip_rate": _as_float64(columns["skip_rate"], missing("skip_rate")),
        "prior_failure_rate": _as_float64(columns["failure_rate"], missing("failure_rate")),
        "prior_retry_rate": _as_float64(columns["retry_rate"], missing("retry_rate")),
        "prior_elapsed_median_seconds": _as_float64(columns["elapsed_median"], missing("elapsed_median")),
        "prior_elapsed_p90_seconds": _as_float64(columns["elapsed_p90"], missing("elapsed_p90")),
        "prior_elapsed_max_seconds": _as_float64(columns["elapsed_max"], missing("elapsed_max")),
        "prior_mean_gap_seconds": _as_float64(columns["mean_gap"], missing("mean_gap")),
        "prior_max_gap_seconds": _as_float64(columns["max_gap"], missing("max_gap")),
        "has_prior_runs": pa.array(~no_prior, pa.bool_()),
        "has_prior_outcomes": pa.array(columns["n_prior_outcomes"] > 0, pa.bool_()),
    }
    return pa.Table.from_pydict(table_columns, schema=ASOF_SCHEMA)


def reconcile(runs, workflows, profile, week, asof, n_weeks):
    """Cross-check row counts and per-workflow conservation between the tables."""
    checks = {}

    def record(name, ok, detail):
        checks[name] = {"ok": bool(ok), **detail}
        if not ok:
            raise ValueError(f"P03 reconciliation failed: {name} {detail}")

    def nan_cells(table):
        count = 0
        for field in table.schema:
            if pa.types.is_floating(field.type):
                values = pc.fill_null(pc.is_nan(table.column(field.name)), False)
                count += int(pc.sum(values).as_py() or 0)
        return count

    run_total = runs.row_count
    profile_runs = int(profile.column("run_count").to_numpy().sum())
    record("profile_run_count", profile_runs == run_total, {"input_runs": run_total, "profile_runs": profile_runs})
    record(
        "profile_workflows",
        len(profile) == len(workflows),
        {"profile_rows": len(profile), "input_workflows": len(workflows)},
    )
    week_runs = int(week.column("run_count").fill_null(0).to_numpy().sum())
    record("week_run_count", week_runs == run_total, {"input_runs": run_total, "week_runs": week_runs})
    record(
        "week_grid",
        len(week) == len(workflows) * n_weeks,
        {"week_rows": len(week), "workflows": len(workflows), "weeks": n_weeks},
    )
    states = set(week.column("observation_state").to_pylist())
    record("week_states", states <= set(WEEK_STATES), {"states": sorted(states)})

    grouped = week.group_by(["repo_id", "workflow_path"]).aggregate([("run_count", "sum")])
    grouped = grouped.sort_by([("repo_id", "ascending"), ("workflow_path", "ascending")])
    profile_sorted = profile.sort_by([("repo_id", "ascending"), ("workflow_path", "ascending")])
    same_keys = grouped.column("repo_id").equals(profile_sorted.column("repo_id")) and grouped.column(
        "workflow_path"
    ).equals(profile_sorted.column("workflow_path"))
    record("week_keys_match_profile", same_keys, {"week_workflows": len(grouped), "profile_rows": len(profile)})
    if same_keys:
        week_sum = grouped.column("run_count_sum").fill_null(0).to_numpy()
        profile_count = profile_sorted.column("run_count").to_numpy()
        mismatched = int(np.count_nonzero(week_sum != profile_count))
        record(
            "week_per_workflow_sums",
            mismatched == 0,
            {"mismatched_workflows": mismatched},
        )

    asof_prior = asof.column("n_prior_runs").to_numpy()
    asof_repo = asof.column("repo_id").to_numpy()
    asof_path = asof.column("workflow_path").to_pylist()
    maximum = {}
    for repo, path, prior in zip(asof_repo, asof_path, asof_prior):
        key = (int(repo), path)
        maximum[key] = max(maximum.get(key, 0), int(prior))
    profile_count = {
        (int(repo), path): int(count)
        for repo, path, count in zip(
            profile_sorted.column("repo_id").to_pylist(),
            profile_sorted.column("workflow_path").to_pylist(),
            profile_sorted.column("run_count").to_pylist(),
        )
    }
    over = {key: (maximum[key], profile_count.get(key)) for key in maximum if maximum[key] > profile_count.get(key, 0)}
    record("asof_prior_within_profile", not over, {"workflows_over_profile": len(over)})
    previous = {}
    increasing = True
    for repo, path, prior in zip(asof_repo, asof_path, asof_prior):
        key = (int(repo), path)
        if prior < previous.get(key, 0):
            increasing = False
            break
        previous[key] = int(prior)
    record("asof_prior_monotonic", increasing, {"rows": len(asof)})
    record("asof_workflows", len(maximum) <= len(workflows), {"asof_workflows": len(maximum)})
    nan_total = sum(nan_cells(table) for table in (profile, week, asof))
    record("no_unmasked_nan", nan_total == 0, {"nan_cells": nan_total})
    return checks


def build_feature_tables(runs_path, cutoff_grid="weekly", cutoffs_path=None,
                         files=None, sample_manifest=None):
    """Read the run table and build every P03 table with reconciliation."""
    runs = read_runs(runs_path, files=files)
    if not runs.row_count:
        raise ValueError("Run input has no rows")
    coverage, manifest_path = load_coverage(runs_path, sample_manifest)
    workflows = group_workflows(runs)
    if coverage is not None:
        known = set(zip(workflows.repo_id.tolist(), workflows.path))
        missing = coverage - known
        if missing:
            raise ValueError(
                f"Sample manifest lists {len(missing)} workflows missing from the run input"
            )
    profile, event_mix = build_profile(runs, workflows)
    week = build_week_panel(runs, workflows, coverage)
    if cutoffs_path is not None:
        cutoffs = load_cutoffs(cutoffs_path)
    elif cutoff_grid == "weekly":
        cutoffs = weekly_cutoffs(runs)
    elif cutoff_grid == "none":
        cutoffs = pa.table({
            "cutoff_id": pa.array([], pa.string()), "cutoff_us": pa.array([], pa.int64()),
            "repo_id": pa.array([], pa.int64()), "workflow_path": pa.array([], pa.string()),
        })
    else:
        raise ValueError(f"Unknown cutoff grid: {cutoff_grid}")
    asof = build_asof(runs, workflows, cutoffs)
    week_base = int(runs.week.min())
    week_max = int(runs.week.max())
    n_weeks = int((week_max - week_base) // 7) + 1
    checks = reconcile(runs, workflows, profile, week, asof, n_weeks)
    return {
        "runs": runs,
        "workflows": workflows,
        "workflow_profile": profile,
        "workflow_event_mix": event_mix,
        "workflow_week": week,
        "workflow_asof": asof,
        "cutoffs": cutoffs,
        "weeks": n_weeks,
        "coverage": coverage,
        "sample_manifest": manifest_path,
        "checks": checks,
    }
