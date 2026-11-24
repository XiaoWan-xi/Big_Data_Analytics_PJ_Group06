# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

P01_RUN_COLUMNS = [
    "run_id",
    "repo_id",
    "workflow_id",
    "workflow_path",
    "created_at",
    "updated_at",
    "event",
    "status",
    "conclusion",
    "run_attempts",
    "log_archive_count",
    "log_file_count",
    "empty_archive_count",
]

UTC_REGEX = (
    r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,9})?(Z|\+00:00)$"
)


def parse_utc_epoch(col_name):
    raw = F.trim(F.col(col_name))
    prefix19 = F.substring(raw, 1, 19)
    epoch = F.unix_timestamp(prefix19, "yyyy-MM-dd'T'HH:mm:ss")
    roundtrip = F.from_unixtime(epoch, "yyyy-MM-dd'T'HH:mm:ss")
    return F.when(
        raw.isNotNull() & raw.rlike(UTC_REGEX) & epoch.isNotNull() & (roundtrip == prefix19),
        epoch,
    ).otherwise(F.lit(None).cast("long"))


def main():
    if len(sys.argv) < 3:
        print("Usage: p02_t03_clean_runs.py <input_parquet_dir> <output_base_dir> [report_local_json]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_base = sys.argv[2].rstrip("/")
    report_local_path = sys.argv[3] if len(sys.argv) >= 4 else "/tmp/p02_t03_report.json"

    spark = (
        SparkSession.builder
        .appName("P02-T03-Clean-Runs")
        .getOrCreate()
    )
    sc = spark.sparkContext
    print("=== APPLICATION ID ===", sc.applicationId)

    raw_df = spark.read.parquet(input_path)
    missing_cols = [c for c in P01_RUN_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        raise ValueError("Input parquet missing P01 required columns: %s" % missing_cols)

    df = raw_df.select(*P01_RUN_COLUMNS)

    dedup_win = Window.partitionBy("run_id").orderBy(
        F.col("updated_at").desc(),
        F.col("run_attempts").desc(),
        F.col("created_at").asc(),
        F.col("repo_id").asc(),
        F.col("workflow_id").asc(),
        F.col("workflow_path").asc(),
        F.col("status").asc(),
        F.col("conclusion").asc(),
        F.col("event").asc(),
        F.col("log_archive_count").desc(),
        F.col("log_file_count").desc(),
        F.col("empty_archive_count").asc(),
    )

    df_checked = (
        df
        .withColumn("_pk_invalid", F.col("run_id").isNull() | (F.col("run_id") <= 0))
        .withColumn("_rn", F.when(F.col("_pk_invalid"), F.lit(1)).otherwise(F.row_number().over(dedup_win)))
        .withColumn("_pk_dup", (~F.col("_pk_invalid")) & (F.col("_rn") > 1))
        .withColumn("created_ts_utc", parse_utc_epoch("created_at"))
        .withColumn("updated_ts_utc", parse_utc_epoch("updated_at"))
        .withColumn("_utc_invalid", F.col("created_ts_utc").isNull() | F.col("updated_ts_utc").isNull())
        .withColumn("duration_sec", F.col("updated_ts_utc") - F.col("created_ts_utc"))
        .withColumn("_neg_duration", (~F.col("_utc_invalid")) & (F.col("duration_sec") < 0))
        .withColumn("_status_null", F.col("status").isNull() | (F.length(F.trim(F.col("status"))) == 0))
        .withColumn("_conclusion_null", F.col("conclusion").isNull() | (F.length(F.trim(F.col("conclusion"))) == 0))
    )

    df_tagged = df_checked.withColumn(
        "quarantine_reason",
        F.when(F.col("_pk_invalid"), F.lit("INVALID_PRIMARY_KEY"))
         .when(F.col("_pk_dup"), F.lit("DUPLICATE_PRIMARY_KEY"))
         .when(F.col("_utc_invalid"), F.lit("INVALID_UTC_TIMESTAMP"))
         .when(F.col("_neg_duration"), F.lit("NEGATIVE_DURATION"))
         .when(F.col("_status_null"), F.lit("NULL_STATUS"))
         .when(F.col("_conclusion_null"), F.lit("NULL_CONCLUSION"))
         .otherwise(F.lit("VALID"))
    ).persist()

    summary_row = df_tagged.agg(
        F.count("*").alias("total_input_rows"),
        F.countDistinct("run_id").alias("distinct_run_ids"),
        F.sum(F.when(F.col("_pk_invalid"), 1).otherwise(0)).alias("ind_invalid_pk"),
        F.sum(F.when(F.col("_pk_dup"), 1).otherwise(0)).alias("ind_duplicate_pk"),
        F.sum(F.when(F.col("_utc_invalid"), 1).otherwise(0)).alias("ind_invalid_utc"),
        F.sum(F.when(F.col("_neg_duration"), 1).otherwise(0)).alias("ind_negative_duration"),
        F.sum(F.when(F.col("_status_null"), 1).otherwise(0)).alias("ind_null_status"),
        F.sum(F.when(F.col("_conclusion_null"), 1).otherwise(0)).alias("ind_null_conclusion"),
    ).collect()[0]

    reason_rows = (
        df_tagged.groupBy("quarantine_reason")
        .count()
        .orderBy("quarantine_reason")
        .collect()
    )
    reason_counts = dict((r["quarantine_reason"], int(r["count"])) for r in reason_rows)

    total_input = int(summary_row["total_input_rows"])
    distinct_runs = int(summary_row["distinct_run_ids"])
    retained_count = reason_counts.get("VALID", 0)
    quarantined_breakdown = dict((k, v) for k, v in reason_counts.items() if k != "VALID")
    total_quarantined = sum(quarantined_breakdown.values())
    reconciled = (retained_count + total_quarantined == total_input)

    null_concl_by_status = (
        df_tagged.filter(F.col("_conclusion_null"))
        .groupBy(F.coalesce(F.col("status"), F.lit("<NULL>")).alias("status_val"))
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    null_concl_dist = dict((r["status_val"], int(r["count"])) for r in null_concl_by_status)

    output_cols = P01_RUN_COLUMNS + ["duration_sec"]
    clean_df = df_tagged.filter(F.col("quarantine_reason") == "VALID").select(*output_cols)
    quarantine_df = df_tagged.filter(F.col("quarantine_reason") != "VALID").select(*(output_cols + ["quarantine_reason"]))

    clean_path = output_base + "/runs_clean"
    quarantine_path = output_base + "/runs_quarantine"

    clean_df.write.mode("overwrite").parquet(clean_path)
    quarantine_df.write.mode("overwrite").parquet(quarantine_path)

    clean_fp = spark.read.parquet(clean_path).agg(
        F.count("*").alias("written_clean_rows"),
        F.min("run_id").alias("min_run_id"),
        F.max("run_id").alias("max_run_id"),
        F.sum("duration_sec").alias("sum_duration_sec"),
    ).collect()[0]

    report = {
        "application_id": sc.applicationId,
        "input_path": input_path,
        "clean_path": clean_path,
        "quarantine_path": quarantine_path,
        "total_input_rows": total_input,
        "distinct_run_ids": distinct_runs,
        "retained_rows": retained_count,
        "total_quarantined_rows": total_quarantined,
        "reconciled_equals_input": reconciled,
        "mutually_exclusive_quarantine_breakdown": quarantined_breakdown,
        "independent_rule_hit_counts": {
            "invalid_primary_key": int(summary_row["ind_invalid_pk"] or 0),
            "duplicate_primary_key": int(summary_row["ind_duplicate_pk"] or 0),
            "invalid_utc_timestamp": int(summary_row["ind_invalid_utc"] or 0),
            "negative_duration": int(summary_row["ind_negative_duration"] or 0),
            "null_status": int(summary_row["ind_null_status"] or 0),
            "null_conclusion": int(summary_row["ind_null_conclusion"] or 0),
        },
        "null_conclusion_by_status": null_concl_dist,
        "clean_output_fingerprint": {
            "written_clean_rows": int(clean_fp["written_clean_rows"] or 0),
            "min_run_id": int(clean_fp["min_run_id"]) if clean_fp["min_run_id"] is not None else None,
            "max_run_id": int(clean_fp["max_run_id"]) if clean_fp["max_run_id"] is not None else None,
            "sum_duration_sec": int(clean_fp["sum_duration_sec"]) if clean_fp["sum_duration_sec"] is not None else None,
        },
    }

    with open(report_local_path, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print("=== P02-T03 RECONCILIATION REPORT ===")
    print(json.dumps(report, indent=2, sort_keys=True))

    df_tagged.unpersist()
    spark.stop()

    if not reconciled:
        raise RuntimeError("Reconciliation failed: retained + quarantined != total_input")


if __name__ == "__main__":
    main()
