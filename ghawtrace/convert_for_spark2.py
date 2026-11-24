# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import pyarrow.parquet as pq

# 对齐 P01 inputs.py 中的 13 列标准字段
P01_COLUMNS = [
    "run_id", "repo_id", "workflow_id", "workflow_path",
    "created_at", "updated_at", "event", "status", "conclusion",
    "run_attempts", "log_archive_count", "log_file_count", "empty_archive_count",
]

def main():
    if len(sys.argv) < 3:
        print("Usage: python convert_for_spark2.py <input_dir> <output_dir> [limit]")
        sys.exit(1)

    in_dir = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])
    limit = int(sys.argv[3]) if len(sys.argv) >= 4 else None
    out_dir.mkdir(parents=True, exist_ok=True)

    shards = sorted(in_dir.glob("*.parquet"))
    if limit is not None:
        shards = shards[:limit]

    total_rows = 0
    for i, src in enumerate(shards, 1):
        table = pq.read_table(src, columns=P01_COLUMNS)
        dst = out_dir / src.name
        pq.write_table(
            table,
            dst,
            compression="snappy",
            version="1.0",
        )
        total_rows += table.num_rows
        if i % 20 == 0 or i == len(shards):
            print(f"[{i}/{len(shards)}] converted {src.name} (rows={table.num_rows})")

    print(f"Done! Converted {len(shards)} shard(s), total rows = {total_rows:,}")

if __name__ == "__main__":
    main()