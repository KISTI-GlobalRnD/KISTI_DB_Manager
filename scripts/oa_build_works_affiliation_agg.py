#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from KISTI_DB_Manager.openalex_serving import build_works_affiliation_agg


def main() -> int:
    ap = argparse.ArgumentParser(description="Build OpenAlex work-level affiliation aggregation parquet.")
    ap.add_argument("--source-dir", required=True, help="Parquet directory for canonical works authorships")
    ap.add_argument("--out-dir", required=True, help="Output parquet directory")
    ap.add_argument("--temp-dir", default=None, help="DuckDB temp directory")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--memory-limit", default="64GB")
    ap.add_argument("--max-rows-per-file", type=int, default=1_000_000)
    ap.add_argument("--source-batch-files", type=int, default=8)
    ap.add_argument("--bucket-count", type=int, default=256)
    ap.add_argument("--fresh", action="store_true", help="Discard previous partial output instead of resuming")
    ap.add_argument("--summary-out", default="", help="Optional JSON file to persist build summary")
    args = ap.parse_args()

    summary = build_works_affiliation_agg(
        source_dir=Path(args.source_dir).expanduser().resolve(),
        out_dir=Path(args.out_dir).expanduser().resolve(),
        temp_dir=Path(args.temp_dir).expanduser().resolve() if args.temp_dir else None,
        threads=int(args.threads),
        memory_limit=str(args.memory_limit),
        max_rows_per_file=int(args.max_rows_per_file),
        source_batch_files=int(args.source_batch_files),
        bucket_count=int(args.bucket_count),
        resume=not bool(args.fresh),
    )
    if args.summary_out:
        out_path = Path(args.summary_out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
