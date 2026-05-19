from __future__ import annotations

import argparse
import os


def build_parser(
    *,
    prog: str | None = None,
    key_pattern_default: str = r"^https://openalex\.org/W[0-9]+$",
) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Validate OpenAlex serving DB reload against source parquet.",
    )
    ap.add_argument("run_dir", help="Serving rebuild run directory")
    ap.add_argument("--config", default="", help="Config JSON path; defaults to <run_dir>/config.json")
    ap.add_argument("--table-specs", default="", help="Table specs JSON path; defaults to <run_dir>/table_specs.json")
    ap.add_argument("--dotenv", default=".env")
    ap.add_argument("--db-name", default="", help="Override target database name from config.json")
    ap.add_argument("--out", default="", help="Output JSON path; defaults to <run_dir>/reload_validation.json")
    ap.add_argument("--table", action="append", default=[], help="Validate only selected target table; repeatable")
    ap.add_argument("--works-table", default="works")
    ap.add_argument("--key-column", default="id")
    ap.add_argument("--key-pattern", default=key_pattern_default)
    ap.add_argument("--prefix-length", type=int, default=64)
    ap.add_argument("--sample-limit", type=int, default=10)
    ap.add_argument("--max-statement-time", type=int, default=0, help="MariaDB max_statement_time seconds; 0 disables")
    ap.add_argument("--resume", action="store_true", help="Reuse completed checks from an existing output JSON")
    ap.add_argument("--skip-literal-null-marker-scan", action="store_true")
    ap.add_argument("--literal-null-marker", default=r"\N")
    ap.add_argument("--literal-null-marker-compare-mode", choices=["utf8mb4_bin", "binary"], default="utf8mb4_bin")
    ap.add_argument("--literal-null-marker-count-mode", choices=["count", "exists"], default="count")
    ap.add_argument(
        "--literal-null-marker-column",
        action="append",
        default=[],
        help="Restrict DB literal marker scan to selected text columns; repeatable",
    )
    ap.add_argument("--literal-null-marker-column-chunk-size", type=int, default=32)
    ap.add_argument(
        "--skip-source-literal-null-marker-scan",
        action="store_true",
        help="Do not compare DB literal marker findings against source Parquet literal marker counts.",
    )
    ap.add_argument("--skip-parquet-key-health", action="store_true")
    ap.add_argument("--skip-db-key-health", action="store_true")
    ap.add_argument("--skip-samples", action="store_true", help="Skip bad/duplicate key samples")
    ap.add_argument(
        "--prefix-collision-sample",
        dest="skip_prefix_collision_sample",
        action="store_false",
        help="Run the deep prefix-collision sample. This can require a full grouped scan on large works tables.",
    )
    ap.add_argument(
        "--skip-prefix-collision-sample",
        dest="skip_prefix_collision_sample",
        action="store_true",
        help="Skip the deep prefix-collision sample (default).",
    )
    ap.set_defaults(skip_prefix_collision_sample=True)
    ap.add_argument("--skip-key-bucket-check", action="store_true")
    ap.add_argument("--key-bucket-prefix-length", type=int, default=1)
    ap.add_argument("--skip-orphans", action="store_true")
    ap.add_argument("--skip-sample-checksum", action="store_true")
    ap.add_argument("--checksum-table", action="append", default=[], help="Table to sample-checksum; defaults to works")
    ap.add_argument("--checksum-column", action="append", default=[], help="Column to include in sample checksum; repeatable")
    ap.add_argument("--checksum-sample-size", type=int, default=1000)
    ap.add_argument("--skip-row-bucket-checksum", action="store_true")
    ap.add_argument(
        "--row-bucket-checksum-table",
        action="append",
        default=[],
        help="Table to row-bucket checksum; repeatable",
    )
    ap.add_argument("--row-bucket-checksum-all-tables", action="store_true")
    ap.add_argument(
        "--row-bucket-checksum-column",
        action="append",
        default=[],
        help="Column to include in row-bucket checksum; repeatable",
    )
    ap.add_argument("--row-bucket-prefix-length", type=int, default=1)
    ap.add_argument("--duckdb-temp-dir", default="")
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--memory-limit", default="64GB")
    ap.add_argument("--no-fail-on-issues", action="store_true", help="Exit 0 even when validation issues are found")
    return ap
