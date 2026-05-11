#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from KISTI_DB_Manager.parquet_finalize import run_finalize_plan


def main() -> int:
    ap = argparse.ArgumentParser(description="Create plan-driven DB indexes/analyze/validation for parquet reloads.")
    ap.add_argument("--plan", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--strict-indexes", action="store_true")
    ap.add_argument("--no-unique-fallback", action="store_true")
    ap.add_argument("--skip-analyze", action="store_true")
    ap.add_argument("--skip-validation", action="store_true")
    args = ap.parse_args()

    report = run_finalize_plan(
        Path(args.plan),
        out_path=Path(args.out).expanduser().resolve() if args.out else None,
        strict_indexes=True if args.strict_indexes else None,
        no_unique_fallback=True if args.no_unique_fallback else None,
        skip_analyze=True if args.skip_analyze else None,
        skip_validation=True if args.skip_validation else None,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
