#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from KISTI_DB_Manager.target_db_preflight import run_target_db_preflight


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect target DB compatibility before parquet reload.")
    ap.add_argument("--plan", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--table", action="append", default=[])
    ap.add_argument("--require-reload-supported", action="store_true")
    args = ap.parse_args()

    report = run_target_db_preflight(
        Path(args.plan),
        out_path=Path(args.out).expanduser().resolve() if args.out else None,
        table_names=[str(item).strip() for item in args.table if str(item).strip()] or None,
        require_reload_supported=bool(args.require_reload_supported),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
