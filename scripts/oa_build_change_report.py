#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from KISTI_DB_Manager.openalex_change_report import build_openalex_change_report


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a change report between two OpenAlex parquet snapshots.")
    ap.add_argument("--base-root", required=True)
    ap.add_argument("--final-root", required=True)
    ap.add_argument("--delta-ids-parquet", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--base-prefix", required=True)
    ap.add_argument("--final-prefix", required=True)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--temp-dir", default=None)
    args = ap.parse_args()

    build_openalex_change_report(
        base_root=Path(args.base_root).expanduser().resolve(),
        final_root=Path(args.final_root).expanduser().resolve(),
        delta_ids_parquet=Path(args.delta_ids_parquet).expanduser().resolve(),
        run_dir=Path(args.run_dir).expanduser().resolve(),
        base_prefix=args.base_prefix,
        final_prefix=args.final_prefix,
        threads=int(args.threads),
        temp_dir=Path(args.temp_dir).expanduser().resolve() if args.temp_dir else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
