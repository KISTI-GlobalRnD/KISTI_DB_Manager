#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.openalex_snapshot import (
    FULL_SERVING_CORE_ENTITIES,
    FULL_SERVING_ENTITIES,
    FULL_SERVING_REFERENCE_ENTITIES,
    next_date,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sync multi-entity OpenAlex delta slices for full serving.")
    ap.add_argument("--base-date", default="2026-02-25")
    ap.add_argument("--target-date", default="2026-03-30")
    ap.add_argument("--group", choices=["full", "core", "reference"], default="full")
    ap.add_argument("--dest-root", required=True, type=Path, help="Root directory that will contain one subdir per entity")
    ap.add_argument("--manifest-root", type=Path, default=None, help="Optional manifest output root")
    ap.add_argument("--max-workers", type=int, default=8, help="Per-entity download workers")
    ap.add_argument("--skip-works", action="store_true", help="Skip works if already downloaded separately")
    return ap.parse_args()


def _entities_for_group(group: str) -> list[str]:
    if group == "core":
        return list(FULL_SERVING_CORE_ENTITIES)
    if group == "reference":
        return list(FULL_SERVING_REFERENCE_ENTITIES)
    return list(FULL_SERVING_ENTITIES)


def main() -> int:
    args = parse_args()
    script_path = Path(__file__).resolve().parent / "openalex_sync_manifest_delta.py"
    entities = _entities_for_group(args.group)
    if args.skip_works:
        entities = [entity for entity in entities if entity != "works"]

    args.dest_root.mkdir(parents=True, exist_ok=True)
    if args.manifest_root is not None:
        args.manifest_root.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for entity in entities:
        dest = args.dest_root / entity
        manifest_out = (args.manifest_root / f"{entity}.json") if args.manifest_root is not None else None
        cmd = [
            sys.executable,
            str(script_path),
            "--entity",
            entity,
            "--start-date",
            next_date(args.base_date),
            "--end-date",
            args.target_date,
            "--dest-root",
            str(dest),
            "--max-workers",
            str(args.max_workers),
        ]
        if manifest_out is not None:
            cmd.extend(["--manifest-out", str(manifest_out)])
        print(json.dumps({"entity": entity, "cmd": cmd}, ensure_ascii=False), flush=True)
        subprocess.run(cmd, check=True)
        results.append({"entity": entity, "dest_root": str(dest), "manifest_out": str(manifest_out) if manifest_out else None})

    print(json.dumps({"done": True, "group": args.group, "entity_count": len(results), "entities": results}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
