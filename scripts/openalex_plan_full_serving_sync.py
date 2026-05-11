#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.openalex_snapshot import (
    FULL_SERVING_CORE_ENTITIES,
    FULL_SERVING_ENTITIES,
    FULL_SERVING_REFERENCE_ENTITIES,
    fetch_manifest,
    filter_entries,
    latest_date,
    next_date,
    total_bytes,
    validate_date,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Plan full OpenAlex serving sync scope for a target date.")
    ap.add_argument("--base-date", default="2026-02-25", help="Existing local full snapshot date (inclusive baseline)")
    ap.add_argument("--target-date", default="2026-03-30", help="Target serving date")
    ap.add_argument("--group", choices=["full", "core", "reference"], default="full")
    ap.add_argument("--json-out", type=Path, default=None, help="Optional JSON plan path")
    ap.add_argument("--md-out", type=Path, default=None, help="Optional Markdown summary path")
    return ap.parse_args()


def _entities_for_group(group: str) -> list[str]:
    if group == "core":
        return list(FULL_SERVING_CORE_ENTITIES)
    if group == "reference":
        return list(FULL_SERVING_REFERENCE_ENTITIES)
    return list(FULL_SERVING_ENTITIES)


def main() -> int:
    args = parse_args()
    base_date = validate_date(args.base_date)
    target_date = validate_date(args.target_date)
    if base_date > target_date:
        raise SystemExit("--base-date must be <= --target-date")

    rows: list[dict] = []
    for entity in _entities_for_group(args.group):
        manifest = fetch_manifest(entity)
        delta_entries = filter_entries(
            manifest,
            start_date=next_date(base_date),
            end_date=target_date,
            entity=entity,
        )
        rows.append(
            {
                "entity": entity,
                "manifest_latest_date": latest_date(manifest),
                "delta_entry_count": len(delta_entries),
                "delta_total_bytes": total_bytes(delta_entries),
                "delta_total_gb": round(total_bytes(delta_entries) / 1024**3, 2),
            }
        )

    plan = {
        "base_date": base_date,
        "target_date": target_date,
        "group": args.group,
        "entity_count": len(rows),
        "total_delta_bytes": sum(int(row["delta_total_bytes"]) for row in rows),
        "total_delta_gb": round(sum(float(row["delta_total_gb"]) for row in rows), 2),
        "entities": rows,
    }

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.md_out is not None:
        args.md_out.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# OpenAlex Full Serving Sync Plan",
            "",
            f"- Base date: `{base_date}`",
            f"- Target date: `{target_date}`",
            f"- Group: `{args.group}`",
            f"- Total delta size: `{plan['total_delta_gb']} GB`",
            "",
            "| Entity | Latest | Delta Files | Delta GB |",
            "| --- | --- | ---: | ---: |",
        ]
        for row in rows:
            lines.append(
                f"| `{row['entity']}` | `{row['manifest_latest_date']}` | {row['delta_entry_count']} | {row['delta_total_gb']} |"
            )
        args.md_out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps(plan, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
