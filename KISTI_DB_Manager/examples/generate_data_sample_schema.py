#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.review import TableInfo, render_mermaid, render_simple_svg  # noqa: E402


def _repo_root() -> Path:
    # KISTI_DB_Manager/examples/<this_file>
    return ROOT


def _strip_numeric_prefix(name: str) -> str:
    return re.sub(r"^[0-9]+__", "", name)


def _load_desc_columns(path: Path) -> list[dict[str, str | None]] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return None
        type_idx = None
        for i, h in enumerate(header):
            if str(h).strip().lower() == "type":
                type_idx = i
                break

        cols: list[dict[str, str | None]] = []
        for row in reader:
            if not row:
                continue
            name = str(row[0]).strip()
            if not name:
                continue
            col_type = None
            if type_idx is not None and type_idx < len(row):
                col_type = str(row[type_idx]).strip() or None
            cols.append({"name": name, "column_type": col_type})
        return cols


def main() -> int:
    root = _repo_root()
    data_dir = root / "Data_Sample"
    out_dir = root / "Image"

    ftrs = sorted(data_dir.glob("*.ftr"))
    if not ftrs:
        raise SystemExit(f"No .ftr files found under: {data_dir}")

    table_infos: list[TableInfo] = []
    candidates: list[str] = []
    for ftr in ftrs:
        stem = ftr.stem
        table = _strip_numeric_prefix(stem)
        candidates.append(table)

        cols = _load_desc_columns(data_dir / f"{stem}_Desc.csv")
        table_infos.append(TableInfo(name_sql=table, name_original=table, columns=cols))

    # Heuristic: pick the first non -SUB table as base.
    base_table = next((t for t in candidates if "-SUB" not in t), candidates[0])

    key_sep = "__"
    svg = render_simple_svg(base_table=base_table, table_infos=table_infos, key_sep=key_sep)
    mermaid = render_mermaid(base_table=base_table, table_infos=table_infos, key_sep=key_sep)

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "Schema_WoS_Sample.svg").write_text(svg, encoding="utf-8")
    (out_dir / "Schema_WoS_Sample.mmd").write_text(mermaid, encoding="utf-8")

    print("Wrote:")
    print(f"- {out_dir / 'Schema_WoS_Sample.svg'}")
    print(f"- {out_dir / 'Schema_WoS_Sample.mmd'}")
    print(f"base_table={base_table!r} tables={len(table_infos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
