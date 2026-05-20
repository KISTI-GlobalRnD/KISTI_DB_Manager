#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.review import TableInfo, render_mermaid, render_simple_svg  # noqa: E402


def _repo_root() -> Path:
    # examples/<this_file>
    return ROOT


def _load_parquet_table_info(path: Path) -> TableInfo:
    try:
        import pyarrow.parquet as pq
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "pyarrow is required to read the OpenAlex parquet sample. "
            "Run with: uv run --all-extras python examples/generate_data_sample_schema.py"
        ) from exc

    parquet_file = pq.ParquetFile(path)
    columns = [
        {"name": field.name, "column_type": str(field.type)}
        for field in parquet_file.schema_arrow
    ]
    table = path.stem
    return TableInfo(
        name_sql=table,
        name_original=table,
        row_count=int(parquet_file.metadata.num_rows),
        row_count_exact=True,
        columns=columns,
    )


def main() -> int:
    root = _repo_root()
    data_dir = root / "Data_Sample"
    out_dir = root / "Image"

    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No .parquet files found under: {data_dir}")

    table_infos = [_load_parquet_table_info(path) for path in parquet_files]
    table_names = [ti.name_original or ti.name_sql for ti in table_infos]

    key_sep = "__"
    base_table = min(table_names, key=lambda name: (name.count(key_sep), len(name), name))

    svg = render_simple_svg(base_table=base_table, table_infos=table_infos, key_sep=key_sep)
    mermaid = render_mermaid(base_table=base_table, table_infos=table_infos, key_sep=key_sep)

    out_dir.mkdir(parents=True, exist_ok=True)
    svg_path = out_dir / "Schema_OpenAlex_Sample.svg"
    mmd_path = out_dir / "Schema_OpenAlex_Sample.mmd"
    svg_path.write_text(svg, encoding="utf-8")
    mmd_path.write_text(mermaid, encoding="utf-8")

    print("Wrote:")
    print(f"- {svg_path}")
    print(f"- {mmd_path}")
    print(f"base_table={base_table!r} tables={len(table_infos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
