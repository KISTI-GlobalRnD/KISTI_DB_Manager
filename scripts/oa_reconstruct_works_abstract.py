#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from KISTI_DB_Manager.openalex_abstract import normalize_openalex_work_id, reconstruct_abstract_text


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _count_source_rows(source_dir: Path) -> int:
    total = 0
    for parquet_path in sorted(source_dir.glob("*.parquet")):
        total += int(pq.ParquetFile(parquet_path).metadata.num_rows)
    return total


def _flush_rows(*, rows: list[dict[str, Any]], out_dir: Path, part_idx: int) -> int:
    if not rows:
        return 0
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_dir / f"part-{int(part_idx):06d}.parquet", compression="zstd")
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconstruct OpenAlex works_abstract from abstract_inverted_index parquet.")
    ap.add_argument("--source-dir", required=True, help="Parquet directory for openalex_works_*__excepted__abstract_inverted_index")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--sample-size", type=int, default=1000, help="Rows to reconstruct; 0 means all rows")
    ap.add_argument("--batch-size", type=int, default=5000, help="Scanner batch size")
    ap.add_argument("--part-rows", type=int, default=100000, help="Rows per output parquet part")
    ap.add_argument("--progress-every", type=int, default=50000, help="Write progress every N processed rows")
    ap.add_argument("--sample-preview", type=int, default=10, help="Number of preview rows to save in summary")
    args = ap.parse_args()

    source_dir = Path(args.source_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    progress_path = out_dir / "progress.json"

    dataset = ds.dataset(str(source_dir), format="parquet")
    scanner = dataset.scanner(columns=["id", "value"], batch_size=max(1, int(args.batch_size)))
    source_rows_total = _count_source_rows(source_dir)

    data_dir = out_dir / ("works_abstract_sample_parquet" if int(args.sample_size) > 0 else "works_abstract_parquet")
    total = 0
    has_abstract_y = 0
    has_abstract_n = 0
    preview: list[dict[str, Any]] = []
    buffer_rows: list[dict[str, Any]] = []
    part_idx = 0
    started_at = time.time()

    def write_progress(status: str) -> None:
        elapsed = max(0.001, time.time() - started_at)
        rate = float(total) / float(elapsed)
        eta_seconds = None
        if rate > 0 and source_rows_total > 0:
            remaining = max(0, int(source_rows_total) - int(total))
            eta_seconds = float(remaining) / float(rate)
        payload = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "status": status,
            "source_dir": str(source_dir),
            "data_dir": str(data_dir),
            "source_rows_total": int(source_rows_total),
            "sample_size_requested": int(args.sample_size),
            "rows_written": int(total),
            "parts_written": int(part_idx),
            "has_abstract_y": int(has_abstract_y),
            "has_abstract_n": int(has_abstract_n),
            "rows_per_sec": float(round(rate, 2)),
            "eta_seconds": None if eta_seconds is None else float(round(eta_seconds, 1)),
        }
        _write_json(progress_path, payload)

    for record_batch in scanner.to_batches():
        for row in record_batch.to_pylist():
            rec = reconstruct_abstract_text(row.get("value"))
            out_row = {
                "id": str(row.get("id") or ""),
                "oaid_w": normalize_openalex_work_id(row.get("id")),
                "has_abstract": rec["has_abstract"],
                "abstract": rec["abstract"],
                "token_count": rec["token_count"],
                "position_count": rec["position_count"],
                "unique_positions": rec["unique_positions"],
                "collisions": rec["collisions"],
            }
            buffer_rows.append(out_row)
            total += 1
            if out_row["has_abstract"] == "Y":
                has_abstract_y += 1
            else:
                has_abstract_n += 1
            if len(preview) < int(max(0, args.sample_preview)):
                preview.append(
                    {
                        "id": out_row["id"],
                        "oaid_w": out_row["oaid_w"],
                        "has_abstract": out_row["has_abstract"],
                        "abstract_preview": out_row["abstract"][:500],
                        "token_count": out_row["token_count"],
                        "position_count": out_row["position_count"],
                        "unique_positions": out_row["unique_positions"],
                        "collisions": out_row["collisions"],
                    }
                )

            if len(buffer_rows) >= int(max(1, args.part_rows)):
                _flush_rows(rows=buffer_rows, out_dir=data_dir, part_idx=part_idx)
                part_idx += 1
                buffer_rows = []

            if int(args.progress_every) > 0 and total % int(args.progress_every) == 0:
                write_progress("running")

            if int(args.sample_size) > 0 and total >= int(args.sample_size):
                break

        if int(args.sample_size) > 0 and total >= int(args.sample_size):
            break

    if buffer_rows:
        _flush_rows(rows=buffer_rows, out_dir=data_dir, part_idx=part_idx)
        part_idx += 1

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source_dir": str(source_dir),
        "data_dir": str(data_dir),
        "sample_size_requested": int(args.sample_size),
        "source_rows_total": int(source_rows_total),
        "rows_written": int(total),
        "parts_written": int(part_idx),
        "has_abstract_y": int(has_abstract_y),
        "has_abstract_n": int(has_abstract_n),
        "preview": preview,
    }
    write_progress("done")
    _write_json(out_dir / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
