#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Convert escaped TSV to chunked parquet parts.")
    ap.add_argument("--input-tsv", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--compression", default="snappy")
    ap.add_argument("--rows-per-file", type=int, default=200_000)
    return ap.parse_args()


def _unescape_load_data_field(value: str | None):
    if value is None:
        return None
    if value == r"\N":
        return None
    mapping = {
        "t": "\t",
        "n": "\n",
        "r": "\r",
        "0": "\0",
        "Z": "\x1a",
        "\\": "\\",
    }
    out: list[str] = []
    i = 0
    text = str(value)
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            nxt = text[i + 1]
            repl = mapping.get(nxt)
            if repl is not None:
                out.append(repl)
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _iter_tsv_chunks(path: Path, *, chunksize: int):
    with path.open("r", encoding="utf-8", newline="") as f:
        header_line = f.readline()
        if not header_line:
            raise SystemExit(f"Empty TSV: {path}")
        columns = header_line.rstrip("\n").rstrip("\r").split("\t")
        rows: list[dict[str, object]] = []
        for line_no, raw in enumerate(f, start=2):
            line = raw.rstrip("\n").rstrip("\r")
            fields = line.split("\t")
            if len(fields) != len(columns):
                raise SystemExit(
                    f"Malformed TSV at line {line_no}: expected {len(columns)} fields, got {len(fields)}"
                )
            row = {col: _unescape_load_data_field(fields[idx]) for idx, col in enumerate(columns)}
            rows.append(row)
            if len(rows) >= chunksize:
                yield columns, rows
                rows = []
        if rows:
            yield columns, rows


def main() -> int:
    args = _parse_args()
    input_tsv = Path(args.input_tsv).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = output_dir / "progress.json"

    rows_written = 0
    parts_written = 0
    started_at = _utc_now()

    for columns, rows in _iter_tsv_chunks(input_tsv, chunksize=int(args.chunksize)):
        chunk = pd.DataFrame.from_records(rows, columns=columns)
        part_path = output_dir / f"part-{parts_written:06d}.parquet"
        chunk.to_parquet(part_path, index=False, compression=str(args.compression))
        parts_written += 1
        rows_written += int(len(chunk))
        progress = {
            "started_at": started_at,
            "updated_at": _utc_now(),
            "input_tsv": str(input_tsv),
            "output_dir": str(output_dir),
            "rows_written": rows_written,
            "parts_written": parts_written,
            "latest_part": str(part_path),
            "done": False,
        }
        progress_path.write_text(json.dumps(progress, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(progress, ensure_ascii=False), flush=True)

    progress = {
        "started_at": started_at,
        "updated_at": _utc_now(),
        "finished_at": _utc_now(),
        "input_tsv": str(input_tsv),
        "output_dir": str(output_dir),
        "rows_written": rows_written,
        "parts_written": parts_written,
        "done": True,
    }
    progress_path.write_text(json.dumps(progress, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(progress, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
