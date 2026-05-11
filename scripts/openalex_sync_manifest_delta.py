#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.openalex_snapshot import (
    ManifestEntry as Entry,
    fetch_manifest,
    filter_entries,
    total_bytes,
    validate_date,
)



def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Sync a filtered OpenAlex manifest slice into a local directory."
    )
    ap.add_argument("--entity", default="works", help="OpenAlex entity, e.g. works")
    ap.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD)")
    ap.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD)")
    ap.add_argument("--dest-root", required=True, type=Path, help="Destination root directory")
    ap.add_argument("--max-workers", type=int, default=8, help="Parallel download workers")
    ap.add_argument("--manifest-out", type=Path, default=None, help="Optional filtered manifest JSON path")
    return ap.parse_args()

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def download_one(entry: Entry, *, dest_root: Path) -> tuple[str, int]:
    dest_path = dest_root / entry.rel_path.replace(f"data/{entry.rel_path.split('/')[1]}/", "", 1)
    # Normalize into dest_root/<entity>/updated_date=.../part_xxxx.gz
    if not str(dest_path).startswith(str(dest_root)):
        raise RuntimeError(f"unsafe destination path: {dest_path}")

    existing_size = dest_path.stat().st_size if dest_path.exists() else -1
    if entry.content_length > 0 and existing_size == entry.content_length:
        return "skipped", entry.content_length

    ensure_parent(dest_path)
    fd, tmp_name = tempfile.mkstemp(prefix=dest_path.name + ".", suffix=".part", dir=str(dest_path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with urllib.request.urlopen(entry.url_http, timeout=300) as response, tmp_path.open("wb") as out:
            shutil.copyfileobj(response, out, length=1024 * 1024)
        size = tmp_path.stat().st_size
        if entry.content_length > 0 and size != entry.content_length:
            raise RuntimeError(
                f"size mismatch for {entry.rel_path}: expected {entry.content_length}, got {size}"
            )
        tmp_path.replace(dest_path)
        return "downloaded", size
    finally:
        tmp_path.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    if start_date > end_date:
        raise SystemExit("--start-date must be <= --end-date")
    if args.max_workers < 1:
        raise SystemExit("--max-workers must be >= 1")

    entries = filter_entries(
        fetch_manifest(args.entity),
        start_date=start_date,
        end_date=end_date,
        entity=args.entity,
    )

    args.dest_root.mkdir(parents=True, exist_ok=True)

    if args.manifest_out is not None:
        args.manifest_out.parent.mkdir(parents=True, exist_ok=True)
        args.manifest_out.write_text(
            json.dumps(
                {
                    "entity": args.entity,
                    "start_date": start_date,
                    "end_date": end_date,
                    "entry_count": len(entries),
                    "total_bytes": sum(entry.content_length for entry in entries),
                    "entries": [
                        {
                            "updated_date": entry.updated_date,
                            "url_s3": entry.url_s3,
                            "url_http": entry.url_http,
                            "content_length": entry.content_length,
                            "rel_path": entry.rel_path,
                        }
                        for entry in entries
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    print(
        json.dumps(
            {
                "entity": args.entity,
                "start_date": start_date,
                "end_date": end_date,
                "dest_root": str(args.dest_root),
                "entry_count": len(entries),
                "total_gb": round(total_bytes(entries) / 1024**3, 2),
                "max_workers": args.max_workers,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    counts = {"downloaded": 0, "skipped": 0}
    bytes_by_status = {"downloaded": 0, "skipped": 0}
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(download_one, entry, dest_root=args.dest_root): entry for entry in entries}
        for future in as_completed(futures):
            entry = futures[future]
            status, size = future.result()
            counts[status] += 1
            bytes_by_status[status] += size
            print(
                f"{status}\t{entry.updated_date}\t{size}\t{entry.rel_path}",
                flush=True,
            )

    print(
        json.dumps(
            {
                "done": True,
                "entity": args.entity,
                "start_date": start_date,
                "end_date": end_date,
                "downloaded_files": counts["downloaded"],
                "skipped_files": counts["skipped"],
                "downloaded_gb": round(bytes_by_status["downloaded"] / 1024**3, 2),
                "skipped_gb": round(bytes_by_status["skipped"] / 1024**3, 2),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
