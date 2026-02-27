#!/usr/bin/env python3
"""
Best-effort resume helper for OpenAlex works ingest.

Goal:
- When a long-running ingest restarts (crash/reboot), reduce duplicates by
  slicing `config.json:data_config.file_names` to start from the last known shard.

Inputs (run_dir):
- config.json
- progress snapshots (any of):
  - run_report.json.progress.json      (internal periodic checkpoint; newer code)
  - progress_external.json             (lsof-based probe; works even with old code)
  - progress_external.jsonl (last line)

This script is intentionally "best effort":
- If anything is missing or doesn't match, it prints a short note and exits 0.
  (So it can be used as systemd ExecStartPre without blocking service start.)
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_last_jsonl(path: Path, *, require_key: str | None = None) -> Optional[dict[str, Any]]:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 0:
                return None
            # Read last up to 64KB for the final line.
            read_n = min(size, 64 * 1024)
            f.seek(size - read_n)
            chunk = f.read(read_n)
    except Exception:
        return None

    try:
        text = chunk.decode("utf-8", errors="ignore")
    except Exception:
        return None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    # Scan backwards; prefer the last entry that contains the required key (if provided).
    for ln in reversed(lines):
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if require_key:
            v = obj.get(require_key)
            if v not in (None, "", [], {}):
                return obj
            continue
        return obj
    return None


def _strip_base(abs_path: str, *, base: str) -> str:
    a = str(abs_path or "").strip()
    b = str(base or "").rstrip("/")
    if a and b and a.startswith(b + "/"):
        return a[len(b) + 1 :]
    return a


def _extract_resume_rel(run_dir: Path, *, base_path: str) -> Optional[str]:
    # Prefer internal progress checkpoint if present.
    internal = run_dir / "run_report.json.progress.json"
    if internal.exists():
        try:
            pj = _read_json(internal)
            cur = pj.get("cursor") or {}
            src = cur.get("source_path")
            if src:
                rel = _strip_base(str(src), base=base_path)
                rel = rel.strip()
                if rel:
                    return rel
        except Exception:
            pass

    external = run_dir / "progress_external.json"
    if external.exists():
        try:
            pj = _read_json(external)
            rel = str(pj.get("open_source_rel") or "").strip()
            if rel:
                return rel
        except Exception:
            pass

    external_log = run_dir / "progress_external.jsonl"
    if external_log.exists():
        pj = _read_last_jsonl(external_log, require_key="open_source_rel")
        if isinstance(pj, dict):
            rel = str(pj.get("open_source_rel") or "").strip()
            if rel:
                return rel

    return None


def _find_index(file_names: list[str], target: str) -> tuple[Optional[int], Optional[str]]:
    t = str(target or "").strip()
    if not t:
        return None, None
    try:
        return int(file_names.index(t)), t
    except ValueError:
        pass

    # Try common OA normalization (with/without leading "works/").
    if t.startswith("works/"):
        alt = t[len("works/") :]
        try:
            return int(file_names.index(alt)), alt
        except ValueError:
            return None, None

    alt = "works/" + t
    try:
        return int(file_names.index(alt)), alt
    except ValueError:
        return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    cfg_path = run_dir / "config.json"
    if not cfg_path.exists():
        print(f"[resume-slice] missing config.json: {cfg_path}")
        return 0

    try:
        cfg = _read_json(cfg_path)
    except Exception as e:
        print(f"[resume-slice] failed to read config.json: {e}")
        return 0

    dc = cfg.get("data_config") or {}
    base_path = str(dc.get("PATH") or "").rstrip("/")
    file_names_raw = dc.get("file_names")
    if not isinstance(file_names_raw, list) or not file_names_raw:
        print("[resume-slice] no data_config.file_names; skip")
        return 0
    file_names = [str(x) for x in file_names_raw if x is not None]
    if not file_names:
        print("[resume-slice] empty data_config.file_names; skip")
        return 0

    resume_rel = _extract_resume_rel(run_dir, base_path=base_path)
    if not resume_rel:
        print("[resume-slice] no progress snapshot found; skip")
        return 0

    idx, normalized_rel = _find_index(file_names, resume_rel)
    if idx is None or normalized_rel is None:
        print(f"[resume-slice] resume shard not in file_names: {resume_rel}")
        return 0

    if idx <= 0:
        print(f"[resume-slice] already aligned (idx={idx}, shard={normalized_rel})")
        return 0

    new_list = file_names[idx:]
    if not new_list:
        print("[resume-slice] slicing would make file_names empty; skip")
        return 0

    # Backup original config as-is.
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bak = cfg_path.with_name(cfg_path.name + f".bak.{ts}")
        bak.write_bytes(cfg_path.read_bytes())
    except Exception:
        pass

    # Update config in-memory.
    dc["file_names"] = new_list
    meta = dc.get("_resume_meta")
    if not isinstance(meta, dict):
        meta = {}

    old_start = meta.get("start_index_1based")
    new_start = None
    try:
        new_start = int(old_start) + int(idx)
    except Exception:
        new_start = None

    def _strip_works_prefix(s: str) -> str:
        return s[len("works/") :] if str(s).startswith("works/") else str(s)

    meta["resumed_at_utc"] = _utc_now_iso()
    meta["resume_rel"] = str(normalized_rel)
    meta["start_rel"] = _strip_works_prefix(str(normalized_rel))
    if new_start is not None:
        meta["start_index_1based"] = int(new_start)
    meta["remaining_files"] = int(len(new_list))
    meta["first_file"] = _strip_works_prefix(str(new_list[0]))
    meta["last_file"] = _strip_works_prefix(str(new_list[-1]))
    dc["_resume_meta"] = meta
    cfg["data_config"] = dc

    # Atomic rewrite.
    try:
        tmp = cfg_path.with_name(cfg_path.name + ".tmp")
        tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, cfg_path)
    except Exception as e:
        print(f"[resume-slice] failed to write config.json: {e}")
        return 0

    print(
        f"[resume-slice] updated file_names: dropped {idx} file(s); "
        f"next shard={normalized_rel}; remaining={len(new_list)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
