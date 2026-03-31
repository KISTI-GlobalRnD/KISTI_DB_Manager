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


def _extract_resume_info(run_dir: Path, *, base_path: str) -> Optional[dict[str, Any]]:
    """
    Return best-effort resume info:
    - source_rel: relative path matching config.file_names
    - source_abs: absolute path (if known)
    - line_no/record_index: from internal progress when available
    """

    # Prefer internal progress checkpoint if present.
    internal = run_dir / "run_report.json.progress.json"
    if internal.exists():
        try:
            pj = _read_json(internal)

            cur: dict[str, Any] = {}
            stage = pj.get("stage")
            updated_at_utc = pj.get("updated_at_utc")

            # Newer pipeline writes a sticky "loaded" cursor; prefer it to reduce duplicates on resume.
            loaded = pj.get("loaded")
            if isinstance(loaded, dict):
                lc = loaded.get("cursor")
                if isinstance(lc, dict) and lc.get("source_path"):
                    cur = dict(lc)
                    stage = "loaded"
                    updated_at_utc = loaded.get("updated_at_utc") or updated_at_utc
                else:
                    cur = dict(pj.get("cursor") or {})
            else:
                cur = dict(pj.get("cursor") or {})

            src = cur.get("source_path")
            if src:
                rel = _strip_base(str(src), base=base_path).strip()
                if rel:
                    return {
                        "source_rel": rel,
                        "source_abs": str(src),
                        "source_member": cur.get("source_member"),
                        "line_no": cur.get("line_no"),
                        "record_index": cur.get("record_index"),
                        "stage": stage,
                        "updated_at_utc": updated_at_utc,
                        "from": "internal",
                    }
        except Exception:
            pass

    external = run_dir / "progress_external.json"
    if external.exists():
        try:
            pj = _read_json(external)
            rel = str(pj.get("open_source_rel") or "").strip()
            if rel:
                abs_path = str(pj.get("open_source_abs") or "").strip() or None
                return {
                    "source_rel": rel,
                    "source_abs": abs_path,
                    "source_member": None,
                    "line_no": None,
                    "record_index": None,
                    "stage": "external",
                    "updated_at_utc": pj.get("timestamp_utc"),
                    "from": "external",
                }
        except Exception:
            pass

    external_log = run_dir / "progress_external.jsonl"
    if external_log.exists():
        pj = _read_last_jsonl(external_log, require_key="open_source_rel")
        if isinstance(pj, dict):
            rel = str(pj.get("open_source_rel") or "").strip()
            if rel:
                abs_path = str(pj.get("open_source_abs") or "").strip() or None
                return {
                    "source_rel": rel,
                    "source_abs": abs_path,
                    "source_member": None,
                    "line_no": None,
                    "record_index": None,
                    "stage": "external_log",
                    "updated_at_utc": pj.get("timestamp_utc"),
                    "from": "external_log",
                }

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

    resume = _extract_resume_info(run_dir, base_path=base_path)
    resume_rel = None if not isinstance(resume, dict) else str(resume.get("source_rel") or "").strip()
    if not resume_rel:
        print("[resume-slice] no progress snapshot found; skip")
        return 0

    idx, normalized_rel = _find_index(file_names, resume_rel)
    if idx is None or normalized_rel is None:
        print(f"[resume-slice] resume shard not in file_names: {resume_rel}")
        return 0

    new_list = file_names[idx:] if int(idx) > 0 else list(file_names)
    if not new_list:
        print("[resume-slice] slicing would make file_names empty; skip")
        return 0

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
    # Extra debug fields (best-effort)
    try:
        meta["resume_stage"] = str(resume.get("stage") or "") if isinstance(resume, dict) else ""
    except Exception:
        meta["resume_stage"] = ""
    if isinstance(resume, dict):
        meta["resume_updated_at_utc"] = resume.get("updated_at_utc")
        meta["resume_line_no"] = resume.get("line_no")
        meta["resume_record_index"] = resume.get("record_index")
    meta["remaining_files"] = int(len(new_list))
    meta["first_file"] = _strip_works_prefix(str(new_list[0]))
    meta["last_file"] = _strip_works_prefix(str(new_list[-1]))
    dc["_resume_meta"] = meta

    # Persist a best-effort line-level cursor so the pipeline can skip within the first shard on restart.
    resume_abs = None
    if isinstance(resume, dict):
        resume_abs = str(resume.get("source_abs") or "").strip() or None
        if not resume_abs and base_path and resume_rel:
            try:
                resume_abs = str((Path(base_path) / str(resume_rel)).resolve())
            except Exception:
                resume_abs = str(Path(base_path) / str(resume_rel))
    new_cursor: dict[str, Any] | None = None
    if resume_abs:
        def _coerce_int(v):
            try:
                if v in (None, ""):
                    return None
                return int(v)
            except Exception:
                return None

        stage = None if not isinstance(resume, dict) else str(resume.get("stage") or "").strip().lower()
        allow_line_resume = stage == "loaded"
        new_cursor = {
            "source_path": str(resume_abs),
            "source_member": None if not isinstance(resume, dict) else (resume.get("source_member") or None),
            "line_no": None if (not allow_line_resume) or (not isinstance(resume, dict)) else _coerce_int(resume.get("line_no")),
            "record_index": None
            if (not allow_line_resume) or (not isinstance(resume, dict))
            else _coerce_int(resume.get("record_index")),
        }

    old_cursor = dc.get("_resume_cursor")
    cursor_changed = bool(new_cursor) and (not isinstance(old_cursor, dict) or old_cursor != new_cursor)
    sliced = bool(int(idx) > 0)
    if (not sliced) and (not cursor_changed):
        print(f"[resume-slice] already aligned (idx={idx}, shard={normalized_rel})")
        return 0

    if new_cursor:
        dc["_resume_cursor"] = new_cursor
    cfg["data_config"] = dc

    # Backup original config as-is.
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bak = cfg_path.with_name(cfg_path.name + f".bak.{ts}")
        bak.write_bytes(cfg_path.read_bytes())
    except Exception:
        pass

    # Atomic rewrite.
    try:
        tmp = cfg_path.with_name(cfg_path.name + ".tmp")
        tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, cfg_path)
    except Exception as e:
        print(f"[resume-slice] failed to write config.json: {e}")
        return 0

    if sliced:
        print(
            f"[resume-slice] updated file_names: dropped {idx} file(s); "
            f"next shard={normalized_rel}; remaining={len(new_list)}"
        )
    else:
        ln = None if not isinstance(resume, dict) else resume.get("line_no")
        print(f"[resume-slice] updated resume cursor: shard={normalized_rel}, line_no={ln}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
