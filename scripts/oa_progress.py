#!/usr/bin/env python3
"""
Quick progress probe for OpenAlex ingest runs.

Why this exists:
- The JSON pipeline only writes the full RunReport at the end.
- When a session drops or a job crashes, we still want an immediate answer:
  "Which shard/file was it on?"

This script reads:
- run_dir/config.json (for data_config.PATH and optional data_config.file_names)
- run_dir/pid (PID of the running job; optional if systemd is used)
- run_dir/systemd_unit (systemd unit name; optional)
and uses lsof to find the currently-open OpenAlex source file.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


_WORKS_RE = re.compile(r"/works/updated_date=[^/]+/part_\d+\.gz$")


@dataclass(frozen=True)
class ProgressProbe:
    timestamp_utc: str
    run_dir: str
    pid: int
    alive: bool
    open_source_abs: Optional[str]
    open_source_rel: Optional[str]
    file_index: Optional[int]
    file_count: Optional[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp_utc": self.timestamp_utc,
            "run_dir": self.run_dir,
            "pid": self.pid,
            "alive": self.alive,
            "open_source_abs": self.open_source_abs,
            "open_source_rel": self.open_source_rel,
            "file_index": self.file_index,
            "file_count": self.file_count,
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_pid_file(run_dir: Path) -> int:
    pid_path = run_dir / "pid"
    if not pid_path.exists():
        raise FileNotFoundError(f"pid file not found: {pid_path}")
    raw = pid_path.read_text(encoding="utf-8").strip()
    return int(raw)


def _pid_alive(pid: int) -> bool:
    try:
        # Signal 0 does not kill; it just checks existence/permission.
        import os

        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _lsof_open_files(pid: int) -> list[str]:
    try:
        cp = subprocess.run(
            ["lsof", "-p", str(pid)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except FileNotFoundError:
        return []
    out = cp.stdout or ""
    return out.splitlines()


def _read_systemd_unit(run_dir: Path) -> Optional[str]:
    p = run_dir / "systemd_unit"
    if not p.exists():
        return None
    try:
        unit = p.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return unit or None


def _systemd_main_pid(unit: str) -> Optional[int]:
    unit = str(unit or "").strip()
    if not unit:
        return None
    try:
        cp = subprocess.run(
            ["systemctl", "--user", "show", "-p", "MainPID", "--value", unit],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except FileNotFoundError:
        return None

    raw = (cp.stdout or "").strip()
    try:
        pid = int(raw)
    except Exception:
        return None
    if pid <= 0:
        return None
    return pid


def _resolve_pid(run_dir: Path) -> tuple[int, bool]:
    """
    Returns (pid, alive).

    Priority:
    1) run_dir/pid if present and alive
    2) systemd unit MainPID if run_dir/systemd_unit exists
    3) run_dir/pid if present (even if not alive)
    """

    pid_from_file: Optional[int] = None
    try:
        pid_from_file = _read_pid_file(run_dir)
    except Exception:
        pid_from_file = None

    if pid_from_file is not None and _pid_alive(pid_from_file):
        return int(pid_from_file), True

    unit = _read_systemd_unit(run_dir)
    pid_from_systemd = _systemd_main_pid(unit) if unit else None
    if pid_from_systemd is not None and _pid_alive(pid_from_systemd):
        # Keep compatibility with older tooling (and oa_progress' own JSON schema)
        # by writing pid back to run_dir/pid.
        try:
            (run_dir / "pid").write_text(str(int(pid_from_systemd)), encoding="utf-8")
        except Exception:
            pass
        return int(pid_from_systemd), True

    if pid_from_file is not None:
        return int(pid_from_file), False
    if pid_from_systemd is not None:
        return int(pid_from_systemd), False
    return 0, False


def _find_open_works_file(lines: list[str], *, base_path: str) -> tuple[Optional[str], Optional[str]]:
    base = str(base_path or "").rstrip("/")
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        path = parts[-1]
        if not path.endswith(".gz"):
            continue
        if "/works/updated_date=" not in path:
            continue
        if not _WORKS_RE.search(path):
            continue
        rel = None
        if base and path.startswith(base + "/"):
            rel = path[len(base) + 1 :]
        return path, rel
    return None, None


def probe_run(run_dir: Path, *, write_json: bool = True) -> ProgressProbe:
    cfg = _read_json(run_dir / "config.json")
    dc = cfg.get("data_config") or {}
    base_path = str(dc.get("PATH") or "")
    file_names = dc.get("file_names")
    file_list: list[str] = []
    if isinstance(file_names, list):
        file_list = [str(x) for x in file_names if x is not None]

    pid, alive = _resolve_pid(run_dir)
    open_abs = None
    open_rel = None
    idx = None
    total = len(file_list) if file_list else None

    if alive:
        lines = _lsof_open_files(pid)
        open_abs, open_rel = _find_open_works_file(lines, base_path=base_path)
        if open_rel and file_list:
            try:
                idx = int(file_list.index(open_rel))
            except ValueError:
                idx = None

    probe_dict: dict[str, Any] = ProgressProbe(
        timestamp_utc=_utc_now_iso(),
        run_dir=str(run_dir),
        pid=int(pid),
        alive=bool(alive),
        open_source_abs=open_abs,
        open_source_rel=open_rel,
        file_index=idx,
        file_count=total,
    ).to_dict()

    if write_json:
        out_path = run_dir / "progress_external.json"
        # Never clobber the last-known shard with nulls: after a crash/restart, we still want
        # progress_external.json to point at the last observed file so oa_resume_slice_config.py can work.
        if not probe_dict.get("open_source_rel") and out_path.exists():
            try:
                prev = _read_json(out_path)
                prev_rel = prev.get("open_source_rel")
                if prev_rel:
                    probe_dict["open_source_rel"] = prev_rel
                    probe_dict["open_source_abs"] = prev.get("open_source_abs")
                    probe_dict["file_index"] = prev.get("file_index")
                    probe_dict["file_count"] = prev.get("file_count")
            except Exception:
                pass

        out_path.write_text(json.dumps(probe_dict, ensure_ascii=False, indent=2), encoding="utf-8")

    return ProgressProbe(
        timestamp_utc=str(probe_dict.get("timestamp_utc") or ""),
        run_dir=str(probe_dict.get("run_dir") or ""),
        pid=int(probe_dict.get("pid") or 0),
        alive=bool(probe_dict.get("alive")),
        open_source_abs=probe_dict.get("open_source_abs"),
        open_source_rel=probe_dict.get("open_source_rel"),
        file_index=probe_dict.get("file_index"),
        file_count=probe_dict.get("file_count"),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--no-write", action="store_true", help="Do not write run_dir/progress_external.json")
    ap.add_argument(
        "--append-log",
        help="Append one-line JSON snapshots to this file (JSONL). Useful for post-mortem shard detection.",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    probe = probe_run(run_dir, write_json=not bool(args.no_write))
    if args.append_log:
        log_path = Path(str(args.append_log)).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(probe.to_dict(), ensure_ascii=False)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    print(json.dumps(probe.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
