#!/usr/bin/env python3
"""
Install a systemd --user timer that periodically snapshots OpenAlex ingest progress.

What you get (per run_dir):
- runs/<run_dir>/progress_external.json         (latest snapshot, rewritten)
- runs/<run_dir>/progress_external.jsonl        (history; one JSON per line)

This is meant to answer quickly after crashes:
  "Which shard/file was it on last?"
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path


def _sanitize_unit_token(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return "run"
    # systemd unit names are picky; keep it conservative.
    return re.sub(r"[^A-Za-z0-9_.@:-]+", "_", s)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir> (absolute or relative)")
    ap.add_argument("--interval", type=int, default=60, help="Seconds between probes (default: 60)")
    ap.add_argument(
        "--unit-prefix",
        default="kisti-oa-progress",
        help="systemd unit prefix (default: kisti-oa-progress)",
    )
    ap.add_argument("--no-enable", action="store_true", help="Only write unit files; do not enable/start timer")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"run_dir not found: {run_dir}")

    repo_root = Path(__file__).resolve().parents[1]
    py = repo_root / ".venv" / "bin" / "python"
    if not py.exists():
        py = Path(os.environ.get("VIRTUAL_ENV", "")) / "bin" / "python"
    if not py.exists():
        py = Path("python3")

    unit_token = _sanitize_unit_token(run_dir.name)
    service_name = f"{_sanitize_unit_token(args.unit_prefix)}-{unit_token}.service"
    timer_name = f"{_sanitize_unit_token(args.unit_prefix)}-{unit_token}.timer"

    user_unit_dir = Path.home() / ".config" / "systemd" / "user"
    service_path = user_unit_dir / service_name
    timer_path = user_unit_dir / timer_name

    progress_script = repo_root / "scripts" / "oa_progress.py"
    progress_jsonl = run_dir / "progress_external.jsonl"

    service_text = f"""[Unit]
Description=OpenAlex ingest progress probe ({run_dir.name})

[Service]
Type=oneshot
WorkingDirectory={repo_root}
ExecStart={py} {progress_script} {run_dir} --append-log {progress_jsonl}
"""

    interval = int(args.interval)
    if interval <= 0:
        interval = 60

    timer_text = f"""[Unit]
Description=OpenAlex ingest progress probe timer ({run_dir.name})

[Timer]
OnBootSec=30s
OnUnitActiveSec={interval}s
Persistent=true
Unit={service_name}

[Install]
WantedBy=timers.target
"""

    _write_text(service_path, service_text)
    _write_text(timer_path, timer_text)

    # Make it executable-ish to be friendly when browsed, although systemd doesn't require it.
    try:
        service_path.chmod(0o644)
        timer_path.chmod(0o644)
    except Exception:
        pass

    if args.no_enable:
        print(str(timer_path))
        return 0

    subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    subprocess.run(["systemctl", "--user", "enable", "--now", timer_name], check=False)
    subprocess.run(["systemctl", "--user", "status", timer_name, "--no-pager"], check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

