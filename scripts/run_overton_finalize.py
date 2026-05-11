#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _default_parsed_dir(run_date: str) -> Path:
    year = str(run_date)[:4]
    return Path("/home/kimyoungjin06/Desktop/HDD/Data/Overton/parsed") / year


def _default_run_dir(repo_root: Path, run_date: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d")
    return repo_root / "runs" / f"overton_finalize_{run_date}_{stamp}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the Overton parquet finalizer with local ops defaults")
    ap.add_argument("--run-date", required=True, help="YYYYMMDD snapshot date")
    ap.add_argument("--parsed-dir", type=Path, default=None, help="Directory containing parsed parquet outputs")
    ap.add_argument("--run-dir", type=Path, default=None, help="Directory for logs and report")
    ap.add_argument("--raw-backup-dir", type=Path, default=None, help="Optional backup directory for pre-dedup parquet")
    ap.add_argument("--table", action="append", default=[], help="Only finalize the given table name; repeatable")
    ap.add_argument("--python-bin", type=Path, default=None, help="Python executable for the Overton parser repo")
    ap.add_argument("--overton-repo", type=Path, default=None, help="Path to the 1.2.6.Overton repo")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    overton_repo = (args.overton_repo or (repo_root.parent / "1.2.6.Overton")).expanduser().resolve()
    python_bin = Path(args.python_bin).expanduser() if args.python_bin else (repo_root / ".venv" / "bin" / "python")
    parsed_dir = (args.parsed_dir or _default_parsed_dir(args.run_date)).expanduser().resolve()
    run_dir = (args.run_dir or _default_run_dir(repo_root, args.run_date)).expanduser().resolve()
    report_path = run_dir / f"final_dedup_report_{args.run_date}.json"
    log_path = run_dir / "finalize.log"

    finalizer = overton_repo / "scripts" / "overton_finalize_parquet.py"
    if not finalizer.is_file():
        raise FileNotFoundError(f"finalizer not found: {finalizer}")
    if not python_bin.is_file():
        raise FileNotFoundError(f"python executable not found: {python_bin}")
    if not parsed_dir.is_dir():
        raise FileNotFoundError(f"parsed dir not found: {parsed_dir}")

    run_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(python_bin),
        str(finalizer),
        "--parsed-dir",
        str(parsed_dir),
        "--run-date",
        str(args.run_date),
        "--report-path",
        str(report_path),
    ]
    if args.raw_backup_dir is not None:
        cmd.extend(["--raw-backup-dir", str(args.raw_backup_dir.expanduser().resolve())])
    for table in args.table:
        cmd.extend(["--table", str(table)])

    with log_path.open("w", encoding="utf-8") as log_fp:
        log_fp.write("COMMAND: " + " ".join(cmd) + "\n")
        log_fp.flush()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            log_fp.write(line)
            sys.stdout.flush()
            log_fp.flush()
        returncode = proc.wait()

    if returncode != 0:
        return returncode

    if report_path.is_file():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        summary = {
            "run_dir": str(run_dir),
            "report_path": str(report_path),
            "rows_removed_total": report.get("rows_removed_total"),
            "table_count": len(report.get("tables", [])),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
