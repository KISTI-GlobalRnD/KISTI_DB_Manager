#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _default_parsed_dir(run_date: str) -> Path:
    return Path("/home/kimyoungjin06/Desktop/HDD/Data/Overton/parsed") / str(run_date)[:4]


def _default_db(run_date: str) -> str:
    return f"overton_{str(run_date)[:6]}_raw"


def _default_out_dir(repo_root: Path, run_date: str, db: str) -> Path:
    return repo_root / "dumps" / f"overton_sqlzst_{db}_{run_date}_finalized"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run Overton sql.zst export from finalized parquet with local ops defaults")
    ap.add_argument("--run-date", required=True, help="YYYYMMDD snapshot date")
    ap.add_argument("--parsed-dir", type=Path, default=None, help="Directory containing finalized Overton parquet outputs")
    ap.add_argument("--db", default=None, help="Target schema name for dump file naming")
    ap.add_argument("--out-dir", type=Path, default=None, help="Dump output directory")
    ap.add_argument("--python-bin", type=Path, default=None, help="Python executable for the Overton parser repo")
    ap.add_argument("--overton-repo", type=Path, default=None, help="Path to the 1.2.6.Overton repo")
    ap.add_argument("--schema-only", action="store_true", help="Only emit schema dump")
    ap.add_argument("--data-only", action="store_true", help="Only emit data dump")
    ap.add_argument("--no-resume", action="store_true", help="Do not resume an existing dump directory")
    ap.add_argument("--include-tables-regex", default=None, help="Only export tables whose name matches regex")
    ap.add_argument("--exclude-tables-regex", default=None, help="Skip tables whose name matches regex")
    ap.add_argument("--limit-tables", type=int, default=None, help="Limit number of tables after filtering")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    overton_repo = (args.overton_repo or (repo_root.parent / "1.2.6.Overton")).expanduser().resolve()
    python_bin = Path(args.python_bin).expanduser() if args.python_bin else (repo_root / ".venv" / "bin" / "python")
    parsed_dir = (args.parsed_dir or _default_parsed_dir(args.run_date)).expanduser().resolve()
    db = str(args.db or _default_db(args.run_date))
    out_dir = (args.out_dir or _default_out_dir(repo_root, args.run_date, db)).expanduser().resolve()

    exporter = overton_repo / "scripts" / "export_overton_sqlzst.py"
    if not exporter.is_file():
        raise FileNotFoundError(f"exporter not found: {exporter}")
    if not python_bin.is_file():
        raise FileNotFoundError(f"python executable not found: {python_bin}")
    if not parsed_dir.is_dir():
        raise FileNotFoundError(f"parsed dir not found: {parsed_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "export.log"
    cmd = [
        str(python_bin),
        str(exporter),
        "--run-date",
        str(args.run_date),
        "--parsed-dir",
        str(parsed_dir),
        "--db",
        db,
        "--out-dir",
        str(out_dir),
    ]
    if args.schema_only:
        cmd.append("--schema-only")
    if args.data_only:
        cmd.append("--data-only")
    if args.no_resume:
        cmd.append("--no-resume")
    if args.include_tables_regex:
        cmd.extend(["--include-tables-regex", str(args.include_tables_regex)])
    if args.exclude_tables_regex:
        cmd.extend(["--exclude-tables-regex", str(args.exclude_tables_regex)])
    if args.limit_tables is not None:
        cmd.extend(["--limit-tables", str(args.limit_tables)])

    with log_path.open("a", encoding="utf-8") as log_fp:
        log_fp.write(f"\n[{datetime.now().isoformat(sep=' ', timespec='seconds')}] COMMAND: {' '.join(cmd)}\n")
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
        return proc.wait()


if __name__ == "__main__":
    raise SystemExit(main())
