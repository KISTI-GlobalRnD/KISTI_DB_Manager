#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from KISTI_DB_Manager.runstate import (
    JsonRunState,
    atomic_write_json,
    open_append_text,
    prepare_output_dir_path,
    read_json,
    safe_unlink_file,
    utc_now_iso,
)


def _log(fp, message: str) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}] {message}"
    fp.write(line + "\n")
    fp.flush()


def _dq(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


@dataclass(frozen=True)
class TablePair:
    suffix: str
    base_dir: Path | None
    delta_dir: Path | None
    out_dir: Path


def _parquet_source_expr(files: list[Path]) -> str:
    items = [str(p) for p in files]
    if not items:
        raise ValueError("files must not be empty")
    if len(items) == 1:
        return json.dumps(items[0])
    return "[" + ", ".join(json.dumps(x) for x in items) + "]"


def _read_parquet_expr(source_expr: str, *, union_by_name: bool = False) -> str:
    options: list[str] = []
    if union_by_name:
        options.append("union_by_name=true")
    if options:
        return f"read_parquet({source_expr}, {', '.join(options)})"
    return f"read_parquet({source_expr})"


def _list_table_dirs(root: Path, prefix: str) -> dict[str, Path]:
    res: dict[str, Path] = {}
    for child in sorted([p for p in root.iterdir() if p.is_dir()]):
        name = child.name
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix) :]
        res[suffix] = child
    return res


def _link_or_copy(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return "exists"
    try:
        os.link(src, dst)
        return "hardlink"
    except Exception:
        shutil.copy2(src, dst)
        return "copy"


def _cleanup_glob(root: Path, pattern: str) -> int:
    removed = 0
    for child in root.glob(pattern):
        if safe_unlink_file(child, purpose="parquet delta cleanup", missing_ok=True):
            removed += 1
    return removed


def _connect_duckdb(*, db_path: Path, temp_dir: Path | None, threads: int):
    try:
        import duckdb
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("duckdb is required for parquet delta merge") from exc

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(max(1, threads))};")
    chosen_temp = temp_dir
    if chosen_temp is not None:
        try:
            chosen_temp.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_temp = db_path.parent / "_duckdb_tmp"
            chosen_temp.mkdir(parents=True, exist_ok=True)
    if chosen_temp:
        con.execute(f"PRAGMA temp_directory={json.dumps(str(chosen_temp))};")
    return con


def _ensure_delta_ids(
    *,
    con,
    delta_main_dir: Path,
    delta_ids_parquet: Path,
    log_fp,
) -> None:
    if delta_ids_parquet.exists():
        _log(log_fp, f"reuse delta ids parquet: {delta_ids_parquet}")
    else:
        _log(log_fp, f"build delta ids parquet from {delta_main_dir}")
        sql = (
            "COPY ("
            f"SELECT DISTINCT id FROM {_read_parquet_expr(json.dumps(str(delta_main_dir / '*.parquet')), union_by_name=True)} "
            "WHERE id IS NOT NULL"
            f") TO {json.dumps(str(delta_ids_parquet))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        con.execute(sql)
    con.execute(
        "CREATE OR REPLACE VIEW delta_ids AS "
        f"SELECT id FROM {_read_parquet_expr(json.dumps(str(delta_ids_parquet)))};"
    )


def _query_scalar(con, sql: str, params: list[Any] | None = None) -> Any:
    row = con.execute(sql, params or []).fetchone()
    return None if row is None else row[0]


def _overlap_exists(con, *, base_file: Path) -> bool:
    sql = (
        "SELECT 1 "
        f"FROM read_parquet({json.dumps(str(base_file))}) b "
        "SEMI JOIN delta_ids d USING (id) "
        "LIMIT 1"
    )
    row = con.execute(sql).fetchone()
    return row is not None


def _copy_filtered_base(
    *,
    con,
    base_file: Path,
    out_file: Path,
    log_fp,
) -> None:
    prepare_output_dir_path(out_file.parent, purpose="parquet delta output")
    tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
    safe_unlink_file(tmp_file, purpose="parquet delta temporary cleanup", missing_ok=True)
    sql = (
        "COPY ("
        "SELECT * "
        f"FROM read_parquet({json.dumps(str(base_file))}) b "
        "ANTI JOIN delta_ids d USING (id)"
        f") TO {json.dumps(str(tmp_file))} (FORMAT PARQUET, COMPRESSION ZSTD);"
    )
    t0 = time.perf_counter()
    con.execute(sql)
    tmp_file.replace(out_file)
    dt = time.perf_counter() - t0
    _log(log_fp, f"filtered base file: {base_file.name} -> {out_file.name} ({dt:.1f}s)")


def _write_query_to_dataset(
    *,
    con,
    sql: str,
    out_dir: Path,
    basename_template: str,
    max_rows_per_file: int,
    log_fp,
    label: str,
) -> int:
    import pyarrow.dataset as ds

    out_dir = prepare_output_dir_path(out_dir, purpose="parquet delta dataset output")
    _cleanup_glob(out_dir, basename_template.replace("{i}", "*"))
    t0 = time.perf_counter()
    reader = con.execute(sql).to_arrow_reader()
    ds.write_dataset(
        reader,
        base_dir=str(out_dir),
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
        basename_template=basename_template,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=min(max_rows_per_file, 100_000),
    )
    dt = time.perf_counter() - t0
    count = sum(1 for _ in out_dir.glob(basename_template.replace("{i}", "*")))
    _log(log_fp, f"{label}: wrote {count} parquet files to {out_dir.name} ({dt/60.0:.1f}m)")
    return count


def _merge_table_tablewise(
    *,
    con,
    pair: TablePair,
    base_files: list[Path],
    delta_files: list[Path],
    tstate: dict[str, Any],
    state: dict[str, Any],
    log_fp,
    max_rows_per_file: int,
) -> None:
    pair.out_dir.mkdir(parents=True, exist_ok=True)
    if base_files:
        base_expr = _parquet_source_expr(base_files)
        sql = (
            "SELECT * "
            f"FROM {_read_parquet_expr(base_expr, union_by_name=True)} b "
            "ANTI JOIN delta_ids d USING (id)"
        )
        base_written = _write_query_to_dataset(
            con=con,
            sql=sql,
            out_dir=pair.out_dir,
            basename_template="base-{i}.parquet",
            max_rows_per_file=max_rows_per_file,
            log_fp=log_fp,
            label=f"tablewise filtered base {pair.out_dir.name}",
        )
        tstate["base_files_done"] = int(tstate.get("base_files_total") or 0)
        tstate["base_files_filtered"] = int(tstate.get("base_files_total") or 0)
        tstate["base_compact_files_written"] = base_written
        state["stats"]["base_files_done"] = int(state["stats"].get("base_files_done") or 0) + int(tstate.get("base_files_total") or 0)
        state["stats"]["base_files_filtered"] = int(state["stats"].get("base_files_filtered") or 0) + int(tstate.get("base_files_total") or 0)
    if delta_files:
        delta_expr = _parquet_source_expr(delta_files)
        sql = f"SELECT * FROM {_read_parquet_expr(delta_expr, union_by_name=True)}"
        delta_written = _write_query_to_dataset(
            con=con,
            sql=sql,
            out_dir=pair.out_dir,
            basename_template="delta-{i}.parquet",
            max_rows_per_file=max_rows_per_file,
            log_fp=log_fp,
            label=f"tablewise compact delta {pair.out_dir.name}",
        )
        tstate["delta_files_done"] = int(tstate.get("delta_files_total") or 0)
        tstate["delta_compact_files_written"] = delta_written
        state["stats"]["delta_files_done"] = int(state["stats"].get("delta_files_done") or 0) + int(tstate.get("delta_files_total") or 0)


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "generated_at": utc_now_iso(),
            "updated_at": None,
            "status": "running",
            "delta_ids": {},
            "profile": {},
            "tables": {},
            "stats": {
                "tables_total": 0,
                "tables_done": 0,
                "base_files_total": 0,
                "base_files_done": 0,
                "base_files_filtered": 0,
                "base_files_linked": 0,
                "delta_files_total": 0,
                "delta_files_done": 0,
            },
        }
    return read_json(path)


def _build_table_pairs(
    *,
    base_root: Path,
    delta_root: Path,
    out_root: Path,
    base_prefix: str,
    delta_prefix: str,
    merged_prefix: str,
    selected_tables: set[str] | None,
) -> list[TablePair]:
    base_dirs = _list_table_dirs(base_root, base_prefix)
    delta_dirs = _list_table_dirs(delta_root, delta_prefix)
    suffixes = sorted(set(base_dirs) | set(delta_dirs))
    pairs: list[TablePair] = []
    for suffix in suffixes:
        out_name = f"{merged_prefix}{suffix}"
        if selected_tables and out_name not in selected_tables:
            continue
        pairs.append(
            TablePair(
                suffix=suffix,
                base_dir=base_dirs.get(suffix),
                delta_dir=delta_dirs.get(suffix),
                out_dir=out_root / out_name,
            )
        )
    return pairs


def _profile_delta_ids(
    *,
    con,
    base_main_dir: Path,
    log_fp,
) -> dict[str, Any]:
    _log(log_fp, "profile delta ids against base main table")
    distinct_delta_ids = int(_query_scalar(con, "SELECT COUNT(*) FROM delta_ids") or 0)
    overlap_ids = int(
        _query_scalar(
            con,
            "SELECT COUNT(DISTINCT b.id) "
            f"FROM {_read_parquet_expr(json.dumps(str(base_main_dir / '*.parquet')), union_by_name=True)} b "
            "SEMI JOIN delta_ids d USING (id)",
        )
        or 0
    )
    return {
        "distinct_delta_ids": distinct_delta_ids,
        "overlap_existing_ids": overlap_ids,
        "new_ids": max(0, distinct_delta_ids - overlap_ids),
    }


def merge_main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Merge OpenAlex full parquet snapshot with updated_date delta parquet")
    ap.add_argument("--base-root", required=True)
    ap.add_argument("--delta-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--base-prefix", required=True)
    ap.add_argument("--delta-prefix", required=True)
    ap.add_argument("--merged-prefix", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--temp-dir", default="")
    ap.add_argument("--tables", default="", help="Comma-separated merged table directory names to process")
    ap.add_argument("--max-base-files-per-table", type=int, default=0)
    ap.add_argument("--strategy", choices=["filewise", "tablewise", "auto"], default="auto")
    ap.add_argument("--max-rows-per-file", type=int, default=250000)
    args = ap.parse_args(argv)

    base_root = Path(args.base_root).expanduser().resolve()
    delta_root = Path(args.delta_root).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve() if str(args.temp_dir).strip() else None
    state_path = run_dir / "merge_state.json"
    log_path = run_dir / "merge.log"
    manifest_path = run_dir / "merge_manifest.json"
    delta_ids_parquet = run_dir / "delta_ids.parquet"
    duckdb_path = run_dir / "merge.duckdb"
    selected_tables = {x.strip() for x in str(args.tables or "").split(",") if x.strip()} or None

    run_dir.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)
    if state_path.exists():
        state = JsonRunState(path=state_path, payload=_load_state(state_path), timestamp_key="updated_at")
    else:
        state = JsonRunState.create(
            state_path,
            _load_state(state_path),
            timestamp_key="updated_at",
        )
    payload = state.payload

    with open_append_text(log_path, purpose="parquet delta merge log") as log_fp:
        _log(log_fp, f"start base_root={base_root} delta_root={delta_root} out_root={out_root}")
        con = _connect_duckdb(db_path=duckdb_path, temp_dir=temp_dir, threads=int(args.threads))
        try:
            delta_main_dir = delta_root / args.delta_prefix
            base_main_dir = base_root / args.base_prefix
            if not delta_main_dir.exists():
                raise FileNotFoundError(f"delta main dir not found: {delta_main_dir}")
            if not base_main_dir.exists():
                raise FileNotFoundError(f"base main dir not found: {base_main_dir}")

            _ensure_delta_ids(con=con, delta_main_dir=delta_main_dir, delta_ids_parquet=delta_ids_parquet, log_fp=log_fp)
            payload["delta_ids"] = {"parquet": str(delta_ids_parquet)}
            if not payload.get("profile"):
                payload["profile"] = _profile_delta_ids(con=con, base_main_dir=base_main_dir, log_fp=log_fp)
                state.write(touch_timestamp=True)

            pairs = _build_table_pairs(
                base_root=base_root,
                delta_root=delta_root,
                out_root=out_root,
                base_prefix=args.base_prefix,
                delta_prefix=args.delta_prefix,
                merged_prefix=args.merged_prefix,
                selected_tables=selected_tables,
            )
            payload["stats"]["tables_total"] = len(pairs)
            payload["table_pairs"] = [
                {
                    "suffix": p.suffix,
                    "base_dir": str(p.base_dir) if p.base_dir else "",
                    "delta_dir": str(p.delta_dir) if p.delta_dir else "",
                    "out_dir": str(p.out_dir),
                }
                for p in pairs
            ]
            state.write(touch_timestamp=True)

            for pair in pairs:
                tkey = pair.out_dir.name
                tstate = payload["tables"].setdefault(
                    tkey,
                    {
                        "suffix": pair.suffix,
                        "base_dir": str(pair.base_dir) if pair.base_dir else "",
                        "delta_dir": str(pair.delta_dir) if pair.delta_dir else "",
                        "out_dir": str(pair.out_dir),
                        "status": "running",
                        "base_files_total": 0,
                        "base_files_done": 0,
                        "delta_files_total": 0,
                        "delta_files_done": 0,
                        "base_files_linked": 0,
                        "base_files_filtered": 0,
                        "delta_files_linked": 0,
                    },
                )
                pair.out_dir.mkdir(parents=True, exist_ok=True)
                _log(log_fp, f"table start: {tkey}")

                base_files = sorted(pair.base_dir.glob("*.parquet")) if pair.base_dir else []
                if int(args.max_base_files_per_table or 0) > 0:
                    base_files = base_files[: int(args.max_base_files_per_table)]
                delta_files = sorted(pair.delta_dir.glob("*.parquet")) if pair.delta_dir else []
                tstate["base_files_total"] = len(base_files)
                tstate["delta_files_total"] = len(delta_files)
                payload["stats"]["base_files_total"] += max(0, len(base_files) - int(tstate.get("base_files_done") or 0))
                payload["stats"]["delta_files_total"] += max(0, len(delta_files) - int(tstate.get("delta_files_done") or 0))
                state.write(touch_timestamp=True)

                strategy = str(args.strategy or "auto")
                if strategy == "auto":
                    strategy = "tablewise"
                    if len(base_files) <= 512 and len(delta_files) <= 128:
                        strategy = "filewise"
                tstate["strategy"] = strategy
                state.write(touch_timestamp=True)

                if strategy == "tablewise":
                    _merge_table_tablewise(
                        con=con,
                        pair=pair,
                        base_files=base_files,
                        delta_files=delta_files,
                        tstate=tstate,
                        state=payload,
                        log_fp=log_fp,
                        max_rows_per_file=int(args.max_rows_per_file),
                    )
                    state.write(touch_timestamp=True)
                else:
                    for base_file in base_files:
                        out_file = pair.out_dir / f"base__{base_file.name}"
                        if out_file.exists():
                            continue
                        if _overlap_exists(con, base_file=base_file):
                            _copy_filtered_base(con=con, base_file=base_file, out_file=out_file, log_fp=log_fp)
                            tstate["base_files_filtered"] = int(tstate.get("base_files_filtered") or 0) + 1
                            payload["stats"]["base_files_filtered"] = int(payload["stats"].get("base_files_filtered") or 0) + 1
                        else:
                            mode = _link_or_copy(base_file, out_file)
                            _log(log_fp, f"reuse unchanged base file: {base_file.name} -> {out_file.name} ({mode})")
                            tstate["base_files_linked"] = int(tstate.get("base_files_linked") or 0) + 1
                            payload["stats"]["base_files_linked"] = int(payload["stats"].get("base_files_linked") or 0) + 1
                        tstate["base_files_done"] = int(tstate.get("base_files_done") or 0) + 1
                        payload["stats"]["base_files_done"] = int(payload["stats"].get("base_files_done") or 0) + 1
                        state.write(touch_timestamp=True)

                    for delta_file in delta_files:
                        out_file = pair.out_dir / f"delta__{delta_file.name}"
                        if out_file.exists():
                            continue
                        mode = _link_or_copy(delta_file, out_file)
                        _log(log_fp, f"attach delta file: {delta_file.name} -> {out_file.name} ({mode})")
                        tstate["delta_files_done"] = int(tstate.get("delta_files_done") or 0) + 1
                        tstate["delta_files_linked"] = int(tstate.get("delta_files_linked") or 0) + 1
                        payload["stats"]["delta_files_done"] = int(payload["stats"].get("delta_files_done") or 0) + 1
                        state.write(touch_timestamp=True)

                tstate["status"] = "done"
                payload["stats"]["tables_done"] = sum(
                    1 for item in payload["tables"].values() if str(item.get("status") or "") == "done"
                )
                _log(log_fp, f"table done: {tkey}")
                state.write(touch_timestamp=True)

            payload["status"] = "done"
            state.write(touch_timestamp=True)

            manifest = {
                "generated_at": utc_now_iso(),
                "base_root": str(base_root),
                "delta_root": str(delta_root),
                "out_root": str(out_root),
                "base_prefix": args.base_prefix,
                "delta_prefix": args.delta_prefix,
                "merged_prefix": args.merged_prefix,
                "delta_ids_parquet": str(delta_ids_parquet),
                "duckdb_path": str(duckdb_path),
                "profile": payload.get("profile") or {},
                "stats": payload.get("stats") or {},
                "tables": payload.get("tables") or {},
            }
            atomic_write_json(manifest_path, manifest)
            _log(log_fp, "merge complete")
        finally:
            con.close()
    return 0


def _systemctl_show(unit: str) -> dict[str, str]:
    out = subprocess.check_output(
        [
            "systemctl",
            "--user",
            "show",
            unit,
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
        ],
        text=True,
    )
    data: dict[str, str] = {}
    for raw in out.splitlines():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        data[key] = value
    return data


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return read_json(path)
    except Exception:
        return {}


def watch_main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Watch parse service, then launch parquet delta merge")
    ap.add_argument("--parse-service", required=True)
    ap.add_argument("--progress-json", required=True)
    ap.add_argument("--python-bin", default=sys.executable)
    ap.add_argument("--merge-script", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--poll-seconds", type=int, default=300)
    ap.add_argument("--base-root", required=True)
    ap.add_argument("--delta-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--base-prefix", required=True)
    ap.add_argument("--delta-prefix", required=True)
    ap.add_argument("--merged-prefix", required=True)
    ap.add_argument("--merge-threads", type=int, default=16)
    ap.add_argument("--merge-temp-dir", default="")
    ap.add_argument("--merge-strategy", choices=["filewise", "tablewise", "auto"], default="auto")
    ap.add_argument("--merge-max-rows-per-file", type=int, default=250000)
    args = ap.parse_args(argv)

    run_dir = Path(args.run_dir).expanduser().resolve()
    progress_json = Path(args.progress_json).expanduser().resolve()
    log_path = run_dir / "watch.log"
    state_path = run_dir / "watch_state.json"
    run_dir.mkdir(parents=True, exist_ok=True)
    watch_state = JsonRunState.create(
        state_path,
        {
            "parse_service": args.parse_service,
            "service": {},
            "progress": {},
            "merge_started": False,
            "updated_at": None,
        },
        timestamp_key="updated_at",
    )

    with open_append_text(log_path, purpose="parquet delta watch log") as log_fp:
        _log(log_fp, f"watch start parse_service={args.parse_service}")
        merge_started = False
        while True:
            svc = _systemctl_show(args.parse_service)
            prog = _load_progress(progress_json)
            watch_state.payload.update(
                {
                "parse_service": args.parse_service,
                "service": svc,
                "progress": prog,
                "merge_started": merge_started,
                }
            )
            watch_state.write(touch_timestamp=True)

            active = str(svc.get("ActiveState") or "")
            sub = str(svc.get("SubState") or "")
            result = str(svc.get("Result") or "")
            status = str(svc.get("ExecMainStatus") or "")
            _log(
                log_fp,
                f"poll active={active} sub={sub} result={result} status={status} "
                f"stage={prog.get('stage')} cursor={((prog.get('cursor') or {}).get('source_path') or '')}",
            )

            if active in {"active", "activating", "reloading"}:
                time.sleep(max(30, int(args.poll_seconds)))
                continue

            if active == "failed" or (result and result not in {"success", "done"} and status not in {"0", ""}):
                _log(log_fp, f"parse service failed: active={active} result={result} status={status}")
                return 1

            if active == "inactive" and status == "0":
                break

            time.sleep(max(30, int(args.poll_seconds)))

        _log(log_fp, "parse service finished successfully; start merge")
        merge_started = True
        watch_state.payload["merge_started"] = True
        watch_state.write(touch_timestamp=True)

        cmd = [
            str(args.python_bin),
            str(Path(args.merge_script).expanduser().resolve()),
            "--base-root",
            args.base_root,
            "--delta-root",
            args.delta_root,
            "--out-root",
            args.out_root,
            "--base-prefix",
            args.base_prefix,
            "--delta-prefix",
            args.delta_prefix,
            "--merged-prefix",
            args.merged_prefix,
            "--run-dir",
            str(run_dir / "merge_run"),
            "--threads",
            str(int(args.merge_threads)),
            "--strategy",
            str(args.merge_strategy),
            "--max-rows-per-file",
            str(int(args.merge_max_rows_per_file)),
        ]
        if str(args.merge_temp_dir).strip():
            cmd.extend(["--temp-dir", str(args.merge_temp_dir)])

        _log(log_fp, "merge command: " + " ".join(cmd))
        subprocess.run(cmd, check=True)
        _log(log_fp, "merge finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(merge_main())
