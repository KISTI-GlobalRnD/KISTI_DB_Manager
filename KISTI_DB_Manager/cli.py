from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from . import __version__
from .naming import make_index_name, truncate_table_name


def _resolve_bool(value: bool | None, default: bool) -> bool:
    return bool(default) if value is None else bool(value)


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency group is required but not installed."""


def _ensure_optional_deps(feature: str, modules: list[str], *, extras: list[str]) -> None:
    missing: list[str] = []
    for mod in modules:
        try:
            importlib.import_module(str(mod))
        except ModuleNotFoundError as e:
            name = getattr(e, "name", None) or str(mod)
            missing.append(str(name))

    if not missing:
        return

    extras_arg = ",".join(str(x) for x in extras if str(x))
    miss = ", ".join(sorted(set(missing)))
    raise MissingDependencyError(
        f"{feature} requires missing dependencies: {miss}. "
        f"Install with: pip install -e '.[{extras_arg}]'"
    )


def _cmd_version(_args: argparse.Namespace) -> int:
    print(__version__)
    return 0


def _cmd_naming_truncate_table(args: argparse.Namespace) -> int:
    print(truncate_table_name(args.name, max_len=args.max_len))
    return 0


def _cmd_naming_index_name(args: argparse.Namespace) -> int:
    print(make_index_name(args.table, args.column, max_len=args.max_len))
    return 0


def _cmd_report_summary(args: argparse.Namespace) -> int:
    path = Path(args.path)
    data = json.loads(path.read_text(encoding="utf-8"))
    stats = data.get("stats", {})
    timings = data.get("timings_ms", {}) or {}
    issues = data.get("issues", [])
    throughput = (data.get("artifacts") or {}).get("throughput") or {}
    print(f"run_id: {data.get('run_id')}")
    print(f"started_at: {data.get('started_at')}")
    if data.get("finished_at"):
        print(f"finished_at: {data.get('finished_at')}")
    if data.get("duration_s") is not None:
        try:
            print(f"duration_s: {float(data.get('duration_s')):.3f}")
        except Exception:
            print(f"duration_s: {data.get('duration_s')}")
    print(f"issues: {len(issues)}")
    for k in sorted(stats):
        print(f"{k}: {stats[k]}")
    if timings:
        print("timings_ms:")
        for k in sorted(timings):
            try:
                ms = int(timings[k])
                print(f"{k}: {ms} ({ms/1000.0:.3f}s)")
            except Exception:
                print(f"{k}: {timings[k]}")
    if throughput:
        print("throughput:")
        for k in sorted(throughput):
            print(f"{k}: {throughput[k]}")
    return 0


def _cmd_report_diff(args: argparse.Namespace) -> int:
    before = json.loads(Path(args.before).read_text(encoding="utf-8"))
    after = json.loads(Path(args.after).read_text(encoding="utf-8"))

    def _issues_by_stage(data: dict) -> dict[str, int]:
        res: dict[str, int] = {}
        for it in (data.get("issues") or []):
            stage = str(it.get("stage") or "")
            if not stage:
                stage = "(unknown)"
            res[stage] = int(res.get(stage, 0)) + 1
        return res

    def _stat(data: dict, k: str):
        return (data.get("stats") or {}).get(k)

    before_stats = before.get("stats") or {}
    after_stats = after.get("stats") or {}
    keys = sorted(set(before_stats.keys()) | set(after_stats.keys()))

    before_t = before.get("timings_ms") or {}
    after_t = after.get("timings_ms") or {}
    t_keys = sorted(set(before_t.keys()) | set(after_t.keys()))

    lines: list[str] = []
    lines.append("# RunReport Diff")
    lines.append("")
    lines.append(f"- before: `{args.before}`")
    lines.append(f"- after: `{args.after}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | before | after | delta |")
    lines.append("|---|---:|---:|---:|")
    for k in keys:
        b = int(before_stats.get(k, 0) or 0)
        a = int(after_stats.get(k, 0) or 0)
        d = a - b
        lines.append(f"| {k} | {b} | {a} | {d:+d} |")

    lines.append("")
    lines.append("| Meta | before | after |")
    lines.append("|---|---|---|")
    lines.append(f"| duration_s | {before.get('duration_s')} | {after.get('duration_s')} |")
    lines.append(f"| issues | {len(before.get('issues') or [])} | {len(after.get('issues') or [])} |")
    lines.append("")

    if t_keys:
        lines.append("## Timings (ms)")
        lines.append("")
        lines.append("| Key | before_ms | after_ms | delta_ms |")
        lines.append("|---|---:|---:|---:|")
        for k in t_keys:
            b = int(before_t.get(k, 0) or 0)
            a = int(after_t.get(k, 0) or 0)
            lines.append(f"| {k} | {b} | {a} | {a - b:+d} |")
        lines.append("")

    b_stage = _issues_by_stage(before)
    a_stage = _issues_by_stage(after)
    stage_keys = sorted(set(b_stage.keys()) | set(a_stage.keys()))
    if stage_keys:
        lines.append("## Issues by stage")
        lines.append("")
        lines.append("| stage | before | after | delta |")
        lines.append("|---|---:|---:|---:|")
        for s in stage_keys:
            b = int(b_stage.get(s, 0))
            a = int(a_stage.get(s, 0))
            lines.append(f"| {s} | {b} | {a} | {a - b:+d} |")
        lines.append("")

    out = "\n".join(lines) + "\n"
    if args.out:
        Path(args.out).write_text(out, encoding="utf-8")
        print(f"diff: {args.out}")
        return 0

    print(out)
    return 0


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _build_run_report_profile(data: dict[str, Any], *, top: int = 8) -> dict[str, Any]:
    stats = data.get("stats") or {}
    timings = data.get("timings_ms") or {}
    artifacts = data.get("artifacts") or {}

    total_ms = _as_int(timings.get("pipeline.json.total"), 0)
    if total_ms <= 0:
        total_ms = _as_int(timings.get("pipeline.tabular.total"), 0)
    if total_ms <= 0:
        total_ms = int(round(_as_float(data.get("duration_s"), 0.0) * 1000.0))
    if total_ms <= 0:
        total_ms = sum(_as_int(v, 0) for v in timings.values() if _as_int(v, 0) > 0)

    def _timing(key: str) -> int:
        return max(0, _as_int(timings.get(key), 0))

    def _share(ms: int) -> float:
        if total_ms <= 0:
            return 0.0
        return (100.0 * float(ms)) / float(total_ms)

    parse_ms = _timing("io.json_parse")
    flatten_ms = _timing("json.flatten")
    db_load_ms = _timing("json.db.load") + _timing("tabular.db.load")
    db_create_ms = _timing("json.db.create") + _timing("tabular.db.create")
    db_index_ms = _timing("json.db.index") + _timing("tabular.db.index")
    db_optimize_ms = _timing("json.db.optimize") + _timing("tabular.db.optimize")
    db_alter_ms = _timing("db.alter")
    db_load_exec_ms = _timing("db.load_data.exec")
    db_tsv_write_ms = _timing("db.load_data.tsv_write")
    db_to_sql_ms = _timing("db.to_sql")

    bottleneck = "mixed"
    reason = "No single stage dominates runtime."
    recommendations: list[str] = []

    if _share(db_load_ms) >= 55.0:
        bottleneck = "db-load-bound"
        reason = f"DB load stages dominate runtime ({db_load_ms}ms, {_share(db_load_ms):.1f}%)."
        recommendations = [
            "Keep ingest and finalize separate (skip index/optimize during ingest).",
            "Prefer LOAD DATA path (`db_load_method=auto` and LOCAL INFILE enabled).",
        ]
        if _share(db_alter_ms) >= 5.0:
            bottleneck = "db-load-bound-drift-ddl"
            reason = f"DDL drift cost is significant (`db.alter`={db_alter_ms}ms, {_share(db_alter_ms):.1f}%)."
            recommendations.append(
                "Use `schema_mode=freeze` (or `schema_mode=hybrid` with a small warmup) to cap ALTER churn on drift-heavy ingest."
            )
        if _share(db_to_sql_ms) >= 20.0:
            recommendations.append("Fallback to `to_sql` is expensive; verify LOCAL INFILE/connectivity.")
        if _share(db_tsv_write_ms) >= 10.0:
            recommendations.append("TSV serialization cost is visible; reduce row shaping overhead or improve disk I/O.")
    elif _share(flatten_ms) >= 50.0:
        bottleneck = "flatten-bound"
        reason = f"JSON flatten dominates runtime ({flatten_ms}ms, {_share(flatten_ms):.1f}%)."
        recommendations = [
            "Increase `parallel_workers` only when flatten is dominant and record complexity is high.",
            "Reduce deep nested branch expansion or except non-critical branches.",
            "Use larger `chunk_size` to reduce per-batch orchestration overhead.",
        ]
    elif _share(parse_ms) >= 35.0:
        bottleneck = "parse-bound"
        reason = f"JSON parsing dominates runtime ({parse_ms}ms, {_share(parse_ms):.1f}%)."
        recommendations = [
            "Use JSONL input and avoid full-file object parsing paths where possible.",
            "Check compression/decompression and storage throughput.",
        ]
    elif _share(db_index_ms + db_optimize_ms) >= 35.0:
        bottleneck = "finalize-bound"
        reason = (
            "Index/optimize stages dominate runtime "
            f"({db_index_ms + db_optimize_ms}ms, {_share(db_index_ms + db_optimize_ms):.1f}%)."
        )
        recommendations = [
            "Run finalize in a separate job window.",
            "Evaluate index scope/prefix length and remove low-value indexes.",
        ]

    top_n = max(1, int(top or 8))
    sorted_timings = sorted(
        [(str(k), _as_int(v, 0)) for k, v in timings.items() if _as_int(v, 0) > 0],
        key=lambda x: x[1],
        reverse=True,
    )[:top_n]

    profile = {
        "run_id": data.get("run_id"),
        "path": None,
        "mode": artifacts.get("mode"),
        "schema_mode": artifacts.get("schema_mode"),
        "schema_hybrid_warmup_batches": artifacts.get("schema_hybrid_warmup_batches"),
        "schema_hybrid_freeze_started_batch": artifacts.get("schema_hybrid_freeze_started_batch"),
        "chunk_size": artifacts.get("chunk_size"),
        "parallel_workers": artifacts.get("parallel_workers"),
        "duration_s": data.get("duration_s"),
        "issues": len(data.get("issues") or []),
        "stats": {
            "records_read": stats.get("records_read"),
            "records_ok": stats.get("records_ok"),
            "rows_loaded": stats.get("rows_loaded"),
            "batches_total": stats.get("batches_total"),
            "tables_total": stats.get("tables_total"),
            "tables_loaded": stats.get("tables_loaded"),
            "tables_created": stats.get("tables_created"),
        },
        "total_ms": total_ms,
        "timings_ms": {
            "io.json_parse": parse_ms,
            "json.flatten": flatten_ms,
            "json.db.create": db_create_ms,
            "json.db.load": db_load_ms,
            "json.db.index": db_index_ms,
            "json.db.optimize": db_optimize_ms,
            "db.alter": db_alter_ms,
            "db.load_data.tsv_write": db_tsv_write_ms,
            "db.load_data.exec": db_load_exec_ms,
            "db.to_sql": db_to_sql_ms,
        },
        "shares_pct": {
            "io.json_parse": _share(parse_ms),
            "json.flatten": _share(flatten_ms),
            "json.db.load": _share(db_load_ms),
            "db.alter": _share(db_alter_ms),
            "db.load_data.exec": _share(db_load_exec_ms),
            "db.to_sql": _share(db_to_sql_ms),
        },
        "top_timings": [
            {"key": key, "ms": ms, "share_pct": _share(ms)} for key, ms in sorted_timings
        ],
        "bottleneck": {
            "class": bottleneck,
            "reason": reason,
            "recommendations": recommendations,
        },
    }
    return profile


def _render_run_report_profile_markdown(profile: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# RunReport Profile")
    lines.append("")
    lines.append(f"- run_id: `{profile.get('run_id')}`")
    lines.append(f"- mode: `{profile.get('mode')}`")
    lines.append(f"- schema_mode: `{profile.get('schema_mode')}`")
    if profile.get("schema_mode") == "hybrid":
        lines.append(f"- schema_hybrid_warmup_batches: `{profile.get('schema_hybrid_warmup_batches')}`")
        lines.append(f"- schema_hybrid_freeze_started_batch: `{profile.get('schema_hybrid_freeze_started_batch')}`")
    lines.append(f"- duration_s: `{profile.get('duration_s')}`")
    lines.append(f"- total_ms: `{profile.get('total_ms')}`")
    lines.append(f"- chunk_size: `{profile.get('chunk_size')}`")
    lines.append(f"- parallel_workers: `{profile.get('parallel_workers')}`")
    lines.append(f"- issues: `{profile.get('issues')}`")
    lines.append("")

    stats = profile.get("stats") or {}
    lines.append("## Stats")
    lines.append("")
    for k in [
        "records_read",
        "records_ok",
        "rows_loaded",
        "batches_total",
        "tables_total",
        "tables_loaded",
        "tables_created",
    ]:
        lines.append(f"- {k}: {stats.get(k)}")
    lines.append("")

    b = profile.get("bottleneck") or {}
    lines.append("## Bottleneck")
    lines.append("")
    lines.append(f"- class: `{b.get('class')}`")
    lines.append(f"- reason: {b.get('reason')}")
    recs = b.get("recommendations") or []
    if recs:
        lines.append("- recommendations:")
        for r in recs:
            lines.append(f"  - {r}")
    lines.append("")

    lines.append("## Top Timings")
    lines.append("")
    lines.append("| key | ms | share_pct |")
    lines.append("|---|---:|---:|")
    for row in profile.get("top_timings") or []:
        lines.append(
            f"| {row.get('key')} | {int(_as_int(row.get('ms'), 0))} | {float(_as_float(row.get('share_pct'), 0.0)):.1f} |"
        )
    lines.append("")
    return "\n".join(lines)


def _cmd_report_profile(args: argparse.Namespace) -> int:
    path = Path(args.path)
    data = json.loads(path.read_text(encoding="utf-8"))
    profile = _build_run_report_profile(data, top=int(args.top))
    profile["path"] = str(path)

    if bool(getattr(args, "as_json", False)):
        out = json.dumps(profile, ensure_ascii=False, indent=2)
    else:
        out = _render_run_report_profile_markdown(profile)

    if args.out:
        Path(args.out).write_text(out + ("" if out.endswith("\n") else "\n"), encoding="utf-8")
        print(f"profile: {args.out}")
        return 0

    print(out)
    return 0


def _cmd_modes(_args: argparse.Namespace) -> int:
    from .modes import list_modes

    for spec in list_modes():
        print(f"- {spec.name}: {spec.description}")
        if spec.data_overrides:
            for k in sorted(spec.data_overrides):
                print(f"  - {k}: {spec.data_overrides[k]}")
        if spec.stage_defaults:
            sd = spec.stage_defaults
            print(f"  - stages: create={sd.get('create')} load={sd.get('load')} index={sd.get('index')} optimize={sd.get('optimize')}")
    return 0


def _cmd_tabular_run(args: argparse.Namespace) -> int:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    data_config = cfg.get("data_config") or cfg.get("data") or {}
    db_config = cfg.get("db_config") or cfg.get("db") or {}
    desc_params = cfg.get("desc_params") or None

    from .modes import apply_mode, resolve_mode_name

    mode_name = resolve_mode_name(getattr(args, "mode", None), data_config)
    mode_spec = apply_mode(mode_name, data_config)

    if getattr(args, "fast_load_session", None) is not None:
        data_config["fast_load_session"] = bool(args.fast_load_session)
    if getattr(args, "db_load_method", None):
        data_config["db_load_method"] = args.db_load_method

    create = _resolve_bool(getattr(args, "create", None), mode_spec.stage_defaults.get("create", True)) and not bool(args.dry_run)
    load = _resolve_bool(getattr(args, "load", None), mode_spec.stage_defaults.get("load", True)) and not bool(args.dry_run)
    index = _resolve_bool(getattr(args, "index", None), mode_spec.stage_defaults.get("index", True)) and not bool(args.dry_run)
    optimize = _resolve_bool(getattr(args, "optimize", None), mode_spec.stage_defaults.get("optimize", True)) and not bool(args.dry_run)

    _ensure_optional_deps("tabular run", ["numpy", "pandas"], extras=["tabular"])
    if create or load or index or optimize:
        db_mods = ["pymysql"]
        if load:
            db_mods.append("sqlalchemy")
        _ensure_optional_deps("tabular DB stages", db_mods, extras=["db"])

    from .pipeline import run_tabular_pipeline
    from .quarantine import QuarantineWriter
    from .report import RunReport

    quarantine = QuarantineWriter(args.quarantine) if args.quarantine else None
    report = RunReport()
    report.set_artifact("mode", mode_spec.name)

    res = run_tabular_pipeline(
        data_config,
        db_config,
        desc_params=desc_params,
        generate_desc=args.generate_desc,
        emit_ddl=args.print_ddl,
        create=create,
        load=load,
        index=index,
        optimize=optimize,
        continue_on_error=not args.fail_fast,
        report=report,
        quarantine=quarantine,
    )

    res.report.finish()

    if args.report:
        res.report.save_json(args.report)
        print(f"report: {args.report}")
    else:
        print(res.report.to_json())

    if args.print_namemap and res.name_map:
        print(json.dumps(res.name_map.to_dict(), ensure_ascii=False, indent=2))

    if args.print_ddl:
        ddl = res.report.artifacts.get("create_table_sql")
        if ddl:
            print(ddl)
        else:
            print("(no DDL available)")

    return 0


def _cmd_json_run(args: argparse.Namespace) -> int:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    data_config = cfg.get("data_config") or cfg.get("data") or {}
    db_config = cfg.get("db_config") or cfg.get("db") or {}

    from .modes import apply_mode, resolve_mode_name

    mode_name = resolve_mode_name(getattr(args, "mode", None), data_config)
    mode_spec = apply_mode(mode_name, data_config)

    if getattr(args, "fast_load_session", None) is not None:
        data_config["fast_load_session"] = bool(args.fast_load_session)
    if getattr(args, "schema_mode", None):
        data_config["schema_mode"] = str(args.schema_mode)
    if getattr(args, "schema_hybrid_warmup_batches", None) is not None:
        data_config["schema_hybrid_warmup_batches"] = int(args.schema_hybrid_warmup_batches)
    if getattr(args, "extra_column_name", None):
        data_config["extra_column_name"] = str(args.extra_column_name)
    if getattr(args, "db_load_method", None):
        data_config["db_load_method"] = args.db_load_method
    if getattr(args, "parallel_workers", None) is not None:
        data_config["parallel_workers"] = int(args.parallel_workers)
    if getattr(args, "db_load_parallel_tables", None) is not None:
        data_config["db_load_parallel_tables"] = int(args.db_load_parallel_tables)
    if getattr(args, "json_streaming_load", None) is not None:
        data_config["json_streaming_load"] = bool(args.json_streaming_load)
    if getattr(args, "chunk_size", None) is not None:
        data_config["chunk_size"] = int(args.chunk_size)

    create = _resolve_bool(getattr(args, "create", None), mode_spec.stage_defaults.get("create", True)) and not bool(args.dry_run)
    load = _resolve_bool(getattr(args, "load", None), mode_spec.stage_defaults.get("load", True)) and not bool(args.dry_run)
    index = _resolve_bool(getattr(args, "index", None), mode_spec.stage_defaults.get("index", True)) and not bool(args.dry_run)
    optimize = _resolve_bool(getattr(args, "optimize", None), mode_spec.stage_defaults.get("optimize", True)) and not bool(args.dry_run)

    _ensure_optional_deps("json run", ["numpy", "pandas", "tqdm", "orjson", "xmltodict"], extras=["json"])
    if create or load or index or optimize:
        db_mods = ["pymysql"]
        if load:
            db_mods.append("sqlalchemy")
        _ensure_optional_deps("json DB stages", db_mods, extras=["db"])

    from .pipeline import run_json_pipeline
    from .quarantine import QuarantineWriter
    from .report import RunReport

    quarantine = QuarantineWriter(args.quarantine) if args.quarantine else None
    report = RunReport()

    report.set_artifact("mode", mode_spec.name)

    res = run_json_pipeline(
        data_config,
        db_config,
        index_key=args.index_key,
        except_keys=args.except_key or None,
        chunk_size=args.chunk_size,
        max_records=args.max_records,
        emit_ddl=args.print_ddl,
        create=create,
        load=load,
        index=index,
        optimize=optimize,
        continue_on_error=not args.fail_fast,
        report=report,
        quarantine=quarantine,
    )

    res.report.finish()

    if args.report:
        res.report.save_json(args.report)
        print(f"report: {args.report}")
    else:
        print(res.report.to_json())

    if args.print_namemap:
        print(json.dumps({k: v.to_dict() for k, v in res.name_maps.items()}, ensure_ascii=False, indent=2))

    if args.print_ddl:
        ddls = res.report.artifacts.get("create_table_sql_json")
        if ddls:
            print(json.dumps(ddls, ensure_ascii=False, indent=2))
        else:
            print("(no DDL available)")

    return 0


def _cmd_review_pack(args: argparse.Namespace) -> int:
    from .review import generate_review_pack

    out_dir = args.out
    if not out_dir:
        stem_src = Path(args.report or args.config)
        out_dir = str(stem_src.with_suffix("")) + "_review"

    res = generate_review_pack(
        config_path=args.config,
        report_path=args.report,
        quarantine_path=getattr(args, "quarantine", None),
        out_dir=out_dir,
        formats=args.formats,
        db_enabled=not bool(args.no_db),
        exact_counts=bool(args.exact_counts),
        sample_rows=args.sample_rows,
        sample_max_tables=args.sample_max_tables,
    )

    print(f"out_dir: {res['out_dir']}")
    print(f"review_md: {res['review_md']}")
    print(f"review_html: {res['review_html']}")
    print(f"schema_svg: {res['schema_svg']}")
    if res.get("schema_png"):
        print(f"schema_png: {res['schema_png']}")
    print(f"schema_mmd: {res['schema_mmd']}")
    print(f"review_json: {res['review_json']}")
    return 0


def _cmd_review_plan(args: argparse.Namespace) -> int:
    from .review import generate_review_plan

    out_dir = args.out
    if not out_dir:
        stem_src = Path(args.config)
        out_dir = str(stem_src.with_suffix("")) + "_plan"

    res = generate_review_plan(
        config_path=args.config,
        out_dir=out_dir,
        formats=args.formats,
        max_records=args.max_records,
        generate_desc=bool(args.generate_desc),
    )

    print(f"out_dir: {res['out_dir']}")
    print(f"plan_md: {res['plan_md']}")
    print(f"plan_html: {res['plan_html']}")
    print(f"schema_svg: {res['schema_svg']}")
    if res.get("schema_png"):
        print(f"schema_png: {res['schema_png']}")
    print(f"schema_mmd: {res['schema_mmd']}")
    print(f"ddl_json: {res['ddl_json']}")
    print(f"ddl_sql: {res['ddl_sql']}")
    print(f"plan_json: {res['plan_json']}")
    print(f"plan_run_report: {res['plan_run_report']}")
    return 0


def _cmd_review_preview(args: argparse.Namespace) -> int:
    from .review_preview import write_review_preview_report

    out_dir = args.out
    if not out_dir:
        stem_src = Path(args.config)
        out_dir = str(stem_src.with_suffix("")) + "_preview"

    res = write_review_preview_report(
        config_path=args.config,
        out_dir=out_dir,
        max_records=int(args.max_records),
        max_nodes=int(args.max_nodes),
        max_union_nodes=int(getattr(args, "max_union_nodes", 20000)),
    )

    print(f"out_dir: {res['out_dir']}")
    print(f"preview_html: {res['preview_html']}")
    print(f"preview_json: {res['preview_json']}")
    return 0


def _cmd_review_diff(args: argparse.Namespace) -> int:
    from .review_diff import diff_review_files, render_review_diff_markdown, write_review_diff_report

    diff = diff_review_files(args.before, args.after)
    md = render_review_diff_markdown(diff, max_list=int(args.max_list))

    if getattr(args, "out_dir", None):
        res = write_review_diff_report(
            before_path=args.before,
            after_path=args.after,
            out_dir=args.out_dir,
            max_list=int(args.max_list),
        )
        print(f"out_dir: {res['out_dir']}")
        print(f"diff_json: {res['diff_json']}")
        print(f"diff_md: {res['diff_md']}")
        print(f"diff_html: {res['diff_html']}")
        print(f"schema_diff_svg: {res['schema_diff_svg']}")
        if args.out:
            Path(args.out).write_text(md, encoding="utf-8")
            print(f"diff: {args.out}")
        return 0

    if args.out:
        Path(args.out).write_text(md, encoding="utf-8")
        print(f"diff: {args.out}")
        return 0

    print(md)
    return 0


def _cmd_quarantine_summary(args: argparse.Namespace) -> int:
    from .quarantine_summary import write_quarantine_report

    out_dir = args.out
    if not out_dir:
        stem = Path(args.path)
        out_dir = str(stem.with_suffix("")) + "_quarantine"

    res = write_quarantine_report(
        path=args.path,
        out_dir=out_dir,
        formats=args.formats,
        max_samples=int(args.max_samples),
        max_entries=args.max_entries,
    )

    print(f"out_dir: {res['out_dir']}")
    print(f"quarantine_md: {res['quarantine_md']}")
    print(f"quarantine_html: {res['quarantine_html']}")
    print(f"quarantine_json: {res['quarantine_json']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kisti-db-manager")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_version = sub.add_parser("version", help="Print package version")
    p_version.set_defaults(func=_cmd_version)

    p_modes = sub.add_parser("modes", help="List built-in run modes/presets")
    p_modes.set_defaults(func=_cmd_modes)

    p_naming = sub.add_parser("naming", help="Identifier utilities")
    naming_sub = p_naming.add_subparsers(dest="naming_cmd", required=True)

    p_tt = naming_sub.add_parser("truncate-table", help="Truncate table name to 64 chars")
    p_tt.add_argument("name")
    p_tt.add_argument("--max-len", type=int, default=64)
    p_tt.set_defaults(func=_cmd_naming_truncate_table)

    p_idx = naming_sub.add_parser("index-name", help="Generate safe index name")
    p_idx.add_argument("table")
    p_idx.add_argument("column")
    p_idx.add_argument("--max-len", type=int, default=64)
    p_idx.set_defaults(func=_cmd_naming_index_name)

    p_report = sub.add_parser("report", help="Report utilities")
    report_sub = p_report.add_subparsers(dest="report_cmd", required=True)

    p_summary = report_sub.add_parser("summary", help="Summarize a RunReport JSON file")
    p_summary.add_argument("path")
    p_summary.set_defaults(func=_cmd_report_summary)

    p_diff = report_sub.add_parser("diff", help="Diff two RunReport JSON files")
    p_diff.add_argument("before")
    p_diff.add_argument("after")
    p_diff.add_argument("--out", help="Write markdown diff to this path (default: stdout)")
    p_diff.set_defaults(func=_cmd_report_diff)

    p_profile = report_sub.add_parser("profile", help="Profile one RunReport JSON and suggest bottlenecks")
    p_profile.add_argument("path")
    p_profile.add_argument("--top", type=int, default=8, help="Show top-N timing keys (default: 8)")
    p_profile.add_argument("--as-json", action="store_true", help="Print machine-readable JSON instead of markdown")
    p_profile.add_argument("--out", help="Write profile output to this path (default: stdout)")
    p_profile.set_defaults(func=_cmd_report_profile)

    p_tabular = sub.add_parser("tabular", help="Tabular -> DB pipeline helpers")
    tabular_sub = p_tabular.add_subparsers(dest="tabular_cmd", required=True)
    p_tabular_run = tabular_sub.add_parser("run", help="Run create/load/index/optimize for a tabular file")
    p_tabular_run.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_tabular_run.add_argument("--generate-desc", action="store_true", help="Generate a new description CSV first")
    p_tabular_run.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    p_tabular_run.add_argument("--dry-run", action="store_true", help="Prepare desc/namemap only (skip DB steps)")
    p_tabular_run.add_argument("--report", help="Write RunReport JSON to this path")
    p_tabular_run.add_argument("--quarantine", help="Write failures as JSONL to this path")
    p_tabular_run.add_argument("--print-namemap", action="store_true", help="Print NameMap JSON after run")
    p_tabular_run.add_argument("--print-ddl", action="store_true", help="Print CREATE TABLE DDL after run")
    from .modes import MODES as _MODES

    p_tabular_run.add_argument("--mode", choices=sorted(_MODES), help="Run mode preset (default: config.mode or default)")
    p_tabular_run.add_argument(
        "--fast-load-session",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Best-effort DB session tuning for ingest speed (default: mode/config)",
    )
    p_tabular_run.add_argument(
        "--db-load-method",
        choices=["auto", "to_sql", "load_data"],
        help="DB load method override (default: config or 'auto')",
    )
    p_tabular_run.add_argument(
        "--create",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable CREATE TABLE (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable LOAD/INSERT (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--index",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable INDEX creation (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--optimize",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable OPTIMIZE TABLE (default: mode preset)",
    )
    p_tabular_run.set_defaults(func=_cmd_tabular_run)

    p_json = sub.add_parser("json", help="JSON -> DB pipeline helpers")
    json_sub = p_json.add_subparsers(dest="json_cmd", required=True)
    p_json_run = json_sub.add_parser("run", help="Run JSON flatten/create/load/index/optimize")
    p_json_run.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_json_run.add_argument("--index-key", help="Override record id key (default: config or 'id')")
    p_json_run.add_argument("--except-key", action="append", help="Exclude a branch from flattening (repeatable)")
    p_json_run.add_argument("--chunk-size", type=int, help="Records per batch (default: config or 1000)")
    p_json_run.add_argument("--max-records", type=int, help="Stop after N records (useful for dry-run/preview)")
    p_json_run.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    p_json_run.add_argument("--dry-run", action="store_true", help="Prepare desc/namemap only (skip DB steps)")
    p_json_run.add_argument("--report", help="Write RunReport JSON to this path")
    p_json_run.add_argument("--quarantine", help="Write failures as JSONL to this path")
    p_json_run.add_argument("--print-namemap", action="store_true", help="Print NameMap JSON after run")
    p_json_run.add_argument("--print-ddl", action="store_true", help="Print CREATE TABLE DDLs after run")
    p_json_run.add_argument("--mode", choices=sorted(_MODES), help="Run mode preset (default: config.mode or default)")
    p_json_run.add_argument(
        "--schema-mode",
        choices=["evolve", "freeze", "hybrid"],
        help="Schema drift strategy (default: mode/config). freeze/hybrid store unknown fields into extra column.",
    )
    p_json_run.add_argument(
        "--schema-hybrid-warmup-batches",
        type=int,
        help="For schema_mode=hybrid: number of initial batches to allow ALTER (default: mode/config, usually 1).",
    )
    p_json_run.add_argument(
        "--extra-column-name",
        dest="extra_column_name",
        help="Extra column name used when schema_mode=freeze/hybrid (default: mode/config, usually '__extra__')",
    )
    p_json_run.add_argument(
        "--fast-load-session",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Best-effort DB session tuning for ingest speed (default: mode/config)",
    )
    p_json_run.add_argument(
        "--db-load-method",
        choices=["auto", "to_sql", "load_data"],
        help="DB load method override (default: config or 'auto')",
    )
    p_json_run.add_argument(
        "--parallel-workers",
        type=int,
        help="ProcessPool workers for JSON flatten (default: config or 0/off)",
    )
    p_json_run.add_argument(
        "--db-load-parallel-tables",
        type=int,
        help="Parallelize LOAD DATA across tables (default: config or 0/off)",
    )
    p_json_run.add_argument(
        "--json-streaming-load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use row-based streaming TSV load when LOAD DATA is enabled (default: config or true)",
    )
    p_json_run.add_argument(
        "--create",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable CREATE TABLE (default: mode preset)",
    )
    p_json_run.add_argument(
        "--load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable LOAD/INSERT (default: mode preset)",
    )
    p_json_run.add_argument(
        "--index",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable INDEX creation (default: mode preset)",
    )
    p_json_run.add_argument(
        "--optimize",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable OPTIMIZE TABLE (default: mode preset)",
    )
    p_json_run.set_defaults(func=_cmd_json_run)

    p_review = sub.add_parser("review", help="Review/visualization helpers")
    review_sub = p_review.add_subparsers(dest="review_cmd", required=True)

    p_pack = review_sub.add_parser("pack", help="Generate a review pack (md/html/svg) from config (+ optional report)")
    p_pack.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_pack.add_argument("--report", help="Optional RunReport JSON to enrich mapping/issues")
    p_pack.add_argument("--quarantine", help="Optional Quarantine JSONL to overlay per-table counts")
    p_pack.add_argument("--out", help="Output directory (default: <config/report>_review)")
    p_pack.add_argument(
        "--formats",
        default="md,html,svg",
        help="Comma-separated: md,html,svg,png,mmd (default: md,html,svg)",
    )
    p_pack.add_argument("--no-db", action="store_true", help="Skip DB introspection (works without `.[db]`)")
    p_pack.add_argument("--exact-counts", dest="exact_counts", action="store_true", help="Use COUNT(*) per table (slow)")
    p_pack.add_argument("--sample-rows", type=int, default=0, help="Embed LIMIT N samples per table in HTML (default: 0/off)")
    p_pack.add_argument("--sample-max-tables", type=int, default=20, help="Max tables to sample when --sample-rows>0 (default: 20)")
    p_pack.set_defaults(func=_cmd_review_pack)

    p_plan = review_sub.add_parser("plan", help="Generate a pre-load review plan (no DB writes)")
    p_plan.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_plan.add_argument("--out", help="Output directory (default: <config>_plan)")
    p_plan.add_argument(
        "--formats",
        default="md,html,svg,mmd",
        help="Comma-separated: md,html,svg,png,mmd (default: md,html,svg,mmd)",
    )
    p_plan.add_argument("--max-records", type=int, default=1000, help="Stop after N records when previewing JSON inputs")
    p_plan.add_argument("--generate-desc", action="store_true", help="(tabular) generate desc CSV first (can be slow)")
    p_plan.set_defaults(func=_cmd_review_plan)

    p_preview = review_sub.add_parser("preview", help="Preview raw structure vs flattened rows (HTML/JSON)")
    p_preview.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_preview.add_argument("--out", help="Output directory (default: <config>_preview)")
    p_preview.add_argument("--max-records", type=int, default=3, help="Max records to preview (default: 3)")
    p_preview.add_argument("--max-nodes", type=int, default=5000, help="Max raw nodes per record (default: 5000)")
    p_preview.add_argument("--max-union-nodes", type=int, default=20000, help="Max union nodes in HTML/JSON (default: 20000)")
    p_preview.set_defaults(func=_cmd_review_preview)

    p_rdiff = review_sub.add_parser("diff", help="Diff two review/plan JSON outputs (review.json/plan.json)")
    p_rdiff.add_argument("before", help="Path to before review.json (or plan.json)")
    p_rdiff.add_argument("after", help="Path to after review.json (or plan.json)")
    p_rdiff.add_argument("--out", help="Write markdown diff to this path (default: stdout)")
    p_rdiff.add_argument("--out-dir", dest="out_dir", help="Write a diff pack directory (md/html/svg/json)")
    p_rdiff.add_argument("--max-list", type=int, default=50, help="Max items per section (default: 50)")
    p_rdiff.set_defaults(func=_cmd_review_diff)

    p_quarantine = sub.add_parser("quarantine", help="Quarantine utilities")
    quarantine_sub = p_quarantine.add_subparsers(dest="quarantine_cmd", required=True)

    p_qsum = quarantine_sub.add_parser("summary", help="Summarize a Quarantine JSONL file (md/html/json)")
    p_qsum.add_argument("path", help="Path to quarantine JSONL")
    p_qsum.add_argument("--out", help="Output directory (default: <path>_quarantine)")
    p_qsum.add_argument("--formats", default="md,html,json", help="Comma-separated: md,html,json (default: md,html,json)")
    p_qsum.add_argument("--max-samples", type=int, default=3, help="Max samples per stage to embed (default: 3)")
    p_qsum.add_argument("--max-entries", type=int, help="Stop after N entries (useful for huge files)")
    p_qsum.set_defaults(func=_cmd_quarantine_summary)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except MissingDependencyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
