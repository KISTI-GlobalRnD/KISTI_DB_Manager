from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from ..runstate import atomic_write_text

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
        atomic_write_text(args.out, out, purpose="report diff output")
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
    db_load_parallel_tables = _as_int(artifacts.get("db_load_parallel_tables"), 0)

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
        tables_total = _as_int(stats.get("tables_total"), 0)
        tables_loaded = _as_int(stats.get("tables_loaded"), 0)
        if tables_total >= 2 and db_load_parallel_tables <= 1:
            # Heuristic: if we load many tables (or many batches), parallelizing LOAD DATA across
            # tables often reduces wall time. This is only meaningful for the LOAD DATA path.
            if tables_loaded >= tables_total:
                recommendations.append(
                    "Consider `db_load_parallel_tables` to parallelize `LOAD DATA` across tables (e.g., 4-8) when DB ingest dominates."
                )
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
        "db_load_parallel_tables": artifacts.get("db_load_parallel_tables"),
        "overlap_batches": artifacts.get("overlap_batches"),
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
    if profile.get("db_load_parallel_tables") is not None:
        lines.append(f"- db_load_parallel_tables: `{profile.get('db_load_parallel_tables')}`")
    if profile.get("overlap_batches") is not None:
        lines.append(f"- overlap_batches: `{profile.get('overlap_batches')}`")
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
        atomic_write_text(args.out, out + ("" if out.endswith("\n") else "\n"), purpose="report profile output")
        print(f"profile: {args.out}")
        return 0

    print(out)
    return 0


def register_report_parser(sub) -> None:
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
