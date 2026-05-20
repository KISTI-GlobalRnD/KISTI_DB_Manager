from __future__ import annotations

import argparse
from pathlib import Path

from ..runstate import atomic_write_text

def _cmd_review_pack(args: argparse.Namespace) -> int:
    from ..review import generate_review_pack

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
    from ..review import generate_review_plan

    out_dir = args.out
    if not out_dir:
        stem_src = Path(args.config)
        out_dir = str(stem_src.with_suffix("")) + "_plan"

    data_overrides: dict[str, Any] = {}
    if getattr(args, "auto_except", None) is not None:
        data_overrides["auto_except"] = bool(args.auto_except)
    if getattr(args, "auto_except_sample_records", None) is not None:
        data_overrides["auto_except_sample_records"] = int(args.auto_except_sample_records)
    if getattr(args, "auto_except_sample_max_sources", None) is not None:
        data_overrides["auto_except_sample_max_sources"] = int(args.auto_except_sample_max_sources)
    if getattr(args, "auto_except_seed", None) is not None:
        data_overrides["auto_except_seed"] = int(args.auto_except_seed)
    if getattr(args, "auto_except_unique_key_threshold", None) is not None:
        data_overrides["auto_except_unique_key_threshold"] = int(args.auto_except_unique_key_threshold)
    if getattr(args, "auto_except_min_observations", None) is not None:
        data_overrides["auto_except_min_observations"] = int(args.auto_except_min_observations)
    if getattr(args, "auto_except_novelty_threshold", None) is not None:
        data_overrides["auto_except_novelty_threshold"] = float(args.auto_except_novelty_threshold)

    res = generate_review_plan(
        config_path=args.config,
        out_dir=out_dir,
        formats=args.formats,
        max_records=args.max_records,
        generate_desc=bool(args.generate_desc),
        data_overrides=data_overrides or None,
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
    from ..review_preview import write_review_preview_report

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


def _cmd_review_schema_viewer(args: argparse.Namespace) -> int:
    from ..review_schema import generate_schema_viewer

    out_dir = args.out
    if not out_dir:
        stem_src = Path(args.config)
        out_dir = str(stem_src.with_suffix("")) + "_schema_viewer"

    res = generate_schema_viewer(
        config_path=args.config,
        out_dir=out_dir,
        report_path=args.report,
        quarantine_path=args.quarantine,
        formats=args.formats,
        db_enabled=not bool(args.no_db),
        exact_counts=bool(args.exact_counts),
        sample_rows=int(args.sample_rows) if args.sample_rows is not None else None,
        sample_max_tables=int(args.sample_max_tables),
        description_profile_path=args.description_profile,
        dataset_profile_path=args.dataset_profile,
    )

    print(f"out_dir: {res['out_dir']}")
    print(f"schema_viewer_html: {res['schema_viewer_html']}")
    print(f"schema_viewer_json: {res['schema_viewer_json']}")
    print(f"schema_svg: {res['schema_svg']}")
    if res.get("schema_png"):
        print(f"schema_png: {res['schema_png']}")
    print(f"schema_mmd: {res['schema_mmd']}")
    return 0


def _cmd_review_diff(args: argparse.Namespace) -> int:
    from ..review_diff import diff_review_files, render_review_diff_markdown, write_review_diff_report

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
            atomic_write_text(args.out, md, purpose="review diff output")
            print(f"diff: {args.out}")
        return 0

    if args.out:
        atomic_write_text(args.out, md, purpose="review diff output")
        print(f"diff: {args.out}")
        return 0

    print(md)
    return 0


def register_review_parser(sub) -> None:
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
    p_plan.add_argument(
        "--auto-except",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable auto-except preflight during review plan (default: config).",
    )
    p_plan.add_argument("--auto-except-sample-records", type=int, help="Auto-except sample size (records).")
    p_plan.add_argument("--auto-except-sample-max-sources", type=int, help="Auto-except random source cap.")
    p_plan.add_argument("--auto-except-seed", type=int, help="Auto-except sampling seed.")
    p_plan.add_argument("--auto-except-unique-key-threshold", type=int, help="Auto-except unique subkey threshold.")
    p_plan.add_argument("--auto-except-min-observations", type=int, help="Auto-except min observations threshold.")
    p_plan.add_argument("--auto-except-novelty-threshold", type=float, help="Auto-except novelty ratio threshold.")
    p_plan.set_defaults(func=_cmd_review_plan)

    p_preview = review_sub.add_parser("preview", help="Preview raw structure vs flattened rows (HTML/JSON)")
    p_preview.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_preview.add_argument("--out", help="Output directory (default: <config>_preview)")
    p_preview.add_argument("--max-records", type=int, default=3, help="Max records to preview (default: 3)")
    p_preview.add_argument("--max-nodes", type=int, default=5000, help="Max raw nodes per record (default: 5000)")
    p_preview.add_argument("--max-union-nodes", type=int, default=20000, help="Max union nodes in HTML/JSON (default: 20000)")
    p_preview.set_defaults(func=_cmd_review_preview)

    p_schema = review_sub.add_parser("schema-viewer", help="Generate a self-contained schema viewer HTML/JSON")
    p_schema.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_schema.add_argument("--report", help="Optional RunReport JSON to enrich table mapping/DDL/issues")
    p_schema.add_argument("--quarantine", help="Optional Quarantine JSONL to overlay per-table counts")
    p_schema.add_argument(
        "--description-profile",
        help="Optional v2 tabular profile JSON; defaults to <PATH>/<table_name>_profile.json when present",
    )
    p_schema.add_argument(
        "--dataset-profile",
        help="Optional dataset_profile.json overlay; defaults to <PATH>/dataset_profile.json when present",
    )
    p_schema.add_argument("--out", help="Output directory (default: <config>_schema_viewer)")
    p_schema.add_argument(
        "--formats",
        default="html,svg,mmd",
        help="Comma-separated: html,svg,png,mmd (default: html,svg,mmd)",
    )
    p_schema.add_argument("--no-db", action="store_true", help="Skip DB introspection (works with config/report only)")
    p_schema.add_argument("--exact-counts", dest="exact_counts", action="store_true", help="Use COUNT(*) per table (slow)")
    p_schema.add_argument("--sample-rows", type=int, default=0, help="Embed LIMIT N samples per table in HTML (default: 0/off)")
    p_schema.add_argument("--sample-max-tables", type=int, default=20, help="Max tables to sample when --sample-rows>0 (default: 20)")
    p_schema.set_defaults(func=_cmd_review_schema_viewer)

    p_rdiff = review_sub.add_parser("diff", help="Diff two review/plan JSON outputs (review.json/plan.json)")
    p_rdiff.add_argument("before", help="Path to before review.json (or plan.json)")
    p_rdiff.add_argument("after", help="Path to after review.json (or plan.json)")
    p_rdiff.add_argument("--out", help="Write markdown diff to this path (default: stdout)")
    p_rdiff.add_argument("--out-dir", dest="out_dir", help="Write a diff pack directory (md/html/svg/json)")
    p_rdiff.add_argument("--max-list", type=int, default=50, help="Max items per section (default: 50)")
    p_rdiff.set_defaults(func=_cmd_review_diff)
