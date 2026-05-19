from __future__ import annotations

import argparse
from pathlib import Path

def _cmd_quarantine_summary(args: argparse.Namespace) -> int:
    from ..quarantine_summary import write_quarantine_report

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


def register_quarantine_parser(sub) -> None:
    p_quarantine = sub.add_parser("quarantine", help="Quarantine utilities")
    quarantine_sub = p_quarantine.add_subparsers(dest="quarantine_cmd", required=True)

    p_qsum = quarantine_sub.add_parser("summary", help="Summarize a Quarantine JSONL file (md/html/json)")
    p_qsum.add_argument("path", help="Path to quarantine JSONL")
    p_qsum.add_argument("--out", help="Output directory (default: <path>_quarantine)")
    p_qsum.add_argument("--formats", default="md,html,json", help="Comma-separated: md,html,json (default: md,html,json)")
    p_qsum.add_argument("--max-samples", type=int, default=3, help="Max samples per stage to embed (default: 3)")
    p_qsum.add_argument("--max-entries", type=int, help="Stop after N entries (useful for huge files)")
    p_qsum.set_defaults(func=_cmd_quarantine_summary)
