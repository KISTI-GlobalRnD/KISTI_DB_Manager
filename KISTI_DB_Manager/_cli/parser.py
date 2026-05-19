from __future__ import annotations

import argparse

from .. import __version__
from .json_commands import register_json_parser
from .modes import register_modes_parser
from .naming import register_naming_parser
from .parquet import register_parquet_parser
from .quarantine import register_quarantine_parser
from .report import register_report_parser
from .review import register_review_parser
from .tabular import register_tabular_parser


def _cmd_version(_args: argparse.Namespace) -> int:
    print(__version__)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kisti-db-manager")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_version = sub.add_parser("version", help="Print package version")
    p_version.set_defaults(func=_cmd_version)

    register_modes_parser(sub)
    register_naming_parser(sub)
    register_report_parser(sub)
    register_tabular_parser(sub)
    register_json_parser(sub)
    register_parquet_parser(sub)
    register_review_parser(sub)
    register_quarantine_parser(sub)

    return parser
