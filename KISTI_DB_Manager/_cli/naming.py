from __future__ import annotations

import argparse

from ..naming import make_index_name, truncate_table_name

def _cmd_naming_truncate_table(args: argparse.Namespace) -> int:
    print(truncate_table_name(args.name, max_len=args.max_len))
    return 0


def _cmd_naming_index_name(args: argparse.Namespace) -> int:
    print(make_index_name(args.table, args.column, max_len=args.max_len))
    return 0


def register_naming_parser(sub) -> None:
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
