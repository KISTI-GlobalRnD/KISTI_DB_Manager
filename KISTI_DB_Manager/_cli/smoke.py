from __future__ import annotations

import argparse
import importlib


def _run_packaged_main(module_name: str, argv: list[str], *, prog: str) -> int:
    module = importlib.import_module(module_name)
    return int(module.main(list(argv), prog=prog))


def _cmd_smoke_rust_db_load(args: argparse.Namespace) -> int:
    return _run_packaged_main(
        "KISTI_DB_Manager.rust_db_smoke",
        args.argv,
        prog="kisti-db-manager smoke rust-db-load",
    )


def register_smoke_parser(sub) -> None:
    p_smoke = sub.add_parser("smoke", help="Operational smoke checks")
    smoke_sub = p_smoke.add_subparsers(dest="smoke_cmd", required=True)

    p_rust_db_load = smoke_sub.add_parser(
        "rust-db-load",
        add_help=False,
        help="Run the Rust-backed JSON-to-DB smoke path",
        description=(
            "Forward arguments to the packaged Rust DB smoke command. "
            "Use `kisti-db-manager smoke rust-db-load --help` for command options."
        ),
    )
    p_rust_db_load.add_argument("argv", nargs="*")
    p_rust_db_load.set_defaults(
        func=_cmd_smoke_rust_db_load,
        forward_unknown_args=True,
        forward_arg_offset=2,
    )
