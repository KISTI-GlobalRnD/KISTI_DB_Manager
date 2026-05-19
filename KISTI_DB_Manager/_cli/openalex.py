from __future__ import annotations

import argparse
import importlib


def _run_packaged_main(module_name: str, argv: list[str], *, prog: str) -> int:
    module = importlib.import_module(module_name)
    return int(module.main(list(argv), prog=prog))


def _cmd_openalex_materialize(args: argparse.Namespace) -> int:
    return _run_packaged_main(
        "KISTI_DB_Manager.openalex_materialize",
        args.argv,
        prog="kisti-db-manager openalex materialize",
    )


def _cmd_openalex_benchmark_load(args: argparse.Namespace) -> int:
    return _run_packaged_main(
        "KISTI_DB_Manager.openalex_benchmark",
        args.argv,
        prog="kisti-db-manager openalex benchmark-load",
    )


def register_openalex_parser(sub) -> None:
    p_openalex = sub.add_parser("openalex", help="OpenAlex parquet materialization helpers")
    openalex_sub = p_openalex.add_subparsers(dest="openalex_cmd", required=True)

    p_materialize = openalex_sub.add_parser(
        "materialize",
        add_help=False,
        help="Materialize persisted OpenAlex parquet artifacts into MariaDB/MySQL",
        description=(
            "Forward arguments to the packaged OpenAlex materializer. "
            "Use `kisti-db-manager openalex materialize --help` for command options."
        ),
    )
    p_materialize.add_argument("argv", nargs="*")
    p_materialize.set_defaults(
        func=_cmd_openalex_materialize,
        forward_unknown_args=True,
        forward_arg_offset=2,
    )

    p_benchmark = openalex_sub.add_parser(
        "benchmark-load",
        add_help=False,
        help="Benchmark DB load-only speed from persisted parquet artifacts",
        description=(
            "Forward arguments to the packaged OpenAlex parquet-load benchmark. "
            "Use `kisti-db-manager openalex benchmark-load --help` for command options."
        ),
    )
    p_benchmark.add_argument("argv", nargs="*")
    p_benchmark.set_defaults(
        func=_cmd_openalex_benchmark_load,
        forward_unknown_args=True,
        forward_arg_offset=2,
    )
