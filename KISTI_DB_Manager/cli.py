from __future__ import annotations

import importlib
import sys

from ._cli.common import ConfigValidationError, MissingDependencyError, _validate_json_run_config
from ._cli.parser import build_parser
from .runstate import UnsafePathError


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


def _sync_command_facades() -> None:
    from ._cli import json_commands, tabular

    json_commands._ensure_optional_deps = _ensure_optional_deps
    tabular._ensure_optional_deps = _ensure_optional_deps


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args, unknown = parser.parse_known_args(raw_argv)
    if getattr(args, "forward_unknown_args", False):
        args.argv = raw_argv[int(getattr(args, "forward_arg_offset", 0) or 0) :]
    elif unknown:
        parser.error("unrecognized arguments: " + " ".join(str(item) for item in unknown))
    try:
        _sync_command_facades()
        return int(args.func(args))
    except (MissingDependencyError, ConfigValidationError, UnsafePathError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
