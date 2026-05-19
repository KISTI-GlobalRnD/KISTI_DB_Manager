from __future__ import annotations

import argparse

def _cmd_modes(_args: argparse.Namespace) -> int:
    from ..modes import list_modes

    for spec in list_modes():
        print(f"- {spec.name}: {spec.description}")
        if spec.data_overrides:
            for k in sorted(spec.data_overrides):
                print(f"  - {k}: {spec.data_overrides[k]}")
        if spec.stage_defaults:
            sd = spec.stage_defaults
            print(f"  - stages: create={sd.get('create')} load={sd.get('load')} index={sd.get('index')} optimize={sd.get('optimize')}")
    return 0


def register_modes_parser(sub) -> None:
    p_modes = sub.add_parser("modes", help="List built-in run modes/presets")
    p_modes.set_defaults(func=_cmd_modes)
