#!/usr/bin/env python3
"""
Rename Overton raw-schema tables to short canonical names.

Why this exists:
- The raw Overton loader currently embeds snapshot/version strings into table names.
- The resulting names are tedious to type and too close to MariaDB's 64-char limit.
- We want short, stable table names for direct SQL use while keeping backward compatibility.

Behavior:
- Rename base tables in a target schema from long generated names to short canonical names.
- Optionally create compatibility views using the original long names.
- Supports dry-run by default.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Iterable

import pymysql


SUFFIX_TO_SHORT_NAME: list[tuple[str, str]] = [
    ("__main", "docs"),
    ("__sub__authors", "authors"),
    ("__sub__topics", "topics"),
    ("__sub__source_tags", "src_tags"),
    ("__sub__sdgcategories", "sdg_cats"),
    ("__sub__classifications", "classifications"),
    ("__sub__entities", "entities"),
    ("__sub__policy_source_region", "policy_src_region"),
    ("__sub__policy_source_country", "policy_src_country"),
    ("__sub__policy_source_type", "policy_src_type"),
    ("__sub__policy_document_ids_cited", "policy_doc_ids_cited"),
    ("__sub__dois_cited", "cited_dois"),
    ("__sub__self_identifiers", "self_ids"),
    ("__sub__mentions_people", "mentions_people"),
    ("__sub__policy_source_country_iso_codes", "policy_src_country_iso"),
    ("__sub__ref_contexts", "ref_ctx"),
    ("__sub__cited_policy_document_dois", "cited_policy_dois"),
    ("__sub__source_function", "src_function"),
    ("__sub__source_sector", "src_sector"),
    ("__sub__source_type", "src_type"),
]

CANONICAL_NAMES = {short for _, short in SUFFIX_TO_SHORT_NAME}


@dataclass(frozen=True)
class RenameAction:
    source_name: str
    target_name: str


def _qi(name: str) -> str:
    return str(name).replace("`", "``")


def _connect(args: argparse.Namespace):
    return pymysql.connect(
        host=args.host,
        port=int(args.port),
        user=args.user,
        password=args.password,
        database=args.schema,
        autocommit=True,
        charset="utf8mb4",
    )


def _load_base_tables(cur, *, schema: str) -> list[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema=%s AND table_type='BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _load_views(cur, *, schema: str) -> set[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema=%s
        """,
        (schema,),
    )
    return {str(row[0]) for row in cur.fetchall()}


def _canonical_name_for(table_name: str) -> str | None:
    if table_name in CANONICAL_NAMES:
        return table_name
    hits = [short for suffix, short in SUFFIX_TO_SHORT_NAME if table_name.endswith(suffix)]
    if len(hits) == 1:
        return hits[0]
    return None


def _build_actions(base_tables: Iterable[str]) -> tuple[list[RenameAction], list[str], list[str]]:
    actions: list[RenameAction] = []
    unknown_tables: list[str] = []
    already_short: list[str] = []
    for table_name in base_tables:
        target = _canonical_name_for(table_name)
        if target is None:
            unknown_tables.append(table_name)
            continue
        if table_name == target:
            already_short.append(table_name)
            continue
        actions.append(RenameAction(source_name=table_name, target_name=target))
    return actions, unknown_tables, already_short


def _validate_actions(actions: list[RenameAction], *, base_tables: set[str], views: set[str]) -> None:
    targets = [action.target_name for action in actions]
    if len(targets) != len(set(targets)):
        raise ValueError(f"target name collision detected: {targets}")
    for action in actions:
        if action.target_name in base_tables:
            raise ValueError(
                f"target base table already exists: {action.target_name} (source={action.source_name})"
            )
        if action.target_name in views:
            raise ValueError(
                f"target view already exists: {action.target_name} (source={action.source_name})"
            )


def _create_compat_view(cur, *, source_name: str, target_name: str) -> None:
    cur.execute(
        f"CREATE OR REPLACE VIEW `{_qi(source_name)}` AS SELECT * FROM `{_qi(target_name)}`"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True, help="Target Overton schema")
    ap.add_argument("--host", default=os.environ.get("MARIADB_BIND_IP", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MARIADB_BIND_PORT", "3306")))
    ap.add_argument("--user", default=os.environ.get("MARIADB_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("MARIADB_PASSWORD", ""))
    ap.add_argument("--create-views", action="store_true", help="Create compatibility views on old long names")
    ap.add_argument("--apply", action="store_true", help="Apply rename; default is dry-run")
    args = ap.parse_args()

    if not args.password:
        raise SystemExit("password is required via --password or environment")

    conn = _connect(args)
    cur = conn.cursor()
    try:
        base_tables = _load_base_tables(cur, schema=args.schema)
        views = _load_views(cur, schema=args.schema)
        actions, unknown_tables, already_short = _build_actions(base_tables)

        _validate_actions(actions, base_tables=set(base_tables), views=views)

        print(f"schema={args.schema}")
        print(f"base_tables={len(base_tables)}")
        print(f"actions={len(actions)}")
        print(f"already_short={len(already_short)}")
        print(f"unknown_tables={len(unknown_tables)}")

        for action in actions:
            print(f"rename {action.source_name} -> {action.target_name}")
        for table_name in unknown_tables:
            print(f"unknown {table_name}")

        if not args.apply:
            print("mode=dry_run")
            return 0

        for action in actions:
            cur.execute(
                f"RENAME TABLE `{_qi(action.source_name)}` TO `{_qi(action.target_name)}`"
            )
            if args.create_views:
                _create_compat_view(
                    cur,
                    source_name=action.source_name,
                    target_name=action.target_name,
                )
        print("mode=applied")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
