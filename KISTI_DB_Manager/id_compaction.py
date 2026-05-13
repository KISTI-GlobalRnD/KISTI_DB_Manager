from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


OPENALEX_PREFIXES = ("https://openalex.org/", "http://openalex.org/")
ROR_PREFIXES = ("https://ror.org/", "http://ror.org/")
DOI_PREFIXES = ("https://doi.org/", "http://doi.org/")
ORCID_PREFIXES = ("https://orcid.org/", "http://orcid.org/")


ENTITY_ALIASES = {
    "author": "author",
    "authors": "author",
    "institution": "institution",
    "institutions": "institution",
    "source": "source",
    "sources": "source",
    "work": "work",
    "works": "work",
    "concept": "concept",
    "concepts": "concept",
    "topic": "topic",
    "topics": "topic",
    "topic_share": "topic",
    "primary_topic": "topic",
    "field": "field",
    "fields": "field",
    "subfield": "subfield",
    "subfields": "subfield",
    "domain": "domain",
    "domains": "domain",
    "funder": "funder",
    "funders": "funder",
    "publisher": "publisher",
    "publishers": "publisher",
    "organization": "organization",
    "organisations": "organization",
    "organizations": "organization",
}

OPENALEX_VALUE_COLUMNS = {
    "referenced_works": "referenced_work",
    "related_works": "related_work",
    "corresponding_author_ids": "corresponding_author",
    "corresponding_institution_ids": "corresponding_institution",
    "host_organization": "host_organization",
    "host_organization_lineage": "host_organization_lineage",
}

RAW_METADATA_COLUMNS = {
    "__except_raw_json__",
}

SUPPORTED_PRESETS = {"openalex"}
SUPPORTED_MODES = {"semantic_column_strip"}
SUPPORTED_COLLISION_POLICIES = {"error", "preserve"}
SUPPORTED_NAMESPACE_CONFLICT_POLICIES = {"error", "preserve"}
RULES_VERSION = "openalex-semantic-column-strip-v2"


class IdCompactionError(ValueError):
    """Raised when ID compaction would make the output ambiguous or lossy."""


def _bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def normalize_id_compaction_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    cfg = dict(config or {})
    nested = dict(cfg.get("id_compaction") or {}) if isinstance(cfg.get("id_compaction"), Mapping) else {}

    if "id_compaction_enabled" in cfg and "enabled" not in nested:
        nested["enabled"] = cfg.get("id_compaction_enabled")
    if "id_compaction_preset" in cfg and "preset" not in nested:
        nested["preset"] = cfg.get("id_compaction_preset")
    if "id_compaction_mode" in cfg and "mode" not in nested:
        nested["mode"] = cfg.get("id_compaction_mode")

    nested.setdefault("enabled", False)
    nested.setdefault("preset", "openalex")
    nested.setdefault("mode", "semantic_column_strip")
    nested.setdefault("description_policy", "required")
    nested.setdefault("apply_to_excepted_raw_json", False)
    nested.setdefault("collision_policy", "error")
    nested.setdefault("namespace_conflict_policy", "error")

    nested["enabled"] = _bool(nested.get("enabled"), default=False)
    nested["preset"] = str(nested.get("preset") or "openalex").strip().lower()
    nested["mode"] = str(nested.get("mode") or "semantic_column_strip").strip().lower()
    nested["description_policy"] = str(nested.get("description_policy") or "required").strip().lower()
    nested["apply_to_excepted_raw_json"] = _bool(nested.get("apply_to_excepted_raw_json"), default=False)
    nested["collision_policy"] = str(nested.get("collision_policy") or "error").strip().lower()
    nested["namespace_conflict_policy"] = str(nested.get("namespace_conflict_policy") or "error").strip().lower()
    return nested


def validate_id_compaction_config(config: Mapping[str, Any]) -> None:
    if not bool(config.get("enabled")):
        return
    preset = str(config.get("preset") or "")
    mode = str(config.get("mode") or "")
    collision_policy = str(config.get("collision_policy") or "")
    namespace_conflict_policy = str(config.get("namespace_conflict_policy") or "")
    if preset not in SUPPORTED_PRESETS:
        raise IdCompactionError(f"id_compaction.preset currently supports only: {sorted(SUPPORTED_PRESETS)}")
    if mode not in SUPPORTED_MODES:
        raise IdCompactionError(f"id_compaction.mode currently supports only: {sorted(SUPPORTED_MODES)}")
    if collision_policy not in SUPPORTED_COLLISION_POLICIES:
        raise IdCompactionError(
            f"id_compaction.collision_policy currently supports only: {sorted(SUPPORTED_COLLISION_POLICIES)}"
        )
    if namespace_conflict_policy not in SUPPORTED_NAMESPACE_CONFLICT_POLICIES:
        raise IdCompactionError(
            "id_compaction.namespace_conflict_policy currently supports only: "
            f"{sorted(SUPPORTED_NAMESPACE_CONFLICT_POLICIES)}"
        )


def _rules_hash(config: Mapping[str, Any]) -> str:
    payload = json.dumps(
        {"config": dict(config), "rules_version": RULES_VERSION},
        sort_keys=True,
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _strip_prefix(value: str, prefixes: tuple[str, ...]) -> tuple[str | None, str | None]:
    lower = value.lower()
    for prefix in prefixes:
        if lower.startswith(prefix):
            tail = value[len(prefix) :]
            if tail:
                return prefix, tail
    return None, None


def _namespace_for_value(value: Any) -> tuple[str | None, str | None, str | None]:
    if not isinstance(value, str):
        return None, None, None
    for namespace, prefixes in (
        ("openalex", OPENALEX_PREFIXES),
        ("ror", ROR_PREFIXES),
        ("doi", DOI_PREFIXES),
        ("orcid", ORCID_PREFIXES),
    ):
        prefix, tail = _strip_prefix(value, prefixes)
        if prefix is not None:
            return namespace, prefix, tail
    return None, None, None


def _default_prefix(namespace: str | None) -> str:
    if namespace == "openalex":
        return OPENALEX_PREFIXES[0]
    if namespace == "ror":
        return ROR_PREFIXES[0]
    if namespace == "doi":
        return DOI_PREFIXES[0]
    if namespace == "orcid":
        return ORCID_PREFIXES[0]
    return ""


def _label_for_entity(entity: str | None, namespace: str) -> str:
    if entity:
        return " ".join(part.capitalize() for part in str(entity).split("_"))
    return namespace.capitalize()


def _description(namespace: str, entity: str | None, removed_prefix: str) -> str:
    if namespace == "openalex":
        if not entity:
            return f"OpenAlex ID. Original URL prefix {removed_prefix} removed during JSON parsing."
        label = _label_for_entity(entity, namespace)
        return f"OpenAlex {label} ID. Original URL prefix {removed_prefix} removed during JSON parsing."
    if namespace == "ror":
        return f"ROR ID. Original URL prefix {removed_prefix} removed during JSON parsing."
    if namespace == "doi":
        return f"DOI. Original URL prefix {removed_prefix} removed during JSON parsing."
    if namespace == "orcid":
        return f"ORCID ID. Original URL prefix {removed_prefix} removed during JSON parsing."
    return f"{namespace} ID. Original URL prefix {removed_prefix} removed during JSON parsing."


def _replace_last(parts: list[str], value: str, sep: str) -> str:
    out = list(parts)
    out[-1] = value
    return sep.join(out)


def _openalex_column_name(column: str, *, index_key: str, sep: str) -> tuple[str | None, str | None]:
    col = str(column)
    if col == str(index_key):
        return col, None

    parts = col.split(sep) if sep and sep in col else [col]
    last = parts[-1]

    if last.endswith("_openalex_id") and len(last) > len("_openalex_id"):
        entity = last[: -len("_openalex_id")]
        return col, entity

    if last == "openalex":
        return _replace_last(parts, "openalex_id", sep), None

    if last == "id" and len(parts) >= 2:
        prev = parts[-2]
        entity = ENTITY_ALIASES.get(prev)
        if entity:
            out = list(parts[:-2]) + [f"{entity}_openalex_id"]
            return sep.join(out), entity

    if last in OPENALEX_VALUE_COLUMNS:
        entity = OPENALEX_VALUE_COLUMNS[last]
        return _replace_last(parts, f"{entity}_openalex_id", sep), entity

    if last.endswith("_ids") and len(last) > 4:
        stem = last[:-4]
        entity = ENTITY_ALIASES.get(stem)
        if entity:
            return _replace_last(parts, f"{entity}_openalex_id", sep), entity

    if last.endswith("_id") and len(last) > 3:
        stem = last[:-3]
        entity = ENTITY_ALIASES.get(stem)
        if entity:
            return _replace_last(parts, f"{entity}_openalex_id", sep), entity

    if last in {"host_organization", "host_organization_lineage"}:
        return _replace_last(parts, f"{last}_openalex_id", sep), last

    return None, None


def _column_namespace_mapping(
    column: str,
    *,
    index_key: str,
    sep: str,
) -> tuple[str | None, str | None, str | None]:
    """Return (new_column, expected_namespace, entity) from column semantics alone."""
    col = str(column)
    parts = col.split(sep) if sep and sep in col else [col]
    last = parts[-1]

    new_col, entity = _openalex_column_name(col, index_key=index_key, sep=sep)
    if new_col:
        return new_col, "openalex", entity

    if last == "ror":
        return _replace_last(parts, "ror_id", sep), "ror", "ror"
    if last == "ror_id":
        return col, "ror", "ror"
    if last == "doi":
        return _replace_last(parts, "doi_id", sep), "doi", "doi"
    if last == "doi_id":
        return col, "doi", "doi"
    if last == "orcid":
        return _replace_last(parts, "orcid_id", sep), "orcid", "orcid"
    if last == "orcid_id":
        return col, "orcid", "orcid"

    return None, None, None


def _namespace_column_name(
    column: str,
    *,
    namespace: str,
    index_key: str,
    sep: str,
) -> tuple[str | None, str | None]:
    col = str(column)
    parts = col.split(sep) if sep and sep in col else [col]
    last = parts[-1]

    if namespace == "openalex":
        return _openalex_column_name(col, index_key=index_key, sep=sep)

    if col == str(index_key):
        return None, None

    if namespace in {"ror", "doi", "orcid"}:
        if last == namespace:
            return _replace_last(parts, f"{namespace}_id", sep), namespace
        if last == f"{namespace}_id":
            return col, namespace

    return None, None


@dataclass
class IdCompactor:
    config: dict[str, Any]
    sep: str = "__"
    index_key: str = "id"
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    ambiguous_counts: dict[str, int] = field(default_factory=dict)
    collision_counts: dict[str, int] = field(default_factory=dict)
    namespace_conflict_counts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        data_config: Mapping[str, Any] | None,
        *,
        sep: str = "__",
        index_key: str = "id",
    ) -> "IdCompactor":
        cfg = normalize_id_compaction_config(data_config)
        validate_id_compaction_config(cfg)
        return cls(config=cfg, sep=str(sep or "__"), index_key=str(index_key or "id"))

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("enabled"))

    @property
    def rules_hash(self) -> str:
        return _rules_hash(self.config)

    def compact_row(self, row: Mapping[str, Any] | None, *, table_name: str) -> dict[str, Any] | Mapping[str, Any] | None:
        if not self.enabled or not isinstance(row, Mapping):
            return row
        out: dict[str, Any] = {}
        origins: dict[str, str] = {}
        changed = False

        def is_blank(value: Any) -> bool:
            return value is None or value == ""

        for key, value in row.items():
            key_s = str(key)
            new_key, new_value, meta = self.compact_field(table_name=str(table_name), column=key_s, value=value)
            if meta:
                changed = True
                self._record(meta)
            if new_key != key_s:
                changed = True
            if new_key in out:
                if is_blank(new_value):
                    continue
                if is_blank(out.get(new_key)):
                    out[new_key] = new_value
                    origins[new_key] = key_s
                elif out.get(new_key) != new_value:
                    collision_key = f"{table_name}.{new_key}"
                    self.collision_counts[collision_key] = self.collision_counts.get(collision_key, 0) + 1
                    if str(self.config.get("collision_policy") or "error") == "error":
                        previous_key = origins.get(new_key, new_key)
                        raise IdCompactionError(
                            f"id compaction collision at {collision_key}: "
                            f"{key_s!r} and {previous_key!r} map to existing output column {new_key!r}"
                        )
                    out[key_s] = value
                continue
            out[new_key] = new_value
            origins[new_key] = key_s
        return out if changed else row

    def compact_rows(self, rows: list[dict], *, table_name: str) -> list[dict]:
        if not self.enabled or not rows:
            return rows
        out: list[dict] = []
        append = out.append
        for row in rows:
            compacted = self.compact_row(row, table_name=table_name)
            append(dict(compacted or {}))
        return out

    def compact_field(self, *, table_name: str, column: str, value: Any) -> tuple[str, Any, dict[str, Any] | None]:
        if str(column) in RAW_METADATA_COLUMNS and not bool(self.config.get("apply_to_excepted_raw_json")):
            return str(column), value, None

        if str(self.config.get("preset")) != "openalex":
            return str(column), value, None
        if str(self.config.get("mode")) != "semantic_column_strip":
            return str(column), value, None

        semantic_column, expected_namespace, entity = _column_namespace_mapping(
            str(column),
            index_key=self.index_key,
            sep=self.sep,
        )
        namespace, removed_prefix, tail = _namespace_for_value(value)

        if semantic_column is None or expected_namespace is None:
            if namespace is not None:
                self.ambiguous_counts[f"{table_name}.{column}"] = self.ambiguous_counts.get(f"{table_name}.{column}", 0) + 1
            return str(column), value, None

        if namespace is not None and namespace != expected_namespace:
            conflict_key = f"{table_name}.{column}"
            self.namespace_conflict_counts[conflict_key] = self.namespace_conflict_counts.get(conflict_key, 0) + 1
            if str(self.config.get("namespace_conflict_policy") or "error") == "error":
                raise IdCompactionError(
                    f"id compaction namespace conflict at {conflict_key}: "
                    f"column expects {expected_namespace!r}, value uses {namespace!r}"
                )
            return str(column), value, None

        if namespace is None or removed_prefix is None or tail is None:
            if semantic_column == str(column):
                return str(column), value, None
            removed_prefix_default = _default_prefix(expected_namespace)
            desc = _description(expected_namespace, entity, removed_prefix_default)
            return (
                str(semantic_column),
                value,
                {
                    "table": str(table_name),
                    "original_column": str(column),
                    "new_column": str(semantic_column),
                    "namespace": str(expected_namespace),
                    "entity": entity,
                    "removed_prefix": removed_prefix_default,
                    "description": desc,
                },
            )

        new_column = semantic_column
        if not new_column:
            self.ambiguous_counts[f"{table_name}.{column}"] = self.ambiguous_counts.get(f"{table_name}.{column}", 0) + 1
            return str(column), value, None

        desc = _description(expected_namespace, entity, removed_prefix)
        return (
            str(new_column),
            tail,
            {
                "table": str(table_name),
                "original_column": str(column),
                "new_column": str(new_column),
                "namespace": str(expected_namespace),
                "entity": entity,
                "removed_prefix": str(removed_prefix),
                "description": desc,
            },
        )

    def _record(self, meta: Mapping[str, Any]) -> None:
        table = str(meta.get("table") or "")
        new_column = str(meta.get("new_column") or "")
        original_column = str(meta.get("original_column") or "")
        removed_prefix = str(meta.get("removed_prefix") or "")
        key = f"{table}.{new_column}"
        self.counts[key] = int(self.counts.get(key, 0)) + 1
        entry_key = f"{table}\0{original_column}\0{new_column}\0{removed_prefix}"
        if entry_key not in self.columns:
            self.columns[entry_key] = dict(meta)
        self.columns[entry_key]["count"] = int(self.columns[entry_key].get("count", 0) or 0) + 1

    def merge_summary(self, summary: Mapping[str, Any] | None) -> None:
        if not isinstance(summary, Mapping):
            return
        for entry in summary.get("columns") or []:
            if not isinstance(entry, Mapping):
                continue
            count = int(entry.get("count", 0) or 0)
            if count <= 0:
                continue
            entry_key = (
                f"{entry.get('table')}\0{entry.get('original_column')}\0"
                f"{entry.get('new_column')}\0{entry.get('removed_prefix')}"
            )
            if entry_key not in self.columns:
                self.columns[entry_key] = dict(entry)
                self.columns[entry_key]["count"] = 0
            self.columns[entry_key]["count"] = int(self.columns[entry_key].get("count", 0) or 0) + count
        for key, count in (summary.get("counts") or {}).items():
            self.counts[str(key)] = int(self.counts.get(str(key), 0)) + int(count or 0)
        for key, count in (summary.get("ambiguous_columns") or {}).items():
            self.ambiguous_counts[str(key)] = int(self.ambiguous_counts.get(str(key), 0)) + int(count or 0)
        for key, count in (summary.get("collisions") or {}).items():
            self.collision_counts[str(key)] = int(self.collision_counts.get(str(key), 0)) + int(count or 0)
        for key, count in (summary.get("namespace_conflicts") or {}).items():
            self.namespace_conflict_counts[str(key)] = int(self.namespace_conflict_counts.get(str(key), 0)) + int(count or 0)

    def summary(self) -> dict[str, Any]:
        columns = sorted(
            (dict(v) for v in self.columns.values()),
            key=lambda x: (str(x.get("table")), str(x.get("new_column")), str(x.get("original_column"))),
        )
        return {
            "enabled": bool(self.enabled),
            "preset": str(self.config.get("preset") or "openalex"),
            "mode": str(self.config.get("mode") or "semantic_column_strip"),
            "description_policy": str(self.config.get("description_policy") or "required"),
            "apply_to_excepted_raw_json": bool(self.config.get("apply_to_excepted_raw_json")),
            "rules_version": RULES_VERSION,
            "rules_hash": self.rules_hash,
            "columns": columns,
            "counts": dict(sorted(self.counts.items())),
            "ambiguous_columns": dict(sorted(self.ambiguous_counts.items())),
            "collisions": dict(sorted(self.collision_counts.items())),
            "namespace_conflicts": dict(sorted(self.namespace_conflict_counts.items())),
        }

    def column_descriptions(self, table_name: str) -> dict[str, str]:
        if not self.enabled:
            return {}
        out: dict[str, str] = {}
        for entry in self.columns.values():
            if str(entry.get("table")) != str(table_name):
                continue
            col = str(entry.get("new_column") or "")
            desc = str(entry.get("description") or "")
            if col and desc:
                out[col] = desc
        return out

    def schema_manifest(
        self,
        *,
        name_maps: Mapping[str, Any] | None = None,
        table_columns: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        tables: dict[str, dict[str, Any]] = {}

        def sql_column_for(table: str, column: str) -> str:
            sql_col = column
            nm = name_maps.get(table) if isinstance(name_maps, Mapping) else None
            try:
                sql_col = nm.map_column(column)
                if sql_col == column and "." in column:
                    sql_col = nm.map_column(column.replace(".", getattr(nm, "key_sep", "__")))
            except Exception:
                sql_col = column
            return str(sql_col)

        if isinstance(table_columns, Mapping):
            for table_raw, columns_raw in table_columns.items():
                table = str(table_raw or "")
                if not table:
                    continue
                t = tables.setdefault(table, {"columns": {}})
                seen: set[str] = set()
                for column_raw in list(columns_raw or []):
                    col = str(column_raw or "")
                    if not col or col in seen:
                        continue
                    seen.add(col)
                    t["columns"].setdefault(
                        col,
                        {
                            "sql_column": sql_column_for(table, col),
                            "source_column": "",
                            "description": "",
                        },
                    )

        for entry in self.columns.values():
            table = str(entry.get("table") or "")
            col = str(entry.get("new_column") or "")
            if not table or not col:
                continue
            t = tables.setdefault(table, {"columns": {}})
            t["columns"][col] = {
                "sql_column": sql_column_for(table, col),
                "source_column": str(entry.get("original_column") or ""),
                "id_namespace": str(entry.get("namespace") or ""),
                "id_entity": entry.get("entity"),
                "removed_prefix": str(entry.get("removed_prefix") or ""),
                "description": str(entry.get("description") or ""),
                "count": int(entry.get("count", 0) or 0),
            }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "id_compaction": self.summary(),
            "tables": tables,
        }
