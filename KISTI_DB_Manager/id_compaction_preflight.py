from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from .config import coerce_data_config
from .id_compaction import IdCompactor, normalize_id_compaction_config, validate_id_compaction_config


def _count_delta(before: Mapping[str, int], after: Mapping[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, value in after.items():
        delta = int(value or 0) - int(before.get(key, 0) or 0)
        if delta > 0:
            out[str(key)] = int(delta)
    return out


def _record_examples(
    examples: dict[str, dict[str, list[dict[str, Any]]]],
    category: str,
    deltas: Mapping[str, int],
    *,
    context: Mapping[str, Any] | None,
    record_index: int,
    max_examples_per_key: int,
) -> None:
    if not deltas:
        return
    by_key = examples.setdefault(str(category), {})
    ctx = dict(context or {})
    item = {
        "record_index": int(record_index),
        "source_path": ctx.get("source_path"),
        "source_member": ctx.get("source_member"),
        "line_no": ctx.get("line_no"),
    }
    for key, count in deltas.items():
        entries = by_key.setdefault(str(key), [])
        if len(entries) >= int(max_examples_per_key):
            continue
        ex = dict(item)
        ex["count_delta"] = int(count)
        entries.append(ex)


def _preflight_compactor(
    data_config: Mapping[str, Any],
    *,
    sep: str,
    index_key: str,
    force_enable: bool,
) -> tuple[IdCompactor, dict[str, Any]]:
    run_cfg = normalize_id_compaction_config(data_config)
    if force_enable and not bool(run_cfg.get("enabled")):
        run_cfg["enabled"] = True
    validate_id_compaction_config(run_cfg)

    scan_cfg = dict(run_cfg)
    # Preflight should collect all findings instead of stopping at the first blocking row.
    scan_cfg["collision_policy"] = "preserve"
    scan_cfg["namespace_conflict_policy"] = "preserve"
    return IdCompactor.from_config({"id_compaction": scan_cfg}, sep=sep, index_key=index_key), run_cfg


def run_id_compaction_preflight(
    data_config: Mapping[str, Any],
    *,
    index_key: str | None = None,
    except_keys: list[str] | None = None,
    max_records: int | None = 10000,
    max_examples_per_key: int = 3,
    force_enable: bool = True,
) -> dict[str, Any]:
    """
    Scan JSON records before a long run and report ID compaction hazards.

    The scanner uses preserve policies internally so it can collect all collisions and
    namespace conflicts in the scan window. The returned report still includes the
    policy configured for the real run.
    """
    from .pipeline import _auto_detect_except_keys, _iter_json_records
    from .processing import extract_rows_from_jsons

    dc = coerce_data_config(data_config)
    key_sep = str(dc.get("KEY_SEP") or "__")
    base_table = str(dc.get("table_name") or "").strip() or "base"
    index_key_s = str(index_key or dc.get("index_key") or dc.get("KEY") or "id")

    existing_except_keys = [str(k).strip() for k in (dc.get("except_keys") or []) if str(k).strip()]
    for key in except_keys or []:
        key_s = str(key).strip()
        if key_s and key_s not in existing_except_keys:
            existing_except_keys.append(key_s)

    auto_except_meta = None
    effective_except_keys = list(existing_except_keys)
    if bool(dc.get("auto_except", False)):
        effective_except_keys, auto_except_meta = _auto_detect_except_keys(dc, existing_except_keys=existing_except_keys)

    scan_limit = None
    if max_records is not None:
        try:
            scan_limit_i = int(max_records)
        except Exception:
            scan_limit_i = 10000
        scan_limit = scan_limit_i if scan_limit_i > 0 else None

    try:
        max_examples_i = int(max_examples_per_key)
    except Exception:
        max_examples_i = 3
    if max_examples_i < 0:
        max_examples_i = 0

    compactor, run_cfg = _preflight_compactor(dc, sep=key_sep, index_key=index_key_s, force_enable=force_enable)
    if not compactor.enabled:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "disabled",
            "input": {
                "base_table": base_table,
                "index_key": index_key_s,
                "key_sep": key_sep,
                "max_records": scan_limit,
                "records_scanned": 0,
            },
            "policies": {
                "run": {
                    "collision_policy": str(run_cfg.get("collision_policy") or "error"),
                    "namespace_conflict_policy": str(run_cfg.get("namespace_conflict_policy") or "error"),
                },
                "preflight_scan": None,
            },
            "except_keys": {
                "configured": list(existing_except_keys),
                "effective": list(effective_except_keys),
                "auto_except": auto_except_meta,
            },
            "id_compaction": compactor.summary(),
            "issues": {
                "collisions": {},
                "namespace_conflicts": {},
                "ambiguous_columns": {},
                "scan_errors": [],
            },
            "examples": {},
            "recommendations": ["ID compaction is disabled; no preflight scan was performed."],
        }
    examples: dict[str, dict[str, list[dict[str, Any]]]] = {}
    errors: list[dict[str, Any]] = []
    records_scanned = 0

    for record_index, out in enumerate(_iter_json_records(dc, max_records=scan_limit, with_context=True)):
        record, context = out
        before_ambiguous = dict(compactor.ambiguous_counts)
        before_collisions = dict(compactor.collision_counts)
        before_namespace = dict(compactor.namespace_conflict_counts)

        try:
            extract_rows_from_jsons(
                [record],
                index_key=index_key_s,
                base_table=base_table,
                except_keys=effective_except_keys,
                sep=key_sep,
                id_compactor=compactor,
                index_offset=int(record_index),
                record_contexts=[context],
                parallel_workers=0,
            )
        except Exception as exc:
            errors.append(
                {
                    "record_index": int(record_index),
                    "source_path": (context or {}).get("source_path") if isinstance(context, Mapping) else None,
                    "source_member": (context or {}).get("source_member") if isinstance(context, Mapping) else None,
                    "line_no": (context or {}).get("line_no") if isinstance(context, Mapping) else None,
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            )
            continue

        records_scanned += 1
        _record_examples(
            examples,
            "ambiguous_columns",
            _count_delta(before_ambiguous, compactor.ambiguous_counts),
            context=context,
            record_index=record_index,
            max_examples_per_key=max_examples_i,
        )
        _record_examples(
            examples,
            "collisions",
            _count_delta(before_collisions, compactor.collision_counts),
            context=context,
            record_index=record_index,
            max_examples_per_key=max_examples_i,
        )
        _record_examples(
            examples,
            "namespace_conflicts",
            _count_delta(before_namespace, compactor.namespace_conflict_counts),
            context=context,
            record_index=record_index,
            max_examples_per_key=max_examples_i,
        )

    summary = compactor.summary()
    collisions = summary.get("collisions") or {}
    namespace_conflicts = summary.get("namespace_conflicts") or {}
    ambiguous = summary.get("ambiguous_columns") or {}

    status = "passed"
    if collisions or namespace_conflicts or errors:
        status = "failed"
    elif ambiguous:
        status = "warning"

    recommendations: list[str] = []
    if collisions:
        recommendations.append(
            "Resolve compacted-column collisions before a production run, or use collision_policy=preserve for a review-only run."
        )
    if namespace_conflicts:
        recommendations.append(
            "Check semantic ID columns whose URL namespace does not match the inferred column namespace."
        )
    if ambiguous:
        recommendations.append(
            "Review ambiguous URL-like columns; add explicit semantic naming rules only when the column meaning is clear."
        )
    if not recommendations:
        recommendations.append("No blocking ID compaction issues found in the scanned records.")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "input": {
            "base_table": base_table,
            "index_key": index_key_s,
            "key_sep": key_sep,
            "max_records": scan_limit,
            "records_scanned": int(records_scanned),
        },
        "policies": {
            "run": {
                "collision_policy": str(run_cfg.get("collision_policy") or "error"),
                "namespace_conflict_policy": str(run_cfg.get("namespace_conflict_policy") or "error"),
            },
            "preflight_scan": {
                "collision_policy": "preserve",
                "namespace_conflict_policy": "preserve",
            },
        },
        "except_keys": {
            "configured": list(existing_except_keys),
            "effective": list(effective_except_keys),
            "auto_except": auto_except_meta,
        },
        "id_compaction": summary,
        "issues": {
            "collisions": dict(collisions),
            "namespace_conflicts": dict(namespace_conflicts),
            "ambiguous_columns": dict(ambiguous),
            "scan_errors": list(errors),
        },
        "examples": examples,
        "recommendations": recommendations,
    }
