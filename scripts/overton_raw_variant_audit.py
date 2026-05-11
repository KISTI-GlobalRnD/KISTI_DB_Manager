#!/usr/bin/env python3
"""
Classify why the same Overton policy_document_id appears multiple times in the raw dump.

Approach:
- Use the parsed MAIN parquet to identify duplicated `policy_document_id` groups.
- Stream the raw Overton dump (JSON Lines inside tar.gz).
- For duplicated ids only, compare top-level field values across variants.
- Aggregate repeated difference signatures and save representative raw samples.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tarfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


PID_RE = re.compile(rb'"policy_document_id"\s*:\s*"([^"]+)"')

LIST_LIKE_KEYS = {
    "authors",
    "topics",
    "source_tags",
    "sdgcategories",
    "classifications",
    "entities",
    "policy_source_region",
    "policy_source_country",
    "policy_source_type",
    "policy_document_ids_cited",
    "source_sector",
    "source_type",
    "source_function",
    "dois_cited",
    "self_identifiers",
    "cited_policy_document_dois",
    "mentions_people",
    "policy_source_country_iso_codes",
    "ref_contexts",
}

PDF_VARIANT_KEYS = {"pdf_url", "pdf_thumbnail", "pdf_document_id"}
LLM_KEYS = {"llm_description", "llm_theme"}
LANGUAGE_KEYS = {"language"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(message: str) -> None:
    print(f"{_utc_now()} {message}", flush=True)


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _canonical_bytes(value: Any) -> bytes:
    try:
        import orjson  # type: ignore

        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
    except Exception:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha1(_canonical_bytes(value)).hexdigest()


def _preview_scalar(value: Any, *, max_text_len: int = 200) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if len(value) > max_text_len:
            return value[:max_text_len] + "...<truncated>"
        return value
    return str(value)


def _preview_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return _preview_scalar(value)
    if isinstance(value, list):
        head = []
        for item in value[:3]:
            if isinstance(item, dict):
                head.append({"type": "dict", "keys": sorted(str(k) for k in list(item.keys())[:6])})
            elif isinstance(item, list):
                head.append({"type": "list", "len": len(item)})
            else:
                head.append(_preview_scalar(item, max_text_len=120))
        return {"type": "list", "len": len(value), "head": head}
    if isinstance(value, dict):
        return {"type": "dict", "len": len(value), "keys": sorted(str(k) for k in list(value.keys())[:8])}
    return {"type": type(value).__name__, "repr": _preview_scalar(value, max_text_len=120)}


def _extract_pid(raw_line: bytes) -> str | None:
    m = PID_RE.search(raw_line)
    if not m:
        return None
    return m.group(1).decode("utf-8", errors="replace")


def _load_json(raw_line: bytes) -> dict[str, Any]:
    try:
        import orjson  # type: ignore

        return orjson.loads(raw_line)
    except Exception:
        return json.loads(raw_line.decode("utf-8"))


def _load_duplicate_id_counts(docs_parquet: Path) -> dict[str, int]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT policy_document_id, COUNT(*) AS variants
            FROM read_parquet(?)
            GROUP BY 1
            HAVING COUNT(*) > 1
            """,
            [str(docs_parquet)],
        ).fetchall()
        return {str(pid): int(variants) for pid, variants in rows}
    finally:
        con.close()


def _classify_pattern(varying_keys: set[str]) -> str:
    if not varying_keys:
        return "no_difference"
    if varying_keys & LIST_LIKE_KEYS:
        if varying_keys <= (LIST_LIKE_KEYS | PDF_VARIANT_KEYS | LLM_KEYS | LANGUAGE_KEYS):
            return "content_variant"
        return "content_plus_metadata_variant"
    if varying_keys <= (PDF_VARIANT_KEYS | LLM_KEYS):
        return "pdf_llm_variant"
    if varying_keys <= (PDF_VARIANT_KEYS | LLM_KEYS | LANGUAGE_KEYS):
        return "multilingual_or_pdf_variant"
    if varying_keys <= LLM_KEYS:
        return "llm_only_variant"
    return "metadata_variant"


def _safe_slug(text: str, *, limit: int = 80) -> str:
    chars = []
    for ch in text.lower():
        if ch.isalnum():
            chars.append(ch)
        elif ch in {"_", "-"}:
            chars.append(ch)
        else:
            chars.append("-")
    slug = "".join(chars).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug[:limit] or "pattern"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs-parquet", required=True, help="Path to Overton MAIN parquet")
    ap.add_argument("--dump-path", required=True, help="Path to Overton dump_YYYYMMDD.tar.gz")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--top-patterns", type=int, default=12, help="How many top patterns to keep sample groups for")
    ap.add_argument("--samples-per-pattern", type=int, default=2, help="How many sample duplicate ids to keep per pattern")
    args = ap.parse_args()

    docs_parquet = Path(args.docs_parquet).expanduser().resolve()
    dump_path = Path(args.dump_path).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not docs_parquet.exists():
        raise SystemExit(f"docs parquet not found: {docs_parquet}")
    if not dump_path.exists():
        raise SystemExit(f"dump path not found: {dump_path}")

    per_pid_path = out_dir / "per_pid.jsonl"
    pattern_path = out_dir / "patterns.jsonl"
    summary_path = out_dir / "summary.json"
    samples_dir = out_dir / "sample_groups"
    state_path = out_dir / "state.json"

    for p in [per_pid_path, pattern_path]:
        if p.exists():
            p.unlink()

    _log(f"[audit] loading duplicate ids from docs parquet={docs_parquet}")
    duplicate_id_counts = _load_duplicate_id_counts(docs_parquet)
    duplicate_ids = set(duplicate_id_counts)
    _log(f"[audit] duplicate_groups={len(duplicate_ids)} duplicate_extra_rows={sum(v - 1 for v in duplicate_id_counts.values())}")

    groups: dict[str, dict[str, Any]] = {}
    matched_raw_rows = 0
    scanned_rows = 0
    duplicate_groups_seen = 0

    with tarfile.open(dump_path, "r:gz") as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".json"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            for line_no, raw in enumerate(f, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                scanned_rows += 1
                pid = _extract_pid(raw)
                if pid is None or pid not in duplicate_ids:
                    continue
                rec = _load_json(raw)
                if rec.get("policy_document_id") != pid:
                    continue
                matched_raw_rows += 1
                group = groups.get(pid)
                if group is None:
                    group = {
                        "pid": pid,
                        "variant_count_raw": 0,
                        "source_members": set(),
                        "source_lines": [],
                        "key_digests": defaultdict(set),
                        "key_presence": Counter(),
                    }
                    groups[pid] = group
                    duplicate_groups_seen += 1

                group["variant_count_raw"] += 1
                group["source_members"].add(member.name)
                if len(group["source_lines"]) < 8:
                    group["source_lines"].append({"source_member": member.name, "source_line_no": int(line_no)})

                for key, value in rec.items():
                    group["key_presence"][key] += 1
                    group["key_digests"][key].add(_digest(value))

                if scanned_rows % 500000 == 0:
                    _write_json(
                        state_path,
                        {
                            "dump_path": str(dump_path),
                            "scanned_rows": scanned_rows,
                            "matched_raw_rows": matched_raw_rows,
                            "duplicate_groups_seen": duplicate_groups_seen,
                            "updated_at_utc": _utc_now(),
                        },
                    )
                    _log(f"[audit] scanned_rows={scanned_rows} matched_raw_rows={matched_raw_rows} groups_seen={duplicate_groups_seen}")

    _log(f"[audit] raw scan done scanned_rows={scanned_rows} matched_raw_rows={matched_raw_rows} groups_seen={duplicate_groups_seen}")

    pattern_stats: dict[tuple[int, tuple[str, ...]], dict[str, Any]] = {}
    variant_size_counter: Counter[int] = Counter()
    category_counter: Counter[str] = Counter()
    missing_in_raw = sorted(pid for pid in duplicate_ids if pid not in groups)

    for pid, group in groups.items():
        variant_count_raw = int(group["variant_count_raw"])
        variant_size_counter[variant_count_raw] += 1
        varying_keys: list[str] = []
        all_keys = set(group["key_presence"]) | set(group["key_digests"])
        for key in sorted(all_keys):
            presence = int(group["key_presence"].get(key, 0))
            distinct_values = len(group["key_digests"].get(key, set()))
            if presence < variant_count_raw or distinct_values > 1:
                if key != "policy_document_id":
                    varying_keys.append(str(key))

        varying_key_set = set(varying_keys)
        category = _classify_pattern(varying_key_set)
        category_counter[category] += 1
        pattern_key = (variant_count_raw, tuple(varying_keys))
        pstat = pattern_stats.get(pattern_key)
        if pstat is None:
            pstat = {
                "variant_count": variant_count_raw,
                "varying_keys": list(varying_keys),
                "category": category,
                "group_count": 0,
                "sample_pids": [],
            }
            pattern_stats[pattern_key] = pstat
        pstat["group_count"] += 1
        if len(pstat["sample_pids"]) < int(args.samples_per_pattern):
            pstat["sample_pids"].append(pid)

        payload = {
            "policy_document_id": pid,
            "variant_count_docs": int(duplicate_id_counts.get(pid, 0)),
            "variant_count_raw": variant_count_raw,
            "varying_keys": list(varying_keys),
            "category": category,
            "source_members": sorted(str(x) for x in group["source_members"]),
            "source_lines": list(group["source_lines"]),
        }
        _append_jsonl(per_pid_path, payload)

    sorted_patterns = sorted(
        pattern_stats.values(),
        key=lambda item: (-int(item["group_count"]), -int(item["variant_count"]), item["category"], item["varying_keys"]),
    )
    for item in sorted_patterns:
        _append_jsonl(pattern_path, item)

    top_patterns = sorted_patterns[: max(1, int(args.top_patterns))]
    sample_pid_to_pattern: dict[str, dict[str, Any]] = {}
    for idx, pattern in enumerate(top_patterns, start=1):
        for pid in pattern["sample_pids"]:
            sample_pid_to_pattern[pid] = {"pattern_rank": idx, **pattern}

    sample_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if sample_pid_to_pattern:
        with tarfile.open(dump_path, "r:gz") as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith(".json"):
                    continue
                f = tar.extractfile(member)
                if f is None:
                    continue
                for line_no, raw in enumerate(f, start=1):
                    raw = raw.strip()
                    if not raw:
                        continue
                    pid = _extract_pid(raw)
                    if pid is None or pid not in sample_pid_to_pattern:
                        continue
                    rec = _load_json(raw)
                    if rec.get("policy_document_id") != pid:
                        continue
                    sample_groups[pid].append(
                        {
                            "source_member": member.name,
                            "source_line_no": int(line_no),
                            "record": rec,
                        }
                    )

    samples_dir.mkdir(parents=True, exist_ok=True)
    written_samples: list[dict[str, Any]] = []
    for pid, records in sample_groups.items():
        pattern = sample_pid_to_pattern[pid]
        rank = int(pattern["pattern_rank"])
        category = str(pattern["category"])
        varying_keys = list(pattern["varying_keys"])
        out_name = f"{rank:02d}__{category}__{_safe_slug(pid)}.json"
        out_path = samples_dir / out_name
        payload = {
            "policy_document_id": pid,
            "pattern_rank": rank,
            "category": category,
            "variant_count": int(pattern["variant_count"]),
            "group_count_for_pattern": int(pattern["group_count"]),
            "varying_keys": varying_keys,
            "records": records,
        }
        _write_json(out_path, payload)
        written_samples.append(
            {
                "policy_document_id": pid,
                "pattern_rank": rank,
                "category": category,
                "path": str(out_path),
            }
        )

    summary = {
        "docs_parquet": str(docs_parquet),
        "dump_path": str(dump_path),
        "saved_at_utc": _utc_now(),
        "scanned_rows": scanned_rows,
        "duplicate_groups_from_docs": len(duplicate_id_counts),
        "duplicate_extra_rows_from_docs": int(sum(v - 1 for v in duplicate_id_counts.values())),
        "matched_duplicate_rows_in_raw": matched_raw_rows,
        "duplicate_groups_seen_in_raw": duplicate_groups_seen,
        "missing_duplicate_groups_in_raw": len(missing_in_raw),
        "variant_count_distribution": {str(k): int(v) for k, v in sorted(variant_size_counter.items())},
        "category_distribution": {str(k): int(v) for k, v in sorted(category_counter.items())},
        "top_patterns": [
            {
                "rank": idx,
                "category": item["category"],
                "variant_count": int(item["variant_count"]),
                "group_count": int(item["group_count"]),
                "varying_keys": list(item["varying_keys"]),
                "sample_pids": list(item["sample_pids"]),
            }
            for idx, item in enumerate(top_patterns, start=1)
        ],
        "sample_group_files": written_samples,
    }
    _write_json(summary_path, summary)
    _write_json(
        state_path,
        {
            "dump_path": str(dump_path),
            "scanned_rows": scanned_rows,
            "matched_raw_rows": matched_raw_rows,
            "duplicate_groups_seen": duplicate_groups_seen,
            "completed": True,
            "updated_at_utc": _utc_now(),
        },
    )
    _log(f"[audit] completed summary={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
