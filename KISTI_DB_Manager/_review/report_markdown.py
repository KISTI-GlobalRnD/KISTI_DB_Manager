from __future__ import annotations

from typing import Any, Mapping

from .core import TableInfo


def _render_markdown(
    *,
    generated_at: str,
    base_table: str,
    base_table_sql: str,
    key_sep: str,
    formats: set[str],
    config_path: str | None,
    report_path: str | None,
    db_enabled: bool,
    exact_counts: bool,
    db_config_masked: Mapping[str, Any] | None,
    db_error: str | None,
    mermaid_text: str | None,
    table_infos: list[TableInfo],
    issues: list[dict[str, Any]] | None,
) -> str:
    total_tables = len(table_infos)
    total_rows_known = 0
    any_estimated = False
    for ti in table_infos:
        if ti.row_count is not None:
            total_rows_known += int(ti.row_count)
        elif ti.table_rows_estimate is not None:
            any_estimated = True

    lines: list[str] = []
    lines.append("# KISTI DB Review Pack")
    lines.append("")
    lines.append(f"- generated_at: `{generated_at}`")
    if config_path:
        lines.append(f"- config: `{config_path}`")
    if report_path:
        lines.append(f"- report: `{report_path}`")
    lines.append(f"- base_table: `{base_table}`")
    lines.append(f"- base_table_sql: `{base_table_sql}`")
    lines.append(f"- key_sep: `{key_sep}`")
    if db_enabled and db_config_masked:
        lines.append(f"- db: `{db_config_masked.get('host')}:{db_config_masked.get('port')}/{db_config_masked.get('database')}`")
    lines.append(f"- db_introspection: `{'on' if db_enabled else 'off'}`")
    if db_error:
        lines.append(f"- db_error: `{db_error}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- tables: `{total_tables}`")
    if exact_counts:
        lines.append("- row_count: **exact** (`COUNT(*)`)")
    else:
        lines.append("- row_count: **estimated** (`information_schema.tables.table_rows`) by default")
    if any_estimated and not exact_counts:
        lines.append("- note: values prefixed with `~` are estimates")
    if issues:
        lines.append(f"- run_issues: `{len(issues)}` (from report)")
    lines.append("")

    if "svg" in formats:
        lines.append("## Diagram")
        lines.append("")
        lines.append("- `schema.svg`")
        if "png" in formats:
            lines.append("- `schema.png` (best-effort; requires `cairosvg`)")
        lines.append("")
        lines.append("![](schema.svg)")
        lines.append("")

    if mermaid_text and (("mmd" in formats) or ("mermaid" in formats)):
        lines.append("## Mermaid")
        lines.append("")
        lines.append("```mermaid")
        lines.append(mermaid_text.rstrip())
        lines.append("```")
        lines.append("")

    lines.append("## Tables")
    lines.append("")
    lines.append("| Table | rows | cols | size | id_index |")
    lines.append("|---|---:|---:|---:|:---:|")
    for ti in table_infos:
        cols_n = len(ti.columns or []) if ti.columns is not None else 0
        size = None
        if ti.data_length is not None or ti.index_length is not None:
            size = int((ti.data_length or 0) + (ti.index_length or 0))
        idx_on_id = False
        if ti.indexes:
            idx_on_id = any(str(ix.get("column_name")).lower() == "id" for ix in ti.indexes)
        lines.append(
            f"| `{ti.name_sql}` | {ti.rows_label()} | {cols_n if cols_n else 'n/a'} | {size if size is not None else 'n/a'} | {'Y' if idx_on_id else ''} |"
        )
    lines.append("")

    lines.append("## Join Tips")
    lines.append("")
    lines.append("All sub-tables (if any) are typically joinable via the `id` column:")
    lines.append("")
    lines.append("```sql")
    lines.append(f"SELECT b.*, s.*")
    lines.append(f"FROM `{base_table_sql}` b")
    lines.append(f"LEFT JOIN `{base_table_sql}{key_sep}<sub>` s ON b.id = s.id")
    lines.append("LIMIT 5;")
    lines.append("```")
    lines.append("")

    if issues:
        lines.append("## Issues (from run report)")
        lines.append("")
        for it in issues[:30]:
            stage = it.get("stage")
            lvl = it.get("level")
            msg = it.get("message")
            lines.append(f"- `{lvl}` `{stage}`: {msg}")
        if len(issues) > 30:
            lines.append(f"- ... +{len(issues) - 30} more")
        lines.append("")

    return "\n".join(lines)


def _render_plan_markdown(
    *,
    generated_at: str,
    config_path: str,
    base_table: str,
    base_table_sql: str,
    key_sep: str,
    file_name: str,
    file_type: str,
    max_records: int | None,
    stats: Mapping[str, Any],
    timings_ms: Mapping[str, Any] | None,
    artifacts: Mapping[str, Any] | None,
    issues: list[dict[str, Any]] | None,
    table_infos: list[TableInfo],
    formats: set[str],
) -> str:
    lines: list[str] = []
    lines.append("# KISTI DB Review Plan (Pre-load)")
    lines.append("")
    lines.append(f"- generated_at: `{generated_at}`")
    lines.append(f"- config: `{config_path}`")
    lines.append(f"- input: `{file_name}` (`{file_type}`)")
    lines.append(f"- base_table: `{base_table}`")
    lines.append(f"- base_table_sql: `{base_table_sql}`")
    lines.append(f"- key_sep: `{key_sep}`")
    if max_records is not None:
        lines.append(f"- max_records: `{max_records}` (sample cap)")
    if stats:
        if "records_read" in stats:
            lines.append(f"- records_read: `{stats.get('records_read')}`")
        if "records_ok" in stats:
            lines.append(f"- records_ok: `{stats.get('records_ok')}`")
        if "records_failed" in stats:
            lines.append(f"- records_failed: `{stats.get('records_failed')}`")
        if "batches_total" in stats:
            lines.append(f"- batches_total: `{stats.get('batches_total')}`")
    if issues:
        lines.append(f"- issues: `{len(issues)}`")
    lines.append("")

    if timings_ms:
        try:
            total_ms = int((timings_ms or {}).get("pipeline.json.total") or 0)
        except Exception:
            total_ms = 0
        if total_ms <= 0:
            try:
                total_ms = sum(int(v) for v in (timings_ms or {}).values() if int(v) > 0)
            except Exception:
                total_ms = 0
        if total_ms > 0:
            lines.append("## Sample Profile")
            lines.append("")
            lines.append(f"- total_ms: `{total_ms}`")
            try:
                rr = int((stats or {}).get("records_read") or 0)
            except Exception:
                rr = 0
            if rr > 0:
                rps = float(rr) / max(0.001, (float(total_ms) / 1000.0))
                lines.append(f"- records_per_sec: `{rps:.2f}`")
            rows: list[tuple[str, int]] = []
            for k, v in (timings_ms or {}).items():
                try:
                    ms = int(v)
                except Exception:
                    continue
                if ms <= 0:
                    continue
                rows.append((str(k), int(ms)))
            rows = sorted(rows, key=lambda x: x[1], reverse=True)[:8]
            if rows:
                lines.append("- top_timings:")
                for k, v in rows:
                    share = (100.0 * float(v) / float(total_ms)) if total_ms > 0 else 0.0
                    lines.append(f"  - `{k}`: `{v}ms` ({share:.1f}%)")
            lines.append("")

    auto_except = (artifacts or {}).get("auto_except") if isinstance(artifacts, Mapping) else None
    if isinstance(auto_except, Mapping) and bool(auto_except.get("enabled")):
        sample = auto_except.get("sample") if isinstance(auto_except.get("sample"), Mapping) else {}
        thresholds = auto_except.get("thresholds") if isinstance(auto_except.get("thresholds"), Mapping) else {}
        estimate = auto_except.get("estimate") if isinstance(auto_except.get("estimate"), Mapping) else {}
        detected = list(auto_except.get("detected_except_keys") or [])
        lines.append("## Auto Except")
        lines.append("")
        lines.append(f"- enabled: `{auto_except.get('enabled')}`")
        if sample:
            lines.append(
                f"- sample: records `{sample.get('records_sampled')}` / requested `{sample.get('records_requested')}`, "
                f"sources `{sample.get('sources_sampled')}` / requested `{sample.get('max_sources_requested')}`, "
                f"duration `{sample.get('duration_s')}`s"
            )
        if thresholds:
            lines.append(
                f"- thresholds: unique_keys>={thresholds.get('unique_key_threshold')}, "
                f"min_obs>={thresholds.get('min_observations')}, "
                f"novelty>={thresholds.get('novelty_threshold')}"
            )
        eta_range = estimate.get("eta_seconds_range") if isinstance(estimate.get("eta_seconds_range"), list) else None
        if eta_range and len(eta_range) == 2:
            try:
                eta_lo = float(eta_range[0])
                eta_hi = float(eta_range[1])
                lines.append(f"- eta_estimate_s: `{eta_lo:.1f} ~ {eta_hi:.1f}`")
            except Exception:
                pass
        lines.append(f"- detected_except_keys: `{len(detected)}`")
        for k in detected[:20]:
            lines.append(f"  - `{k}`")
        if len(detected) > 20:
            lines.append(f"  - ... +{len(detected) - 20} more")
        lines.append("")

    id_compaction = (artifacts or {}).get("id_compaction") if isinstance(artifacts, Mapping) else None
    if isinstance(id_compaction, Mapping) and bool(id_compaction.get("enabled")):
        columns = list(id_compaction.get("columns") or [])
        ambiguous = id_compaction.get("ambiguous_columns") if isinstance(id_compaction.get("ambiguous_columns"), Mapping) else {}
        lines.append("## ID Compaction")
        lines.append("")
        lines.append(f"- preset: `{id_compaction.get('preset')}`")
        lines.append(f"- mode: `{id_compaction.get('mode')}`")
        lines.append(f"- rules_hash: `{id_compaction.get('rules_hash')}`")
        lines.append(f"- compacted_columns: `{len(columns)}`")
        for entry in columns[:20]:
            if not isinstance(entry, Mapping):
                continue
            lines.append(
                f"  - `{entry.get('table')}.{entry.get('original_column')}` -> "
                f"`{entry.get('new_column')}` ({entry.get('removed_prefix')})"
            )
        if len(columns) > 20:
            lines.append(f"  - ... +{len(columns) - 20} more")
        if ambiguous:
            lines.append(f"- ambiguous_url_like_columns_skipped: `{len(ambiguous)}`")
        lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- This plan is generated **before DB load**.")
    lines.append("- If later records have new keys/branches, **additional tables/columns may appear** at load time.")
    lines.append("")

    if "svg" in formats:
        lines.append("## Diagram")
        lines.append("")
        lines.append("- `schema.svg`")
        if "png" in formats:
            lines.append("- `schema.png` (best-effort; requires `cairosvg`)")
        lines.append("")
        lines.append("![](schema.svg)")
        lines.append("")

    if "mmd" in formats or "mermaid" in formats:
        lines.append("## Mermaid")
        lines.append("")
        lines.append("- `schema.mmd`")
        lines.append("")

    lines.append("## Tables (predicted)")
    lines.append("")
    lines.append("| Table | cols |")
    lines.append("|---|---:|")
    for ti in table_infos:
        cols_n = len(ti.columns or []) if ti.columns is not None else 0
        lines.append(f"| `{ti.name_sql}` | {cols_n if cols_n else 'n/a'} |")
    lines.append("")

    if "ddl" in formats or True:
        lines.append("## DDL")
        lines.append("")
        lines.append("- `ddl.sql` (concatenated)")
        lines.append("- `ddl.json` (per-table mapping)")
        lines.append("")

    lines.append("## Join Tips")
    lines.append("")
    lines.append("All sub-tables (if any) are typically joinable via the `id` column:")
    lines.append("")
    lines.append("```sql")
    lines.append("SELECT b.*, s.*")
    lines.append(f"FROM `{base_table_sql}` b")
    lines.append(f"LEFT JOIN `{base_table_sql}{key_sep}<sub>` s ON b.id = s.id")
    lines.append("LIMIT 5;")
    lines.append("```")
    lines.append("")

    if issues:
        lines.append("## Issues (from plan run)")
        lines.append("")
        for it in issues[:30]:
            stage = it.get("stage")
            lvl = it.get("level")
            msg = it.get("message")
            lines.append(f"- `{lvl}` `{stage}`: {msg}")
        if len(issues) > 30:
            lines.append(f"- ... +{len(issues) - 30} more")
        lines.append("")

    return "\n".join(lines) + "\n"
