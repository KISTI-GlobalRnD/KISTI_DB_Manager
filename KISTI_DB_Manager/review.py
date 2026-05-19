from __future__ import annotations

from ._review.common import (
    _bool,
    _load_json,
    _mask_db_config,
    _parse_formats,
    _safe_json,
    _utc_now_iso,
    _write_text,
)
from ._review.core import (
    DBIntrospector,
    TableInfo,
    _collect_table_infos_from_db_prefix,
    _collect_table_infos_from_report,
    _merge_db_details,
)
from ._review.pack import generate_review_pack
from ._review.plan import generate_review_plan
from ._review.report_html import _render_html
from ._review.report_markdown import _render_markdown, _render_plan_markdown
from ._review.schema_render import (
    _maybe_svg_to_png,
    build_table_edges,
    render_mermaid,
    render_simple_svg,
)
