from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ..runstate import atomic_write_text


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    atomic_write_text(path, content, purpose="review output")


def _parse_formats(value: str | None) -> set[str]:
    if not value:
        return {"md", "html", "svg"}
    items = [v.strip().lower() for v in str(value).split(",")]
    return {v for v in items if v}


def _bool(value: Any) -> bool:
    return bool(value) and str(value).lower() not in {"0", "false", "no", "off", "none"}


def _mask_db_config(db_config: Mapping[str, Any]) -> dict[str, Any]:
    masked = dict(db_config)
    if masked.get("password"):
        masked["password"] = "***"
    return masked
