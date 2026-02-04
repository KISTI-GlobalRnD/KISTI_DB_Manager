from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


@dataclass
class QuarantineWriter:
    """
    Append-only JSONL writer for records that failed parsing/normalization.

    Each line is a JSON object with metadata + best-effort serialized record.
    """

    path: str | Path
    flush: bool = True
    ensure_parent: bool = True

    def __post_init__(self) -> None:
        self.path = str(self.path)
        self._fh = None

    def __enter__(self) -> "QuarantineWriter":
        if self.ensure_parent:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None

    def write(
        self,
        *,
        stage: str,
        record: Any,
        index: int | None = None,
        exc: BaseException | None = None,
        **context: Any,
    ) -> None:
        if self._fh is None:
            raise RuntimeError("QuarantineWriter is not opened. Use it as a context manager.")

        entry: dict[str, Any] = {
            "timestamp": _iso_now(),
            "stage": stage,
            "index": index,
            "record": _safe_json(record),
            "context": {k: _safe_json(v) for k, v in context.items()},
        }
        if exc is not None:
            entry["exception_type"] = type(exc).__name__
            entry["exception_message"] = str(exc)

        self._fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if self.flush:
            self._fh.flush()


class NullQuarantineWriter:
    def __enter__(self) -> "NullQuarantineWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, *, stage: str, record: Any, index: int | None = None, exc: BaseException | None = None, **context: Any) -> None:
        return None

