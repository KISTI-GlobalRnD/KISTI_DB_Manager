from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass
class JsonRunState:
    path: Path
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp_key: str = "generated_at"

    @classmethod
    def create(
        cls,
        path: Path,
        payload: dict[str, Any],
        *,
        timestamp_key: str = "generated_at",
        write: bool = True,
    ) -> "JsonRunState":
        state = cls(path=Path(path).expanduser().resolve(), payload=dict(payload), timestamp_key=str(timestamp_key))
        if timestamp_key and timestamp_key not in state.payload:
            state.payload[timestamp_key] = utc_now_iso()
        if write:
            state.write()
        return state

    def write(self, *, touch_timestamp: bool = False) -> None:
        if touch_timestamp and self.timestamp_key:
            self.payload[self.timestamp_key] = utc_now_iso()
        atomic_write_json(self.path, self.payload)

    def update(self, touch_timestamp: bool = True, **changes: Any) -> dict[str, Any]:
        self.payload.update(changes)
        self.write(touch_timestamp=touch_timestamp)
        return self.payload

    def set_status(self, status: str, *, touch_timestamp: bool = True, **changes: Any) -> dict[str, Any]:
        self.payload["status"] = str(status)
        if changes:
            self.payload.update(changes)
        self.write(touch_timestamp=touch_timestamp)
        return self.payload

    def add_list_item(self, key: str, item: Any, *, unique: bool = True, touch_timestamp: bool = False) -> dict[str, Any]:
        values = self.payload.setdefault(str(key), [])
        if not isinstance(values, list):
            raise TypeError(f"run-state field `{key}` is not a list")
        if not unique or item not in values:
            values.append(item)
            self.write(touch_timestamp=touch_timestamp)
        return self.payload
