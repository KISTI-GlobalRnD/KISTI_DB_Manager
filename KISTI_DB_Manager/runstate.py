from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class UnsafePathError(RuntimeError):
    """Raised when an output or cleanup path is unsafe to write/delete."""


def _absolute_no_resolve(path: Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded
    return Path.cwd() / expanded


def _assert_no_symlink_components(path: Path, *, purpose: str) -> Path:
    path_abs = _absolute_no_resolve(path)
    current = Path(path_abs.anchor)
    normalized_parts: list[str] = []
    parts = path_abs.parts[1:] if path_abs.anchor else path_abs.parts
    for part in parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if normalized_parts:
                normalized_parts.pop()
            current = Path(path_abs.anchor).joinpath(*normalized_parts)
            continue
        current = current / part
        if current.is_symlink():
            raise UnsafePathError(f"{purpose} path contains a symlink component: {current}")
        normalized_parts.append(part)
    return Path(path_abs.anchor).joinpath(*normalized_parts)


def _validate_output_file(path: Path, *, purpose: str) -> None:
    _assert_no_symlink_components(path, purpose=purpose)
    try:
        path.lstat()
    except FileNotFoundError:
        return
    if path.is_symlink() or path.is_dir():
        raise UnsafePathError(f"{purpose} path already exists and is not a safe file: {path}")


def prepare_output_file_path(path: str | Path, *, purpose: str = "output") -> Path:
    path = _assert_no_symlink_components(Path(path), purpose=purpose)
    parent = _assert_no_symlink_components(path.parent, purpose=f"{purpose} parent")
    parent.mkdir(parents=True, exist_ok=True)
    parent = _assert_no_symlink_components(parent, purpose=f"{purpose} parent")
    if parent.is_symlink() or not parent.is_dir():
        raise UnsafePathError(f"{purpose} parent is not a safe directory: {parent}")
    _validate_output_file(path, purpose=purpose)
    return path


def prepare_output_dir_path(path: str | Path, *, purpose: str = "output directory") -> Path:
    path = _assert_no_symlink_components(Path(path), purpose=purpose)
    path.mkdir(parents=True, exist_ok=True)
    path = _assert_no_symlink_components(path, purpose=purpose)
    if path.is_symlink() or not path.is_dir():
        raise UnsafePathError(f"{purpose} path is not a safe directory: {path}")
    return path


def safe_unlink_file(path: str | Path, *, purpose: str = "cleanup", missing_ok: bool = True) -> bool:
    path = _assert_no_symlink_components(Path(path), purpose=purpose)
    try:
        st = path.lstat()
    except FileNotFoundError:
        if missing_ok:
            return False
        raise
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise UnsafePathError(f"{purpose} path is not a safe regular file: {path}")
    path.unlink()
    return True


def safe_rmtree(path: str | Path, *, purpose: str = "cleanup", missing_ok: bool = True) -> bool:
    if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
        raise UnsafePathError("refusing to delete directory because shutil.rmtree is not symlink-attack resistant")
    path = _assert_no_symlink_components(Path(path), purpose=purpose)
    try:
        st = path.lstat()
    except FileNotFoundError:
        if missing_ok:
            return False
        raise
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise UnsafePathError(f"{purpose} path is not a safe directory: {path}")
    shutil.rmtree(path)
    return True


def atomic_write_text(path: str | Path, content: str, *, purpose: str = "output") -> None:
    path = prepare_output_file_path(path, purpose=purpose)
    parent = path.parent

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        _validate_output_file(path, purpose=purpose)
        os.replace(tmp_name, path)
    except Exception:
        try:
            tmp_path = _assert_no_symlink_components(Path(tmp_name), purpose=f"{purpose} temporary file")
            if not tmp_path.is_symlink() and tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def open_append_text(path: str | Path, *, purpose: str = "output") -> TextIO:
    path = prepare_output_file_path(path, purpose=purpose)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        return os.fdopen(fd, "a", encoding="utf-8")
    except Exception:
        os.close(fd)
        raise


def append_text(path: str | Path, content: str, *, purpose: str = "output") -> None:
    handle = open_append_text(path, purpose=purpose)
    with handle:
        handle.write(content)


def atomic_write_json(path: str | Path, payload: dict[str, Any], *, indent: int = 2, purpose: str = "run state") -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=indent), purpose=purpose)


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
        state = cls(path=_absolute_no_resolve(Path(path)), payload=dict(payload), timestamp_key=str(timestamp_key))
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
