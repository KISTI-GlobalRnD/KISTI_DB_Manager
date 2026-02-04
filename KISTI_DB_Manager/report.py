from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


@dataclass(frozen=True)
class Issue:
    level: str
    stage: str
    message: str
    timestamp: str = field(default_factory=_iso_now)
    exception_type: str | None = None
    exception_message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_exception(
        cls,
        *,
        level: str,
        stage: str,
        message: str,
        exc: BaseException,
        context: dict[str, Any] | None = None,
    ) -> "Issue":
        return cls(
            level=level,
            stage=stage,
            message=message,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
            context=context or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "stage": self.stage,
            "message": self.message,
            "timestamp": self.timestamp,
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "context": {k: _safe_json(v) for k, v in self.context.items()},
        }


@dataclass
class RunReport:
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: str = field(default_factory=_iso_now)
    finished_at: str | None = None
    duration_s: float | None = None
    issues: list[Issue] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    timings_ms: dict[str, int] = field(default_factory=dict)

    def finish(self) -> None:
        """
        Mark this report as finished and compute duration in seconds.

        Idempotent: calling multiple times will not change an existing finished_at.
        """
        if self.finished_at:
            return
        now = datetime.now(timezone.utc)
        self.finished_at = now.isoformat()
        try:
            started = datetime.fromisoformat(self.started_at)
            self.duration_s = float((now - started).total_seconds())
        except Exception:
            self.duration_s = None

    def bump(self, key: str, n: int = 1) -> None:
        self.stats[key] = int(self.stats.get(key, 0)) + int(n)

    def add_time_ms(self, key: str, ms: float | int) -> None:
        """
        Accumulate elapsed time under `timings_ms[key]` (integer milliseconds).
        """
        k = str(key)
        try:
            add = int(round(float(ms)))
        except Exception:
            add = 0
        if add <= 0:
            return
        self.timings_ms[k] = int(self.timings_ms.get(k, 0)) + int(add)

    def add_time_s(self, key: str, seconds: float | int) -> None:
        try:
            self.add_time_ms(key, float(seconds) * 1000.0)
        except Exception:
            return

    @contextmanager
    def timer(self, key: str):
        """
        Context manager to time a block and accumulate into `timings_ms[key]`.
        """
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.add_time_s(key, time.perf_counter() - t0)

    def set_artifact(self, key: str, value: Any) -> None:
        self.artifacts[str(key)] = value

    def add(self, issue: Issue) -> None:
        self.issues.append(issue)
        self.bump(f"issues_{issue.level}")

    def warn(self, *, stage: str, message: str, **context: Any) -> None:
        self.add(Issue(level="warning", stage=stage, message=message, context=context))

    def error(self, *, stage: str, message: str, **context: Any) -> None:
        self.add(Issue(level="error", stage=stage, message=message, context=context))

    def exception(self, *, stage: str, message: str, exc: BaseException, **context: Any) -> None:
        self.add(
            Issue.from_exception(
                level="error",
                stage=stage,
                message=message,
                exc=exc,
                context=context,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": self.duration_s,
            "stats": dict(self.stats),
            "timings_ms": dict(self.timings_ms),
            "issues": [issue.to_dict() for issue in self.issues],
            "artifacts": {k: _safe_json(v) for k, v in self.artifacts.items()},
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    def save_json(self, path: str, *, indent: int = 2) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json(indent=indent))
