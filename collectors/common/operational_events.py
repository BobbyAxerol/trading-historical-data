"""Minimal, secret-safe operational event evidence for the B0 monitor.

Collectors emit only bounded metadata under ``state/operational-events`` when
``PRIMUS_HMD_OPERATIONAL_EVENTS=enabled``.  The Discord monitor reads these
files; webhook credentials are never handled here or written into state.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .env import state_root


EVENT_CATEGORIES = {"collector_exit", "retry_rate_limit", "validation_repair", "backup_failure"}
RATE_LIMIT_MARKERS = ("429", "rate limit", "retry-after", "too many requests")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


def _events_enabled() -> bool:
    return os.getenv("PRIMUS_HMD_OPERATIONAL_EVENTS", "").strip().lower() == "enabled"


def _event_path(category: str) -> Path:
    if category not in EVENT_CATEGORIES:
        raise ValueError(f"unsupported operational event category: {category!r}")
    return state_root() / "operational-events" / f"{category}.json"


def _read(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.stem}-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def record_retry_exception(exc: BaseException, *, attempt: int, attempts: int) -> None:
    """Escalate repeated rate-limit retries without logging request secrets."""

    if not _events_enabled():
        return
    message = str(exc).lower()
    if not any(marker in message for marker in RATE_LIMIT_MARKERS):
        return
    path = _event_path("retry_rate_limit")
    current = _read(path)
    now = _utc_now()
    window_seconds = max(1, int(os.getenv("PRIMUS_HMD_RETRY_ALERT_WINDOW_SECONDS", "600")))
    threshold = max(1, int(os.getenv("PRIMUS_HMD_RETRY_ALERT_THRESHOLD", "3")))
    previous_at = current.get("updated_at")
    try:
        previous = datetime.fromisoformat(str(previous_at).replace("Z", "+00:00"))
        if previous.tzinfo is None:
            raise ValueError
        in_window = (now - previous.astimezone(timezone.utc)).total_seconds() <= window_seconds
    except (TypeError, ValueError):
        in_window = False
    count = int(current.get("count", 0)) + 1 if in_window else 1
    _write(
        path,
        {
            "schema_version": 1,
            "category": "retry_rate_limit",
            "status": "alert" if count >= threshold else "observed",
            "count": count,
            "threshold": threshold,
            "window_seconds": window_seconds,
            "last_exception_type": type(exc).__name__,
            "updated_at": _utc_now_iso(),
        },
    )


def record_event(category: str, *, status: str, summary: str) -> None:
    """Record an explicit bounded event for a collector integration or drill."""

    if not _events_enabled():
        return
    if status not in {"alert", "ok", "observed"}:
        raise ValueError(f"unsupported operational event status: {status!r}")
    _write(
        _event_path(category),
        {
            "schema_version": 1,
            "category": category,
            "status": status,
            "summary": str(summary)[:240],
            "updated_at": _utc_now_iso(),
        },
    )
