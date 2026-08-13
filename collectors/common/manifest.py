from __future__ import annotations

import json
import os
import resource
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .env import state_root
from .locks import FileLock


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class JsonState:
    def __init__(self, relative_path: str):
        self.path = state_root() / relative_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except json.JSONDecodeError:
            backup = self.path.with_suffix(self.path.suffix + ".corrupt")
            self.path.replace(backup)
            return {}

    def write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.path)


class Manifest:
    def __init__(self, dataset: str):
        self.dataset = dataset
        self.state = JsonState(f"manifests/{dataset}.json")

    def read(self) -> dict[str, Any]:
        payload = self.state.read()
        payload.setdefault("dataset", self.dataset)
        payload.setdefault("symbols", {})
        return payload

    def symbol_state(self, symbol: str) -> dict[str, Any]:
        return self.read().setdefault("symbols", {}).get(symbol, {})

    def update_symbol(self, symbol: str, **values: Any) -> None:
        # Live tails and an approved one-shot rebuild can share a dataset.
        # Serialize read-modify-write so either writer cannot discard the
        # other's evidence while partition writes are already independently
        # protected by their dataset/symbol lock.
        with FileLock(f"manifest/{self.dataset}"):
            payload = self.read()
            payload.setdefault("symbols", {})
            current = dict(payload["symbols"].get(symbol, {}))
            current.update(values)
            current["updated_at"] = utc_now_iso()
            payload["symbols"][symbol] = current
            self.state.write(payload)


class Heartbeat:
    def __init__(self, service: str):
        self.state = JsonState(f"heartbeats/{service}.json")
        self.service = service

    def beat(self, status: str = "ok", **values: Any) -> None:
        peak_rss_mb = values.pop("peak_rss_mb", None)
        if peak_rss_mb is None:
            # Linux ru_maxrss is KiB.  This is process-local peak RSS, which is
            # the safe measurement available without granting the monitor a
            # Docker socket or host-administrator privilege.
            peak_rss_mb = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 2)
        payload = {
            "service": self.service,
            "status": status,
            "updated_at": utc_now_iso(),
            "peak_rss_mb": peak_rss_mb,
            **values,
        }
        self.state.write(payload)


def sleep_with_heartbeat(
    heartbeat: Heartbeat,
    seconds: float,
    *,
    heartbeat_interval_seconds: float = 300,
    **values: Any,
) -> None:
    """Sleep a live collector while keeping its healthy heartbeat fresh.

    Long tail intervals must not look like stopped services to the B0 monitor.
    Conversely, a collector error remains visible until a later successful
    cycle writes a healthy heartbeat; this helper never overwrites ``error``
    with ``sleeping``.
    """

    remaining = float(seconds)
    interval = float(heartbeat_interval_seconds)
    if remaining <= 0:
        return
    if interval <= 0:
        raise ValueError("heartbeat_interval_seconds must be positive")

    while remaining > 0:
        chunk = min(interval, remaining)
        time.sleep(chunk)
        remaining -= chunk
        current = heartbeat.state.read()
        if str(current.get("status", "")).lower() == "error":
            continue
        heartbeat.beat(status="sleeping", **values)
