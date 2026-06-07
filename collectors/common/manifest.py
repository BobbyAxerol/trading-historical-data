from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .env import state_root


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
        payload = {
            "service": self.service,
            "status": status,
            "updated_at": utc_now_iso(),
            **values,
        }
        self.state.write(payload)

