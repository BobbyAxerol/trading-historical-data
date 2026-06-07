from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .env import config_root


def load_yaml(name: str) -> dict[str, Any]:
    path = config_root() / name
    if not path.exists():
        return {}
    with path.open() as fh:
        return yaml.safe_load(fh) or {}

