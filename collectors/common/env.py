from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is optional at import time
    load_dotenv = None


GET_DATA_ROOT = Path(__file__).resolve().parents[2]


def load_environment() -> None:
    if load_dotenv is not None:
        load_dotenv(GET_DATA_ROOT / ".env")


def data_root() -> Path:
    return Path(os.getenv("DATA_ROOT", str(GET_DATA_ROOT / "storage"))).resolve()


def state_root() -> Path:
    return Path(os.getenv("STATE_ROOT", str(GET_DATA_ROOT / "state"))).resolve()


def logs_root() -> Path:
    return Path(os.getenv("LOG_ROOT", str(GET_DATA_ROOT / "logs"))).resolve()


def config_root() -> Path:
    return Path(os.getenv("CONFIG_ROOT", str(GET_DATA_ROOT / "configs"))).resolve()

