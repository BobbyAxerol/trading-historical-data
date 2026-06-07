from __future__ import annotations

import fcntl
import os
from pathlib import Path

from .env import state_root


class FileLock:
    def __init__(self, name: str):
        safe_name = name.replace("/", "__").replace(" ", "_")
        self.path = state_root() / "locks" / f"{safe_name}.lock"
        self.fd: int | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None

