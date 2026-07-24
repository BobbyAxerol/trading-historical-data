from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collectors.common.storage import release_unused_memory
from collectors.deribit.config import DeribitConfig
from collectors.deribit.validate import DeribitValidator


@dataclass(frozen=True)
class CleanupReport:
    status: str
    phase: str
    dry_run: bool
    files_seen: int
    files_deleted: int
    bytes_seen: int
    validation_status: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "phase": self.phase,
            "dry_run": self.dry_run,
            "files_seen": self.files_seen,
            "files_deleted": self.files_deleted,
            "bytes_seen": self.bytes_seen,
            "validation_status": self.validation_status,
        }


class DeribitCleanup:
    def __init__(self, config: DeribitConfig):
        self.config = config

    def run(self, *, confirm: bool = False) -> dict[str, Any]:
        validation = DeribitValidator(self.config).run()
        files = self._staging_files()
        bytes_seen = sum(path.stat().st_size for path in files if path.exists())
        if validation["status"] != "ok":
            return CleanupReport("blocked", "Phase 4", not confirm, len(files), 0, bytes_seen, str(validation["status"])).as_payload()
        deleted = 0
        if confirm:
            for path in files:
                path.unlink()
                deleted += 1
            release_unused_memory()
        return CleanupReport("ok", "Phase 4", not confirm, len(files), deleted, bytes_seen, str(validation["status"])).as_payload()

    def _staging_files(self) -> list[Path]:
        if not self.config.staging_root.exists():
            return []
        return sorted(path for path in self.config.staging_root.rglob("*.parquet") if not path.name.endswith(".tmp"))
